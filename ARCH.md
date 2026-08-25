# Architecture of netring

Core architecture of a high-performance Go networking subsystem based on Linux `io_uring`
using Data-Oriented Design (DOD).
It provides zero allocations (0-alloc) per network request and bypasses the standard `netpoll` (epoll) by working in
user-space without extra kernel context switches.

## System components

1. **Submission Queue (SQ)** — the `io_uring` ring buffer in the Linux kernel. It is filled with tasks 
   (Read/Write/Accept/Close/etc.). It runs in `IORING_SETUP_SQPOLL` mode (the kernel picks up tasks from memory itself,
    minimizing syscalls).
1. **Task channels (Worker -> Translator)** — lock-free or buffered Go channels, split by descriptor hash (FD), to keep
   strict packet ordering for a given connection.
1. **Task Translator** — a dedicated single-threaded loop that drains tasks from the channels and fills the SQ.
1. **TaskSlots** — an ultra-fast flat table of in-flight tasks built on bit operations without Bounds Checks. It is 
   controlled exclusively by the Translator.
1. **CQ handler (Poller)** — a dedicated thread that parses completed operations from the kernel’s Completion Queue, 
   maps them through `TaskSlots`, and safely wakes goroutines.

---

## Task Descriptor format

Each network operation is described by a structure allocated via `sync.Pool`.
It combines the `io_uring` syscall parameters and the Go runtime synchronization context.

```go
type TaskState uint32

const (
	stateInCore TaskState = iota // 0: Task went into io_uring / the translator
	stateDone                    // 1: The poller already processed the CQE and wrote the result
	stateParked                  // 2: The worker successfully entered gopark and went to sleep
)

type OpcodeType uint32

const (
	opcodeTypeInvalid OpcodeType = iota
	OpcodeTypeAccept
    OpcodeTypeClose
	OpcodeTypeTimer
	OpcodeTypeRecv
	OpcodeTypeSend
	// And more
)

type Task struct {
	// --- Data for io_uring.SQEntry ---
	Opcode OpcodeType
	FD     int32
	Addr   uint64
	Len    uint32 
	Offset uint64

	// --- Data for Go runtime synchronization ---
	G     uintptr       // Address of the sending goroutine (runtime.getg())
	Res   int32         // Result of the operation from the CQE (bytes count or -ERRNO)
	State atomic.Uint32 // Atomic task status to prevent races
}
```

---

## How it works (Request lifecycle)

### 1. Enqueueing a task by the Worker Goroutine
The goroutine performs an operation (e.g. `Recv`) entirely in its own context, without locks:
1. Gets a `NetTask` structure from `sync.Pool`.
1. Fills in the operation parameters (`FD`, `Addr`, `Len`, `Opcode`).
1. **Critical:** Before sending, it fixes its own address in the runtime: `task.G = runtime.getg()`.
1. Sets `task.State` to `stateInCore`.
1. Sends the pointer to the `NetTask` into the Translator channel (the channel is chosen as `FD % len(chans)`).
1. Calls `runtime.gopark` with a special `unlockf` function.

**The `unlockf` logic inside `gopark`:**
```go
gopark(func(g *g, p unsafe.Pointer) bool {
	t := (*NetTask)(p)
	
	// The worker atomically sets the status to stateParked (2)
	// and looks at WHAT WAS THERE BEFORE.
	old := t.State.Swap(uint32(stateParked))
	
	if old == uint32(stateInCore) {
		// If there was 0 (stateInCore) before us, the poller has NOT yet
		// processed the task. Now it is firmly 2 (stateParked).
		// When the poller comes, it will fail its CAS(0->1) and is guaranteed to call goready.
		// We return true and go to sleep with peace of mind.
		return true
	}
	
	// We reach here only if the poller has ALREADY moved the status to stateDone (1).
	// Per the gopark contract: if we return false, we must guarantee that
	// no one on the outside will call goready anymore.
	// The poller has already finished and did NOT call goready (it saw stateInCore and left).
	// So we can safely return false, and the runtime itself restores us immediately.
	return false
}, unsafe.Pointer(task), waitReasonZero, traceBlockGeneric, 0)
```

### 2. The Translator’s work (0 atomics in the slots)
Because the Translator’s channel is processed strictly in **one thread**, the internal `TaskSlots`
storage is protected from concurrency architecturally. The atomic tax on slot allocation is completely absent.
1. Reads the `NetTask` from the channel.
1. Calls `slots.Add(task)` — an ultra-hack that finds a free bit in the bitmap in one CPU tick and returns `slot_idx`.
1. Builds the `io_uring.SQEntry`, where `UserData = slot_idx`.
1. Pushes the `SQEntry` into the SQ ring.
    If the kernel is in `SQPOLL` mode, no enter syscall is needed — the kernel takes the task itself.

### 3. CQ handler’s work (CQ Poller)
The CQ poller processes the kernel’s ready results.
It protects the Go runtime from the `goready: bad g state` panic
(when a goroutine is woken up before it has physically entered the waiting state).
It also utilizes **adaptive busy-spinning** to avoid kernel context switches during periods of light traffic.

```go
func (nr *NetRing) Poll() {
	var spinCount int
	for {
		// ... check if stop requested ...

		read := *nr.ring.CQHead
		if read == atomic.LoadUint32(nr.ring.CQTail) {
			spinCount++
			if spinCount < nr.opts.idleAfter { // default: 50000
				// Repeat until the spin limit is reached, staying in user-space
				runtime.Gosched()
				continue
			}

			// Total silence. Ask the kernel to freeze the thread until new events arrive.
			_, _, errno := syscall.Syscall6(
				unix.SYS_IO_URING_ENTER,
				uintptr(nr.ring.FD),
				0, 1, // to_submit = 0, min_complete = 1
				enterGetEvents, 0, 0,
			)
			spinCount = 0
			// ... handle EINTR / errors ...
			continue
		}

		// --- Hot path ---
		spinCount = 0
		idx := read & nr.ring.CQLengthMask
		cqe := nr.ring.CQ[idx]
		atomic.AddUint32(nr.ring.CQHead, 1)

		// Dispatch CQE (Timer task vs Regular network task)
		if cqe.UserData == periodicalTimerTaskID {
			nr.timerTriggerCh <- struct{}{} // Signal the standalone re-arm goroutine
			continue
		}

		// Regular task path
		slotIdx := cqe.UserData
		task, _ := nr.slots.Get(slotIdx)
		nr.slots.Del(slotIdx)

		task.Res = cqe.Res // Write the kernel result

		// The poller tries to atomically move the status from "in kernel" (0) to "done" (1)
		if task.State.CompareAndSwap(uint32(stateInCore), uint32(stateDone)) {
			// The "fast round". The worker hasn't even reached the gopark call yet,
			// OR is inside gopark but hasn't reached executing unlockf.
			// We've set stateDone. When the worker enters unlockf, it will see stateDone,
			// return false, and safely continue execution. We do NOT need to call goready.
			continue
		}

		// If the CAS failed, the worker has ALREADY read stateInCore inside unlockf,
		// returned true, and stayed asleep (status is stateParked).
		// Since it is guaranteed to be in _Gwaiting, calling goready is 100% safe.
		runtime.goready(task.G, 0)
	}
}
```

---

## Periodical timer task

The Poller needs a periodic tick (housekeeping, timing wheel ticks, heartbeat checks, etc.) without extra syscalls or
OS timers. It gets one from `io_uring` itself via a self-rearming timeout task:

1. **Reserved ID**: The token `periodicalTimerTaskID` is set to `math.MaxUint64 - math.MaxUint32`. This sits far above
   any possible `TaskSlots` index, preventing collisions.
1. **Non-blocking Dispatch**: When a CQE with `UserData == periodicalTimerTaskID` arrives, the poller bypasses 
   `TaskSlots` and pushes to a buffered channel (`timerTriggerCh`). The poller never blocks.
1. **Safe Re-arming**: A standalone re-arm goroutine drains `timerTriggerCh`, formats a fresh `IORING_OP_TIMEOUT` task,
   and submits it to the Translator's channel. The Translator pushes it to the SQ, safely enclosing the 
   self-perpetuating cycle within a single thread.

---

## Final advantages of the architecture

- **0-Alloc on the hot path:** Using `sync.Pool` for `Task` eliminates the load on the Garbage Collector (GC).
- **Maximum CPU Cache Efficiency:** `TaskSlots` uses dense data structures and direct memory addressing by pointers, 
  without Bounds Checks and without a single atomic operation inside the bitmap.
- **Scheduler safety:** The atomic state machine (`stateInCore -> stateParked / stateDone`) completely removes the race 
  between the asynchronous CQ poller of the runtime and the Go scheduler.
- **No syscalls:** With `SQPOLL` for submission and direct user-space parsing of the CQ ring for reading, the system 
  can process hundreds of thousands of RPS without a single kernel context switch during peak loads.
