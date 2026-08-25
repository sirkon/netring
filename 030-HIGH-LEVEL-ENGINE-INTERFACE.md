# Corrected Specification: NetRing Architecture & Interfaces

## 1. Task Descriptors & Flow Constraints

* **Worker Thread Isolation:** Goroutines (Workers) NEVER access the `taskslots.Slots` storage. Allocation and layout 
  management are handled strictly by the single-threaded **Translator Loop**.
* **Zero Allocation Copy-by-Value:** Workers submit plain `ringTask` values into shard channels. The Go runtime passes 
  these compact footprints directly via hardware CPU registers, causing 0 heap overhead.

```go
// ringTask is a lightweight Plain Old Data (POD) structure sent over channels.
type ringTask struct {
	Opcode opcodeType
	FD     int32
	Addr   uint64 // Used as time.Duration value for Timer op or raw payload pointers
	Len    uint32 // Target SizeClass maximum buffer capability for Recv ops
	Offset uint64
	G      uintptr // Address of the sending goroutine (runtime.getg())
}
```

---

## 2. High-Level Network Engine Interface (`netring.NetRing`)

Public runtime API utilized by Go workers. It delegates requests down to partitioned communication streams and handles 
runtime scheduling transitions.

```go
package netring

// New instantiates the NetRing execution engine subsystem.
func New(entries uint32, channelsCount int) (*NetRing, error) {
	// Initializes low-level iouring instance, configures size-classed provided buffer rings,
	// provisions the flat 1-threaded taskslots arena, and spawns loop worker routines.
	panic("implement me")
}

// Accept suspends the calling goroutine until a new client connection arrives.
func (nr *NetRing) Accept(listenFD int) (int32, error) {
	// 1. Build a local ringTask token (Opcode = opcodeTypeAccept, G = runtime.getg(), FD = listenFD).
	// 2. Dispatch the token down to a dedicated channel shard (chosen as FD % len(chans)).
	// 3. Suspend immediately via runtime.gopark with the custom lock-free Swap handshake.
	// 4. (Woken up by CQ Poller) Read processed file descriptor result from taskContext, clear, return.
	panic("implement me")
}

// Recv acquires incoming stream payloads directly into an implicit kernel-provided buffer ring.
func (nr *NetRing) Recv(fd int, sizeClass SizeClass) ([]byte, error) {
	// 1. Build a local ringTask token (Opcode = opcodeTypeRecv, G = runtime.getg(), FD = fd).
	//    Sets Len = SizeClassToBytes(sizeClass).
	// 2. Route the task token to the respective shard channel.
	// 3. Suspend via runtime.gopark.
	// 4. (Woken up by CQ Poller) Extract non-copying slice constructed from the kernel-allocated memory.
	panic("implement me")
}

// Send dispatches raw outbound data segments into the network interface.
func (nr *NetRing) Send(fd int, data []byte) (int, error) {
	// 1. Build a local ringTask token (Opcode = opcodeTypeSend, G = runtime.getg(), FD = fd).
	//    Maps Addr to raw memory pointer from data slice, Len = len(data).
	// 2. Dispatch down to channel mesh, park execution thread.
	// 3. Return payload execution throughput metrics or map errors back to standard syscall.Errno.
	panic("implement me")
}

// Close gracefully terminates an active socket connection context descriptor.
func (nr *NetRing) Close(fd int) error {
	// Standard async submission workflow to perform orderly kernel-level descriptor destruction.
	panic("implement me")
}
```

---

## 3. Single-Threaded Translator Workflow

The Translator Loop serves as the sole coordinator for the allocation tracking tables. It works continuously in a 
single thread context without any external atomic locks:

1. Drains a `ringTask` token from its designated channel mesh.
2. Invokes **`slotIdx := nr.slots.Add(taskContext{g: task.G})`**. This updates the private L1 `bitmap` and maps 
   context variables without thread-contention overloads.
3. Maps the returned `slotIdx` directly into the `UserData` parameter of the target `io_uring.SQEntry`.
4. Executes the corresponding non-blocking submission call (`r.r.ExpectRecv`, `r.r.ExpectSend`, etc.) to register 
   the operation within the SQ ring buffer.
