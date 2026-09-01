package netring

import (
	"fmt"
	"math"
	"sync/atomic"
	"syscall"
	"unsafe"
)

func (nr *NetRing) taskCell() *taskCell {
	ctx := nr.pool.Get().(*taskCell)
	*ctx = taskCell{
		g: getg(),
	}

	return ctx
}

// taskCell is the park cell: one per in-flight parked operation, recycled
// through sync.Pool. It resides strictly within the pre-allocated
// taskslots arena. It uses atomic primitives safely since methods like Recv/Send
// access it via direct pointers, completely avoiding value copying across threads.
type taskCell struct {
	taskState atomic.Uint32
	g         uintptr // written by the translator from the POD G field at submit time

	res     int32
	flags   uint32
	err     error
	opCode  opcodeType
	isAsync bool
}

// ringTask is a lightweight Plain Old Data (POD) structure: no atomics, no
// interfaces, no locks inside. A channel send copies the value into the
// preallocated channel buffer: zero heap allocations per submit, the compiler
// passes it through hardware CPU registers where it fits.
type ringTask struct {
	// --- Data for io_uring.SQEntry ---
	Opcode opcodeType
	FD     int32
	Addr   uint64 // reserved for the Timer subsystem (duration value); zero for 031-034
	Len    uint32 // Recv: size-class capacity in bytes; Send: payload length
	BGID   uint16 // Recv: buffer group id == uint16(SizeClass); zero otherwise
	Offset uint64 // reserved; zero for 031-034

	// --- Data for Go runtime synchronization ---
	Ctx     *taskCell      // park cell; nil for fire-and-forget opcodes
	Payload unsafe.Pointer // Send: data pointer; ReleaseBuffer: view pointer; nil otherwise
}

// Finite State Machine (FSM) constants synchronized with taskCell.taskState layout.
const (
	taskStateInCore uint32 = iota // 0: ringTask went into io_uring / the translator
	taskStateDone                 // 1: The poller already processed the CQE and wrote the result
	taskStateParked               // 2: The worker successfully entered gopark and went to sleep
)

type opcodeType uint32

const (
	opcodeTypeInvalid opcodeType = iota

	// System opcodes.
	opcodeTypeReleaseBuffer
	opcodeTypeTimer

	// Network meta opcodes.
	opcodeTypeAccept
	opcodeTypeClose
	opcodeTypeConnect

	// Network IO opcodes.
	opcodeTypeRecv
	opcodeTypeSend
	// And further...
)

var isAsyncOp = [8]bool{opcodeTypeSend: true}

// String implements fmt.Stringer.
func (t opcodeType) String() string {
	switch t {
	case opcodeTypeInvalid:
		return "Invalid"
	case opcodeTypeReleaseBuffer:
		return "ReleaseBuffer"
	case opcodeTypeTimer:
		return "Timer"
	case opcodeTypeAccept:
		return "Accept"
	case opcodeTypeClose:
		return "Close"
	case opcodeTypeConnect:
		return "Connect"
	case opcodeTypeRecv:
		return "Recv"
	case opcodeTypeSend:
		return "Send"
	default:
		return fmt.Sprintf("opcodeType(%d)", t)
	}
}

// noWaiterTaskID marks SQEs nobody waits for: distinct from the periodic-timer
// id and above the taskslots sysIds boundary, so an accidental slots.Del on it
// is a no-op. No opcode in the current surface submits it (Close
// parks its caller, 034 section 4), but the poller dispatch defensive branch and
// this sentinel stay for future fire-and-forget opcodes.
const noWaiterTaskID uint64 = math.MaxUint64

// kernelResultToError maps a negative-errno kernel result back to a raw
// syscall.Errno without wrapping, so callers can errors.Is on it.
func kernelResultToError(res int32) error {
	if res >= 0 {
		return nil
	}
	return syscall.Errno(-res)
}
