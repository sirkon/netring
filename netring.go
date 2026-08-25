package netring

import (
	"sync/atomic"

	"github.com/sirkon/blog"

	"github.com/sirkon/netring/internal/iouring"
	"github.com/sirkon/netring/internal/taskslots"
)

type NetRing struct {
	r      *iouring.IOUring
	logger *blog.Logger

	slots *taskslots.Slots[taskContext]
	chans []chan ringTask

	timerTask chan struct{}
}

// taskContext resides strictly within the pre-allocated taskslots arena.
// It uses atomic primitives safely since methods like Recv/Send access it
// via direct pointers, completely avoiding value copying across threads.
type taskContext struct {
	taskState atomic.Uint32
	g         uintptr

	// buf stores the active network data chunk slice.
	// For reads, the CQ Poller constructs it dynamically from kernel-provided buffers.
	buf []byte
}

// ringTask is a lightweight Plain Old Data (POD) structure.
// It contains no atomic or runtime tracking objects, allowing the Go compiler
// to pass it through channels via hardware CPU registers with zero heap overhead.
type ringTask struct {
	// --- Data for io_uring.SQEntry ---
	Opcode opcodeType
	FD     int32
	Addr   uint64 // Used as time.Duration value for Timer op.
	Len    uint32
	Offset uint64

	// --- Data for Go runtime synchronization ---
	G       uintptr // Address of the sending goroutine (runtime.getg())
	SlotIdx uint64  // Preserved slot identifier assigned during allocation phase
}

// Finite State Machine (FSM) constants synchronized with taskContext.taskState layout.
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

	// Network IO opcodes.
	opcodeTypeRecv
	opcodeTypeSend
	// And further...
)
