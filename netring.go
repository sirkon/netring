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

type taskContext struct {
	taskState atomic.Uint64
	g         uintptr

	// buf for receive
	buf []byte
}

type ringTask struct {
	// --- Data for io_uring.SQEntry ---
	Opcode opcodeType
	FD     int32
	Addr   uint64 // Used as time.Duration value for Timer op.
	Len    uint32
	Offset uint64

	// --- Data for Go runtime synchronization ---
	G     uintptr       // Address of the sending goroutine (runtime.getg())
	Res   int32         // Result of the operation from the CQE (bytes count or -ERRNO)
	State atomic.Uint32 // Atomic task status to prevent races
}

// There constants are used to represent
const (
	taskStateInCore = iota // 0: ringTask went into io_uring / the translator
	taskStateDone          // 1: The poller already processed the CQE and wrote the result
	taskStateParked        // 2: The worker successfully entered gopark and went to sleep
)

type opcodeType uint32

const (
	opcodeTypeInvalid opcodeType = iota
	opcodeTypeAccept
	opcodeTypeClose
	opcodeTypeTimer
	opcodeTypeRecv
	opcodeTypeSend
	// And further...
)
