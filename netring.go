package netring

import (
	"fmt"
	"sync"
	"sync/atomic"

	"github.com/sirkon/blog"
	"github.com/sirkon/blog/beer"

	"github.com/sirkon/netring/internal/iouring"
	"github.com/sirkon/netring/internal/taskslots"
)

type NetRing struct {
	r      *iouring.IOUring
	logger *blog.Logger

	slots *taskslots.Slots[*taskCell] // arena stores cell pointers, see 030 section 9

	pbrs []*iouring.ProvidedBufferRing // indexed by SizeClass value (== bgid)
	pool sync.Pool                     // *taskCell recycling

	fallbackChan   chan ringTask
	barrier        atomic.Uint64 // [active:1 | epoch:63]
	finishedPushes atomic.Uint64
	ticketTail     atomic.Uint32 // see §3.2: 32-bit domain

	failedSockets [8192]uint64

	timerTask chan struct{}
	stop      chan struct{}
	// fallbackLoopStopped is closed by the translator on return, so Stop can prove
	// the translator is gone before its shared memory is torn down.
	fallbackLoopStopped chan struct{}
}

const (
	statusFallbackActive uint64 = 1 << 63
	epochMask            uint64 = ^statusFallbackActive
)

// New creates io_uring and envelope over it.
func New(entries uint32, logger *blog.Logger, options ...OptionSetter) (*NetRing, error) {
	if logger == nil {
		return nil, beer.New("logger must be provided")
	}

	opts := netringOptions{
		tasksChanBuffer: defaultShardBuffering,
		tasksChanShards: defaultShardsCount,
	}
	var hadSlots bool
	for _, option := range options {
		_, ok := option.(*netringOptionsSlotsSetter)
		if ok {
			hadSlots = true
		}
		if err := option.apply(&opts); err != nil {
			return nil, beer.Wrap(err, option.String())
		}
	}
	if !hadSlots {
		opt := &netringOptionsSlotsSetter{
			noOfSlots: defaultSlotsCapacity,
		}

		if err := opt.apply(&opts); err != nil {
			return nil, beer.Wrap(err, opt.String())
		}
	}

	ring, err := iouring.New(entries, logger)
	if err != nil {
		return nil, beer.Wrap(err, "create io_uring instance")
	}

	nr := &NetRing{
		r:      ring,
		logger: logger,

		slots:        opts.slots,
		fallbackChan: make(chan ringTask, opts.tasksChanBuffer),
		pbrs:         make([]*iouring.ProvidedBufferRing, sizeClassesCount),
		pool: sync.Pool{
			New: func() any { return new(taskCell) },
		},

		timerTask:           make(chan struct{}, 1),
		stop:                make(chan struct{}),
		fallbackLoopStopped: make(chan struct{}),
	}

	go nr.fallbackLoop()

	return nr, nil
}

// SizeClass is a provided-buffer size class. The numeric value IS the buffer
// group id (bgid) AND the index into NetRing.pbrs.
type SizeClass uint64

const sizeClassesCount = 5

var sizeClassesCapacity = [sizeClassesCount]uint32{
	SizeClassTiny:   128,
	SizeClassSmall:  512,
	SizeClassMedium: 1024,
	SizeClassBig:    4096,
	SizeClassHuge:   16384,
}

const (
	// SizeClassTiny for 128 bytes buffer.
	SizeClassTiny SizeClass = iota
	// SizeClassSmall for 512 bytes buffer.
	SizeClassSmall
	// SizeClassMedium for 1024 bytes buffer.
	SizeClassMedium
	// SizeClassBig for 4096 bytes buffer.
	SizeClassBig
	// SizeClassHuge for 16384 bytes buffer.
	SizeClassHuge
)

func (s SizeClass) Size() uint32 {
	if int(s) < len(sizeClassesCapacity) {
		return sizeClassesCapacity[s]
	}

	return 0
}

func (s SizeClass) String() string {
	switch s {
	case SizeClassTiny:
		return "tiny"
	case SizeClassSmall:
		return "small"
	case SizeClassMedium:
		return "medium"
	case SizeClassBig:
		return "big"
	case SizeClassHuge:
		return "huge"
	default:
		return fmt.Sprintf("invalid-size-class(%d)", s)
	}
}
