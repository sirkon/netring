package iouring

import (
	"fmt"
	"math/bits"
	"syscall"
	"unsafe"

	"github.com/sirkon/blog"
	"github.com/sirkon/blog/beer"
	"golang.org/x/sys/unix"
)

// IOUring envelope.
type IOUring struct {
	FD     int
	Params Params

	// Submission Queue (SQ)
	SQRingPtr uintptr
	SQHead    *uint32
	SQTail    *uint32
	SQMask    *uint32
	SQEntries *uint32
	SQFlags   *uint32
	SQArray   []uint32  // Array of indices
	SQ        []SQEntry // Array of SQEs themselves (64 bytes each)

	// Completion Queue (CQ)
	CQRingPtr uintptr
	CQHead    *uint32
	CQTail    *uint32
	CQMask    *uint32
	CQEntries *uint32
	CQ        []CQEntry // Array of CQEs (16 bytes each)

	CQLengthMask uint32
	logger       *blog.Logger
}

func New(entries uint32, logger *blog.Logger) (*IOUring, error) {
	if bits.OnesCount(uint(entries)) != 1 {
		return nil, beer.New("number of entries must be a power of 2")
	}
	if entries < 256 {
		return nil, beer.New("number of entries must be at least 256")
	}

	ring := &IOUring{
		CQLengthMask: entries - 1,
	}

	ring.Params.Flags = setupSQPoll | setupSQAff //| featSingleMMap
	fd, _, errno := syscall.Syscall(
		unix.SYS_IO_URING_SETUP,
		uintptr(entries),
		uintptr(unsafe.Pointer(&ring.Params)),
		0,
	)
	if errno != 0 {
		return nil, beer.Wrap(errno, "setup io_uring")
	}

	ring.FD = int(fd)

	sqRingSize := ring.Params.SQOff.Array + ring.Params.SQEntries*4
	cqRingSize := ring.Params.CQOff.Cqes + ring.Params.CQEntries*uint32(unsafe.Sizeof(CQEntry{}))
	if ring.Params.Features&featSingleMMap != 0 {
		if cqRingSize > sqRingSize {
			sqRingSize = cqRingSize
		}
		cqRingSize = sqRingSize
	}

	sqPtr, err := unix.Mmap(
		ring.FD,
		int64(0),
		int(sqRingSize),
		unix.PROT_READ|unix.PROT_WRITE,
		unix.MAP_SHARED|unix.MAP_POPULATE,
	)
	if err != nil {
		return nil, beer.Wrap(err, "mmap SQ")
	}

	ring.SQRingPtr = uintptr(unsafe.Pointer(&sqPtr[0]))

	ring.SQHead = (*uint32)(unsafe.Pointer(ring.SQRingPtr + uintptr(ring.Params.SQOff.Head)))
	ring.SQTail = (*uint32)(unsafe.Pointer(ring.SQRingPtr + uintptr(ring.Params.SQOff.Tail)))
	ring.SQMask = (*uint32)(unsafe.Pointer(ring.SQRingPtr + uintptr(ring.Params.SQOff.RingMask)))
	ring.SQEntries = (*uint32)(unsafe.Pointer(ring.SQRingPtr + uintptr(ring.Params.SQOff.RingEntries)))
	ring.SQFlags = (*uint32)(unsafe.Pointer(ring.SQRingPtr + uintptr(ring.Params.SQOff.Flags)))
	ring.SQArray = unsafe.Slice(
		(*uint32)(unsafe.Pointer(ring.SQRingPtr+uintptr(ring.Params.SQOff.Array))),
		ring.Params.SQEntries,
	)

	sqesSize := int(ring.Params.SQEntries) * 64
	sqesPtr, err := unix.Mmap(
		ring.FD,
		offSQes,
		sqesSize,
		unix.PROT_READ|unix.PROT_WRITE,
		unix.MAP_SHARED|unix.MAP_POPULATE,
	)
	if err != nil {
		return nil, beer.Wrap(err, "mmap SQes")
	}
	ring.SQ = unsafe.Slice((*SQEntry)(unsafe.Pointer(&sqesPtr[0])), ring.Params.SQEntries)

	var cqRingPtr uintptr
	if (ring.Params.Features & featSingleMMap) != 0 {
		// If the kernel supports SINGLE_MMAP, CQ shares memory with SQ
		cqRingPtr = ring.SQRingPtr
	} else {
		// For old kernels we do a separate mmap
		cqPtr, err := unix.Mmap(ring.FD, int64(offCQRing), int(cqRingSize), unix.PROT_READ|unix.PROT_WRITE, unix.MAP_SHARED|unix.MAP_POPULATE)
		if err != nil {
			return nil, fmt.Errorf("mmap CQ: %w", err)
		}
		cqRingPtr = uintptr(unsafe.Pointer(&cqPtr[0]))
	}

	ring.CQRingPtr = cqRingPtr
	ring.CQHead = (*uint32)(unsafe.Pointer(ring.CQRingPtr + uintptr(ring.Params.CQOff.Head)))
	ring.CQTail = (*uint32)(unsafe.Pointer(ring.CQRingPtr + uintptr(ring.Params.CQOff.Tail)))
	ring.CQMask = (*uint32)(unsafe.Pointer(ring.CQRingPtr + uintptr(ring.Params.CQOff.RingMask)))
	ring.CQEntries = (*uint32)(unsafe.Pointer(ring.CQRingPtr + uintptr(ring.Params.CQOff.RingEntries)))

	// Array of ready CQE results
	ring.CQ = unsafe.Slice(
		(*CQEntry)(unsafe.Pointer(ring.CQRingPtr+uintptr(ring.Params.CQOff.Cqes))),
		ring.Params.CQEntries,
	)

	ring.CQLengthMask = ring.Params.CQEntries - 1
	ring.logger = logger

	return ring, nil
}
