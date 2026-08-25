package iouring

import (
	"math"
	"unsafe"

	"github.com/sirkon/blog"
	"github.com/sirkon/blog/beer"
	"golang.org/x/sys/unix"
)

// Close properly releases the memory shared with the kernel and closes the ring
func (r *IOUring) Close() error {
	// 1. Check whether the ring has already been closed (protection from double close)
	if r.FD == 0 || r.FD == math.MaxInt32 {
		return nil
	}

	var failed bool

	// 2. Unmap the SQEs array (the physical warehouse of 64-byte tasks)
	// Size is computed exactly as at creation: number of elements * 64 bytes
	if len(r.SQ) > 0 {
		sqesSize := len(r.SQ) * 64 // or unsafe.Sizeof(SQEntry{})
		// Extract the raw pointer to the beginning of the slice in memory
		sqesPtr := unsafe.SliceData(r.SQ)
		err := unix.Munmap(unsafe.Slice((*byte)(unsafe.Pointer(sqesPtr)), sqesSize))
		if err != nil {
			r.logger.Error(nil, "failed to unmap SQes buffer", blog.Err(err))
		}
		failed = true
	}

	// 3. Unmap the CQ Ring and SQ Ring
	// In IORING_FEAT_SINGLE_MMAP mode (which is standard for modern Linux kernels)
	// CQ and SQ share a single memory region, so their start addresses are identical.
	// If you mapped them with one call sized to the larger of the two rings, unmap it that way too.

	// Take the base pointer to the SQ Ring (the queue control memory) that we saved
	if r.SQRingPtr != 0 {
		// Compute the size of the SQ control ring (Array indices + offset)
		sqRingSize := r.Params.SQOff.Array + r.Params.SQEntries*4

		// If SINGLE_MMAP was used, cqRingSize could be larger, and we aligned sqRingSize to it:
		cqRingSize := r.Params.CQOff.Cqes + r.Params.CQEntries*16 // 16 bytes per CQEntry
		if (r.Params.Features & featSingleMMap) != 0 {
			if cqRingSize > sqRingSize {
				sqRingSize = cqRingSize
			}
		}

		// Free the SQ control region
		err := unix.Munmap(unsafe.Slice((*byte)(unsafe.Pointer(r.SQRingPtr)), int(sqRingSize)))
		if err != nil {
			r.logger.Error(nil, "failed to unmap SQRing buffer", blog.Err(err))
			failed = true
		}

		// If there was no SINGLE_MMAP (old kernel), CQ was mapped separately: unmap it
		if (r.Params.Features&featSingleMMap) == 0 && r.CQRingPtr != 0 {
			err = unix.Munmap(unsafe.Slice((*byte)(unsafe.Pointer(r.CQRingPtr)), int(cqRingSize)))
			if err != nil {
				r.logger.Error(nil, "failed to unmap CQRing buffer", blog.Err(err))
				failed = true
			}
		}
	}

	// 4. Close the file descriptor of the ring itself.
	// This destroys the io_uring context in the kernel and stops the SQPOLL thread.
	if err := unix.Close(r.FD); err != nil {
		r.logger.Error(nil, "failed to close io_uring descriptor", blog.Err(err))
		failed = true
	}

	// Mark the ring as invalid
	r.FD = math.MaxInt32
	r.SQRingPtr = 0
	r.CQRingPtr = 0

	if failed {
		return beer.New("there were errors on ring deconstruction")
	}

	return nil
}
