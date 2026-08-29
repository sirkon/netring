package netring

import (
	"unsafe"

	"github.com/sirkon/blog/beer"
)

// Close submits an asynchronous destruction of the kernel descriptor context
// and blocks the calling goroutine until io_uring reports the descriptor is
// gone. The fd must never be used again after a successful submission, and
// never closed twice.
func (nr *NetRing) Close(fd int) error {
	if fd < 0 {
		return beer.Newf("close: invalid descriptor %d, expected a non-negative one", fd)
	}

	// Arm the cell: the arm protocol must precede the channel
	// send with no exceptions, otherwise the poller could race a half-armed cell.
	cell := nr.taskCell()

	// Build the POD. io_close_prep rejects SQEs with off/addr/
	// len/buf_index set, so the reserves stay zero (ExpectClose contract).
	task := ringTask{
		Opcode:  opcodeTypeClose,
		FD:      int32(fd),
		Addr:    0,
		Len:     0,
		BGID:    0,
		Offset:  0,
		Ctx:     cell,
		Payload: nil,
	}

	// Submit into the per-descriptor shard: it keeps all operations of one fd
	// FIFO in the SQ, so a close lands strictly after any in-flight op it must
	// cancel.
	nr.chans[fd%len(nr.chans)] <- task

	// Suspend until the CQ poller delivers the result. Both the slept and the
	// never-slept paths return here with the results already in the cell.
	gopark(
		netringParkUnlock,
		unsafe.Pointer(cell),
		waitReasonIOWait,
		traceBlockGeneric,
		parkTraceSkip,
	)

	// Wakeup path (slept or never slept, identical): read the result from the
	// cell. Close reads only cell.res.
	res := cell.res

	// Return the cell to the pool. No scrubbing: the next arm protocol
	// overwrites every field.
	nr.pool.Put(cell)

	if res < 0 {
		// The kernel reported the close failed (e.g. -EBADF for an already
		// closed or never-open descriptor). map it to a raw errno.
		return kernelResultToError(res)
	}
	if cell.err != nil {
		return cell.err
	}

	// res == 0 on success: the descriptor context is destroyed. The fd number
	// may immediately be reused by another socket, so the caller must never
	// close it again (ExpectClose contract).
	return nil
}
