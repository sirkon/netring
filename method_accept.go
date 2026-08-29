package netring

import (
	"unsafe"

	"github.com/sirkon/blog/beer"
)

// Accept suspends the calling goroutine until a new client connection arrives.
// It returns the accepted client file descriptor, or a negative-errno error.
// The listen descriptor stays functional; -EINTR/-EAGAIN/-ECONNABORTED are
// transient, the caller may retry Accept.
func (nr *NetRing) Accept(listenFD int) (int32, error) {
	if listenFD < 0 {
		return 0, beer.Newf("accept: invalid descriptor %d, expected a non-negative one", listenFD)
	}

	// Arm the cell: the arm protocol must precede the channel
	// send with no exceptions, otherwise the poller could race a half-armed cell.
	cell := nr.taskCell()

	// Build the POD. The accepted peer address is discarded: the
	// iouring layer parks it in package dummies, no sockaddr returns through this
	// API. Length/offset/bgid are unused by ACCEPT.
	task := ringTask{
		Opcode:  opcodeTypeAccept,
		FD:      int32(listenFD),
		Addr:    0,
		Len:     0,
		BGID:    0,
		Offset:  0,
		Ctx:     cell,
		Payload: nil,
	}

	// Submit into the per-listener shard: it keeps all ACCEPTs of one listener
	// FIFO in the SQ. The channel send may block on
	// backpressure; nothing can complete before the task reaches the translator,
	// so that is fine.
	nr.chans[listenFD%len(nr.chans)] <- task

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
	// cell. Accept reads only cell.res; cell.flags is ignored for this opcode
	//.
	res := cell.res

	// Return the cell to the pool. No scrubbing: the next arm protocol
	// overwrites every field.
	nr.pool.Put(cell)

	if res < 0 {
		// The kernel reported the accept failed (e.g. -EBADF for a descriptor
		// that is not a listening socket). Map to a raw errno.
		// The listen socket stays functional; the caller may retry on transient
		// errors.
		return 0, kernelResultToError(res)
	}
	if cell.err != nil {
		return 0, cell.err
	}

	// res >= 0: res is the accepted client fd (the kernel wrote it into
	// CQE.Res, ExpectAccept contract). The fd is not registered anywhere by
	// Accept; the caller owns it and later uses it for Recv/Send/Close on this
	// same NetRing.
	return res, nil
}
