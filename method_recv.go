package netring

import (
	"unsafe"

	"github.com/sirkon/blog/beer"

	"github.com/sirkon/netring/internal/iouring"
)

// Recv acquires an incoming payload directly into a kernel-provided buffer of
// the requested size class and returns a zero-copy view into it. The view is
// valid until ReleaseBuffer(sizeClass, view) hands it back to the kernel
// .
func (nr *NetRing) Recv(fd int, sizeClass SizeClass) ([]byte, error) {
	// 1. Validate fd and sizeClass. Unknown classes must fail before anything
	// is touched; the class must also be provisioned, else the translator
	// would dereference a nil ring.
	if fd < 0 {
		return nil, beer.Newf("recv: invalid descriptor %d, expected a non-negative one", fd)
	}
	capacity := sizeClass.Size()
	if capacity == 0 {
		return nil, beer.Newf("recv: %s", sizeClass)
	}
	if nr.pbrs[sizeClass] == nil {
		return nil, beer.Newf("recv: size class %d is not provisioned", uint16(sizeClass))
	}

	// 3. Arm the cell: the arm protocol must precede the
	// channel send with no exceptions, otherwise the poller could race a
	// half-armed cell.
	cell := nr.taskCell(opcodeTypeRecv, fd)

	// 4. Build the POD. Recv is address-less: Addr and Offset
	// stay zero, the kernel picks the buffer at completion time. BGID IS the
	// SizeClass value.
	task := ringTask{
		Opcode:  opcodeTypeRecv,
		Addr:    0,
		Len:     0,
		BGID:    uint16(sizeClass),
		Offset:  0,
		Ctx:     cell,
		Payload: nil,
	}

	// 5. Submit into the per-fd shard. The channel send may
	// block on backpressure; nothing can complete before the task reaches the
	// translator, so that is fine. fd was validated >= 0
	// above, so the modulo cannot be negative.
	if !nr.submit(task) {
		return nil, beer.New("failed to submit task")
	}

	// 6. Suspend until the CQ poller delivers the result. Both the slept and
	// the never-slept paths return here with the result already in the cell.
	gopark(
		netringParkUnlock,
		unsafe.Pointer(cell),
		waitReasonIOWait,
		traceBlockGeneric,
		parkTraceSkip,
	)

	// 7. Wakeup path (slept or never slept, identical): read the results.
	res := cell.res
	flags := cell.flags

	// 8. res < 0: the completion failed. The kernel recycled the picked buffer
	// in place (io_kbuf_recycle), the CQE carries no ioUringCQEFBuffer, and no
	// buffer is consumed: nothing to release, no leak.
	if res < 0 {
		nr.pool.Put(cell)
		return nil, kernelResultToError(res)
	}
	if cell.err != nil {
		nr.pool.Put(cell)
		return nil, cell.err
	}

	// 9. res == 0: the peer performed an orderly shutdown (EOF). The buffer
	// was recycled in-kernel, nothing is consumed; return the empty view, not
	// an error. No ReleaseBuffer call here.
	if res == 0 {
		nr.pool.Put(cell)
		return nil, nil
	}

	// 10. res > 0: res is the byte count in the kernel-picked buffer.

	// 11. Defensive branch (must not fire): every
	// successful BUFFER_SELECT RECV CQE carries ioUringCQEFBuffer via
	// io_put_kbuf; its absence with res > 0 would be a malformed completion.
	if flags&iouring.CQEFBuffer == 0 {
		nr.pool.Put(cell)
		return nil, beer.New("recv: successful completion carried no buffer-select flag")
	}

	// 12. Extract the bid: CQE flags >> ioUringCQEBufferShift (16).
	bid := uint16(flags >> iouring.CQEBufferShift)

	// 13. Build the zero-copy view through the size-class ring and reslice it
	// to the received length. The gc keeps the underlying mmap'ed slice alive
	// through pbr, so the view stays rooted.
	pbr := nr.pbrs[sizeClass]
	view := pbr.Buffer(bid)
	view = view[:res]

	// 14. Return the cell to the pool (like Accept/Close) and hand the view
	// to the caller, who owns the ReleaseBuffer obligation.
	nr.pool.Put(cell)
	return view, nil
}

// ReleaseBuffer hands a consumed buffer view (a loan from Recv) back to the
// kernel-managed ring of the given size class. It never parks: it routes the
// release through the translator and returns immediately.
func (nr *NetRing) ReleaseBuffer(sizeClass SizeClass, view []byte) {
	if len(view) == 0 {
		return
	}

	task := ringTask{
		Opcode:  opcodeTypeReleaseBuffer,
		BGID:    uint16(sizeClass),
		Payload: unsafe.Pointer(unsafe.SliceData(view)),
	}

	nr.submit(task)
}
