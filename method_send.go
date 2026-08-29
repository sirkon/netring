package netring

import (
	"runtime"
	"unsafe"

	"github.com/sirkon/blog/beer"
)

// Send dispatches raw outbound data and returns the number of bytes accepted by
// the kernel, or a negative-errno error. The payload memory belongs to the
// caller and must stay alive and unmodified for the whole flight: this layer
// takes no ownership and makes no copies.
func (nr *NetRing) Send(fd int, data []byte) (int, error) {
	// 1. Validate fd before touching the kernel: the modulo used for shard
	// routing must never see a negative index.
	if fd < 0 {
		return 0, beer.Newf("send: invalid descriptor %d, expected a non-negative one", fd)
	}

	// 2. A zero-length SEND is legal to the kernel but pointless to park for:
	// return immediately, touching neither the cell nor the channel (033
	// section 2 step 2).
	if len(data) == 0 {
		return 0, nil
	}

	// 3. Extract the raw payload pointer. data stays alive and unmodified for
	// the whole flight; KeepAlive below holds it across the park.
	bufPtr := unsafe.Pointer(unsafe.SliceData(data))

	cell := nr.taskCell()

	// 5. Build the POD. Send carries the payload as a
	// GC-rooted typed unsafe.Pointer (Payload), never as uint64/uintptr, so
	// the GC keeps scanning it for the flight. SQE.Addr is filled later by
	// ExpectSend from Payload, so Addr stays zero here.
	task := ringTask{
		Opcode:  opcodeTypeSend,
		FD:      int32(fd),
		Addr:    0,
		Len:     uint32(len(data)), // must fit uint32; a send this size is absurd in practice
		BGID:    0,
		Offset:  0,
		G:       getg(),
		Ctx:     cell,
		Payload: bufPtr,
	}

	// 6. Submit into the per-fd shard: it keeps all sends of one fd FIFO in
	// the SQ, so ordering is preserved. The channel send may block on
	// backpressure; nothing can complete before the task reaches the
	// translator, so that is fine.
	nr.chans[fd%len(nr.chans)] <- task

	// 7. Suspend until the CQ poller delivers the result. Both the slept and
	// the never-slept paths return here with the results already in the cell.
	gopark(
		netringParkUnlock,
		unsafe.Pointer(cell),
		waitReasonIOWait,
		traceBlockGeneric,
		parkTraceSkip,
	)

	// 8. Wakeup path (slept or never slept, identical): read the result from
	// the cell. Send ignores cell.flags entirely.
	res := cell.res

	// 9. Keep the payload reference alive until after the cell read, so the
	// GC cannot collect the payload while the kernel may still be reading it
	//.
	runtime.KeepAlive(data)

	// Return the cell to the pool. No scrubbing: the next arm protocol
	// overwrites every field.
	nr.pool.Put(cell)

	// 10. res < 0: the completion failed, mapped back to a raw errno
	//.
	if res < 0 {
		return 0, kernelResultToError(res)
	}
	if cell.err != nil {
		return 0, cell.err
	}

	// 11. res >= 0: res is the number of bytes accepted.
	return int(res), nil
}
