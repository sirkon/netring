package netring

import (
	"runtime"
	"unsafe"

	"github.com/sirkon/blog/beer"
)

// Send makes sure the previous send on this socket was completed and queue its own send task.
// Returns an error if the previous sent failed. Complete the previous sent too.
func (nr *NetRing) Send(fd int, data []byte) error {
	// 1. Validate fd before touching the kernel: the modulo used for shard
	// routing must never see a negative index.
	if fd < 0 || fd > 65535 {
		return beer.Newf("send: invalid descriptor %d, expected something in 0...65535 range", fd)
	}

	// 2. A zero-length SEND is legal to the kernel but pointless to park for:
	// return immediately, touching neither the cell nor the channel (033
	// section 2 step 2).
	if len(data) == 0 {
		return nil
	}

	if err := nr.previousSendWait(fd); err != nil {
		return beer.Wrap(err, "")
	}

	// 3. Extract the raw payload pointer. data stays alive and unmodified for
	// the whole flight; KeepAlive below holds it across the park.
	bufPtr := unsafe.Pointer(unsafe.SliceData(data))

	cell := nr.taskCell(opcodeTypeSend, fd)

	// 5. Build the POD. Send carries the payload as a
	// GC-rooted typed unsafe.Pointer (Payload), never as uint64/uintptr, so
	// the GC keeps scanning it for the flight. SQE.Addr is filled later by
	// ExpectSend from Payload, so Addr stays zero here.
	task := ringTask{
		Opcode:  opcodeTypeSend,
		Addr:    0,
		Len:     uint32(len(data)), // must fit uint32; a send this size is absurd in practice
		BGID:    0,
		Offset:  0,
		Ctx:     cell,
		Payload: bufPtr,
	}
	sCell := &nr.sendCells[fd]
	sCell.len = uint64(len(data))
	sCell.sent = 0
	sCell.buf = bufPtr
	sCell.queued++

	// 6. Submit into the per-fd shard: it keeps all sends of one fd FIFO in
	// the SQ, so ordering is preserved. The channel send may block on
	// backpressure; nothing can complete before the task reaches the
	// translator, so that is fine.
	if !nr.submit(task) {
		return beer.New("failed to submit task")
	}

	return nil
}

func (nr *NetRing) FlushFDSends(fd int) error {
	return nr.previousSendWait(fd)
}

func (nr *NetRing) previousSendWait(fd int) error {
waitForPreviousOpOrItsContinuation:

	// Spins until the previous operation finished.
	sCell := &nr.sendCells[fd]
	if sCell.queued == 0 {
		return nil
	}

	for {
		finished := sCell.finished.Load()
		if sCell.err != nil {
			return beer.Wrap(sCell.err, "check previous send operation status")
		}

		if finished == sCell.queued {
			break
		}

		runtime.Gosched()
	}

	if sCell.sent == sCell.len {
		return nil
	}

	// Compensate send for the incomplete one.
	cell := nr.taskCell(opcodeTypeSend, fd)
	task := ringTask{
		Opcode:  opcodeTypeSend,
		Addr:    0,
		Len:     uint32(sCell.len - sCell.sent),
		BGID:    0,
		Offset:  0,
		Ctx:     cell,
		Payload: unsafe.Add(sCell.buf, sCell.sent),
	}
	sCell.queued++
	sCell.buf = unsafe.Add(sCell.buf, sCell.len)
	sCell.len -= sCell.sent
	sCell.sent = 0
	if !nr.submit(task) {
		return beer.Wrap(sCell.err, "failed to submit compensation for partial send")
	}

	goto waitForPreviousOpOrItsContinuation
}
