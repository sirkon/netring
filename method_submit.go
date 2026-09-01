package netring

import (
	"runtime"
	"sync/atomic"
	"unsafe"

	"github.com/sirkon/blog"
)

var everActive bool

const statusCheckCountSubmit = 128

// submit submits a task either directly to the SQ, or tries to a fallback way
// with a channel. Returns true, if the task was submitted.
// IMPORTANT: Goroutines who tried to submit and got false MUST NOT PARK.
func (nr *NetRing) submit(task ringTask) bool {
	if task.Opcode == opcodeTypeReleaseBuffer {
		// No slot, no SQE: user-space only.
		view := unsafe.Slice((*byte)(task.Payload), 1)
		if err := nr.pbrs[task.BGID].ReleaseView(view); err != nil {
			nr.logger.Error(nil, "failed to release view", blog.Err(err))
		}
		return true
	}

	status := nr.submitIntention()

	var statusCheckCount int

statusCheck:
	if status == statusFallbackActive {
		select {
		case nr.fallbackChan <- task:
			return true
		case <-nr.stop:
			return false
		}
	}

	ticketTail := nr.ticketTail.Load()
	tail := atomic.LoadUint32(nr.r.SQTail)
	if ticketTail != tail {
		procyield(30)
		status = nr.barrier.Load() & statusFallbackActive

		statusCheckCount++
		if statusCheckCount == statusCheckCountSubmit {
			statusCheckCount = 0
			runtime.Gosched()
		}

		goto statusCheck
	}

	if !nr.ticketTail.CompareAndSwap(tail, tail+1) {
		procyield(30)
		status = nr.barrier.Load() & statusFallbackActive

		statusCheckCount++
		if statusCheckCount == statusCheckCountSubmit {
			statusCheckCount = 0
			runtime.Gosched()
		}

		goto statusCheck
	}

	if tail-atomic.LoadUint32(nr.r.SQHead) == *nr.r.SQEntries {
		nr.setFallbackActive()
		select {
		case nr.fallbackChan <- task:
			return true
		case <-nr.stop:
			return false
		}
	}

	nr.dispatch(task)
	nr.finishedPushes.Add(1)

	return true
}

func (nr *NetRing) submitIntention() uint64 {
	var status uint64
	for {
		b := nr.barrier.Load()
		status = b & statusFallbackActive
		epoch := b & epochMask

		if !nr.barrier.CompareAndSwap(b, status|((epoch+1)&epochMask)) {
			procyield(30)
			continue
		}

		return status
	}
}

func (nr *NetRing) setFallbackActive() {
	for {
		b := nr.barrier.Load()
		if nr.barrier.CompareAndSwap(b, statusFallbackActive|b) {
			return
		}

		procyield(30)
	}
}

// dispatch performs the per-POD submission. The slotIdx always
// lands in the SQE UserData; the non-blocking Expect* builders from
// internal/iouring fill the SQE.
func (nr *NetRing) dispatch(task ringTask) {
	switch task.Opcode {
	case opcodeTypeAccept:
		slotIdx := nr.slots.Add(task.Ctx)
		if err := nr.r.ExpectAccept(int32(task.Ctx.fd), slotIdx); err != nil {
			nr.abortIssuer(slotIdx, true, err)
		}

	case opcodeTypeConnect:
		slotIdx := nr.slots.Add(task.Ctx)
		if err := nr.r.ExpectConnect(int(task.Ctx.fd), task.Payload, uint32(task.Addr), slotIdx); err != nil {
			nr.abortIssuer(slotIdx, true, err)
		}

	case opcodeTypeRecv:
		slotIdx := nr.slots.Add(task.Ctx)
		if err := nr.r.ExpectRecv(int32(task.Ctx.fd), task.BGID, task.Len, slotIdx); err != nil {
			nr.abortIssuer(slotIdx, true, err)
		}

	case opcodeTypeRead:
		slotIdx := nr.slots.Add(task.Ctx)
		if err := nr.r.ExpectRead(int32(task.Ctx.fd), task.BGID, task.Len, slotIdx); err != nil {
			nr.abortIssuer(slotIdx, true, err)
		}

	case opcodeTypeSend:
		slotIdx := nr.slots.Add(task.Ctx)
		if err := nr.r.ExpectSend(int32(task.Ctx.fd), task.Payload, task.Len, slotIdx); err != nil {
			nr.abortIssuer(slotIdx, false, err)
		}

	case opcodeTypeClose:
		// Close parks its caller like any other opcode, so a
		// slot is allocated and the CQE dispatches through the standard path.
		slotIdx := nr.slots.Add(task.Ctx)
		if err := nr.r.ExpectClose(int32(task.Ctx.fd), slotIdx); err != nil {
			nr.abortIssuer(slotIdx, true, err)
		}

	case opcodeTypeReleaseBuffer:
		// Processed before.
		panicMustNotBeHere()

	case opcodeTypeTimer:
		nr.ticketTail.Store(atomic.LoadUint32(nr.r.SQTail))
		// Reserved for the poller re-arm subsystem; log-and-skip for now.
		nr.logger.Error(nil, task.Opcode.String()+" is not implemented yet")

	default:
		nr.ticketTail.Store(atomic.LoadUint32(nr.r.SQTail))
		nr.logger.Error(
			nil, "unsupported opcode type submitted",
			blog.Group("opcode",
				blog.Uint64("code", uint64(task.Opcode)), blog.Stg("name", task.Opcode),
			),
		)
	}
}

func (nr *NetRing) abortIssuer(slotIdx uint64, needWake bool, err error) {
	// We know the task is in the slots. So, don't check it.
	cell, _ := nr.slots.Get(slotIdx)
	cell.err = err

	if !needWake {
		return
	}

	if !cell.taskState.CompareAndSwap(taskStateInCore, taskStateDone) {
		// The state is Parked, written strictly after the runtime placed
		// the goroutine into _Gwaiting: goready is always safe, the
		// "goready: bad g state" panic is structurally impossible
		//. Convert and call in one expression.
		goready((*g)(unsafe.Pointer(cell.g)), parkTraceSkip)
	}
}

func panicMustNotBeHere() {
	panic("must not be here")
}
