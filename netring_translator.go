package netring

import (
	"runtime"
	"unsafe"

	"github.com/sirkon/blog"
)

// translate is the single Translator goroutine. It is the ONLY
// user of the slots table (taskslots is not thread-safe by design) and the only
// goroutine allowed to call ProvidedBufferRing release routines (the shared ring
// tail is not thread-safe).
func (nr *NetRing) translate() {
	chansLen := len(nr.chans)
	for {
		select {
		case <-nr.stop:
			return
		default:
		}

		progress := false
		for i := range chansLen {
			select {
			case task := <-nr.chans[i]:
				nr.dispatch(task)
				progress = true
			default:
			}
		}
		if !progress {
			runtime.Gosched()
		}
	}
}

// dispatch performs the per-POD submission. The slotIdx always
// lands in the SQE UserData; the non-blocking Expect* builders from
// internal/iouring fill the SQE.
func (nr *NetRing) dispatch(task ringTask) {
	switch task.Opcode {
	case opcodeTypeAccept:
		slotIdx := nr.slots.Add(task.Ctx)
		if err := nr.r.ExpectAccept(task.FD, slotIdx); err != nil {
			nr.abortIssuer(slotIdx, err)
		}

	case opcodeTypeRecv:
		slotIdx := nr.slots.Add(task.Ctx)
		if err := nr.r.ExpectRecv(task.FD, task.BGID, task.Len, slotIdx); err != nil {
			nr.abortIssuer(slotIdx, err)
		}

	case opcodeTypeSend:
		slotIdx := nr.slots.Add(task.Ctx)
		if err := nr.r.ExpectSend(task.FD, task.Payload, task.Len, slotIdx); err != nil {
			nr.abortIssuer(slotIdx, err)
		}

	case opcodeTypeClose:
		// Close parks its caller like any other opcode, so a
		// slot is allocated and the CQE dispatches through the standard path.
		slotIdx := nr.slots.Add(task.Ctx)
		if err := nr.r.ExpectClose(task.FD, slotIdx); err != nil {
			nr.abortIssuer(slotIdx, err)
		}

	case opcodeTypeReleaseBuffer:
		// No slot, no SQE: user-space only.
		view := unsafe.Slice((*byte)(task.Payload), 1)
		if err := nr.pbrs[task.BGID].ReleaseView(view); err != nil {
			nr.logger.Error(nil, "failed to release view", blog.Err(err))
		}

	case opcodeTypeTimer:
		// Reserved for the poller re-arm subsystem; log-and-skip for now
		//.
		nr.logger.Error(nil, "netring: opcodeTypeTimer is not implemented yet")

	default:
		nr.logger.Error(nil, "netring: unknown opcode submitted into the translator",
			blog.Uint32("opcode", uint32(task.Opcode)))
	}
}

func (nr *NetRing) abortIssuer(slotIdx uint64, err error) {
	// We know the task is in the slots. So, don't check it.
	cell, _ := nr.slots.Get(slotIdx)
	cell.err = err

	if !cell.taskState.CompareAndSwap(taskStateInCore, taskStateDone) {
		// The state is Parked, written strictly after the runtime placed
		// the goroutine into _Gwaiting: goready is always safe, the
		// "goready: bad g state" panic is structurally impossible
		//. Convert and call in one expression.
		goready((*g)(unsafe.Pointer(cell.g)), parkTraceSkip)
	}
}
