package netring

import (
	"runtime"
	"sync/atomic"
	"time"
	"unsafe"
)

// fallbackLoop is the single Translator goroutine. It is the ONLY
// user of the slots table (taskslots is not thread-safe by design) and the only
// goroutine allowed to call ProvidedBufferRing release routines (the shared ring
// tail is not thread-safe).
func (nr *NetRing) fallbackLoop() {
	runtime.LockOSThread()
	defer close(nr.fallbackLoopStopped)

	for {
		var task ringTask

		select {
		case task = <-nr.fallbackChan:
			everActive = true

		loop:
			// Снимаем слепок эпохи
			oldBarrier := nr.barrier.Load()
			currentEpoch := oldBarrier & epochMask

			M := currentEpoch - (nr.finishedPushes.Load() & epochMask)

			for {
				for atomic.LoadUint32(nr.r.SQTail)-atomic.LoadUint32(nr.r.SQHead) >= *nr.r.SQEntries {
					runtime.Gosched()
				}

				nr.ticketTail.Add(1) // dispatch will raise SQ tail by 1. We need to make some room.
				nr.dispatch(task)

				M--
				if M == 0 {
					break
				}

				select {
				case task = <-nr.fallbackChan:
				case <-nr.stop:
					nr.drainFallback()
					return
				}
			}

			nr.finishedPushes.Store(currentEpoch)

			if !nr.barrier.CompareAndSwap(oldBarrier, currentEpoch) {
				select {
				case task = <-nr.fallbackChan:
					goto loop
				case <-nr.stop:
					nr.drainFallback()
					return
				}
			}

			nr.ticketTail.Store(atomic.LoadUint32(nr.r.SQTail))

		case <-nr.stop:
			nr.drainFallback()
			return
		}
	}
}

func (nr *NetRing) drainFallback() {
	// TODO need the deadline option probably...
	deadline := time.NewTimer(time.Second)
	defer deadline.Stop()

	for {
		// Pushes into fallback only happen if stop was not closed.
		// So, we have some time to pull what have been pushed
		// before the close and that will unlock
		select {
		case task := <-nr.fallbackChan:
			needWake := isAsyncOp[task.Opcode]
			if needWake {
				if !task.Ctx.taskState.CompareAndSwap(taskStateInCore, taskStateDone) {
					goready((*g)(unsafe.Pointer(task.Ctx.g)), parkTraceSkip)
				}
				nr.finishedPushes.Add(1)
			}
		case <-deadline.C:
		}
	}
}
