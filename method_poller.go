package netring

import (
	"errors"
	"runtime"
	"sync/atomic"
	"time"
	"unsafe"

	"github.com/sirkon/blog"
	"golang.org/x/sys/unix"
)

// Poll starts CQ polling.
func (nr *NetRing) Poll(
	stop *atomic.Bool,
	finish chan struct{},
	options ...CQPollerOption,
) {
	runtime.LockOSThread()
	defer close(finish)

	opts := cqPollerOptions{
		timePeriod: time.Second / 2,
		idleAfter:  maxSpinLimit,
	}
	for _, option := range options {
		option.applyPollerOption(&opts)
	}

	// TODO should put timer task in here. Must have periodicalTimerTaskID UserData value.
	//      Put it back once it appeared in CQ: use a standalone goroutine with buffered
	//      channel to do this. That goroutine eats the data from channel and puts a timer
	//      task into some channel of Translator goroutine. It ends up in the SQ eventually
	//      and will hit us back in some time.

	var spinCount int

	for {
		if stop.Load() {
			break
		}

		resp, ready := nr.r.GetTask()
		if !ready {
			spinCount++

			if spinCount < opts.idleAfter {
				// Repeat until the spin limit is not reached.
				runtime.Gosched()
				continue
			}

			// The limit was not reached, total silence it is.
			// Ask the kernel to wake us up for new events.
			spinCount = 0
			if err := nr.r.WaitEvents(); err != nil {
				if errors.Is(err, unix.EINTR) {
					continue
				}

				nr.logger.Error(nil, "io_uring_enter hybrid freeze failed", blog.Err(err))
				runtime.Gosched()
			}
		}

		// Hot path.
		spinCount = 0

		// Dispatch the CQE: the poller needs no opcode
		// knowledge, res and flags are interpreted by the woken worker method.
		switch {
		case resp.UserData == periodicalTimerTaskID:
			// The timer subsystem is out of scope for 031-034; park the
			// notification into the buffered channel and keep draining.
			select {
			case nr.timerTask <- struct{}{}:
			default:
			}
			continue

		case resp.UserData == noWaiterTaskID:
			// Handle raw fire-and-forget submission completion entries
			// natively: no tracking slot was consumed (TASK.md section 4) and
			// nobody waits, so nothing is dispatched and there is no goready.
			// A failure is still recorded, at warning level, via diagnostics
			// logging fields.
			if resp.Res < 0 {
				nr.logger.Warn(nil, "netring: asynchronous send operation completed with error",
					blog.Int("result", int(resp.Res)))
			}
			continue
		}

		cell, ok := nr.slots.Get(resp.UserData)
		if !ok {
			// Defensive: must not happen.
			nr.logger.Error(nil, "netring: CQE carries an unknown slot index",
				blog.Uint64("slot", resp.UserData))
			continue
		}

		// Results are written BEFORE the CAS: the
		// seq-cst Swap/CAS pair on taskState is the release/acquire edge.
		cell.res = resp.Res
		cell.flags = resp.Flags

		switch cell.opCode {
		case opcodeTypeSend:
			sCell := &nr.sendCells[cell.fd]
			if resp.Res < 0 {
				sCell.err = unix.Errno(-resp.Res)
			} else {
				sCell.sent += uint64(resp.Res)
			}
			nr.pool.Put(cell)
			nr.slots.Del(resp.UserData)
			sCell.finished.Add(1)
			continue
		default:
		}

		if !cell.taskState.CompareAndSwap(taskStateInCore, taskStateDone) {
			// The state is Parked, written strictly after the runtime placed
			// the goroutine into _Gwaiting: goready is always safe, the
			// "goready: bad g state" panic is structurally impossible
			//. Convert and call in one expression.
			goready((*g)(unsafe.Pointer(cell.g)), parkTraceSkip)
		}
	}
}

// CQPollerOption poller options.
type CQPollerOption interface {
	applyPollerOption(options *cqPollerOptions)
}

// WithCQPollerOptionTimer sets periodical timer event.
func WithCQPollerOptionTimer(dur time.Duration) CQPollerOption {
	return cqPollerOptionTimerPeriod(dur)
}

type cqPollerOptions struct {
	timePeriod time.Duration
	idleAfter  int
}

type cqPollerOptionTimerPeriod time.Duration

func (c cqPollerOptionTimerPeriod) applyPollerOption(options *cqPollerOptions) {
	options.timePeriod = time.Duration(c)
}

type cqPollerOptionIdleAfter int

func (c cqPollerOptionIdleAfter) applyPollerOption(options *cqPollerOptions) {
	options.idleAfter = int(c)
}
