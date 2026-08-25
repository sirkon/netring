package netring

import (
	"errors"
	"runtime"
	"sync/atomic"
	"time"

	"github.com/sirkon/blog"
	"golang.org/x/sys/unix"
)

// Poll starts CQ polling.
func (r *NetRing) Poll(
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

		resp, ready := r.r.GetTask()
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
			if err := r.r.WaitEvents(); err != nil {
				if errors.Is(err, unix.EINTR) {
					continue
				}

				r.logger.Error(nil, "io_uring_enter hybrid freeze failed", blog.Err(err))
				runtime.Gosched()
			}
		}

		// Hot path.
		spinCount = 0

		// TODO determine whether it that timer task or just regular task. In case it is a time task
		//      do what's been instructed in ARCH.md. In case of regular task get a respective slot and call
		//      goready as has been written in ARCH.md.
		_ = resp
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
