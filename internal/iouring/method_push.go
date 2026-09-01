package iouring

import (
	"sync/atomic"

	"github.com/sirkon/blog/beer"
)

// Push adds a task to the SQ.
// May return ErrSQFull if the task queue is full; it returns exactly the ErrSQFull value itself, without wrapping.
func (r *IOUring) Push(entry SQEntry) error {
	tail := *r.SQTail
	mask := *r.SQMask

	index := tail & mask

	// Write the data into the mmap'ed memory
	r.SQ[index] = entry
	r.SQArray[index] = index

	// Move the tail atomically
	atomic.StoreUint32(r.SQTail, tail+1)

	// Atomically read the flags. If the kernel is asleep, wake it up!
	if (atomic.LoadUint32(r.SQFlags) & ioUringSQNeedWakeup) != 0 {
		if err := r.Wakeup(); err != nil {
			return beer.Wrap(err, "wakeup kernel poller")
		}
	}

	return nil
}
