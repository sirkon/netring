package iouring

import (
	"sync/atomic"
)

// GetTask gets a fresh task from CQ if it available.
// Boolean flag set true only if a task actually exists.
func (r *IOUring) GetTask() (CQEntry, bool) {
	read := *r.CQHead
	if read == atomic.LoadUint32(r.CQTail) {
		return CQEntry{}, false
	}

	idx := read & r.CQLengthMask
	resp := r.CQ[idx]
	atomic.AddUint32(r.CQHead, 1)

	return resp, true
}
