package netring

import (
	"github.com/sirkon/blog"
	"github.com/sirkon/blog/beer"
)

// Stop shuts the NetRing down: the translator stops draining task channels,
// every registered buffer ring is unregistered and the io_uring itself is
// destroyed. Contract: the caller must have stopped (and waited for) the CQ
// poller and must have no operations in flight; Stop must be called at most
// once.
//
// The returned error is the single teardown error: unregister failures are
// logged and swallowed (the ring's memory is about to go away either way),
// while the io_uring destroy error is wrapped with "destroy io_uring" and
// returned.
func (nr *NetRing) Stop() error {
	// 1. The translator returns after its current iteration; buffered release
	// tasks are dropped, which is safe because the rings are munmap'ed right
	// after and no one else writes them.
	close(nr.stop)

	// 2. The translator is provably gone before its shared memory (pbuf ring
	// tails) is touched.
	<-nr.translateDone

	// 3. Unregister every provisioned ring; failures end their lifecycle here
	// and are logged (ERRORS.md rules 2, 3, 7).
	for _, pbr := range nr.pbrs {
		if pbr == nil {
			continue
		}
		if err := pbr.Unregister(); err != nil {
			nr.logger.Error(nil, "failed to unregister buffer ring", blog.Err(err))
		}
	}

	// 4. Destroy the io_uring itself: munmap SQ/CQ, close the io_uring fd,
	// kill the SQPOLL thread. Its error is the single returned error.
	if err := nr.r.Close(); err != nil {
		return beer.Wrap(err, "destroy io_uring")
	}

	return nil
}