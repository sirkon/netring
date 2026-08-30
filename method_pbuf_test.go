package netring

import (
	"sync/atomic"
	"testing"

	"github.com/alecthomas/assert/v2"
)

// TestRegisterBufferRingProvisionsTiny checks the happy path: a fresh tiny
// ring becomes usable by Recv. The duplicate provisioning must fail instead
// of leaking the old ring.
func TestRegisterBufferRingProvisionsTiny(t *testing.T) {
	nr := newTestNetRing(t)

	assert.NoError(t, nr.RegisterBufferRing(SizeClassTiny, 4))
	// Direct internal check that the ring actually got stored: the
	// translator would dereference it on the next Recv.
	assert.True(t, nr.pbrs[SizeClassTiny] != nil, "the tiny ring must be provisioned")

	err := nr.RegisterBufferRing(SizeClassTiny, 4)
	assert.Error(t, err, "duplicate provisioning must fail")
}

// TestRegisterBufferRingInvalidClass checks the validation guard: a size
// class with no known size must fail before anything is touched.
func TestRegisterBufferRingInvalidClass(t *testing.T) {
	nr := newTestNetRing(t)

	err := nr.RegisterBufferRing(42, 4)
	assert.Error(t, err, "an invalid size class must fail")

	// Out-of-range indexes have Size() == 0 too, so they must fail the same
	// way; netring must never touch the kernel with them.
	err = nr.RegisterBufferRing(SizeClass(64), 4)
	assert.Error(t, err)
}

// TestStopFreshRing checks the whole teardown path on a fresh ring with a
// provisioned tiny class: after a poller start/stop cycle Stop returns nil
// and the process stays clean. The ring is created directly (not through
// newTestNetRing, whose poller would still be alive at Stop time) so the
// Stop contract holds: poller dead before the ring is destroyed.
func TestStopFreshRing(t *testing.T) {
	nr, err := New(256, testLogger(t))
	if err != nil {
		t.Skipf("io_uring not available here: %v", err)
	}

	assert.NoError(t, nr.RegisterBufferRing(SizeClassTiny, 4))

	var stop atomic.Bool
	finish := make(chan struct{})
	go nr.Poll(&stop, finish)
	stop.Store(true)
	<-finish

	assert.NoError(t, nr.Stop())
}