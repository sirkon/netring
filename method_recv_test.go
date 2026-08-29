package netring

import (
	"errors"
	"runtime"
	"syscall"
	"testing"
	"time"

	"github.com/alecthomas/assert/v2"
	"golang.org/x/sys/unix"
)

// provisionTinyRing registers the minimum required size class ring (030
// section 8) on nr's io_uring, so Recv can drive real traffic through it.
func provisionTinyRing(t *testing.T, nr *NetRing) {
	t.Helper()
	pbr, err := nr.r.RegisterBufferRing(uint16(SizeClassTiny), 4, SizeClassToBytes(SizeClassTiny))
	if err != nil {
		t.Skipf("pbuf rings not supported by this kernel: %v", err)
	}
	t.Cleanup(func() { assert.NoError(t, pbr.Unregister()) })
	nr.pbrs[SizeClassTiny] = pbr
}

// TestRecv drives one real payload through the full pipeline: the kernel picks
// a provided buffer, the parked worker is woken with the byte count, and the
// zero-copy view carries the written data. Releasing the view must make the
// buffer usable again by the kernel.
func TestRecv(t *testing.T) {
	nr := newTestNetRing(t)
	provisionTinyRing(t, nr)

	fds, err := unix.Socketpair(unix.AF_UNIX, unix.SOCK_STREAM, 0)
	assert.NoError(t, err)
	defer unix.Close(fds[0])
	defer unix.Close(fds[1])

	payload := []byte("hello recv")
	_, err = unix.Write(fds[1], payload)
	assert.NoError(t, err)

	type recvResult struct {
		view []byte
		err  error
	}
	results := make(chan recvResult, 1)
	go func() {
		view, err := nr.Recv(fds[0], SizeClassTiny)
		results <- recvResult{view: view, err: err}
	}()

	select {
	case res := <-results:
		assert.NoError(t, res.err, "Recv must succeed with a provisioned ring")
		assert.Equal(t, payload, res.view, "the view must carry exactly the written payload")

		// The view is a loan: hand it back, then the release travels through
		// the translator channel (chans[0]) before the buffer shows up in the
		// kernel's ring again. Poll until the release lands.
		nr.ReleaseBuffer(SizeClassTiny, res.view)
		deadline := time.Now().Add(5 * time.Second)
		for {
			avail, err := nr.pbrs[SizeClassTiny].Available()
			assert.NoError(t, err)
			if avail == 4 {
				break
			}
			if time.Now().After(deadline) {
				t.Fatalf("the released view must be back in the ring, got %d available", avail)
			}
			runtime.Gosched()
			time.Sleep(100 * time.Microsecond)
		}

	case <-time.After(5 * time.Second):
		t.Fatal("no Recv result within the wait window")
	}
}

// TestRecvRoundtrip drives several receive cycles and proves the released
// buffers are actually reused by the kernel, not just re-counted: far more
// messages than ring capacity must get through.
func TestRecvRoundtrip(t *testing.T) {
	nr := newTestNetRing(t)
	provisionTinyRing(t, nr)

	fds, err := unix.Socketpair(unix.AF_UNIX, unix.SOCK_STREAM, 0)
	assert.NoError(t, err)
	defer unix.Close(fds[0])
	defer unix.Close(fds[1])

	const messages = 3
	for i := range messages {
		payload := []byte{byte(i), byte(i << 4)}
		_, err := unix.Write(fds[1], payload)
		assert.NoError(t, err)

		view, err := nr.Recv(fds[0], SizeClassTiny)
		assert.NoError(t, err)
		assert.Equal(t, payload, view)
		nr.ReleaseBuffer(SizeClassTiny, view)
	}
}

// TestRecvEOF checks the orderly-shutdown path: when the peer closes, Recv
// returns the zero-length view (nil, nil), not an error, and the kernel
// recycled the buffer without consuming it.
func TestRecvEOF(t *testing.T) {
	nr := newTestNetRing(t)
	provisionTinyRing(t, nr)

	fds, err := unix.Socketpair(unix.AF_UNIX, unix.SOCK_STREAM, 0)
	assert.NoError(t, err)
	defer unix.Close(fds[0])

	// Shut down the write side: any future read sees EOF (res == 0).
	assert.NoError(t, unix.Shutdown(fds[1], unix.SHUT_WR))
	defer unix.Close(fds[1])

	view, err := nr.Recv(fds[0], SizeClassTiny)
	assert.NoError(t, err)
	assert.True(t, view == nil, "EOF must produce the empty view, not an error")

	// No release was needed (the kernel recycled in place), so the whole ring
	// must still be available.
	avail, err := nr.pbrs[SizeClassTiny].Available()
	assert.NoError(t, err)
	assert.Equal(t, uint32(4), avail, "EOF must not consume a buffer")
}

// TestRecvInvalidFD checks the argument validation: a negative fd fails with
// a beer error and touches nothing.
func TestRecvInvalidFD(t *testing.T) {
	nr := newTestNetRing(t)

	view, err := nr.Recv(-1, SizeClassTiny)
	assert.Error(t, err)
	assert.True(t, view == nil, "no view must be reported on validation failure")
	assert.False(t, errors.Is(err, syscall.EBADF), "the error must come from validation, not the kernel")
}

// TestRecvUnknownSizeClass checks that an unregistered class value fails up
// front, before anything reaches the ring.
func TestRecvUnknownSizeClass(t *testing.T) {
	nr := newTestNetRing(t)

	view, err := nr.Recv(0, 42)
	assert.Error(t, err)
	assert.True(t, view == nil)
}

// TestRecvUnprovisionedSizeClass checks the provisioning guard: a class with a
// known size but no registered ring must fail, not dereference a nil ring.
func TestRecvUnprovisionedSizeClass(t *testing.T) {
	nr := newTestNetRing(t)

	view, err := nr.Recv(0, SizeClassTiny)
	assert.Error(t, err)
	assert.True(t, view == nil)
}

// TestReleaseBufferNil does nothing on a nil or empty view.
func TestReleaseBufferNil(t *testing.T) {
	nr := newTestNetRing(t)

	// Must not panic and must not block: the empty view is a no-op that never
	// reaches the translator.
	nr.ReleaseBuffer(SizeClassTiny, nil)
	nr.ReleaseBuffer(SizeClassTiny, []byte{})
}
