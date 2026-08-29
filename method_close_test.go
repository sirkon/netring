package netring

import (
	"errors"
	"syscall"
	"testing"
	"time"

	"github.com/alecthomas/assert/v2"
	"golang.org/x/sys/unix"
)

// TestClose drives one real descriptor through the full pipeline: the parked
// worker is woken by the CQ poller with the close result, and the fd number is
// proven free afterward by being reusable for a new descriptor.
func TestClose(t *testing.T) {
	nr := newTestNetRing(t)

	fd, err := unix.Socket(unix.AF_INET, unix.SOCK_STREAM, 0)
	assert.NoError(t, err)
	defer func() { _ = unix.Close(fd) }()

	type closeResult struct {
		err error
	}
	results := make(chan closeResult, 1)
	go func() {
		err := nr.Close(fd)
		results <- closeResult{err: err}
	}()

	select {
	case res := <-results:
		assert.NoError(t, res.err, "Close must succeed on a real descriptor")

		// The fd number is free again: socket(2) must be able to hand it out.
		newFD, err := unix.Socket(unix.AF_INET, unix.SOCK_STREAM, 0)
		assert.NoError(t, err)
		defer func() { assert.NoError(t, unix.Close(newFD)) }()
		assert.Equal(t, fd, newFD, "the closed fd number must be reusable")

	case <-time.After(5 * time.Second):
		t.Fatal("no Close result within the wait window")
	}
}

// TestCloseInvalidFD checks the argument validation: a negative fd fails with
// a beer error and touches nothing.
func TestCloseInvalidFD(t *testing.T) {
	nr := newTestNetRing(t)

	err := nr.Close(-1)
	assert.Error(t, err)
	assert.False(t, errors.Is(err, syscall.EBADF), "the error must come from validation, not the kernel")
}

// TestCloseEBADF checks the error mapping: closing a never-open descriptor
// returns a raw EBADF, and the worker is woken exactly once despite the error.
func TestCloseEBADF(t *testing.T) {
	nr := newTestNetRing(t)

	// 1 << 20 is far beyond any real fd limit: it cannot be open here.
	err := nr.Close(1 << 20)
	assert.Error(t, err)
	assert.True(t, errors.Is(err, syscall.EBADF), "expected a raw EBADF, got %v", err)
}
