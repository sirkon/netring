package iouring

import (
	"testing"

	"github.com/alecthomas/assert/v2"
	"golang.org/x/sys/unix"
)

// TestExpectClose drives a real CLOSE through the ring: one end of a socketpair is closed
// by the kernel, and the other end must then hit EOF on read, proving the fd is really gone.
func TestExpectClose(t *testing.T) {
	ring, err := New(256, testLogger(t))
	if err != nil {
		t.Skipf("io_uring not available here: %v", err)
	}
	defer func() { assert.NoError(t, ring.Close()) }()

	fds, err := unix.Socketpair(unix.AF_UNIX, unix.SOCK_STREAM, 0)
	assert.NoError(t, err)

	// fds[0] is closed by the ring below, only fds[1] stays ours to clean up.
	defer func() { assert.NoError(t, unix.Close(fds[1])) }()

	assert.NoError(t, unix.SetNonblock(fds[1], true), "non-blocking fds[1] so the EOF read can never hang")

	assert.NoError(t, ring.ExpectClose(int32(fds[0]), 42))

	cqe, ok := waitCQE(t, ring)
	assert.True(t, ok, "no CQE for the CLOSE within the wait window")
	assert.Equal(t, uint64(42), cqe.UserData, "CQE must be addressed by the submitted slotIdx")
	assert.Equal(t, int32(0), cqe.Res, "CLOSE must complete with Res == 0")

	// Prove the fd is really gone: the peer now reads EOF (0 bytes, no error).
	buf := make([]byte, 16)
	n, err := unix.Read(fds[1], buf)
	assert.NoError(t, err)
	assert.Equal(t, 0, n, "peer must see EOF once the ring has closed fds[0]")
}
