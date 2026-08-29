package iouring

import (
	"testing"
	"unsafe"

	"github.com/alecthomas/assert/v2"
	"golang.org/x/sys/unix"
)

// TestExpectSend drives a real SEND through the ring: a socketpair carries the payload, and
// the CQE must confirm every byte before the peer reads the exact payload back.
func TestExpectSend(t *testing.T) {
	ring, err := New(256, testLogger(t))
	if err != nil {
		t.Skipf("io_uring not available here: %v", err)
	}
	defer func() { assert.NoError(t, ring.Close()) }()

	fds, err := unix.Socketpair(unix.AF_UNIX, unix.SOCK_STREAM, 0)
	assert.NoError(t, err)
	defer func() { assert.NoError(t, unix.Close(fds[0])) }()
	defer func() { assert.NoError(t, unix.Close(fds[1])) }()

	const ud = uint64(77)
	payload := []byte("hello ExpectSend")

	// The payload stays alive until after the CQE is reaped, so the raw pointer handed to
	// the kernel remains valid for the whole lifetime of the operation.
	assert.NoError(t, ring.ExpectSend(int32(fds[0]), unsafe.Pointer(&payload[0]), uint32(len(payload)), ud))

	cqe, ok := waitCQE(t, ring)
	assert.True(t, ok, "no CQE for the SEND within the wait window")
	assert.Equal(t, ud, cqe.UserData, "CQE must be addressed by the submitted slotIdx")
	assert.Equal(t, len(payload), int(cqe.Res), "Res must carry the accepted byte count")

	// The exact payload reaching the peer proves the raw pointer really made it to the kernel.
	buf := make([]byte, len(payload))
	n, err := unix.Read(fds[1], buf)
	assert.NoError(t, err)
	assert.Equal(t, len(payload), n)
	assert.Equal(t, string(payload), string(buf))
}
