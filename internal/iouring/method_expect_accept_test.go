package iouring

import (
	"testing"

	"github.com/alecthomas/assert/v2"
	"golang.org/x/sys/unix"
)

// TestExpectAccept drives a real ACCEPT through the ring: a loopback listener is fed
// one connection from the same process, and the CQE must come back with the accepted fd.
func TestExpectAccept(t *testing.T) {
	ring, err := New(256, testLogger(t))
	if err != nil {
		t.Skipf("io_uring not available here: %v", err)
	}
	defer func() { assert.NoError(t, ring.Close()) }()

	listenFD, err := unix.Socket(unix.AF_INET, unix.SOCK_STREAM, 0)
	assert.NoError(t, err)
	defer func() { assert.NoError(t, unix.Close(listenFD)) }()

	assert.NoError(t, unix.SetsockoptInt(listenFD, unix.SOL_SOCKET, unix.SO_REUSEADDR, 1))
	assert.NoError(t, unix.Bind(listenFD, &unix.SockaddrInet4{Addr: [4]byte{127, 0, 0, 1}}))

	addr, err := unix.Getsockname(listenFD)
	assert.NoError(t, err)
	inet, ok := addr.(*unix.SockaddrInet4)
	assert.True(t, ok, "expected an IPv4 sockaddr back")
	assert.NotZero(t, inet.Port, "ephemeral bind must have picked a port")

	assert.NoError(t, unix.Listen(listenFD, 16))

	assert.NoError(t, ring.ExpectAccept(int32(listenFD), 42))

	connFD, err := unix.Socket(unix.AF_INET, unix.SOCK_STREAM, 0)
	assert.NoError(t, err)
	defer func() { assert.NoError(t, unix.Close(connFD)) }()
	assert.NoError(t, unix.Connect(connFD, &unix.SockaddrInet4{
		Addr: [4]byte{127, 0, 0, 1},
		Port: inet.Port,
	}))

	cqe, ok := waitCQE(t, ring)
	assert.True(t, ok, "no CQE for the ACCEPT within the wait window")
	assert.Equal(t, uint64(42), cqe.UserData, "CQE must be addressed by the submitted slotIdx")
	assert.True(t, cqe.Res > 0, "Res must carry the accepted fd, got %d", cqe.Res)

	// The accepted fd is closed exactly once, right here, and never via defer.
	assert.NoError(t, unix.Close(int(cqe.Res)))
}
