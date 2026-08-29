package iouring

import (
	"testing"
	"unsafe"

	"github.com/alecthomas/assert/v2"
	"golang.org/x/sys/unix"
)

// TestExpectConnect drives a real CONNECT through the ring: a loopback
// listener is targeted from a client socket, and the CQE must confirm the
// handshake before the client side becomes usable for a real read/write.
func TestExpectConnect(t *testing.T) {
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

	clientFD, err := unix.Socket(unix.AF_INET, unix.SOCK_STREAM, 0)
	assert.NoError(t, err)
	defer func() { assert.NoError(t, unix.Close(clientFD)) }()

	// Marshalled exactly like NetRing.Connect does: the raw kernel sockaddr_in
	// layout in network byte order (the port is byte-swapped).
	var raw unix.RawSockaddrInet4
	raw.Family = unix.AF_INET
	raw.Port = uint16((inet.Port >> 8) | (inet.Port << 8))
	raw.Addr = inet.Addr

	assert.NoError(t, ring.ExpectConnect(clientFD, unsafe.Pointer(&raw), uint32(unsafe.Sizeof(raw)), 55))

	cqe, ok := waitCQE(t, ring)
	assert.True(t, ok, "no CQE for the CONNECT within the wait window")
	assert.Equal(t, uint64(55), cqe.UserData, "CQE must be addressed by the submitted slotIdx")
	assert.Zero(t, cqe.Res, "Res must be 0 for a successful CONNECT, got %d", cqe.Res)

	// The handshake is done: a byte written by the other side must come back
	// out through the connected client fd.
	peerFD, _, err := unix.Accept(listenFD)
	assert.NoError(t, err)
	defer func() { assert.NoError(t, unix.Close(peerFD)) }()

	_, err = unix.Write(peerFD, []byte("x"))
	assert.NoError(t, err)
	buf := make([]byte, 1)
	n, err := unix.Read(clientFD, buf)
	assert.NoError(t, err)
	assert.Equal(t, 1, n)
	assert.Equal(t, "x", string(buf))
}
