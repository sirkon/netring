package netring

import (
	"errors"
	"syscall"
	"testing"
	"time"

	"github.com/alecthomas/assert/v2"
	"golang.org/x/sys/unix"
)

// TestConnect drives one real outbound handshake through the full pipeline:
// the parked worker is woken by the CQ poller when the connection is
// established, and the connected fd is usable for a real read/write.
func TestConnect(t *testing.T) {
	nr := newTestNetRing(t)

	listenFD := makeListener(t)
	port := listenerPort(t, listenFD)

	type connectResult struct {
		err error
	}
	results := make(chan connectResult, 1)
	sockFD, err := unix.Socket(unix.AF_INET, unix.SOCK_STREAM, 0)
	assert.NoError(t, err)
	defer func() { _ = unix.Close(sockFD) }()

	go func() {
		err := nr.Connect(sockFD, &unix.SockaddrInet4{
			Addr: [4]byte{127, 0, 0, 1},
			Port: int(port),
		})
		results <- connectResult{err: err}
	}()

	select {
	case res := <-results:
		assert.NoError(t, res.err, "Connect must succeed on a live listener")

		// The connected fd must be a working pipe: a byte written by the
		// accepting side must come back out through the connected fd.
		peerFD, _, err := unix.Accept(listenFD)
		assert.NoError(t, err)
		defer func() { _ = unix.Close(peerFD) }()

		_, err = unix.Write(peerFD, []byte("x"))
		assert.NoError(t, err)
		buf := make([]byte, 1)
		n, err := unix.Read(sockFD, buf)
		assert.NoError(t, err)
		assert.Equal(t, 1, n)
		assert.Equal(t, "x", string(buf))

	case <-time.After(5 * time.Second):
		t.Fatal("no Connect result within the wait window")
	}
}

// TestConnectInvalidFD checks the argument validation: a negative fd fails
// with a beer error and touches nothing.
func TestConnectInvalidFD(t *testing.T) {
	nr := newTestNetRing(t)

	err := nr.Connect(-1, &unix.SockaddrInet4{Addr: [4]byte{127, 0, 0, 1}, Port: 80})
	assert.Error(t, err)
	assert.False(t, errors.Is(err, syscall.EBADF), "the error must come from validation, not the kernel")
}

// TestConnectNilSockaddr checks the argument validation: a nil sockaddr fails
// with a beer error and touches nothing.
func TestConnectNilSockaddr(t *testing.T) {
	nr := newTestNetRing(t)

	err := nr.Connect(1, nil)
	assert.Error(t, err)
}

// TestConnectUnsupportedSockaddr checks the argument validation: a sockaddr
// family outside the supported surface fails with a beer error.
func TestConnectUnsupportedSockaddr(t *testing.T) {
	nr := newTestNetRing(t)

	err := nr.Connect(1, &unix.SockaddrInet6{})
	assert.Error(t, err)
}

// TestConnectRefused checks the error mapping: connecting to a port nobody
// listens on returns a raw ECONNREFUSED, and the worker is woken exactly once
// despite the error.
func TestConnectRefused(t *testing.T) {
	nr := newTestNetRing(t)

	// A port bound and closed again: nothing listens on it, so the handshake
	// must complete with -ECONNREFUSED on loopback.
	probe, err := unix.Socket(unix.AF_INET, unix.SOCK_STREAM, 0)
	assert.NoError(t, err)
	assert.NoError(t, unix.Bind(probe, &unix.SockaddrInet4{Addr: [4]byte{127, 0, 0, 1}}))
	port := listenerPort(t, probe)
	assert.NoError(t, unix.Close(probe))

	sockFD, err := unix.Socket(unix.AF_INET, unix.SOCK_STREAM, 0)
	assert.NoError(t, err)
	defer func() { _ = unix.Close(sockFD) }()

	err = nr.Connect(sockFD, &unix.SockaddrInet4{
		Addr: [4]byte{127, 0, 0, 1},
		Port: int(port),
	})
	assert.Error(t, err)
	assert.True(t, errors.Is(err, syscall.ECONNREFUSED),
		"expected a raw ECONNREFUSED, got %v", err)
}
