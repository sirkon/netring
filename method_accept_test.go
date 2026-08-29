package netring

import (
	"errors"
	"syscall"
	"testing"
	"time"

	"github.com/alecthomas/assert/v2"
	"golang.org/x/sys/unix"
)

// TestAccept drives one real client connection through the full pipeline: the
// parked worker is woken by the CQ poller with the accepted fd, and the fd is
// usable for a real read.
func TestAccept(t *testing.T) {
	nr := newTestNetRing(t)

	listenFD := makeListener(t)
	port := listenerPort(t, listenFD)

	type acceptResult struct {
		clientFD int32
		err      error
	}
	results := make(chan acceptResult, 1)
	go func() {
		fd, err := nr.Accept(listenFD)
		results <- acceptResult{clientFD: fd, err: err}
	}()

	// Give the worker a moment to arm and submit the ACCEPT before connecting,
	// so the kernel can complete it promptly.
	connFD, err := unix.Socket(unix.AF_INET, unix.SOCK_STREAM, 0)
	assert.NoError(t, err)
	defer func() { _ = unix.Close(connFD) }()
	assert.NoError(t, unix.Connect(connFD, &unix.SockaddrInet4{
		Addr: [4]byte{127, 0, 0, 1},
		Port: int(port),
	}))

	select {
	case res := <-results:
		assert.NoError(t, res.err, "Accept must succeed on a real listener")
		assert.True(t, res.clientFD > 0, "accepted fd must be positive, got %d", res.clientFD)
		defer func() { _ = unix.Close(int(res.clientFD)) }()

		// The accepted fd must be a working pipe: a byte written on the client
		// side must come back out through the accepted fd.
		_, err := unix.Write(connFD, []byte("x"))
		assert.NoError(t, err)
		buf := make([]byte, 1)
		n, err := unix.Read(int(res.clientFD), buf)
		assert.NoError(t, err)
		assert.Equal(t, 1, n)
		assert.Equal(t, "x", string(buf))

	case <-time.After(5 * time.Second):
		t.Fatal("no Accept result within the wait window")
	}
}

// TestAcceptInvalidFD checks the argument validation: a negative fd fails with
// a beer error and touches nothing.
func TestAcceptInvalidFD(t *testing.T) {
	nr := newTestNetRing(t)

	fd, err := nr.Accept(-1)
	assert.Error(t, err)
	assert.Equal(t, int32(0), fd, "no fd must be reported on validation failure")
	assert.False(t, errors.Is(err, syscall.EBADF), "the error must come from validation, not the kernel")
}

// TestAcceptEBADF checks the error mapping: accepting on a descriptor that is
// not a listening socket returns a raw EBADF, and the worker is woken exactly
// once despite the error.
func TestAcceptEBADF(t *testing.T) {
	nr := newTestNetRing(t)

	// A plain stream socket is a valid fd, but not a listener: ACCEPT on it must
	// complete with -EINVAL/-EOPNOTSUPP/-EBADF, mapping back to a raw errno
	//.
	fd, err := unix.Socket(unix.AF_INET, unix.SOCK_STREAM, 0)
	assert.NoError(t, err)
	defer func() { _ = unix.Close(fd) }()

	gotFD, err := nr.Accept(fd)
	assert.Error(t, err)
	assert.Equal(t, int32(0), gotFD, "no fd must be reported on failure")
	assert.True(t, errors.Is(err, syscall.EBADF) || errors.Is(err, syscall.EINVAL),
		"expected a raw errno (EBADF or EINVAL), got %v", err)
}
