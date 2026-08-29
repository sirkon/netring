package netring

import (
	"sync/atomic"
	"testing"

	"github.com/sirkon/blog"
	"golang.org/x/sys/unix"
)

// discardWriter swallows all log output; tests run quiet unless they fail.
type discardWriter struct{}

func (discardWriter) Write(p []byte) (int, error) { return len(p), nil }
func (discardWriter) Sync() error                 { return nil }

// testLogger builds a logger that discards everything, so the SQPOLL io_uring
// setup (which may require elevated privileges) is the only thing that can make
// New fail in tests.
func testLogger(t *testing.T) *blog.Logger {
	t.Helper()
	logger, err := blog.NewLogger(blog.NewSyncWriter(discardWriter{}))
	if err != nil {
		t.Fatal(err)
	}
	return logger
}

// newTestNetRing creates a NetRing and attaches the CQ poller to it on a
// dedicated OS thread (Poll calls runtime.LockOSThread itself and closes finish
// on exit, AGENTS.md).
func newTestNetRing(t *testing.T) *NetRing {
	t.Helper()
	nr, err := New(256, testLogger(t))
	if err != nil {
		t.Skipf("io_uring not available here: %v", err)
	}

	var stop atomic.Bool
	finish := make(chan struct{})
	go nr.Poll(&stop, finish)
	t.Cleanup(func() {
		stop.Store(true)
		<-finish
	})
	return nr
}

// makeListener opens a loopback IPv4 listening socket that can drive Accept.
func makeListener(t *testing.T) int {
	t.Helper()
	fd, err := unix.Socket(unix.AF_INET, unix.SOCK_STREAM, 0)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = unix.Close(fd) })

	if err := unix.SetsockoptInt(fd, unix.SOL_SOCKET, unix.SO_REUSEADDR, 1); err != nil {
		t.Fatal(err)
	}
	if err := unix.Bind(fd, &unix.SockaddrInet4{Addr: [4]byte{127, 0, 0, 1}}); err != nil {
		t.Fatal(err)
	}
	if err := unix.Listen(fd, 16); err != nil {
		t.Fatal(err)
	}
	return fd
}

// listenerPort recovers the ephemeral port a bound-but-unconnected listener
// picked, so a test client can connect to it.
func listenerPort(t *testing.T, fd int) uint16 {
	t.Helper()
	addr, err := unix.Getsockname(fd)
	if err != nil {
		t.Fatal(err)
	}
	inet, ok := addr.(*unix.SockaddrInet4)
	if !ok {
		t.Fatalf("expected an IPv4 sockaddr back, got %T", addr)
	}
	if inet.Port == 0 {
		t.Fatal("ephemeral bind must have picked a port")
	}
	return uint16(inet.Port)
}
