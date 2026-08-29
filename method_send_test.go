package netring

import (
	"errors"
	"syscall"
	"testing"
	"time"

	"github.com/alecthomas/assert/v2"
	"golang.org/x/sys/unix"
)

// TestSend drives one real payload through the full pipeline: the parked
// worker is woken by the CQ poller with the accepted byte count, and the
// receiver reads exactly the sent data back.
func TestSend(t *testing.T) {
	nr := newTestNetRing(t)

	fds, err := unix.Socketpair(unix.AF_UNIX, unix.SOCK_STREAM, 0)
	assert.NoError(t, err)
	defer unix.Close(fds[0])
	defer unix.Close(fds[1])

	payload := []byte("hello send")

	type sendResult struct {
		n   int
		err error
	}
	results := make(chan sendResult, 1)
	go func() {
		n, err := nr.Send(fds[0], payload)
		results <- sendResult{n: n, err: err}
	}()

	select {
	case res := <-results:
		assert.NoError(t, res.err, "Send must succeed on a real socketpair")
		assert.Equal(t, len(payload), res.n, "the kernel must accept the whole payload")

		buf := make([]byte, len(payload))
		_, err := unix.Read(fds[1], buf)
		assert.NoError(t, err)
		assert.Equal(t, payload, buf, "the peer must receive exactly the sent payload")

	case <-time.After(5 * time.Second):
		t.Fatal("no Send result within the wait window")
	}
}

// TestSendRoundtrip sends several messages over the same socketpair and
// proves each arrives in order: sends are per-fd FIFO in the SQ.
func TestSendRoundtrip(t *testing.T) {
	nr := newTestNetRing(t)

	fds, err := unix.Socketpair(unix.AF_UNIX, unix.SOCK_STREAM, 0)
	assert.NoError(t, err)
	defer unix.Close(fds[0])
	defer unix.Close(fds[1])

	const messages = 3
	for i := 0; i < messages; i++ {
		payload := []byte{byte(i), byte(i << 4)}
		n, err := nr.Send(fds[0], payload)
		assert.NoError(t, err)
		assert.Equal(t, len(payload), n)

		buf := make([]byte, len(payload))
		_, err = unix.Read(fds[1], buf)
		assert.NoError(t, err)
		assert.Equal(t, payload, buf)
	}
}

// TestSendEmpty checks the zero-length fast path: Send returns 0, nil
// immediately, touching neither the cell nor the channel (033 section 2
// step 2).
func TestSendEmpty(t *testing.T) {
	nr := newTestNetRing(t)

	n, err := nr.Send(0, nil)
	assert.NoError(t, err)
	assert.Equal(t, 0, n)

	n, err = nr.Send(0, []byte{})
	assert.NoError(t, err)
	assert.Equal(t, 0, n)
}

// TestSendInvalidFD checks the argument validation: a negative fd fails with
// a beer error and touches nothing.
func TestSendInvalidFD(t *testing.T) {
	nr := newTestNetRing(t)

	n, err := nr.Send(-1, []byte("x"))
	assert.Error(t, err)
	assert.Equal(t, 0, n, "no byte count must be reported on validation failure")
	assert.False(t, errors.Is(err, syscall.EBADF), "the error must come from validation, not the kernel")
}

// TestSendEBADF checks the error mapping: writing to a closed socketpair end
// returns a raw EPIPE (or ECONNRESET/EAGAIN), and the worker is woken exactly
// once despite the error.
func TestSendEBADF(t *testing.T) {
	nr := newTestNetRing(t)

	fds, err := unix.Socketpair(unix.AF_UNIX, unix.SOCK_STREAM, 0)
	assert.NoError(t, err)
	defer unix.Close(fds[0])

	// Close the read end first: SEND on the peer must eventually fail. The
	// kernel ORs MSG_NOSIGNAL itself (ExpectSend contract), so no SIGPIPE
	// kills the test process.
	assert.NoError(t, unix.Close(fds[1]))

	n, err := nr.Send(fds[0], []byte("x"))
	assert.Error(t, err)
	assert.Equal(t, 0, n, "no byte count must be reported on failure")
	assert.True(t, errors.Is(err, syscall.EPIPE) || errors.Is(err, syscall.ECONNRESET) ||
		errors.Is(err, syscall.EAGAIN),
		"expected a raw errno (EPIPE, ECONNRESET or EAGAIN), got %v", err)
}
