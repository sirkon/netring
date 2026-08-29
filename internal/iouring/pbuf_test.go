package iouring

import (
	"runtime"
	"testing"
	"time"

	"github.com/alecthomas/assert/v2"
	"github.com/sirkon/blog"
	"golang.org/x/sys/unix"
)

func testLogger(t *testing.T) *blog.Logger {
	t.Helper()
	logger, err := blog.NewLogger(blog.NewSyncWriter(discardWriter{}))
	if err != nil {
		t.Fatal(err)
	}
	return logger
}

type discardWriter struct{}

func (discardWriter) Write(p []byte) (int, error) { return len(p), nil }
func (discardWriter) Sync() error                 { return nil }

// TestRegisterBufferRing_Guards checks the user-space validation before any syscall.
func TestRegisterBufferRing_Guards(t *testing.T) {
	ring, err := New(256, testLogger(t))
	if err != nil {
		t.Skipf("io_uring not available here: %v", err)
	}
	defer func() { assert.NoError(t, ring.Close()) }()

	t.Run("capacity must be a power of 2", func(t *testing.T) {
		_, err := ring.RegisterBufferRing(1, 300, 128)
		assert.Error(t, err)
	})

	t.Run("capacity must not exceed 32768", func(t *testing.T) {
		_, err := ring.RegisterBufferRing(1, 65536, 128)
		assert.Error(t, err)
	})

	t.Run("buffer size must be a multiple of 64", func(t *testing.T) {
		_, err := ring.RegisterBufferRing(1, 4, 100)
		assert.Error(t, err)
	})

	t.Run("buffer size must be greater than 0", func(t *testing.T) {
		_, err := ring.RegisterBufferRing(1, 4, 0)
		assert.Error(t, err)
	})
}

// TestRegisterBufferRing_RegisterUnregister checks the registration lifecycle against
// the kernel: register, duplicate bgid must fail, head query, unregister, and unregister
// of a missing group must fail.
func TestRegisterBufferRing_RegisterUnregister(t *testing.T) {
	ring, err := New(256, testLogger(t))
	if err != nil {
		t.Skipf("io_uring not available here: %v", err)
	}
	defer func() { assert.NoError(t, ring.Close()) }()

	pbr, err := ring.RegisterBufferRing(5, 8, 128)
	if err != nil {
		t.Skipf("pbuf rings not supported by this kernel: %v", err)
	}

	avail, err := pbr.Available()
	assert.NoError(t, err)
	assert.Equal(t, uint32(8), avail, "all buffers must be visible to the kernel after registration")

	_, err = ring.RegisterBufferRing(5, 8, 128)
	assert.Error(t, err, "duplicate bgid must be rejected by the kernel")

	assert.NoError(t, pbr.Unregister())
	assert.NoError(t, pbr.Unregister(), "double unregister must be a no-op")

	_, err = pbr.Available()
	assert.Error(t, err, "status of an unregistered group must fail")
}

// TestProvidedBufferRing_RecvWithProvidedBuffers drives the whole point of this machinery:
// a RECV with IOSQE_BUFFER_SELECT gets a kernel-picked buffer from the ring, the CQE flags
// carry the bid, and the buffer is recycled back into the ring.
func TestProvidedBufferRing_RecvWithProvidedBuffers(t *testing.T) {
	ring, err := New(256, testLogger(t))
	if err != nil {
		t.Skipf("io_uring not available here: %v", err)
	}
	defer func() { assert.NoError(t, ring.Close()) }()

	const (
		bgid     = 7
		capacity = 4
		bufSize  = 128
	)
	pbr, err := ring.RegisterBufferRing(bgid, capacity, bufSize)
	if err != nil {
		t.Skipf("pbuf rings not supported by this kernel: %v", err)
	}
	defer func() { assert.NoError(t, pbr.Unregister()) }()

	fds, err := unix.Socketpair(unix.AF_UNIX, unix.SOCK_STREAM, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer unix.Close(fds[0])
	defer unix.Close(fds[1])

	payload := []byte("hello provided buffers")
	if _, err := unix.Write(fds[1], payload); err != nil {
		t.Fatal(err)
	}

	// The data is already in the socket buffer, so the kernel completes the request
	// immediately, picking a buffer from the ring at completion time.
	assert.NoError(t, ring.ExpectRecv(int32(fds[0]), bgid, bufSize, 0xdeadbeef))

	cqe, ok := waitCQE(t, ring)
	assert.True(t, ok, "the RECV must complete")
	assert.Equal(t, uint64(0xdeadbeef), cqe.UserData)
	assert.Equal(t, int32(len(payload)), cqe.Res, "the RECV must return the payload size")
	assert.True(t, cqe.Flags&CQEFBuffer != 0, "CQE must carry IORING_CQE_F_BUFFER")

	bid := uint16(cqe.Flags >> CQEBufferShift)
	buf := pbr.Buffer(bid)
	assert.Equal(t, payload, buf[:cqe.Res], "the kernel must have received into the provided buffer")

	// Hand the buffer back and check it becomes visible to the kernel again.
	pbr.ReleaseBuffer(bid)
	avail, err := pbr.Available()
	assert.NoError(t, err)
	assert.Equal(t, uint32(capacity), avail, "the released buffer must be back in the ring")
}

// TestProvidedBufferRing_ReleaseRecycles drives several receive cycles to prove buffers
// are actually reused by the kernel, not just re-counted in user space.
func TestProvidedBufferRing_ReleaseRecycles(t *testing.T) {
	ring, err := New(256, testLogger(t))
	if err != nil {
		t.Skipf("io_uring not available here: %v", err)
	}
	defer func() { assert.NoError(t, ring.Close()) }()

	const (
		bgid     = 9
		capacity = 2
		bufSize  = 64
	)
	pbr, err := ring.RegisterBufferRing(bgid, capacity, bufSize)
	if err != nil {
		t.Skipf("pbuf rings not supported by this kernel: %v", err)
	}
	defer func() { assert.NoError(t, pbr.Unregister()) }()

	fds, err := unix.Socketpair(unix.AF_UNIX, unix.SOCK_STREAM, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer unix.Close(fds[0])
	defer unix.Close(fds[1])

	// With only capacity buffers in flight at a time, far more messages than capacity
	// must go through: this only works if ReleaseBuffer really makes buffers reusable.
	const messages = 32
	usedBids := make(map[uint16]bool)
	for i := 0; i < messages; i++ {
		payload := []byte{byte(i), byte(i >> 8), byte(i >> 16)}
		if _, err := unix.Write(fds[1], payload); err != nil {
			t.Fatal(err)
		}

		assert.NoError(t, ring.ExpectRecv(int32(fds[0]), bgid, bufSize, uint64(i)))

		cqe, ok := waitCQE(t, ring)
		assert.True(t, ok)
		assert.Equal(t, uint64(i), cqe.UserData)
		assert.Equal(t, int32(len(payload)), cqe.Res)

		bid := uint16(cqe.Flags >> CQEBufferShift)
		assert.Equal(t, payload, pbr.Buffer(bid)[:cqe.Res])
		usedBids[bid] = true
		pbr.ReleaseBuffer(bid)
	}

	// Both buffers must have been handed out at least once over 32 messages.
	assert.Equal(t, capacity, len(usedBids), "the kernel must rotate over the recycled buffers")
}

// waitCQE polls the CQ for a while, since with SQPOLL the completion may take a few spins.
func waitCQE(t *testing.T, ring *IOUring) (CQEntry, bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if cqe, ok := ring.GetTask(); ok {
			return cqe, true
		}
		if ring.NeedWakeup() {
			if err := ring.Wakeup(); err != nil {
				t.Fatal(err)
			}
		}
		runtime.Gosched()
		time.Sleep(100 * time.Microsecond)
	}
	return CQEntry{}, false
}
