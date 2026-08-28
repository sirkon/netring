package iouring

import (
	"sync/atomic"
	"syscall"

	"golang.org/x/sys/unix"
)

func (r *IOUring) Wakeup() error {
	// Doing syscall.Syscall6 (because io_uring_enter has many parameters)
	// Signature: io_uring_enter(fd, to_submit, min_complete, flags, sig, sz)
	_, _, errno := syscall.Syscall6(
		unix.SYS_IO_URING_ENTER,
		uintptr(r.FD),        // 1. FD of your ring
		0,                    // 2. to_submit = 0 (the kernel picks up from the SQ itself, no need to specify the count)
		0,                    // 3. min_complete = 0 (we don't wait for tasks to complete, only wake it)
		ioUringEnterSQWakeup, // 4. Wakeup flag
		0,                    // 5. arg_p / sigmask = nil
		0,                    // 6. argsz = 0
	)
	if errno != 0 {
		return errno
	}

	return nil
}

func (r *IOUring) NeedWakeup() bool {
	return atomic.LoadUint32(r.SQFlags)&ioUringSQNeedWakeup != 0
}
