package iouring

import (
	"syscall"

	"golang.org/x/sys/unix"
)

// WaitEvents blocks until the kernel sends a new completed event into CQ.
func (r *IOUring) WaitEvents() error {
	_, _, errno := syscall.Syscall6(
		unix.SYS_IO_URING_ENTER,
		uintptr(r.FD),
		0,                     // 2. to_submit = 0
		1,                     // 3. min_complete = 1 (one CQE is enough).
		ioUringEnterGetEvents, // 4. IORING_ENTER_GETEVENTS
		0,                     // 5. arg_p = nil
		0,                     // 6. argsz = 0
	)
	if errno != 0 {
		return errno
	}

	return nil
}
