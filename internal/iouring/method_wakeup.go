package iouring

import (
	"sync/atomic"
	"syscall"

	"golang.org/x/sys/unix"
)

func (r *IOUring) Wakeup() error {
	// Делаем syscall.Syscall6 (так как параметров у io_uring_enter много)
	// Сигнатура: io_uring_enter(fd, to_submit, min_complete, flags, sig, sz)
	_, _, errno := syscall.Syscall6(
		unix.SYS_IO_URING_ENTER,
		uintptr(r.FD), // 1. FD твоего ринга
		0,             // 2. to_submit = 0 (ядро само заберет из SQ, указывать количество не нужно)
		0,             // 3. min_complete = 0 (мы не ждем завершения задач, только будим)
		enterSQWakeup, // 4. Флаг пробуждения
		0,             // 5. arg_p / sigmask = nil
		0,             // 6. argsz = 0
	)
	if errno != 0 {
		return errno
	}

	return nil
}

func (r *IOUring) NeedWakeup() bool {
	return atomic.LoadUint32(r.SQFlags)&sqNeedWakup != 0
}
