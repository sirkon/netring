package iouring

import (
	"unsafe"

	"golang.org/x/sys/unix"
)

const OpAccept = 13 // Linux kernel constant for the Accept operation

// Set up persistent structures somewhere so they aren't allocated on the heap each time
var (
	dummyAddr unix.RawSockaddrAny
	dummyLen  uint32 = uint32(unix.SizeofSockaddrAny)
)

func (r *IOUring) ExpectAccept(listenFD int32, userData uint64) error {
	var sqe SQEntry
	sqe.Opcode = OpAccept
	sqe.FD = listenFD

	// Pass honest pointers to the kernel
	sqe.Addr = uint64(uintptr(unsafe.Pointer(&dummyAddr)))

	// In io_uring for ACCEPT the sockaddr length is passed in the Off field as a pointer!
	// (Yes, the C union maps this field as a pointer to socklen_t)
	sqe.Off = uint64(uintptr(unsafe.Pointer(&dummyLen)))

	sqe.Len = 0 // The Len field for accept is not used on modern kernels
	sqe.OpFlags = unix.SOCK_CLOEXEC
	sqe.UserData = userData

	return r.Push(sqe)
}
