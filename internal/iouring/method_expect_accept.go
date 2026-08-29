package iouring

import (
	"unsafe"

	"golang.org/x/sys/unix"
)

// Set up persistent structures somewhere so they aren't allocated on the heap each time
var (
	dummyAddr unix.RawSockaddrAny
	dummyLen  uint32 = uint32(unix.SizeofSockaddrAny)
)

// ExpectAccept submits an ACCEPT for listenFD. The completing CQE carries the accepted fd
// in Res (or a negative -errno on failure), addressed by the submitted slotIdx.
func (r *IOUring) ExpectAccept(listenFD int32, slotIdx uint64) error {
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
	sqe.UserData = slotIdx

	return r.Push(sqe)
}
