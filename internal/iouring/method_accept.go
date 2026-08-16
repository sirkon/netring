package iouring

import (
	"unsafe"

	"golang.org/x/sys/unix"
)

const OpAccept = 13 // Константа ядра Linux для операции Accept

// Заведи где-нибудь постоянные структуры, чтобы они не аллоцировались в куче каждый раз
var (
	dummyAddr unix.RawSockaddrAny
	dummyLen  uint32 = uint32(unix.SizeofSockaddrAny)
)

func (r *IOUring) ExpectAccept(listenFD int32, userData uint64) error {
	var sqe SQEntry
	sqe.Opcode = OpAccept
	sqe.FD = listenFD

	// Передаем честные указатели в ядро
	sqe.Addr = uint64(uintptr(unsafe.Pointer(&dummyAddr)))

	// В io_uring для ACCEPT длина sockaddr передается в поле Off в виде указателя!
	// (Да-да, Си-шный union мапит это поле как указатель на socklen_t)
	sqe.Off = uint64(uintptr(unsafe.Pointer(&dummyLen)))

	sqe.Len = 0 // Поле Len для accept в современных ядрах не используется
	sqe.OpFlags = unix.SOCK_CLOEXEC
	sqe.UserData = userData

	return r.Push(sqe)
}
