package iouring

import (
	"unsafe"
)

// ExpectSendAsync prepares a raw write of bufLen bytes from bufPtr. The payload memory must stay
// alive and unmodified until the CQE arrives; this layer takes no ownership and makes no
// copies. The kernel ORs MSG_NOSIGNAL in itself (net.c, io_sendmsg_prep), so there is no
// SIGPIPE suppression to set and OpFlags stays zero.
//
// CQE Res is the number of bytes accepted; a short send (0 <= Res < bufLen) must be retried
// with the remaining tail (that bookkeeping belongs to the caller, not this layer).
func (r *IOUring) ExpectSendAsync(fd int32, bufPtr unsafe.Pointer, bufLen uint32, slotIdx uint64) error {
	var sqe SQEntry
	sqe.Opcode = OpSend
	sqe.FD = fd
	sqe.Addr = uint64(uintptr(bufPtr))
	sqe.Len = bufLen
	sqe.UserData = slotIdx
	sqe.Flags = ioUringSQEAsync

	return r.Push(sqe)
}
