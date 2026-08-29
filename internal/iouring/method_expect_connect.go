package iouring

import (
	"unsafe"
)

// ExpectConnect submits an asynchronous CONNECT for fd against the raw
// sockaddr (network byte order) pointed to by sockaddr and sized socklen.
//
// The kernel contract (io_connect_prep, io_uring/net.c) puts the sockaddr
// pointer in SQE.Addr and the sockaddr length in SQE.Off (the addr2 field of
// the C union), while Len, OpFlags (rw_flags), BufIndex and SpliceFdIn must
// stay zero.
//
// The sockaddr memory belongs to the caller and must stay alive, unmodified,
// until the CQE arrives; this layer takes no ownership and makes no copies.
//
// The completing CQE carries Res == 0 on a successful handshake, or a
// negative -errno (e.g. -ECONNREFUSED); it is addressed by the submitted
// slotIdx.
func (r *IOUring) ExpectConnect(fd int, sockaddr unsafe.Pointer, socklen uint32, userData uint64) error {
	var sqe SQEntry
	sqe.Opcode = OpConnect
	sqe.FD = int32(fd)
	sqe.Addr = uint64(uintptr(sockaddr))
	sqe.Off = uint64(socklen)
	sqe.UserData = userData

	return r.Push(sqe)
}
