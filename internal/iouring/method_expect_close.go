package iouring

// ExpectClose prepares an asynchronous close of fd. io_close_prep (openclose.c) rejects any
// SQE with off/addr/len/rw_flags/buf_index set, so everything except FD and UserData stays
// zero; the zero-value SQEntry below is exactly what the kernel wants.
//
// Res == 0 on success; the descriptor is destroyed once the CQE arrives, so never close the
// same fd again afterwards (double close would hit an unrelated fd after the number gets
// reused).
func (r *IOUring) ExpectClose(fd int32, slotIdx uint64) error {
	var sqe SQEntry
	sqe.Opcode = OpClose
	sqe.FD = fd
	sqe.UserData = slotIdx

	return r.Push(sqe)
}
