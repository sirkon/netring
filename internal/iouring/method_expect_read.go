package iouring

// ExpectRead prepares a described read, the IORING_OP_READ analogue of
// ExpectRecv: no destination buffer is given, instead the kernel picks one at
// completion time from the provided buffer ring registered under bgid (see
// RegisterBufferRing). maxLen caps how many bytes may land in the picked
// buffer; the kernel clamps it to the buffer size, and 0 means "whole buffer"
// (rw.c, io_ring_buffer_select).
//
// Read is byte-oriented like read(2): it does not consume a message boundary
// when the socket is in datagram mode. On a stream socket it behaves exactly
// like ExpectRecv.
//
// The completed CQE carries the picked buffer id in Flags>>16 with
// IORING_CQE_F_BUFFER set; hand the buffer back with
// ProvidedBufferRing.ReleaseBuffer once consumed. A CQE Res of -ENOBUFS means
// the ring was empty when data arrived; resubmit after a ReleaseBuffer.
func (r *IOUring) ExpectRead(fd int32, bgid uint16, maxLen uint32, slotIdx uint64) error {
	var sqe SQEntry
	sqe.Opcode = OpRead
	sqe.FD = fd
	sqe.Flags = ioUringSQEBufferSelect // IOSQE_BUFFER_SELECT: the kernel picks from bgid
	sqe.BufIndex = bgid                // the C union field is buf_group here
	sqe.Len = maxLen
	sqe.UserData = slotIdx

	return r.Push(sqe)
}