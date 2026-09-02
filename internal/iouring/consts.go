package iouring

import (
	"math"
)

const (
	ioUringSetupSQPoll    = 1 << 1
	ioUringSetupSQAff     = 1 << 2 // Bind to a specific CPU core
	ioUringFeatSingleMMap = 1 << 0 // Shared memory for SQ and CQ (kernel 5.4+)

	ioUringEnterSQWakeup  = 1 << 1
	ioUringSQNeedWakeup   = 1 << 0
	ioUringEnterGetEvents = 1 << 0

	// IORING_REGISTER_* opcodes for the SYS_IO_URING_REGISTER syscall (the io_uring_register(2)
	// subsystem opcodes, not to be confused with the SQE opcodes below).
	ioUringRegisterPbufRing   = 22 // IORING_REGISTER_PBUF_RING, kernel 5.19+
	ioUringUnregisterPbufRing = 23 // IORING_UNREGISTER_PBUF_RING
	ioUringRegisterPbufStatus = 26 // IORING_REGISTER_PBUF_STATUS

	ioUringSQEBufferSelect = 1 << 5 // IOSQE_BUFFER_SELECT: the kernel picks a buffer from buf_group
	ioUringSQEAsync        = 1 << 4
	ioUringRecvSendBundle  = 1 << 3

	ioUringPbufRingInc = 1 << 0

	// CQEFBuffer and CQEBufferShift are exported: the high-level Recv method
	// (netring package) must interpret CQE flags to recover the selected
	// buffer id.
	CQEFBuffer     = 1 << 0 // IORING_CQE_F_BUFFER: the CQE flags carry the selected buffer ID
	CQEFBufMore    = 1 << 4 // IORING_CQE_F_BUF_MORE: incremental buffer rings only, unused here
	CQEBufferShift = 16     // flags >> 16 is the selected buffer ID

	sysOffSQes   = 0x10000000
	sysOffSQRing = 0
	sysOffCQRing = 0x8000000
)

// IORING_OP_* opcodes for the SQE.Opcode field, hand-copied from
// include/uapi/linux/io_uring.h enum io_uring_op. The values follow the enum declaration
// order, not anything you'd guess from the names: recount them whenever the ABI changes.
const (
	OpAccept  = 13 // IORING_OP_ACCEPT
	OpConnect = 16 // IORING_OP_CONNECT (13 ACCEPT, 14 ASYNC_CANCEL, 15 LINK_TIMEOUT)
	OpClose   = 19 // IORING_OP_CLOSE
	OpRead    = 22 // IORING_OP_READ (NOT RECV; the task doc mixed exactly these two up)
	OpSend    = 26 // IORING_OP_SEND
	OpRecv    = 27 // IORING_OP_RECV
)

const (
	periodicalTimerTaskID = math.MaxUint64 - math.MaxUint32
)
