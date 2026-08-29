package netring

import (
	"runtime"
	"unsafe"

	"github.com/sirkon/blog/beer"
	"golang.org/x/sys/unix"
)

// Connect initiates an asynchronous outbound handshake through the kernel and
// blocks the calling goroutine until io_uring reports the connection is
// established or failed. On success the fd is connected and may be used for
// Recv/Send/Close on this same NetRing.
func (nr *NetRing) Connect(fd int, sa unix.Sockaddr) error {
	// 1. Validate fd and sockaddr before anything is touched: the modulo used
	// for shard routing must never see a negative index, and a nil sockaddr
	// cannot be marshalled.
	if fd < 0 {
		return beer.Newf("connect: invalid descriptor %d, expected a non-negative one", fd)
	}
	if sa == nil {
		return beer.New("connect: sockaddr must be provided")
	}

	// 2. Marshal the high-level sockaddr into the raw kernel layout with
	// Network Byte Order (Big-Endian) alignment: the port is byte-swapped.
	var rawAddr unsafe.Pointer
	var sockLen uint32

	switch v := sa.(type) {
	case *unix.SockaddrInet4:
		var raw unix.RawSockaddrInet4
		raw.Family = unix.AF_INET
		raw.Port = uint16((v.Port >> 8) | (v.Port << 8)) // Big-Endian byte swap
		raw.Addr = v.Addr
		rawAddr = unsafe.Pointer(&raw)
		sockLen = uint32(unsafe.Sizeof(raw))
	default:
		return beer.Newf("connect: unsupported sockaddr type %T", sa)
	}

	// 3. Arm the cell: the arm protocol must precede the channel send with no
	// exceptions, otherwise the poller could race a half-armed cell. taskCell
	// zeroes res/flags and records g = getg().
	cell := nr.taskCell()

	// 4. Build the POD. The raw sockaddr travels as a GC-rooted typed
	// unsafe.Pointer (Payload), never as uint64/uintptr, so the GC keeps
	// scanning it for the flight; sockLen rides in Addr (ExpectConnect reads
	// it from there) and Later SQE.Off is filled by the translator.
	task := ringTask{
		Opcode:  opcodeTypeConnect,
		FD:      int32(fd),
		Addr:    uint64(sockLen),
		Len:     0,
		BGID:    0,
		Offset:  0,
		Ctx:     cell,
		Payload: rawAddr,
	}

	// 5. Submit into the per-fd shard: it keeps all operations of one fd FIFO
	// in the SQ, so a connect lands strictly after any in-flight op it must
	// follow. The channel send may block on backpressure; nothing can complete
	// before the task reaches the translator, so that is fine.
	nr.chans[fd&(len(nr.chans)-1)] <- task

	// 6. Suspend until the CQ poller delivers the result. Both the slept and
	// the never-slept paths return here with the results already in the cell.
	gopark(
		netringParkUnlock,
		unsafe.Pointer(cell),
		waitReasonIOWait,
		traceBlockGeneric,
		parkTraceSkip,
	)

	// 7. Keep the sockaddr reference alive until after the wake-up path: the
	// GC cannot collect the marshalled sockaddr while the kernel may still be
	// reading it.
	runtime.KeepAlive(sa)

	// 8. Wakeup path (slept or never slept, identical): read the result.
	res := cell.res

	// 9. Return the cell to the pool. No scrubbing: the next arm protocol
	// overwrites every field.
	nr.pool.Put(cell)

	// 10. res < 0: the handshake failed, mapped back to a raw errno (e.g.
	// -ECONNREFUSED). res == 0: the connection is established.
	if res < 0 {
		return kernelResultToError(res)
	}
	if cell.err != nil {
		return cell.err
	}

	return nil
}
