package iouring

import (
	"math/bits"
	"sync/atomic"
	"syscall"
	"unsafe"

	"github.com/sirkon/blog"
	"github.com/sirkon/blog/beer"
	"golang.org/x/sys/unix"
)

// Provided buffers enable zero-allocation reads: memory is provisioned once, registered with
// the kernel, and recycled in place. The kernel picks a buffer from the ring for every
// IOSQE_BUFFER_SELECT request that is ready for data, so the read hot path never touches
// the Go allocator.
//
// The ring protocol (struct io_uring_buf_ring) is shared memory between us and the kernel:
//
//	struct io_uring_buf_ring {
//	    union {
//	        struct { resv1; resv2; resv3; u16 tail; } // the ring bookkeeping
//	        struct io_uring_buf bufs[];               // the buffer descriptors
//	    };
//	}
//
// The union overlays the bookkeeping with the very first descriptor: bufs[0].addr is resv1,
// bufs[0].len is resv2, bufs[0].bid is resv3 and bufs[0].resv IS the 16-bit ring tail
// (offset 14). Yes, the first descriptor is fully usable — only its resv field is hijacked.
// We publish a buffer by writing its descriptor into bufs[tail & mask] (never touching the
// resv of bufs[0]) and then bumping the tail with a release store; the kernel consumes from
// its private head. The head is not part of the shared memory at all, it can only be fetched
// with IORING_REGISTER_PBUF_STATUS.

const (
	// pbufRingEntrySize is sizeof(struct io_uring_buf): addr(8) + len(4) + bid(2) + resv(2).
	pbufRingEntrySize = 16
	// pbufRingTailOffset is offsetof(struct io_uring_buf_ring, tail): the tail lives inside
	// the resv field of the first buffer descriptor.
	pbufRingTailOffset = 14

	// maxPbufRingEntries is the kernel limit: ring_entries must be a power of 2 and
	// strictly below 65536, because a 16-bit head/tail pair cannot tell full from empty.
	maxPbufRingEntries = 32768
)

// Compile-time guards for the hand-copied kernel layouts: bufEntry must stay exactly
// 16 bytes with resv at offset 14, or the ring protocol silently corrupts.
const (
	_ = uint(unsafe.Sizeof(bufEntry{})) - pbufRingEntrySize // fails if it grows past 16
	_ = pbufRingEntrySize - uint(unsafe.Sizeof(bufEntry{})) // fails if it shrinks below 16
	_ = uint(unsafe.Offsetof(bufEntry{}.Resv)) - pbufRingTailOffset
)

// bufEntry mirrors struct io_uring_buf, a single provided buffer descriptor.
type bufEntry struct {
	Addr uint64
	Len  uint32
	BID  uint16
	Resv uint16
}

// bufReg mirrors struct io_uring_buf_reg, the argument of IORING_REGISTER_PBUF_RING.
type bufReg struct {
	RingAddr    uint64
	RingEntries uint32
	BGID        uint16
	Flags       uint16
	MinLeft     uint32
	Resv        [5]uint32
}

// bufStatus mirrors struct io_uring_buf_status, the argument of IORING_REGISTER_PBUF_STATUS.
type bufStatus struct {
	BufGroup uint32
	Head     uint32
	Resv     [8]uint32
}

// ProvidedBufferRing is a single size-classed buffer ring shared with the kernel.
//
// Not thread-safe: exactly like the rest of the ring, it is driven from the single
// Translator goroutine (see ARCH.md). The tail is updated with compare-and-swap purely to
// order the descriptor stores against the kernel's acquire load of the tail (the kernel
// reads this memory concurrently even though it never writes it: only bufs[0].resv is
// kernel-visible bookkeeping, and it is written by us alone).
type ProvidedBufferRing struct {
	bgid     uint16 // Buffer Group ID, referenced by SQE.BufIndex of IOSQE_BUFFER_SELECT requests
	capacity uint32 // Number of buffer slots in the ring, always a power of 2
	bufSize  uint32 // Size of a single buffer in bytes
	mask     uint32 // capacity - 1, to wrap the ring indexes

	dataBytes   []byte    // The mmap'ed data block, kept for teardown
	basePtr     uintptr   // Base of the data block, page (hence cache-line) aligned
	bufPointers []uintptr // bid -> buffer address, O(1) user-space lookup

	ringPtr    uintptr // Base of the struct io_uring_buf_ring descriptor array, page aligned
	ringBytes  []byte  // The mmap'ed ring memory, kept for teardown
	ringLength uint32  // Page-rounded ring size, must be remembered for munmap
	ringTail   *uint16 // The overlaid 16-bit tail (bufs[0].resv), bumped to publish buffers

	fd     int  // io_uring fd, kept for unregister and head queries
	closed bool // Set by Unregister
}

// RegisterBufferRing provisions a provided buffer ring of capacity buffers of bufSize bytes
// each and registers it within the kernel under the given bgid.
//
// The buffer data is one contiguous, page-aligned block split into capacity equal chunks;
// the ring descriptors live in a separate page-aligned block, exactly as the kernel
// requires. On return all buffers are already published and available to
// IOSQE_BUFFER_SELECT requests.
func (r *IOUring) RegisterBufferRing(bgid uint16, capacity uint32, bufSize uint32) (*ProvidedBufferRing, error) {
	// The kernel would reject the registration itself for bad capacities (power of 2,
	// < 65536), but we validate up front to fail before allocating anything.
	if capacity == 0 || bits.OnesCount32(capacity) != 1 {
		return nil, beer.Newf("capacity must be a power of 2, got %d", capacity)
	}
	if capacity > maxPbufRingEntries {
		return nil, beer.Newf("capacity must not exceed %d, got %d", maxPbufRingEntries, capacity)
	}
	if bufSize == 0 {
		return nil, beer.New("buffer size must be greater than 0")
	}
	// bufSize being a multiple of 64 keeps every buffer start cache-line aligned: the
	// hardware prefetcher then works for us instead of bouncing single lines.
	if bufSize&0x3f != 0 {
		return nil, beer.Newf("buffer size must be a multiple of 64, got %d", bufSize)
	}

	// 1. The data block: capacity * bufSize bytes. Page-aligned anonymous memory, split into
	// capacity equal chunks (bufSize is a multiple of 64, so every chunk start is cache-line
	// aligned and the hardware prefetcher works for us rather than against us).
	//
	// The block is long-lived and recycled forever, so once it reaches the usual 2 MiB huge
	// page size it is pushed into a transparent huge page: the mapping is created without
	// MAP_POPULATE and MADV_HUGEPAGE'd before the first fault, so the kernel allocates the
	// huge page directly instead of collapsing small pages later. An explicit MAP_HUGETLB
	// would be the stronger option, but it needs pre-reserved huge pages
	// (/proc/sys/vm/nr_hugepages) and fails hard with ENOMEM otherwise; MADV_HUGEPAGE works
	// wherever transparent hugepages are enabled.
	dataLen := uint64(capacity) * uint64(bufSize)
	bigBlock := dataLen >= 1<<21
	populate := unix.MAP_POPULATE
	if bigBlock {
		populate = 0
	}
	dataMem, err := unix.Mmap(
		-1,
		0,
		int(dataLen),
		unix.PROT_READ|unix.PROT_WRITE,
		unix.MAP_PRIVATE|unix.MAP_ANONYMOUS|populate,
	)
	if err != nil {
		return nil, beer.Wrap(err, "mmap pbuf data block")
	}
	basePtr := uintptr(unsafe.Pointer(unsafe.SliceData(dataMem)))
	if bigBlock {
		if err := unix.Madvise(dataMem, unix.MADV_HUGEPAGE); err != nil {
			r.logger.Error(nil, "failed to enable huge pages for pbuf data block", blog.Err(err))
		}
		// Fault the whole block in right now, while it is cheap: every page is touched
		// exactly once and the first requests never take soft page faults mid-flight.
		for i := 0; i < len(dataMem); i += 4096 {
			dataMem[i] = 0
		}
	}

	// 2. The ring descriptors: capacity entries of 16 bytes each, page aligned.
	// The registration pins these pages into the kernel, so the mapping must stay alive
	// for the whole lifetime of the ring.
	ringLen := uint64(capacity) * pbufRingEntrySize
	ringLen = (ringLen + 4095) &^ 4095
	ringMem, err := unix.Mmap(
		-1,
		0,
		int(ringLen),
		unix.PROT_READ|unix.PROT_WRITE,
		unix.MAP_PRIVATE|unix.MAP_ANONYMOUS|unix.MAP_POPULATE,
	)
	if err != nil {
		_ = unix.Munmap(dataMem)
		return nil, beer.Wrap(err, "mmap pbuf ring")
	}
	ringPtr := uintptr(unsafe.Pointer(unsafe.SliceData(ringMem)))

	pbr := &ProvidedBufferRing{
		bgid:     bgid,
		capacity: capacity,
		bufSize:  bufSize,
		mask:     capacity - 1,

		dataBytes:   dataMem,
		basePtr:     basePtr,
		bufPointers: make([]uintptr, capacity),

		ringPtr:    ringPtr,
		ringBytes:  ringMem,
		ringLength: uint32(ringLen),
		ringTail:   (*uint16)(unsafe.Pointer(ringPtr + pbufRingTailOffset)),

		fd: r.FD,
	}

	// 3. Register the ring with the kernel. From this moment the pages are pinned and
	// visible to the kernel; the syscall takes exactly one argument (nr_args == 1).
	reg := bufReg{
		RingAddr:    uint64(ringPtr),
		RingEntries: capacity,
		BGID:        bgid,
	}
	_, _, errno := syscall.Syscall6(
		unix.SYS_IO_URING_REGISTER,
		uintptr(pbr.fd),
		uintptr(ioUringRegisterPbufRing),
		uintptr(unsafe.Pointer(&reg)),
		1, // nr_args, the kernel requires exactly 1
		0,
		0,
	)
	if errno != 0 {
		_ = unix.Munmap(ringMem)
		_ = unix.Munmap(dataMem)
		return nil, beer.Wrap(errno, "register pbuf ring")
	}

	// 4. Fill the ring and publish. Registration does not zero the memory, and the kernel
	// starts consuming from head == 0, so descriptors are filled sequentially and the
	// whole ring is published with one release store of the tail.
	for i := range capacity {
		addr := basePtr + uintptr(i)*uintptr(bufSize)
		pbr.bufPointers[i] = addr
		entry := pbr.entry(i)
		entry.Addr = uint64(addr)
		entry.Len = bufSize
		entry.BID = uint16(i)
	}
	storeTailRelease(pbr.ringTail, uint16(capacity))

	return pbr, nil
}

// Unregister detaches the buffer ring from the kernel and releases its memory. It must be
// called only after every request that used these buffers has completed, otherwise the
// kernel would be writing into unmapped memory.
func (pbr *ProvidedBufferRing) Unregister() error {
	if pbr.closed {
		return nil
	}

	reg := bufReg{
		BGID: pbr.bgid,
	}
	_, _, errno := syscall.Syscall6(
		unix.SYS_IO_URING_REGISTER,
		uintptr(pbr.fd),
		uintptr(ioUringUnregisterPbufRing),
		uintptr(unsafe.Pointer(&reg)),
		1,
		0,
		0,
	)
	if errno != 0 {
		return beer.Wrap(errno, "unregister pbuf ring")
	}

	if err := unix.Munmap(pbr.ringBytes); err != nil {
		return beer.Wrap(err, "munmap pbuf ring")
	}
	if err := unix.Munmap(pbr.dataBytes); err != nil {
		return beer.Wrap(err, "munmap pbuf data block")
	}

	pbr.closed = true
	return nil
}

// ReleaseBuffer hands a consumed buffer (identified by its bid, taken from the CQE flags)
// back to the kernel-managed ring. The descriptor is written at the current tail position
// and the tail is bumped with a release store, making the buffer visible to the kernel
// again; it may then be picked for any upcoming incoming network read.
func (pbr *ProvidedBufferRing) ReleaseBuffer(bid uint16) {
	tail := uint32(*pbr.ringTail)
	idx := tail & pbr.mask

	entry := pbr.entry(idx)
	entry.Addr = uint64(pbr.bufPointers[bid])
	entry.Len = pbr.bufSize
	entry.BID = bid

	storeTailRelease(pbr.ringTail, uint16(tail+1))
}

// Buffer returns the data of the buffer with the given bid. It is a zero-copy view into
// the preallocated data block, valid until the buffer is handed back with ReleaseBuffer.
func (pbr *ProvidedBufferRing) Buffer(bid uint16) []byte {
	return unsafe.Slice((*byte)(unsafe.Pointer(pbr.bufPointers[bid])), int(pbr.bufSize))
}

// ReleaseView hands a consumed buffer view back to the kernel-managed ring.
// The view must come from Buffer(bid) of this same ring.
func (pbr *ProvidedBufferRing) ReleaseView(view []byte) error {
	// Foreign pointers below the data block are caught by unsigned-safe math:
	// ptr - basePtr underflows into a huge offset, which then fails the range
	// check; never compare after subtracting.
	ptr := uintptr(unsafe.Pointer(unsafe.SliceData(view)))
	if ptr < pbr.basePtr {
		return beer.New("buffer view does not belong to this ring")
	}
	offset := ptr - pbr.basePtr
	if offset >= uintptr(pbr.capacity)*uintptr(pbr.bufSize) || offset%uintptr(pbr.bufSize) != 0 {
		return beer.New("buffer view does not belong to this ring")
	}

	pbr.ReleaseBuffer(uint16(offset / uintptr(pbr.bufSize)))
	return nil
}

// Available returns the number of buffers currently visible to the kernel. It goes through
// a syscall (IORING_REGISTER_PBUF_STATUS), so use it for diagnostics only, never on the
// hot path.
func (pbr *ProvidedBufferRing) Available() (uint32, error) {
	status := bufStatus{
		BufGroup: uint32(pbr.bgid),
	}
	_, _, errno := syscall.Syscall6(
		unix.SYS_IO_URING_REGISTER,
		uintptr(pbr.fd),
		uintptr(ioUringRegisterPbufStatus),
		uintptr(unsafe.Pointer(&status)),
		1,
		0,
		0,
	)
	if errno != 0 {
		return 0, beer.Wrap(errno, "get pbuf ring head")
	}

	return uint32(uint16(uint32(*pbr.ringTail) - status.Head)), nil
}

// entry returns the idx-th buffer descriptor, idx must be already masked.
// Entry 0 is not special-cased: its Addr/Len/BID fields are a normal descriptor, only its
// Resv field is the ring tail and is written exclusively via ringTail.
func (pbr *ProvidedBufferRing) entry(idx uint32) *bufEntry {
	return (*bufEntry)(unsafe.Pointer(pbr.ringPtr + uintptr(idx)*pbufRingEntrySize))
}

// storeTailRelease publishes buffers with release semantics, pairing with the kernel's
// smp_load_acquire(&br->tail).
func storeTailRelease(tail *uint16, value uint16) {
	// The tail is a 16-bit field, and for this ring layout it sits at offset 14 of a
	// page-aligned mapping: the unaligned high half of the 32-bit word at offset 12. Go has
	// no 16-bit atomics, so the update goes through a compare-and-swap on the enclosing
	// aligned word, replacing only the tail half; the CAS is also what orders the
	// descriptor stores above against the kernel's tail load.
	addr := uintptr(unsafe.Pointer(tail))
	var word *uint32
	var shift uint32
	if addr&0x3 == 0 {
		word = (*uint32)(unsafe.Pointer(addr))
		shift = 0
	} else {
		word = (*uint32)(unsafe.Pointer(addr - 2))
		shift = 16
	}
	tailBits := uint32(0xffff) << shift
	for {
		old := atomic.LoadUint32(word)
		newWord := (old &^ tailBits) | (uint32(value) << shift)
		if atomic.CompareAndSwapUint32(word, old, newWord) {
			return
		}
	}
}
