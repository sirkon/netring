package netring

import (
	"sync/atomic"
	"unsafe"
)

type sendCell struct {
	finished atomic.Uint64
	_        [56]byte

	queued uint64
	err    error
	buf    unsafe.Pointer
	len    uint64
	sent   uint64

	_ [16]byte // Padding to 64 bytes.
}

const sizeofSendCell = 128

func init() {
	var buf [256]byte
	ptr := unsafe.Pointer(&buf)
	aligned := (*sendCell)(unsafe.Pointer((uintptr(ptr) + 63) &^ 63))

	_ = [1]struct{}{}[unsafe.Sizeof(*aligned)-128]
}
