# Low-Level Ring Engine Interface (`internal/iouring/iouring.go` and `method_Method_Name.go` files)

The iouring.IOUring instance acts as a single-threaded execution layer. It does not handle Go routine context states, 
channels, or gopark abstractions. It manipulates raw descriptors, memory locations, and bit masks.

```go
package iouring

import (
	"unsafe"
)

// ExpectAccept prepares a non-blocking connection acceptance task inside the SQ.
func (r *IOUring) ExpectAccept(listenFD int32, slotIdx uint64) error {
	// Sets Opcode = IORING_OP_ACCEPT (13), OpFlags = unix.SOCK_CLOEXEC.
	// Maps internal dummy address pointers to accept structures.
	panic("implement me")
}

// ExpectRecv prepares a безадресная read request leveraging kernel buffer rings.
func (r *IOUring) ExpectRecv(fd int32, bgid uint16, maxLen uint32, slotIdx uint64) error {
	// Sets Opcode = IORING_OP_RECV (22).
	// Sets Flags |= IOSQE_BUFFER_SELECT.
	// Set Addr = 0 (ignored by kernel), BufGroup = bgid, Len = maxLen, UserData = slotIdx.
	panic("implement me")
}

// ExpectSend prepares a raw data packet submission write request.
func (r *IOUring) ExpectSend(fd int32, bufPtr unsafe.Pointer, bufLen uint32, slotIdx uint64) error {
	// Sets Opcode = IORING_OP_SEND (23).
	// Maps Addr = bufPtr, Len = bufLen, UserData = slotIdx.
	panic("implement me")
}

// ExpectClose prepares a file descriptor destruction cleanup command.
func (r *IOUring) ExpectClose(fd int32, slotIdx uint64) error {
	// Sets Opcode = IORING_OP_CLOSE (26), UserData = slotIdx.
	panic("implement me")
}

// GetTask extracts a single completed CQEntry from the mmap shared memory layer.
func (r *IOUring) GetTask() (CQEntry, bool) {
	// Check existing implementation.
}

// WaitEvents freezes the active thread inside the kernel context until new events settle.
func (r *IOUring) WaitEvents() error {
	// Check existing implementation.
}
```
