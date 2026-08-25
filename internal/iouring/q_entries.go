package iouring

import (
	"fmt"
	"unsafe"
)

// SQEntry a task for the SQ.
type SQEntry struct {
	Opcode   uint8  // Operation code (e.g. OpAccept)
	Flags    uint8  // Flags (e.g. IOSQE_IO_LINK)
	Priority uint16 // I/O priority
	FD       int32  // Your socket / file descriptor

	// In C this is a union: off (offset) or addr2
	Off uint64

	// In C this is a union: addr (pointer to buffer/structure) or splice_off_in
	Addr uint64

	Len uint32 // Buffer length / number of iovecs

	// In C this is a union: flags unpacks into op_flags
	OpFlags uint32

	UserData uint64 // Your context, which will be returned in the CQE

	// In C this is a union: buf_index or buf_group
	BufIndex uint16

	Personality uint16 // User ID for executing the command
	SpliceFdIn  int32  // For the splice operation

	// Padding/reserve for the future, to pad the structure to exactly 64 bytes
	Pad2 [2]uint64
}

type CQEntry struct {
	UserData uint64 // That same context of yours from the SQE
	Res      int32  // Result of the syscall (e.g. number of bytes read or -ERRNO)
	Flags    uint32 // Kernel flags
}

func init() {
	fmt.Println(unsafe.Sizeof(SQEntry{}))
}
