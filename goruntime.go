package netring

import (
	"unsafe"
	// The blank import is required for //go:linkname: without it the
	// directives below are rejected by the compiler. It must stay even
	// though unsafe itself is imported.
	_ "unsafe"
)

// g is a stub for the runtime-internal goroutine struct. It is never
// allocated and never dereferenced; it exists only to type the linknamed
// gopark/goready declarations (the *g stub approach).
type g struct{}

//go:linkname gopark runtime.gopark
func gopark(
	unlockf func(gp *g, lock unsafe.Pointer) bool,
	lock unsafe.Pointer,
	reason uint8,
	traceReason uint8,
	traceskip int,
)

//go:linkname goready runtime.goready
func goready(gp *g, traceskip int)

// Fixed parking arguments for all park call sites (031-034).
const (
	waitReasonIOWait  uint8 = 2 // hand-copied from runtime/runtime2.go
	traceBlockGeneric uint8 = 0 // hand-copied from runtime/traceruntime.go
	parkTraceSkip     int   = 1
)

// netringParkUnlock is the unlockf callback handed to gopark. The handshake
// contract lives in section 6; the body must stay a single atomic Swap.
//
//go:nosplit
func netringParkUnlock(gp *g, lock unsafe.Pointer) bool {
	cell := (*taskCell)(lock)
	return cell.taskState.Swap(taskStateParked) == taskStateInCore
}

// getg returns the calling goroutine's runtime g pointer. It is implemented
// in per-GOARCH assembly (sections 2-4): runtime.getg is a compiler
// intrinsic with no emitted symbol, linknaming it fails at link time
// ("relocation target runtime.getg not defined"). The value is opaque:
// never dereferenced, never stored as anything but uintptr, and only ever
// handed back to goready through a (*g)(unsafe.Pointer(...)) conversion
// done in one expression, never kept around.
func getg() uintptr
