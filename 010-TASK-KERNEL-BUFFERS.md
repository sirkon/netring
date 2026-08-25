# Kernel Buffer Pool Management (internal/iouring)

To avoid dynamic memory allocations on the hot path, io_uring features a kernel-managed buffer pool using 
Provided Buffer Rings (IORING_OP_REGISTER_PBUF_RING). Memory is statically allocated in user-space, aligned 
to cache lines, and registered within the kernel context.

# Memory layout and registration.

- **Alignment**: Every allocated memory chunk must be strictly cache-line aligned (64 bytes) or page-aligned (4096 bytes)
  using unsafe pointer arithmetic to optimize hardware prefetcher operations and bypass L3 cache 
  line bouncing [INDEX, INDEX].
- **Registration Open Opcodes**: Use unix.SYS_IO_URING_REGISTER with the opcode IORING_REGISTER_PBUF_RING to provision 
  structures directly inside the kernel subsystem.

# Expected Implementation Surface (`internal/iouring/pbuf.go`)

```go
package iouring

import (
	"golang.org/x/sys/unix"
)

// ProvidedBufferRing represents a single size-classed ring buffer shared with the kernel.
type ProvidedBufferRing struct {
	bgid        uint16         // Buffer Group ID mapped from SizeClass
	capacity    uint32         // Number of buffers in this ring (must be power of 2)
	bufSize     uint32         // Size of each single buffer (e.g., 128, 512, 4096)
	basePtr     uintptr        // Base memory address of the raw contiguous buffer array
	ringPtr     uintptr        // Memory address of the allocated raw io_uring_buf_ring
	bufPointers []uintptr      // Fast lookup array: index (bid) -> memory address pointer
}

// RegisterBufferRing provisions memory and registers a provided buffer ring within the kernel.
func (r *IOUring) RegisterBufferRing(bgid uint16, capacity uint32, bufSize uint32) (*ProvidedBufferRing, error) {
	// 1. Validate capacity is a power of 2.
	// 2. Allocate contiguous memory block for data: capacity * bufSize.
	// 3. Allocate descriptor memory for struct io_uring_buf_ring.
	// 4. Fill bufPointers for fast user-space O(1) tracking lookups.
	// 5. Execute syscall.Syscall6 with unix.SYS_IO_URING_REGISTER.
	panic("implement me")
}

// ReleaseBuffer returns a used buffer index (bid) back to the kernel-managed ring.
func (pbr *ProvidedBufferRing) ReleaseBuffer(bid uint16) {
	// 1. Write the buffer address back into the user-space side of io_uring_buf_ring descriptor.
	// 2. Adjust ring tail pointer atomically.
	// 3. The kernel automatically re-acquires it on the next incoming network operation.
	panic("implement me")
}
```

# Important.

Consider using Huge Pages.

