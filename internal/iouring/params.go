package iouring

type Params struct {
	SQEntries    uint32 // Filled in by the kernel
	CQEntries    uint32 // Filled in by the kernel
	Flags        uint32 // Here we write setupSQPoll
	SQThreadCpu  uint32 // CPU core for the poll thread (if SQ_AFF is needed)
	SQThreadIdle uint32 // Idle time of the kernel thread in ms before sleeping
	Features     uint32 // Kernel features (e.g. SINGLE_MMAP)
	WqFd         uint32
	Resv         [3]uint32
	SQOff        TasksOffsets     // Offsets for SQ
	CQOff        ResponsesOffsets // Offsets for CQ
}

type TasksOffsets struct {
	Head        uint32
	Tail        uint32
	RingMask    uint32
	RingEntries uint32
	Flags       uint32
	Dropped     uint32
	Array       uint32
	Resv1       uint32
	Resv2       uint64 // Padding for 64-bit Linux architecture
}

type ResponsesOffsets struct {
	Head        uint32
	Tail        uint32
	RingMask    uint32
	RingEntries uint32
	Overflow    uint32
	Cqes        uint32
	Flags       uint32
	Resv1       uint32
	Resv2       uint64
}
