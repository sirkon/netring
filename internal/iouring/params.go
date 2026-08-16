package iouring

type Params struct {
	SQEntries    uint32 // Заполняет ядро
	CQEntries    uint32 // Заполняет ядро
	Flags        uint32 // Сюда мы пишем setupSQPoll
	SQThreadCpu  uint32 // CPU-ядро для треда поллинга (если нужен SQ_AFF)
	SQThreadIdle uint32 // Время простоя треда ядра в мс перед сном
	Features     uint32 // Фичи ядра (например, SINGLE_MMAP)
	WqFd         uint32
	Resv         [3]uint32
	SQOff        TasksOffsets     // Смещения для SQ
	CQOff        ResponsesOffsets // Смещения для CQ
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
	Resv2       uint64 // Выравнивание под 64-битную архитектуру Linux
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
