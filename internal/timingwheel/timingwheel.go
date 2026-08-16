package timingwheel

const (
	invalidIndex = 0xFFFFFFFF
	numBuckets   = 4096 // Степень двойки для быстрой битовой маски & 4095
)

// NodeLinks хранит только топологию и эпоху (SoA). Весит всего 16 байт!
type NodeLinks struct {
	prev        uint32
	next        uint32
	bucketIndex uint32
	generation  uint32
}

// NodeValue хранит бизнес-логику и файловый дескриптор
type NodeValue struct {
	fd       int
	callback func(fd int) // Наш колбек для отправки close в io_uring
}

type TimingWheel struct {
	currentBucket uint32
	buckets       [numBuckets]uint32 // Индексы голов списков

	// Плоские предвыделенные массивы (Слайсы) — НУЛЬ фрагментации
	linksPool  []NodeLinks
	valuesPool []NodeValue
	freeNodes  []uint32 // Стек свободных индексов
}

func NewTimingWheel(maxConnections int) *TimingWheel {
	tw := &TimingWheel{
		linksPool:  make([]NodeLinks, maxConnections),
		valuesPool: make([]NodeValue, maxConnections),
		freeNodes:  make([]uint32, maxConnections),
	}

	// Заполняем buckets дефолтными INVALID значениями
	for i := range numBuckets {
		tw.buckets[i] = invalidIndex
	}

	// Инициализируем стек свободных индексов
	for i := range maxConnections {
		tw.freeNodes[i] = uint32(i)
		tw.linksPool[i].prev = invalidIndex
		tw.linksPool[i].next = invalidIndex
	}

	return tw
}

// Tick вызывается раз в секунду из вашего Event Loop (например, по сигналу или syscall.Timerfd)
func (tw *TimingWheel) Tick() {
	bucketToProcess := tw.currentBucket
	tw.currentBucket = (tw.currentBucket + 1) & (numBuckets - 1) // Быстрая маска вместо %

	currentIdx := tw.buckets[bucketToProcess]
	tw.buckets[bucketToProcess] = invalidIndex // Очищаем сектор

	// Итерируемся ИСКЛЮЧИТЕЛЬНО по легкому слайсу linksPool
	for currentIdx != invalidIndex {
		link := &tw.linksPool[currentIdx]
		nextIdx := link.next

		// Вызываем колбек только если сокет реально протух
		val := &tw.valuesPool[currentIdx]
		if val.callback != nil {
			val.callback(val.fd) // Тут летит асинхронный close в io_uring
		}

		// Освобождаем ячейку
		tw.releaseNode(currentIdx)
		currentIdx = nextIdx
	}
}

func (tw *TimingWheel) releaseNode(idx uint32) {
	tw.linksPool[idx].generation++
	tw.linksPool[idx].prev = invalidIndex
	tw.linksPool[idx].next = invalidIndex
	tw.valuesPool[idx].callback = nil
	tw.freeNodes = append(tw.freeNodes, idx)
}

// Add вставляет новый сокет в колесо времени
func (tw *TimingWheel) Add(ttlSeconds uint32, fd int, cb func(int)) TimerId {
	if len(tw.freeNodes) == 0 || ttlSeconds == 0 {
		return TimerId{Index: invalidIndex, Generation: 0}
	}

	// 1. Достаем свободный индекс из конца слайса за O(1)
	idx := tw.freeNodes[len(tw.freeNodes)-1]
	tw.freeNodes = tw.freeNodes[:len(tw.freeNodes)-1]

	// 2. Инициализируем данные (Values)
	tw.valuesPool[idx].fd = fd
	tw.valuesPool[idx].callback = cb

	// 3. Считаем целевую эпоху (сектор) с быстрой маской
	targetEpoch := (tw.currentBucket + ttlSeconds) & (numBuckets - 1)

	link := &tw.linksPool[idx]
	link.bucketIndex = targetEpoch

	// 4. Вставляем в ГОЛОВУ двусвязного списка целевой эпохи
	oldHeadIdx := tw.buckets[targetEpoch]
	link.next = oldHeadIdx
	link.prev = invalidIndex

	if oldHeadIdx != invalidIndex {
		tw.linksPool[oldHeadIdx].prev = idx
	}
	tw.buckets[targetEpoch] = idx // Теперь эта ячейка — новая голова сектора

	return TimerId{Index: idx, Generation: link.generation}
}

type TimerId struct {
	Index      uint32
	Generation uint32
}
