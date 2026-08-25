package timingwheel

const (
	invalidIndex = 0xFFFFFFFF
	numBuckets   = 4096 // Power of two for a fast bit mask & 4095
)

// NodeLinks stores only topology and epoch (SoA). It weighs just 16 bytes!
type NodeLinks struct {
	prev        uint32
	next        uint32
	bucketIndex uint32
	generation  uint32
}

// NodeValue stores business logic and the file descriptor
type NodeValue struct {
	fd       int
	callback func(fd int) // Our callback for sending close to io_uring
}

type TimingWheel struct {
	currentBucket uint32
	buckets       [numBuckets]uint32 // Head indices of the lists

	// Flat preallocated arrays (slices); ZERO fragmentation
	linksPool  []NodeLinks
	valuesPool []NodeValue
	freeNodes  []uint32 // Stack of free indices
}

func NewTimingWheel(maxConnections int) *TimingWheel {
	tw := &TimingWheel{
		linksPool:  make([]NodeLinks, maxConnections),
		valuesPool: make([]NodeValue, maxConnections),
		freeNodes:  make([]uint32, maxConnections),
	}

	// Fill buckets with default INVALID values
	for i := range numBuckets {
		tw.buckets[i] = invalidIndex
	}

	// Initialize the stack of free indices
	for i := range maxConnections {
		tw.freeNodes[i] = uint32(i)
		tw.linksPool[i].prev = invalidIndex
		tw.linksPool[i].next = invalidIndex
	}

	return tw
}

// Tick is called once per second from your Event Loop (e.g. via a signal or syscall.Timerfd)
func (tw *TimingWheel) Tick() {
	bucketToProcess := tw.currentBucket
	tw.currentBucket = (tw.currentBucket + 1) & (numBuckets - 1) // Fast mask instead of %

	currentIdx := tw.buckets[bucketToProcess]
	tw.buckets[bucketToProcess] = invalidIndex // Clear the bucket

	// Iterate EXCLUSIVELY over the light linksPool slice
	for currentIdx != invalidIndex {
		link := &tw.linksPool[currentIdx]
		nextIdx := link.next

		// Call the callback only if the socket has really expired
		val := &tw.valuesPool[currentIdx]
		if val.callback != nil {
			val.callback(val.fd) // Here the async close flies to io_uring
		}

		// Release the cell
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

// Add inserts a new socket into the timing wheel
func (tw *TimingWheel) Add(ttlSeconds uint32, fd int, cb func(int)) TimerId {
	if len(tw.freeNodes) == 0 || ttlSeconds == 0 {
		return TimerId{Index: invalidIndex, Generation: 0}
	}

	// 1. Take a free index from the end of the slice in O(1)
	idx := tw.freeNodes[len(tw.freeNodes)-1]
	tw.freeNodes = tw.freeNodes[:len(tw.freeNodes)-1]

	// 2. Initialize the data (Values)
	tw.valuesPool[idx].fd = fd
	tw.valuesPool[idx].callback = cb

	// 3. Compute the target epoch (bucket) with a fast mask
	targetEpoch := (tw.currentBucket + ttlSeconds) & (numBuckets - 1)

	link := &tw.linksPool[idx]
	link.bucketIndex = targetEpoch

	// 4. Insert into the HEAD of the target epoch's doubly linked list
	oldHeadIdx := tw.buckets[targetEpoch]
	link.next = oldHeadIdx
	link.prev = invalidIndex

	if oldHeadIdx != invalidIndex {
		tw.linksPool[oldHeadIdx].prev = idx
	}
	tw.buckets[targetEpoch] = idx // Now this cell is the new head of the bucket

	return TimerId{Index: idx, Generation: link.generation}
}

type TimerId struct {
	Index      uint32
	Generation uint32
}
