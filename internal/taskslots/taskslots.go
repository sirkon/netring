package taskslots

import (
	"math/bits"
	"unsafe"

	"github.com/kelindar/bitmap"
	"github.com/sirkon/blog/beer"
)

type Slots[T any] struct {
	cap           int
	tasks         []T
	bitmap        bitmap.Bitmap
	bitmapLenMask uint64
	wave          uint64
	free          int
	fallback      map[uint64]T
	fullCount     uint64
}

func New[T any](capacity int) (*Slots[T], error) {
	if bits.OnesCount(uint(capacity)) != 1 {
		return nil, beer.New("capacity must be power of 2")
	}
	if capacity <= 4096 {
		return nil, beer.New("capacity must be at least 4096")
	}

	// Number of uint64 words. For 131072 that's 2048 elements
	wordsCount := capacity >> 6
	bm := allocAlign(wordsCount)

	return &Slots[T]{
		cap:           capacity,
		bitmap:        bm,
		bitmapLenMask: uint64(wordsCount - 1),
		free:          capacity,
		tasks:         make([]T, capacity),
		fallback:      make(map[uint64]T),
	}, nil
}

func (s *Slots[T]) Add(v T) uint64 {
	// Strict control: if free == 0, there is physically no room on the hot path
	if s.free == 0 {
		idx := uint64(s.cap) + s.fullCount
		s.fallback[idx] = v
		s.fullCount++
		return idx
	}

	// 1. Bind the wave to the ring mask of uint64 words
	wave := s.wave & s.bitmapLenMask

	// Slice off from the current word to the end of the bitmap
	bitmp := s.bitmap[wave:]

	localBitIdx, found := bitmp.MinZero()
	var globalSlotIdx uint64

	if found {
		globalSlotIdx = (wave << 6) + uint64(localBitIdx)

		// ULTRA-HACK: Direct bit write to a memory address WITHOUT Bounds Check!
		// Get the base pointer to the start of s.bitmap
		basePtr := unsafe.Pointer(unsafe.SliceData(s.bitmap))
		wordIdx := globalSlotIdx >> 6
		bitAt := globalSlotIdx & 63

		// Find the exact address of the needed uint64 word: basePtr + wordIdx * 8 bytes
		wordPtr := (*uint64)(unsafe.Add(basePtr, wordIdx<<3))
		// Atomically for this thread set the mask in one CPU tick
		*wordPtr |= (uint64(1) << bitAt)

		s.wave += uint64(localBitIdx >> 6)
	} else {
		// 2. NOT FOUND in the tail: reset the search and look from the very beginning of the bitmap.
		// Since s.free > 0, a free bit there is guaranteed to exist!
		globalZeroIdx, _ := s.bitmap.MinZero()
		globalSlotIdx = uint64(globalZeroIdx)
		s.bitmap.Set(globalZeroIdx)

		// Reset the wave to the word where we just found the hole at the start
		s.wave = globalSlotIdx >> 6
	}

	// Decrement the honest counter of free slots
	s.free--

	// Write the task on the hot path
	s.tasks[globalSlotIdx] = v

	return globalSlotIdx
}

func (s *Slots[T]) Get(idx uint64) (T, bool) {
	// If the index fits within the capacity, it's the hot path
	if idx < uint64(s.cap) {
		basePtr := unsafe.Pointer(unsafe.SliceData(s.bitmap))

		// 1. Divide by 64 to find the index of the uint64 word in the bitmap slice
		wordIdx := idx >> 6
		// 2. Modulo 64 to find the bit position inside that word
		bitAt := idx & 63

		// 3. Multiply wordIdx by 8 (shift << 3), since uint64 weighs 8 bytes
		wordPtr := (*uint64)(unsafe.Add(basePtr, wordIdx<<3))
		blk := *wordPtr

		// Check whether the bit is set
		exists := (blk & (uint64(1) << bitAt)) != 0

		return s.tasks[idx], exists
	}

	// Otherwise, it's the fallback map
	res, exists := s.fallback[idx]
	return res, exists
}

func (s *Slots[T]) Del(idx uint64) {
	if idx >= uint64(s.cap) {
		delete(s.fallback, idx)
		return
	}

	s.free++

	// ULTRA-HACK: clear the bit in one tick without any length checks
	basePtr := unsafe.Pointer(unsafe.SliceData(s.bitmap))
	wordIdx := idx >> 6
	bitAt := idx & 63

	wordPtr := (*uint64)(unsafe.Add(basePtr, wordIdx<<3))
	*wordPtr &^= (uint64(1) << bitAt)
}

// Reset completely clears the Slots state for reuse without allocations.
func (s *Slots[T]) Reset() {
	s.free = s.cap
	s.wave = 0
	s.fullCount = 0

	// 1. Quickly zero out the bitmap.
	// Go optimizes this loop into an efficient memclr / vzeroupper assembly instruction.
	clear(s.bitmap)

	// 3. Clear the fallback map.
	// If it has grown large, it is easier to recreate it, but if it was empty there will be no allocation.
	clear(s.fallback)
}

func allocAlign(wordsCount int) []uint64 {
	// Allocate memory with a margin for alignment (64 bytes = 8 uint64s)
	buf := make([]uint64, wordsCount+8)
	ptr := uintptr(unsafe.Pointer(&buf[0]))
	aptr := (ptr + 63) &^ 63
	gap := (aptr - ptr) >> 3 // Offset in uint64 elements (division by 8 bytes)

	return buf[int(gap) : int(gap)+wordsCount]
}
