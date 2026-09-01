package taskslots

import (
	"math"
	"math/bits"
	"sync"
	"sync/atomic"
	"unsafe"

	"github.com/kelindar/bitmap"
	"github.com/sirkon/blog/beer"
)

const (
	sysIds uint64 = 1 << 63
)

type Slots[T any] struct {
	mu            sync.Mutex // Guards fallback and fullCount fields ONLY
	cap           uint64
	tasks         []T           // Flat pre-allocated array of structures
	bitmap        bitmap.Bitmap // Owned by Translator (No atomics on hot path)
	pollerBitmap  []uint64      // Shared with Poller (Atomic updates strictly on step boundary)
	bitmapLenMask uint64        // Ring mask for uint64 words array boundaries
	wave          uint64        // Current tracking word index pointer
	free          uint64        // Remaining slots on the hot path

	delCount     uint64 // Counts deletions by the poller.
	lastDelCount uint64 // Counts how many deletions were caught by the translator.

	// fallback indices are within [2^k, 2^63-1] values, where k is the power of two of slot slots capacity.
	// So, the length of fallback indices in bits is 63 - k, and we use 2^{63 - k} - 1 to get their values.
	fallbackMask uint64
	fallback     map[uint64]T
	fallbackWave uint64
}

func New[T any](capacity int) (*Slots[T], error) {
	if bits.OnesCount(uint(capacity)) != 1 {
		return nil, beer.New("capacity must be power of 2")
	}
	if capacity < 4096 {
		return nil, beer.New("capacity must be at least 4096")
	}
	fallbackMask := 63 - bits.TrailingZeros64(uint64(capacity))

	wordsCount := capacity >> 6
	bm := allocAlign(wordsCount)

	// Generic type must have a size 8*x, where x > 0.
	{
		t := *new(T)
		sizeof := unsafe.Sizeof(t)
		if sizeof == 0 {
			return nil, beer.Newf("generic type %T size must be greater than 0", t)
		}

		if sizeof&0x07 != 0 {
			return nil, beer.Newf(
				"generic type %T size must be a factor of 8, got %d = 8*%d + %d",
				t, sizeof, sizeof>>3, sizeof&0x07,
			)
		}
	}

	res := &Slots[T]{
		cap:           uint64(capacity),
		bitmap:        bm,
		pollerBitmap:  make([]uint64, wordsCount), // Allocated aligned internally if needed, or plain slice
		bitmapLenMask: uint64(wordsCount - 1),
		free:          uint64(capacity),
		tasks:         make([]T, capacity),
		fallbackMask:  (1 << fallbackMask) - 1,
		fallback:      make(map[uint64]T),
	}

	return res, nil
}

// Add is executed strictly inside the Single-Threaded Translator Loop.
// 0 atomics when allocating hot-path slots.
//
// The situation when we ran out of slots and fallbacks is impossible, since
// that generic type T size is 8 bytes long at least and that would mean we have
// 2^(63 + 3) = 2^66 bytes of tasks pending in slots and the fallback map.
// We will get OOM long before...
func (s *Slots[T]) Add(v T) uint64 {
	// Guarded by mutex, executed only on absolute saturation
	if s.free|(s.delCount-s.lastDelCount) == 0 {
		for {
			s.mu.Lock()
			idx := (s.fallbackWave & s.fallbackMask) + s.cap
			s.fallbackWave++
			if _, exists := s.fallback[idx]; exists {
				s.mu.Unlock()
				continue
			}
			s.fallback[idx] = v
			s.mu.Unlock()
			return idx
		}
	}

	basePtr := unsafe.Pointer(unsafe.SliceData(s.bitmap))
	basePollerPtr := unsafe.Pointer(unsafe.SliceData(s.pollerBitmap))

	for {
		wordIdx := s.wave & s.bitmapLenMask
		wordPtr := (*uint64)(unsafe.Add(basePtr, wordIdx<<3))
		w := math.MaxUint64 ^ *wordPtr

		// Look for first zero bit.
		emptyBitIdx := bits.TrailingZeros64(w)

		const slotsInAWord = 64
		if emptyBitIdx < slotsInAWord {
			// This is the hot path.
			globalSlotIdx := (wordIdx << 6) + uint64(emptyBitIdx)
			atomic.OrUint64(wordPtr, 1<<emptyBitIdx)
			s.tasks[globalSlotIdx] = v
			s.free--
			return globalSlotIdx
		}

		// Every slot of this word is taken. Refreshing the word.
		pollerWordPtr := (*uint64)(unsafe.Add(basePollerPtr, wordIdx<<3))

		// 1. Raw non-atomic load (MOV assembly hint). Fast L1 cache access.
		pBits := *pollerWordPtr
		if pBits == 0 {
			// No releases for this word. Move the wave 64 slots to the right.
			s.wave += 1
			continue
		}

		// There are released slots.
		w = s.syncWord(wordPtr, pollerWordPtr)
	}
}

// Del is executed inside the CQ Poller thread.
// 100% Lock-free and atomic-free on the poller side.
func (s *Slots[T]) Del(idx uint64) {
	switch {
	case idx > sysIds:
		return
	case idx >= s.cap:
		// Slow Path: Protect fallback map operations with the mutex
		s.mu.Lock()
		delete(s.fallback, idx)
		s.mu.Unlock()
	}

	wordIdx := idx >> 6
	bitAt := idx & 63

	// HOT PATH: NO ATOMICS, NO LOCKS.
	// Simply set a 1-bit meaning "this slot is now released by the poller".
	// Cache line remains strictly within the Poller's L1 workspace.
	basePtr := unsafe.Pointer(unsafe.SliceData(s.pollerBitmap))
	wordPtr := (*uint64)(unsafe.Add(basePtr, wordIdx<<3))
	atomic.OrUint64(wordPtr, 1<<bitAt)
	s.delCount++
}

func (s *Slots[T]) Get(idx uint64) (T, bool) {
	if idx < s.cap {
		basePtr := unsafe.Pointer(unsafe.SliceData(s.bitmap))
		wordIdx := idx >> 6
		bitAt := idx & 63

		wordPtr := (*uint64)(unsafe.Add(basePtr, wordIdx<<3))
		blk := *wordPtr

		exists := (blk & (uint64(1) << bitAt)) != 0
		return s.tasks[idx], exists
	}

	s.mu.Lock()
	res, exists := s.fallback[idx]
	s.mu.Unlock()
	return res, exists
}

func (s *Slots[T]) syncWord(wordPtr *uint64, pollerWordPtr *uint64) uint64 {
	pw := atomic.SwapUint64(pollerWordPtr, 0)
	*wordPtr ^= pw
	freedCount := uint64(bits.OnesCount64(pw))
	s.free += freedCount
	s.lastDelCount += freedCount

	return *wordPtr
}

func allocAlign(wordsCount int) []uint64 {
	buf := make([]uint64, wordsCount+8)
	ptr := uintptr(unsafe.Pointer(&buf[0]))
	aptr := (ptr + 63) &^ 63
	gap := (aptr - ptr) >> 3
	return buf[int(gap) : int(gap)+wordsCount]
}
