package taskslots

import (
	"math/bits"
	"sync"
	"sync/atomic"
	"unsafe"

	"github.com/kelindar/bitmap"
	"github.com/sirkon/blog/beer"
)

type Slots[T any] struct {
	mu            sync.Mutex // Guards fallback and fullCount fields ONLY
	cap           int
	tasks         []T           // Flat pre-allocated array of structures
	bitmap        bitmap.Bitmap // Owned by Translator (No atomics on hot path)
	pollerBitmap  []uint64      // Shared with Poller (Atomic updates strictly on step boundary)
	bitmapLenMask uint64        // Ring mask for uint64 words array boundaries
	wave          uint64        // Current tracking word index pointer
	free          int           // Remaining slots on the hot path
	fallback      map[uint64]T
	fullCount     uint64
}

func New[T any](capacity int) (*Slots[T], error) {
	if bits.OnesCount(uint(capacity)) != 1 {
		return nil, beer.New("capacity must be power of 2")
	}
	if capacity < 4096 {
		return nil, beer.New("capacity must be at least 4096")
	}

	wordsCount := capacity >> 6
	bm := allocAlign(wordsCount)

	return &Slots[T]{
		cap:           capacity,
		bitmap:        bm,
		pollerBitmap:  make([]uint64, wordsCount), // Allocated aligned internally if needed, or plain slice
		bitmapLenMask: uint64(wordsCount - 1),
		free:          capacity,
		tasks:         make([]T, capacity),
		fallback:      make(map[uint64]T),
	}, nil
}

// Del is executed inside the CQ Poller thread.
// 100% Lock-free and atomic-free on the poller side.
func (s *Slots[T]) Del(idx uint64) {
	if idx >= uint64(s.cap) {
		// Slow Path: Protect fallback map operations with the mutex
		s.mu.Lock()
		delete(s.fallback, idx)
		s.mu.Unlock()
		return
	}

	wordIdx := idx >> 6
	bitAt := idx & 63

	// HOT PATH: NO ATOMICS, NO LOCKS.
	// Simply set a 1-bit meaning "this slot is now released by the poller".
	// Cache line remains strictly within the Poller's L1 workspace.
	basePtr := unsafe.Pointer(unsafe.SliceData(s.pollerBitmap))
	wordPtr := (*uint64)(unsafe.Add(basePtr, wordIdx<<3))
	*wordPtr |= (uint64(1) << bitAt)
}

// syncWord performs the step-by-step XOR handshake when the wave advances.
func (s *Slots[T]) syncWord(wordIdx uint64) int {
	basePollerPtr := unsafe.Pointer(unsafe.SliceData(s.pollerBitmap))
	pollerWordPtr := (*uint64)(unsafe.Add(basePollerPtr, wordIdx<<3))

	// 1. Raw non-atomic load (MOV assembly hint). Fast L1 cache access.
	pBits := *pollerWordPtr
	if pBits == 0 {
		return 0
	}

	// 2. Non-atomic XOR locally in the Translator's L1 workspace.
	// Flipping matching 1s (released by poller) into 0s (free to use for translator).
	baseTranslatorPtr := unsafe.Pointer(unsafe.SliceData(s.bitmap))
	translatorWordPtr := (*uint64)(unsafe.Add(baseTranslatorPtr, wordIdx<<3))
	*translatorWordPtr ^= pBits

	// 3. Lock-free CAS loop to simulate non-existent atomic.XorUint64 in Go.
	// Deducts only the applied pBits mask chunk from the Poller's tracking register.
	// If the Poller concurrently added a new bit flag, it is safely retained for the next wave sweep.
	for {
		oldVal := pBits
		newVal := oldVal ^ pBits

		if atomic.CompareAndSwapUint64(pollerWordPtr, oldVal, newVal) {
			break // Success: Bits cleared without dynamic races
		}

		// CAS failed: Poller appended new entries. Reload target word state.
		pBits = *pollerWordPtr

		// Isolate and apply only those bit states that were already cleared on Step 2.
		pBits &= oldVal
		if pBits == 0 {
			break // Nothing left to deduct from the state space
		}
	}

	return bits.OnesCount64(pBits)
}

// Add is executed strictly inside the Single-Threaded Translator Loop.
// 0 atomics when allocating hot-path slots.
func (s *Slots[T]) Add(v T) uint64 {
	// Guarded by mutex, executed only on absolute saturation
	if s.free == 0 {
		s.mu.Lock()
		idx := uint64(s.cap) + s.fullCount
		s.fallback[idx] = v
		s.fullCount++
		s.mu.Unlock()
		return idx
	}

	basePtr := unsafe.Pointer(unsafe.SliceData(s.bitmap))

	for {
		wordIdx := s.wave & s.bitmapLenMask

		// Sync current word with Poller state data before scanning
		cleared := s.syncWord(wordIdx)
		s.free += cleared

		// Fetch the current word state using raw pointers (No Bounds Check)
		wordPtr := (*uint64)(unsafe.Add(basePtr, wordIdx<<3))
		w := *wordPtr

		// Find trailing zero (empty slot marker where 0 means free)
		localBitIdx := bits.TrailingZeros64(^w)

		if localBitIdx < 64 {
			globalSlotIdx := (wordIdx << 6) + uint64(localBitIdx)

			// Mark occupied (1) in Translator's bitmap via raw pointer write
			*wordPtr |= (uint64(1) << localBitIdx)

			s.free--
			s.tasks[globalSlotIdx] = v
			return globalSlotIdx
		}

		// Current word is fully saturated. Increment wave to step into the next word.
		s.wave++
	}
}

func (s *Slots[T]) Get(idx uint64) (T, bool) {
	if idx < uint64(s.cap) {
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

func (s *Slots[T]) Reset() {
	s.mu.Lock()
	s.free = s.cap
	s.wave = 0
	s.fullCount = 0
	clear(s.bitmap)
	clear(s.fallback)
	s.mu.Unlock()
}

func allocAlign(wordsCount int) []uint64 {
	buf := make([]uint64, wordsCount+8)
	ptr := uintptr(unsafe.Pointer(&buf[0]))
	aptr := (ptr + 63) &^ 63
	gap := (aptr - ptr) >> 3
	return buf[int(gap) : int(gap)+wordsCount]
}
