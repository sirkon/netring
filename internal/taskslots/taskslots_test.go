package taskslots

import (
	"fmt"
	"math"
	"math/bits"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/alecthomas/assert/v2"
	"github.com/sirkon/blog/beer"
	"github.com/sirkon/deepequal"

	"github.com/sirkon/netring/internal/pqueue"
)

const capacity = 1 << 17

type TaskSession struct {
	FD  int
	Buf []byte
}

func TestSlots_Playground(t *testing.T) {
	t.Run("check all tasks are stored", func(t *testing.T) {
		slots, err := New[TaskSession](capacity)
		if err != nil {
			t.Fatal(beer.Wrap(err, "create slots"))
		}

		var lim = capacity + 17
		type taskCase struct {
			idx  uint64
			task TaskSession
		}
		var cases = []taskCase{}
		sharedBuf := make([]byte, lim*2)

		for i := range lim {
			task := TaskSession{
				FD:  i,
				Buf: sharedBuf[i*2 : i*2+2],
			}

			ttt := TaskSession{
				FD:  task.FD,
				Buf: task.Buf,
			}
			idx := slots.Add(ttt)
			cases = append(cases, taskCase{idx, task})

			// Immediately check that the stored task is returned.
			storedTask, ok := slots.Get(idx)
			if !ok {
				t.Fatalf("task %d not found", idx)
			}

			if !deepequal.Equal(task, storedTask) {
				deepequal.SideBySide(t, fmt.Sprintf("task 0x%x", idx), task, storedTask)
			}
		}

		// Check that all stored tasks are present.
		for _, cs := range cases {
			task, ok := slots.Get(cs.idx)
			if !ok {
				t.Fatalf("task %d not found", cs.idx)
			}
			if !reflect.DeepEqual(task, cs.task) {
				deepequal.SideBySide(t, fmt.Sprintf("task %x", cs.idx), cs.task, task)
			}
		}

		// Now remove a task from slots and check if this idx will be reused.
		taskIdx := cases[100].idx
		assert.True(t, taskIdx < slots.cap, "expected slotted task, got a task from the fallback")
		slots.Del(taskIdx)
		newIdx := slots.Add(TaskSession{
			FD:  -100,
			Buf: make([]byte, 16),
		})
		assert.Equal(t, taskIdx, newIdx)

		// Implementation specific. We put the fallback wave back to see if it will be "repaired".
		assert.True(t, slots.fallbackWave > 0, "expected mutated fallback wave")
		sampleTask := TaskSession{
			FD:  -1_000_000,
			Buf: make([]byte, 16),
		}
		fallbackIdx := slots.Add(sampleTask)
		assert.True(t, fallbackIdx >= slots.cap, "expected the task to end up in the fallback, got it slotted")
		slots.fallbackWave = 0
		yetAnotherFallbackIdx := slots.Add(sampleTask)
		assert.Equal(t, yetAnotherFallbackIdx, fallbackIdx+1)

		// Now, check fallback size is decreased if we removed fallback task.
		last := cases[len(cases)-1]
		oldFallbackLen := len(slots.fallback)
		assert.True(t, last.idx >= slots.cap, "the last task should be in the fallback")
		slots.Del(last.idx)
		newFallbackLen := len(slots.fallback)
		assert.False(
			t, newFallbackLen >= oldFallbackLen,
			"the fallback length must be decreased after we remove a fallback task",
		)

		// Delete "system" task (NOP)
		// Implementation specific: put match.MaxUint idx into fallbacks and check it won't touch it and
		// delCount won't change too.
		func() {
			slots.fallback[math.MaxUint64] = TaskSession{}
			fallbackLen := len(slots.fallback)
			delCount := slots.delCount
			defer delete(slots.fallback, math.MaxUint64)

			slots.Del(math.MaxUint64)
			assert.Equal(
				t,
				len(slots.fallback), fallbackLen,
				"fallback length must not change after system deletion",
			)
			assert.Equal(
				t,
				slots.delCount, delCount,
				"delete count must not change after system deletion",
			)
		}()
	})

	t.Run("slots capacity guards", func(t *testing.T) {
		t.Run("only allow the power of two capacity", func(t *testing.T) {
			_, err := New[[8]byte](88888)
			assert.Error(t, err, "capacity must be a power of 2")
		})

		t.Run("capacity must be greater than 4096", func(t *testing.T) {
			_, err := New[TaskSession](1024)
			assert.Error(t, err, "capacity must not be lower than 4096")
		})
	})

	t.Run("generic type guards", func(t *testing.T) {
		t.Run("generic type must not be empty", func(t *testing.T) {
			_, err := New[struct{}](capacity)
			assert.Error(t, err, "generic type must not be empty")
		})

		t.Run("generic type must size must be a factor of 8", func(t *testing.T) {
			_, err := New[[5]byte](capacity)
			assert.Error(t, err, "generic type must size must be a factor of 8")

			_, err = New[[15]byte](capacity)
			assert.Error(t, err, "generic type must size must be a factor of 8")
		})
	})

}

func FuzzSlots_SinglePollerStress(f *testing.F) {
	// The only seed.
	f.Add(capacity)

	const pqCap = (1 << 12) - 1

	f.Fuzz(func(t *testing.T, cap int) {
		// Allow only good seeds, just because.
		if bits.OnesCount(uint(cap)) != 1 || cap < 4096 || cap > 1<<20 {
			t.Skip()
		}

		pq := pqueue.New[uint64](pqCap)

		slots, err := New[TaskSession](cap)
		if err != nil {
			t.Fatal(err)
		}

		var wg sync.WaitGroup
		stopChan := make(chan struct{})

		// Channel of idx transmission from translator to poller which mimics CQ ring of io_uring.
		// The capacity of channel is intentionally big to provoke fallbacks.
		cqBufferChan := make(chan uint64, cap*2)

		// 1. Translator thread (Strictly Single-Threaded).
		wg.Go(func() {
			defer close(cqBufferChan)
			rnd := NewFastRand(math.MaxUint64)

			var taskID int
			const flushBarrier = 1 << 14
			var flushCount int
			for {
				select {
				case <-stopChan:
					// Got stop signal. Push everything from the queue and exit.
					for {
						idx, ok := pq.Pop()
						if !ok {
							return
						}

						cqBufferChan <- idx
					}

				default:
				}

				idx := slots.Add(TaskSession{FD: taskID})
				for {
					// Try to push into the queue first.
					if pq.Push(rnd.NextRangePow2(2048), idx) {
						break
					}

					// Nah, queue is full. Take an element from there and push it. Then push the new one
					// as one slot has been freed.

					oldIdx, _ := pq.Pop()
					cqBufferChan <- oldIdx
				}

				// Empty the queue once we hit the barrier.
				flushCount++
				if flushCount&(flushBarrier-1) == 0 {
					for {
						flushIdx, ok := pq.Pop()
						if !ok {
							break
						}

						select {
						case cqBufferChan <- flushIdx:
						}
					}
				}
			}
		})

		// 2. Single CQ POLLER (Strictly Single-Threaded)
		wg.Go(func() {
			for idx := range cqBufferChan {
				// 100% Lock-free data validation before the removal.
				task, ok := slots.Get(idx)
				if !ok {
					t.Errorf("CRITICAL INVARIANT VIOLATION: task with idx %d has lost!", idx)
					return
				}
				if idx < uint64(cap) && task.FD < 0 {
					t.Errorf("CRITICAL DATA CORRUPTION: slot %d contains corrupted task memory %#v", idx, task)
					return
				}

				slots.Del(idx)
			}
		})

		// Fuzzer gets 30-50 millisecs.
		time.Sleep(30 * time.Millisecond)
		close(stopChan)
		wg.Wait()
	})
}

type FastRand struct {
	state uint64
}

func NewFastRand(seed uint64) *FastRand {
	return &FastRand{state: seed}
}

func (r *FastRand) Next() uint64 {
	r.state = r.state*6364136223846793005 + 1442695040888963407
	return r.state
}

func (r *FastRand) NextRangePow2(pow2 uint64) int {
	return int(r.Next() & (pow2 - 1))
}
