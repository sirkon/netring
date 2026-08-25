package taskslots

import (
	"fmt"
	"math/rand"
	"reflect"
	"testing"

	"github.com/sirkon/blog/beer"
	"github.com/sirkon/deepequal"
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
	})
}

func BenchmarkSlots_Playground(b *testing.B) {
	ring := make([]uint64, capacity)
	ringMask := capacity - 1

	slots, err := New[TaskSession](capacity)
	if err != nil {
		b.Fatal(beer.Wrap(err, "create slots"))
	}

	slotsMap := make(map[uint64]TaskSession, capacity)

	b.Run("slots", func(b *testing.B) {
		b.ReportAllocs()

		for b.Loop() {
			slots.Reset()

			ringPos := 0
			for range capacity / 2 {
				ttt := TaskSession{
					FD:  ringPos + 1,
					Buf: nil,
				}
				idx := slots.Add(ttt)
				ring[ringPos] = idx
				ringPos++
			}

			var delPos int
			for range capacity * 8 {
				ttt := TaskSession{
					FD:  ringPos + 1,
					Buf: nil,
				}
				idx := slots.Add(ttt)
				ring[ringPos&ringMask] = idx
				ringPos++

				delIdx := ring[delPos&ringMask]
				slots.Del(delIdx)
				delPos++
			}
		}
	})

	b.Run("map", func(b *testing.B) {
		b.ReportAllocs()

		for b.Loop() {
			clear(slotsMap)
			ringPos := 0
			for range capacity / 2 {
				idx := len(slotsMap)
				slotsMap[uint64(idx)] = TaskSession{FD: ringPos + 1}
				ring[ringPos] = uint64(idx)
				ringPos++
			}

			var delPos int
			for range capacity * 8 {
				idx := len(slotsMap)
				slotsMap[uint64(idx)] = TaskSession{FD: ringPos + 1}
				ring[ringPos&ringMask] = uint64(idx)
				ringPos++

				delIdx := ring[delPos&ringMask]
				delete(slotsMap, delIdx)
				delPos++
			}
		}
	})
}

func BenchmarkSlogtsGet(b *testing.B) {
	capacities := []int{1 << 14, capacity, 1 << 20, 1 << 24, 1 << 26}
	for _, customCap := range capacities {
		b.Run(fmt.Sprintf("capacity-%d", customCap), func(b *testing.B) {
			slots, err := New[TaskSession](customCap)
			if err != nil {
				b.Fatal(beer.Wrap(err, "create slots"))
			}
			slotsMap := make(map[uint64]TaskSession, customCap)
			var idxs []uint64

			for i := range customCap * 3 / 4 {
				ttt := TaskSession{
					FD:  i + 1,
					Buf: nil,
				}
				idx := slots.Add(ttt)
				idxs = append(idxs, idx)
				slotsMap[idx] = ttt
			}

			rand.Shuffle(len(idxs), func(i, j int) {
				idxs[i], idxs[j] = idxs[j], idxs[i]
			})

			var slotsAcc int
			b.Run("slots", func(b *testing.B) {
				for b.Loop() {
					var acc int
					for _, idx := range idxs {
						t, ok := slots.Get(idx)
						if !ok {
							b.Fatalf("task %d not found", idx)
						}

						acc += t.FD
					}
					slotsAcc = acc
				}
			})

			var mapAcc int
			b.Run("map", func(b *testing.B) {
				for b.Loop() {
					var acc int
					for _, idx := range idxs {
						t, ok := slotsMap[idx]
						if !ok {
							b.Fatalf("task %d not found", idx)
						}

						acc += t.FD
					}
					mapAcc = acc
				}
			})

			if slotsAcc != mapAcc {
				b.Fatalf("slotsAcc(%d) != mapAcc(%d)", slotsAcc, mapAcc)
			}
		})
	}
}
