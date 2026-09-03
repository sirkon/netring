package taskslots

import (
	"runtime"
	"testing"

	"github.com/sirkon/blog/beer"
)

func TestRegression(t *testing.T) {
	t.Run("infinite loop on afteer hot path overload", func(t *testing.T) {
		// An implementation specific test.

		const capacity = 8192
		ts, err := New[uint64](capacity)
		if err != nil {
			t.Fatal(beer.Wrap(err, "create task slots"))
		}

		var slotsIDx uint64
		for i := range capacity + 1 {
			slotsIDx = ts.Add(uint64(i))
		}

		// -------------
		oldWave := ts.wave
		finished := make(chan struct{})
		ts.Del(slotsIDx)
		go func() {
			defer close(finished)

			ts.Add(slotsIDx + 1)
		}()

		// -------------
		// Wait till that goroutine pushed wave too far.
		var count uint64
		for abs(int(ts.wave)-int(oldWave)) <= 1 {
			count++
			if count&1023 == 0 {
				runtime.Gosched()
			}
			select {
			case <-finished:
				// That Add ended, return.
				return
			default:
			}
		}

		t.Fatal("stuck in the loop")
	})
}

func abs(n int) int {
	if n < 0 {
		return -n
	}

	return n
}
