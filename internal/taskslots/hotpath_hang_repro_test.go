package taskslots

import (
	"testing"
	"time"
)

// TestTaskslotsHotPathSpin reproduces the stuck Add hot loop on a fully busy
// ring and verifies the fix: a Del of a fallback task must release no ring
// slot and must not inflate delCount, otherwise the translator's saturation
// gate (free|(delCount-lastDelCount)) sees phantom pending releases and Add
// spins forever even though every word of the ring is full and idle.
func TestTaskslotsHotPathSpin(t *testing.T) {
	const capacity = 4096
	slots, err := New[TaskSession](capacity)
	if err != nil {
		t.Fatal(err)
	}

	// 1. Fill the ring completely.
	var prev uint64
	for i := range capacity {
		prev = slots.Add(TaskSession{FD: i})
	}
	if slots.free != 0 {
		t.Fatalf("expected free==0 after filling the ring, got %d", slots.free)
	}

	// 2. Two workload pairs with a late poller (Add before Del):
	//    * 1st Add goes to the fallback, its Del is a fallback deletion.
	//    * 2nd Add re-uses the slot just released by Del(4095) (the ring's
	//      only free slot at that moment), its Del releases a ring slot.
	//    The fallback deletion previously overflowed pollerBitmap (wordIdx
	//    out of bounds) and inflated delCount, leaving
	//    delCount-lastDelCount == 1 while free == 0: the stuck state.
	if slots.free != 0 {
		t.Fatalf("expected the ring to be fully busy, got free=%d", slots.free)
	}
	for i := range 2 {
		idx := slots.Add(TaskSession{FD: i})
		if i == 0 && idx < slots.cap {
			t.Fatalf("expected the first Add to go to the fallback on a full ring, got slotted idx=%d", idx)
		}
		slots.Del(prev)
		prev = idx
	}

	// Ledger must be consistent: every deletion accounted for, no phantom
	// pending releases, and the ring still fully busy.
	if slots.delCount != slots.lastDelCount {
		t.Fatalf("unexpected ledger: free=%d delCount=%d lastDelCount=%d (uncaught releases=%d) "+
			"the fallback deletion must not inflate delCount",
			slots.free, slots.delCount, slots.lastDelCount, slots.delCount-slots.lastDelCount)
	}
	if slots.free != 0 {
		t.Fatalf("expected free==0, got %d", slots.free)
	}

	// 3. A further Add must complete promptly: on the fully busy ring the
	//    wave walks all words, finds no released slots, but the saturation
	//    gate opens the fallback because delCount==lastDelCount. Watchdog:
	//    fail fast with a descriptive error instead of hanging the suite.
	finished := make(chan uint64, 1)
	go func() {
		finished <- slots.Add(TaskSession{FD: -1})
	}()

	select {
	case idx := <-finished:
		if idx < slots.cap {
			t.Fatalf("expected the Add on a fully busy ring to land in the fallback, got slotted idx=%d", idx)
		}
		t.Logf("successfully finished: Add fell back to idx=%d", idx)
	case <-time.After(3 * time.Second):
		t.Fatal("hot loop confirmed: Add spins forever with a fully busy ring and delCount > lastDelCount")
	}
}