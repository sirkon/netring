package pqueue

import (
	"slices"
	"testing"

	"github.com/alecthomas/assert/v2"
)

func TestPriorityQueue_Playground(t *testing.T) {
	weights := []int{
		10,
		100,
		99,
		1000,
		995,
		9,
		1001,
		8,
	}

	wantSeq := slices.Clone(weights)
	slices.Sort(wantSeq)

	pq := New[int](100)
	for _, weight := range weights {
		pq.Push(weight, weight)
	}
	assert.Equal(t, len(weights), pq.Len())

	var got []int
	for {
		w, ok := pq.Pop()
		if !ok {
			break
		}

		got = append(got, w)
	}

	assert.Equal(t, wantSeq, got)
	assert.Equal(t, 0, pq.Len())
}
