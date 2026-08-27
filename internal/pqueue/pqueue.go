package pqueue

type PriorityQueue[T any] struct {
	data  []queueSlot[T]
	len   int
	cap   int
	depth int
}

func New[T any](capacity int) *PriorityQueue[T] {
	data := make([]queueSlot[T], capacity)
	for i := range data {
		data[i].weight = -1
	}

	return &PriorityQueue[T]{
		data: data,
		len:  0,
		cap:  capacity,
	}
}

func (q *PriorityQueue[T]) Len() int {
	return q.len
}

func (q *PriorityQueue[T]) Cap() int {
	return q.cap
}

func (q *PriorityQueue[T]) Push(weight int, payload T) bool {
	if q.len == q.cap {
		return false
	}

	idx := q.len
	q.data[idx] = queueSlot[T]{
		weight:  weight,
		payload: payload,
	}
	q.len++

	// Now, pop the last element up if needed.
	for idx > 0 {
		pidx := parentIdx(idx)
		if q.data[pidx].weight > q.data[idx].weight {
			q.data[pidx], q.data[idx] = q.data[idx], q.data[pidx]
		} else {
			break
		}
		idx = pidx
	}

	return true
}

func (q *PriorityQueue[T]) Pop() (T, bool) {
	if q.len == 0 {
		var zero T
		return zero, false
	}

	v := q.data[0].payload
	lastIdx := q.len - 1
	q.data[0], q.data[lastIdx] = q.data[lastIdx], queueSlot[T]{
		weight: -1,
	}
	q.len--

	// Now, sink the current head where it belongs.
	var idx int
	for idx < q.len {
		// Take both children. If one is missing pick the existing and swap if needed, then stop.
		// If both are missing - stop.
		lidx := childrenArrayStartIdx(idx)
		if lidx >= q.len {
			return v, true
		}
		ridx := lidx + 1

		var pickIdx int
		if ridx >= q.len {
			// There's only left child
			pickIdx = lidx
		} else {
			// Choose the smallest if there are both of them.
			if q.data[lidx].weight <= q.data[ridx].weight {
				pickIdx = lidx
			} else {
				pickIdx = ridx
			}
		}

		// Now, look if we really need to swap. Stop otherwise.
		if q.data[idx].weight <= q.data[pickIdx].weight {
			return v, true
		}

		q.data[idx], q.data[pickIdx] = q.data[pickIdx], q.data[idx]
		idx = pickIdx
	}

	return v, true
}

func parentIdx(idx int) int {
	return (idx - 1) / 2
}

func childrenArrayStartIdx(idx int) int {
	return 2*idx + 1
}

type queueSlot[T any] struct {
	// negative weight means the value is absent.
	weight  int
	payload T
}
