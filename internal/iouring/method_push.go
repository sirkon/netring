package iouring

import (
	"sync/atomic"

	"github.com/sirkon/blog/beer"
)

// Push добавляет задачу в SQ.
// Может возвратить ошибку ErrSQFull если очередь задач забита - именно само значение ErrSQFull, без врапов.
func (r *IOUring) Push(entry SQEntry) error {
	tail := *r.SQTail
	head := atomic.LoadUint32(r.SQHead)
	mask := *r.SQMask

	if tail-head >= *r.SQEntries {
		return ErrSQFull
	}

	index := tail & mask

	// Записываем данные в mmap-нутую память
	r.SQ[index] = entry
	r.SQArray[index] = index

	// Атомарно двигаем хвост
	atomic.StoreUint32(r.SQTail, tail+1)

	// Атомарно читаем флаги. Если ядро дрыхнет — будим его!
	if (atomic.LoadUint32(r.SQFlags) & sqNeedWakup) != 0 {
		if err := r.Wakeup(); err != nil {
			return beer.Wrap(err, "wakeup kernel poller")
		}
	}

	return nil
}
