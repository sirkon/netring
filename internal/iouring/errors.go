package iouring

import (
	"fmt"
)

// ErrorSQ ошибки добавления в очередь.
type ErrorSQ int

const (
	// ErrSQFull не удалось положить задачу в SQ.
	ErrSQFull ErrorSQ = iota + 1
)

func (e ErrorSQ) Error() string {
	switch e {
	case ErrSQFull:
		return "io_uring:SQ_FULL"
	default:
		return fmt.Sprintf("io_uring:SQ_UNKNOWN(%d)", e)
	}
}
