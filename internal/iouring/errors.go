package iouring

import (
	"fmt"
)

// ErrorSQ defines submission queue errors.
type ErrorSQ int

const (
	// ErrSQFull is returned when a task cannot be placed into the SQ.
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
