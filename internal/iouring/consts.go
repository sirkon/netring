package iouring

import (
	"math"
)

const (
	setupSQPoll    = 1 << 1
	setupSQAff     = 1 << 2 // Bind to a specific CPU core
	featSingleMMap = 1 << 0 // Shared memory for SQ and CQ (kernel 5.4+)

	offSQes   = 0x10000000
	offSQRing = 0
	offCQRing = 0x8000000

	enterSQWakeup  = 1 << 1
	sqNeedWakup    = 1 << 0
	enterGetEvents = 1 << 0

	OPRead = 22

	maxSpinLimit = 50_000
)

const (
	periodicalTimerTaskID = math.MaxUint64 - math.MaxUint32
)
