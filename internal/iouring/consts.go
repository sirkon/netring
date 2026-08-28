package iouring

import (
	"math"
)

const (
	ioUringSetupSQPoll    = 1 << 1
	ioUringSetupSQAff     = 1 << 2 // Bind to a specific CPU core
	ioUringFeatSingleMMap = 1 << 0 // Shared memory for SQ and CQ (kernel 5.4+)

	ioUringEnterSQWakeup  = 1 << 1
	ioUringSQNeedWakeup   = 1 << 0
	ioUringEnterGetEvents = 1 << 0

	sysOffSQes   = 0x10000000
	sysOffSQRing = 0
	sysOffCQRing = 0x8000000

	ioUringOPRead = 22
)

const (
	periodicalTimerTaskID = math.MaxUint64 - math.MaxUint32
)
