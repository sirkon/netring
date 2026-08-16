package iouring

const (
	setupSQPoll    = 1 << 1
	setupSQAff     = 1 << 2 // Привязка к конкретному CPU-ядру
	featSingleMMap = 1 << 0 // Общая память для SQ и CQ (ядра 5.4+)

	offSQes   = 0x10000000
	offSQRing = 0
	offCQRing = 0x8000000

	enterSQWakeup = 1 << 1
	sqNeedWakup   = 1 << 0

	OPRead = 22
)
