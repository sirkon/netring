package netring

import (
	"math"
)

const (
	maxSpinLimit = 50_000

	// defaultSlotsCapacity is the hot-path size of the in-flight task slot
	// table (taskslots requires a power of 2 strictly greater than 4096).
	defaultSlotsCapacity = 1 << 20

	// defaultShardsCount is the number of worker -> translator shard channels.
	// Per-fd ordering is preserved by routing fd % shardCount; the count is a
	// power of 2 so the modulo compiles into a bitmask.
	defaultShardsCount = 64

	// defaultShardBuffering is the per-shard channel buffer: enough slack to
	// absorb translator hiccups before workers block on backpressure.
	defaultShardBuffering = 256

	// maxPbufGroups is the kernel limit on distinct buffer group ids (a 16-bit
	// bgid space, IORING_REGISTER_PBUF_RING contract); it sizes NetRing.pbrs.
	maxPbufGroups = 1 << 16

	// periodicalTimerTaskID is the reserved UserData of the self-rearming
	// periodic timer SQE (ARCH.md); it sits far above any taskslots index.
	periodicalTimerTaskID = math.MaxUint64 - math.MaxUint32
)
