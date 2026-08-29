package netring

import (
	"fmt"
	"math/bits"
	"strconv"

	"github.com/sirkon/blog/beer"

	"github.com/sirkon/netring/internal/taskslots"
)

// WithTasksBufferSize use this buffer size for task delivery channels.
func WithTasksBufferSize(n int) OptionSetter {
	return &netringOptionTasksChanBufferSize{
		size: n,
	}
}

// WithTasksChannelsCount use this to set up no of task delivery channels.
func WithTasksChannelsCount(n int) OptionSetter {
	return &netringOptionTasksChansShards{
		shards: n,
	}
}

// WithSlots use this to set up custom no of fast slots.
func WithSlots(n int) OptionSetter {
	return &netringOptionsSlotsSetter{
		noOfSlots: n,
	}
}

type netringOptions struct {
	tasksChanBuffer int
	tasksChanShards int

	slots *taskslots.Slots[*taskCell]
}

type OptionSetter interface {
	fmt.Stringer
	apply(*netringOptions) error
}

// --------------------------------------------------------------------------------------------------------------------

type netringOptionTasksChanBufferSize struct {
	size int
}

func (n *netringOptionTasksChanBufferSize) String() string {
	return "set tasks channel buffer size to " + strconv.Itoa(n.size)
}

func (n *netringOptionTasksChanBufferSize) apply(options *netringOptions) error {
	if n.size <= 0 {
		return beer.Newf("buffer size must be positive, got %d", n.size)
	}

	options.tasksChanBuffer = n.size
	return nil
}

// --------------------------------------------------------------------------------------------------------------------

type netringOptionTasksChansShards struct {
	shards int
}

func (n *netringOptionTasksChansShards) String() string {
	return "set no of tasks channels to " + strconv.Itoa(n.shards)
}

func (n *netringOptionTasksChansShards) apply(options *netringOptions) error {
	if n.shards <= 0 {
		return beer.Newf("no of tasks channels must be positive, got %d", n.shards)
	}

	if bits.OnesCount(uint(n.shards)) != 1 {
		return beer.Newf("no of tasks channels must be a power of two, was requested with odd %d", n.shards)
	}

	options.tasksChanShards = n.shards
	return nil
}

// --------------------------------------------------------------------------------------------------------------------

type netringOptionsSlotsSetter struct {
	noOfSlots int
}

func (n *netringOptionsSlotsSetter) String() string {
	return fmt.Sprintf("create slotter with %d fast slots", n.noOfSlots)
}

func (n *netringOptionsSlotsSetter) apply(options *netringOptions) error {
	slots, err := taskslots.New[*taskCell](n.noOfSlots)
	if err != nil {
		return err
	}

	options.slots = slots
	return nil
}
