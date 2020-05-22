package queue

import (
	"fmt"
	"sync"
	"time"

	"github.com/deepfabric/beehive/util"
	"github.com/deepfabric/busybee/pkg/metric"
	"github.com/deepfabric/busybee/pkg/pb/metapb"
	"github.com/deepfabric/busybee/pkg/pb/rpcpb"
	"github.com/deepfabric/busybee/pkg/storage"
	"github.com/fagongzi/goetty"
	"github.com/fagongzi/log"
	"github.com/fagongzi/util/protoc"
)

var (
	logger log.Logger
)

func init() {
	logger = log.NewLoggerWithPrefix("[queue]")
}

// Consumer a simple queue consumer
type Consumer interface {
	// Start start the consumer
	Start(batch uint64, cb func(uint32, uint64, ...[]byte) (uint64, error))
	// Stop stop consumer
	Stop()
}

type consumer struct {
	sync.RWMutex

	rejoin        bool
	group         metapb.Group
	consumerGroup []byte
	consumer      uint32
	id            uint64
	store         storage.Storage

	running    bool
	partitions map[uint32]struct{}
	cb         func(uint32, uint64, ...[]byte) (uint64, error)
	batch      uint64
}

// NewConsumer returns a queue consumer
func NewConsumer(id uint64, rejoin bool, store storage.Storage, consumerGroup []byte) (Consumer, error) {
	return newConsumerWithGroup(id, rejoin, store, consumerGroup, metapb.TenantInputGroup)
}

func newConsumerWithGroup(id uint64, rejoin bool, store storage.Storage, consumerGroup []byte, group metapb.Group) (Consumer, error) {
	value, err := store.Get(storage.TenantMetadataKey(id))
	if err != nil {
		metric.IncStorageFailed()
		return nil, err
	}
	if len(value) == 0 {
		return nil, fmt.Errorf("tenant %d not created",
			id)
	}

	buf := goetty.NewByteBuf(32)
	defer buf.Release()

	value, err = store.GetWithGroup(storage.QueueMetaKey(id, 0), group)
	if err != nil {
		metric.IncStorageFailed()
		return nil, err
	}
	if len(value) == 0 {
		return nil, fmt.Errorf("tenant %d queue state not created",
			id)
	}

	return &consumer{
		id:            id,
		store:         store,
		consumerGroup: consumerGroup,
		group:         group,
		rejoin:        rejoin,
	}, nil
}

func (c *consumer) Start(batch uint64, cb func(uint32, uint64, ...[]byte) (uint64, error)) {
	c.Lock()

	if c.running {
		c.Unlock()
		return
	}

	c.running = true
	c.batch = batch
	c.cb = cb
	c.Unlock()

	c.doJoin(nil)
}

func (c *consumer) Stop() {
	c.Lock()
	defer c.Unlock()

	c.running = false
}

func (c *consumer) stopped() bool {
	c.RLock()
	defer c.RUnlock()

	return !c.running
}

func (c *consumer) doJoin(arg interface{}) {
	if c.stopped() {
		return
	}

	req := rpcpb.AcquireQueueJoinGroupRequest()
	req.ID = c.id
	req.Group = c.consumerGroup
	req.Rejoin = c.rejoin
	req.Consumer = c.consumer
	c.store.AsyncExecCommandWithGroup(req, c.group, c.onJoinResp, nil)
}

func (c *consumer) onJoinResp(arg interface{}, value []byte, err error) {
	c.Lock()
	defer c.Unlock()

	if err != nil {
		c.retryJoin()
		return
	}

	resp := &rpcpb.QueueJoinGroupResponse{}
	protoc.MustUnmarshal(resp, value)

	// wait, retry later
	if len(resp.Partitions) == 0 {
		c.retryJoin()
		return
	}

	c.consumer = resp.Index
	c.partitions = make(map[uint32]struct{})
	for idx, partition := range resp.Partitions {
		c.partitions[partition] = struct{}{}
		c.startPartition(resp.Index, partition, resp.Versions[idx])
	}
}

func (c *consumer) removed(partition uint32) {
	c.Lock()
	defer c.Unlock()

	delete(c.partitions, partition)

	// all partition removed
	if c.running && len(c.partitions) == 0 {
		c.retryJoin()
	}
}

func (c *consumer) retryJoin() {
	util.DefaultTimeoutWheel().Schedule(time.Second, c.doJoin, nil)
}

func (c *consumer) startPartition(consumerIndex, partition uint32, version uint64) {
	tag := fmt.Sprintf("%d/%s/p(%d)/g(%s)/v%d",
		c.id,
		c.group.String(),
		partition,
		string(c.consumerGroup),
		version,
	)

	logger.Infof("%s started", tag)

	offset := uint64(0)
	key := storage.PartitionKey(c.id, partition)
	go func() {
		resp := &rpcpb.QueueFetchResponse{}
		for {
			if c.stopped() {
				c.removed(partition)

				logger.Infof("%s stopped", tag)
				return
			}

			req := rpcpb.AcquireQueueFetchRequest()
			req.Key = key
			req.ID = c.id
			req.Group = c.consumerGroup
			req.Consumer = consumerIndex
			req.Partition = partition
			req.Version = version
			req.CompletedOffset = offset
			req.Count = c.batch

			value, err := c.store.ExecCommandWithGroup(req, c.group)
			if err != nil {
				metric.IncStorageFailed()
				logger.Errorf("%s failed with %+v, retry after 10s",
					tag,
					err)
				time.Sleep(time.Second * 10)
				continue
			}

			resp.Reset()
			protoc.MustUnmarshal(resp, value)

			if resp.Removed {
				c.removed(partition)

				logger.Infof("%s stopped with stale version", tag)
				return
			}

			if len(resp.Items) == 0 {
				time.Sleep(time.Second)
				continue
			}

			completedOffset, err := c.cb(partition, resp.LastOffset, resp.Items...)
			if completedOffset > offset {
				offset = completedOffset
			}
			if err != nil {
				logger.Errorf("%s handle completed at %d, failed with %+v",
					tag,
					completedOffset,
					err)
				continue
			}
			offset = resp.LastOffset
		}
	}()
}
