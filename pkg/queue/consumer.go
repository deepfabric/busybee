package queue

import (
	"context"
	"fmt"
	"github.com/deepfabric/busybee/pkg/pb/metapb"
	"github.com/deepfabric/busybee/pkg/pb/rpcpb"
	"github.com/deepfabric/busybee/pkg/storage"
	"github.com/fagongzi/goetty"
	"github.com/fagongzi/log"
	"github.com/fagongzi/util/protoc"
	"time"
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
	Start(batch uint64, fn func(uint64, ...[]byte) error)
	// Stop stop consumer
	Stop()
}

type consumer struct {
	consumer   []byte
	id         uint64
	group      metapb.Group
	partitions uint64
	store      storage.Storage
	cancels    []context.CancelFunc
}

// NewConsumer returns a consumer
func NewConsumer(id uint64, group metapb.Group, store storage.Storage, name []byte) (Consumer, error) {
	value, err := store.Get(storage.QueueMetadataKey(id, group))
	if err != nil {
		return nil, err
	}
	if len(value) == 0 {
		return nil, fmt.Errorf("%s group %s queue %d not created",
			string(name),
			group.String(),
			id)
	}

	partitions := goetty.Byte2UInt64(value)
	return &consumer{
		id:         id,
		partitions: partitions,
		store:      store,
		group:      group,
		consumer:   name,
	}, nil
}

func (c *consumer) Start(batch uint64, fn func(uint64, ...[]byte) error) {
	for i := uint64(0); i < c.partitions; i++ {
		ctx, cancel := context.WithCancel(context.Background())
		c.startPartition(ctx, i, batch, fn)
		c.cancels = append(c.cancels, cancel)
	}
}

func (c *consumer) Stop() {
	for _, cancel := range c.cancels {
		cancel()
	}
}

func (c *consumer) startPartition(ctx context.Context, idx, batch uint64, fn func(uint64, ...[]byte) error) {
	offset := uint64(0)
	key := PartitionKey(c.id, idx)
	go func() {
		for {
			select {
			case <-ctx.Done():
				logger.Infof("%s partition %d stopped",
					string(c.consumer),
					idx)
				return
			default:
				req := rpcpb.AcquireQueueFetchRequest()
				req.Key = key
				req.AfterOffset = offset
				req.Consumer = c.consumer
				req.Count = batch

				value, err := c.store.ExecCommandWithGroup(req, c.group)
				if err != nil {
					logger.Errorf("%s failed with %+v, retry after 10s",
						string(c.consumer),
						err)
					time.Sleep(time.Second * 10)
					continue
				}

				resp := rpcpb.AcquireBytesSliceResponse()
				protoc.MustUnmarshal(resp, value)

				if len(resp.Items) == 0 {
					time.Sleep(time.Second)
					continue
				}

				err = fn(resp.LastOffset, resp.Items...)
				if err != nil {
					logger.Errorf("%s handle failed with %+v",
						string(c.consumer),
						err)
					continue
				}
				offset = resp.LastOffset
			}
		}
	}()
}
