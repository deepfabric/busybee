package queue

import (
	"context"
	"fmt"
	"time"

	"github.com/deepfabric/busybee/pkg/metric"
	"github.com/deepfabric/busybee/pkg/pb/metapb"
	"github.com/deepfabric/busybee/pkg/pb/rpcpb"
	"github.com/deepfabric/busybee/pkg/storage"
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
	Start(batch, concurrency uint64, fn func(uint64, ...[]byte) (uint64, error))
	// Stop stop consumer
	Stop()
}

type consumer struct {
	consumer   []byte
	id         uint64
	partitions uint32
	store      storage.Storage
	cancels    []context.CancelFunc
}

// NewConsumer returns a consumer
func NewConsumer(id uint64, store storage.Storage, name []byte) (Consumer, error) {
	value, err := store.Get(storage.TenantMetadataKey(id))
	if err != nil {
		metric.IncStorageFailed()
		return nil, err
	}
	if len(value) == 0 {
		return nil, fmt.Errorf("%s queue %d not created",
			string(name),
			id)
	}

	meta := &metapb.Tenant{}
	protoc.MustUnmarshal(meta, value)
	return &consumer{
		id:         id,
		partitions: meta.Input.Partitions,
		store:      store,
		consumer:   name,
	}, nil
}

func (c *consumer) Start(batch, concurrency uint64, fn func(uint64, ...[]byte) (uint64, error)) {
	for i := uint32(0); i < c.partitions; i++ {
		ctx, cancel := context.WithCancel(context.Background())
		c.startPartition(ctx, i, batch, concurrency, fn)
		c.cancels = append(c.cancels, cancel)
	}
}

func (c *consumer) Stop() {
	for _, cancel := range c.cancels {
		cancel()
	}
}

func (c *consumer) startPartition(ctx context.Context, idx uint32, batch, concurrency uint64, fn func(uint64, ...[]byte) (uint64, error)) {
	offset := uint64(0)
	key := storage.PartitionKey(c.id, idx)
	go func() {
		resp := &rpcpb.BytesSliceResponse{}
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
				req.CompletedOffset = offset
				req.Consumer = c.consumer
				req.Count = batch

				value, err := c.store.ExecCommandWithGroup(req, metapb.TenantInputGroup)
				if err != nil {
					metric.IncStorageFailed()
					logger.Errorf("%s failed with %+v, retry after 10s",
						string(c.consumer),
						err)
					time.Sleep(time.Second * 10)
					continue
				}

				resp.Reset()
				protoc.MustUnmarshal(resp, value)

				if len(resp.Values) == 0 {
					time.Sleep(time.Second)
					continue
				}

				completedOffset, err := fn(resp.LastValue, resp.Values...)
				if completedOffset > offset {
					offset = completedOffset
				}
				if err != nil {
					logger.Errorf("%s handle completed at %d, failed with %+v",
						string(c.consumer),
						completedOffset,
						err)
					continue
				}
				offset = resp.LastValue
			}
		}
	}()
}
