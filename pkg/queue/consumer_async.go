package queue

import (
	"fmt"
	"sync"
	"time"

	"github.com/deepfabric/busybee/pkg/metric"
	"github.com/deepfabric/busybee/pkg/pb/metapb"
	"github.com/deepfabric/busybee/pkg/pb/rpcpb"
	"github.com/deepfabric/busybee/pkg/storage"
	"github.com/fagongzi/util/protoc"
)

var (
	scanSize = uint64(128)
)

// AsyncConsumer async consumer
type AsyncConsumer interface {
	// Start start the consumer
	Start(cb func(uint32, uint64, [][]byte))
	// Commit commit completed offset
	Commit(map[uint32]uint64) error
	// Stop stop consumer
	Stop()
}

type asyncConsumer struct {
	sync.RWMutex

	tid      uint64
	group    metapb.Group
	store    storage.Storage
	consumer []byte
	state    metapb.TenantQueue
	running  bool
}

// NewAsyncConsumer create a async consumer
func NewAsyncConsumer(tid uint64, store storage.Storage, consumer []byte) (AsyncConsumer, error) {
	return newAsyncConsumerWithGroup(tid, store, consumer, metapb.TenantInputGroup)
}

func newAsyncConsumerWithGroup(tid uint64, store storage.Storage, consumer []byte, group metapb.Group) (AsyncConsumer, error) {
	value, err := store.Get(storage.TenantMetadataKey(tid))
	if err != nil {
		metric.IncStorageFailed()
		return nil, err
	}
	if len(value) == 0 {
		return nil, fmt.Errorf("tenant %d not created",
			tid)
	}

	metadata := &metapb.Tenant{}
	protoc.MustUnmarshal(metadata, value)

	return &asyncConsumer{
		tid:      tid,
		group:    group,
		store:    store,
		consumer: consumer,
		state:    metadata.Input,
	}, nil
}

// Start start the consumer
func (c *asyncConsumer) Start(cb func(uint32, uint64, [][]byte)) {
	c.Lock()
	defer c.Unlock()

	if c.running {
		return
	}

	c.running = true
	for i := uint32(0); i < c.state.Partitions; i++ {
		go func(p uint32) {
			tag := fmt.Sprintf("%d/%s/p(%d)/c(%s)",
				c.tid,
				c.group.String(),
				p,
				string(c.consumer))

			key := storage.PartitionKey(c.tid, p)
			resp := &rpcpb.QueueFetchResponse{}
			from := uint64(0)

			for {
				if c.stopped() {
					logger.Infof("%s stopped", tag)
					return
				}

				req := rpcpb.AcquireQueueScanRequest()
				req.Key = key
				req.CompletedOffset = from
				req.Count = scanSize
				req.Consumer = c.consumer
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

				if len(resp.Items) == 0 {
					cb(p, resp.LastOffset, resp.Items)
					time.Sleep(time.Second)
					continue
				}

				cb(p, resp.LastOffset, resp.Items)
				from = resp.LastOffset
			}
		}(i)
	}
}

func (c *asyncConsumer) Commit(completed map[uint32]uint64) error {
	if len(completed) == 0 {
		return nil
	}

	var err error
	var wg sync.WaitGroup
	wg.Add(len(completed))

	cb := func(arg interface{}, data []byte, rerr error) {
		if rerr != nil {
			err = rerr
		}
		wg.Done()
	}

	for p, offset := range completed {
		req := rpcpb.AcquireQueueCommitRequest()
		req.ID = c.tid
		req.Consumer = c.consumer
		req.Partition = p
		req.CompletedOffset = offset
		c.store.AsyncExecCommandWithGroup(req, c.group, cb, nil)
	}

	wg.Wait()
	return err
}

func (c *asyncConsumer) Stop() {
	c.Lock()
	defer c.Unlock()

	c.running = false
}

func (c *asyncConsumer) stopped() bool {
	c.RLock()
	defer c.RUnlock()

	return !c.running
}
