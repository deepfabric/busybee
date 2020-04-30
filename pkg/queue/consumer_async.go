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
	Start(expect uint64, filter func([]byte) (interface{}, bool), cb func(uint32, []interface{}) (int, error))
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
func (c *asyncConsumer) Start(expect uint64, filter func([]byte) (interface{}, bool), cb func(uint32, []interface{}) (int, error)) {
	c.Lock()
	defer c.Unlock()

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
			var values []interface{}

			for {
				values = values[:0]
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

					// no new data and no fetched data, re-fetch after 1 seconds
					if len(resp.Items) == 0 && len(values) == 0 {
						time.Sleep(time.Second)
						continue
					}

					for _, value := range resp.Items {
						if v, ok := filter(value); ok {
							values = append(values, v)
						}
					}

					if resp.LastOffset > 0 {
						from = resp.LastOffset
					}

					if uint64(len(values)) >= expect || // fetched >= expect
						(len(resp.Items) == 0 && len(values) > 0) { // no new data

						for {
							if c.stopped() {
								logger.Infof("%s stopped", tag)
								return
							}

							completedIndex, err := cb(p, values)
							if err != nil {
								logger.Errorf("%s handle completed at %d, failed with %+v",
									tag,
									completedIndex,
									err)
								values = values[completedIndex+1:]
								continue
							}

							break
						}

						err := c.commit(p, from)
						if err != nil {
							// just log it, ant commit next time
							logger.Errorf("%s commit completed offset %d failed with %+v",
								tag,
								resp.LastOffset,
								err)
						}
						break
					}
				}
			}
		}(i)
	}
}

func (c *asyncConsumer) commit(partition uint32, completed uint64) error {
	req := rpcpb.AcquireQueueCommitRequest()
	req.ID = c.tid
	req.Consumer = c.consumer
	req.Partition = partition
	req.CompletedOffset = completed

	_, err := c.store.ExecCommandWithGroup(req, c.group)
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
