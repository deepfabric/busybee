package queue

import (
	"testing"
	"time"

	"github.com/deepfabric/busybee/pkg/pb/metapb"
	"github.com/deepfabric/busybee/pkg/storage"
	"github.com/fagongzi/goetty"
	"github.com/fagongzi/util/protoc"
	"github.com/stretchr/testify/assert"
)

func TestConsumerWithTenantNotCreated(t *testing.T) {
	store, deferFunc := storage.NewTestStorage(t, true)
	defer deferFunc()

	tid := uint64(10000)
	g1 := []byte("g1")
	c, err := newConsumerWithGroup(tid, store, g1, metapb.DefaultGroup)
	assert.Nil(t, c, "TestConsumerWithTenantNotCreated failed")
	assert.Error(t, err, "TestConsumerWithTenantNotCreated failed")
}

func TestConsumerWithQueueMetaNotCreated(t *testing.T) {
	store, deferFunc := storage.NewTestStorage(t, true)
	defer deferFunc()

	tid := uint64(10000)
	g1 := []byte("g1")

	err := store.Set(storage.TenantMetadataKey(tid), protoc.MustMarshal(&metapb.Tenant{}))
	assert.NoError(t, err, "TestConsumerWithQueueMetaNotCreated failed")

	c, err := newConsumerWithGroup(tid, store, g1, metapb.DefaultGroup)
	assert.Nil(t, c, "TestConsumerWithQueueMetaNotCreated failed")
	assert.Error(t, err, "TestConsumerWithQueueMetaNotCreated failed")
}

func TestConsumerStartAndStop(t *testing.T) {
	store, deferFunc := storage.NewTestStorage(t, true)
	defer deferFunc()

	buf := goetty.NewByteBuf(32)
	tid := uint64(10000)
	g1 := []byte("g1")

	err := store.Set(storage.TenantMetadataKey(tid), protoc.MustMarshal(&metapb.Tenant{}))
	assert.NoError(t, err, "TestConsumerStartAndStop failed")

	err = store.Set(storage.QueueMetaKey(tid, buf), protoc.MustMarshal(&metapb.QueueState{
		Partitions: 2,
		Timeout:    60,
		States:     make([]metapb.Partiton, 2, 2),
	}))
	assert.NoError(t, err, "TestConsumerStartAndStop failed")

	c, err := newConsumerWithGroup(tid, store, g1, metapb.DefaultGroup)
	assert.NoError(t, err, "TestConsumerStartAndStop failed")
	assert.NotNil(t, c, "TestConsumerStartAndStop failed")

	cb := func(uint32, uint64, ...[]byte) (uint64, error) {
		return 0, nil
	}
	c.Start(16, cb)

	time.Sleep(time.Second)
	assert.Equal(t, 2, len(c.(*consumer).partitions), "TestConsumerStartAndStop failed")

	c.Stop()
	time.Sleep(time.Second * 2)
	assert.Equal(t, 0, len(c.(*consumer).partitions), "TestConsumerStartAndStop failed")
}

func TestConsumer(t *testing.T) {
	store, deferFunc := storage.NewTestStorage(t, true)
	defer deferFunc()

	buf := goetty.NewByteBuf(32)
	tid := uint64(10000)
	g1 := []byte("g1")

	store.Set(storage.TenantMetadataKey(tid), protoc.MustMarshal(&metapb.Tenant{}))
	store.Set(storage.QueueMetaKey(tid, buf), protoc.MustMarshal(&metapb.QueueState{
		Partitions: 3,
		Timeout:    60,
		States:     make([]metapb.Partiton, 3, 3),
	}))

	c, err := newConsumerWithGroup(tid, store, g1, metapb.DefaultGroup)
	assert.NoError(t, err, "TestConsumer failed")
	assert.NotNil(t, c, "TestConsumer failed")

	c0 := 0
	c1 := 0
	c2 := 0
	cb := func(partition uint32, offset uint64, data ...[]byte) (uint64, error) {
		if partition == 0 {
			c0 += len(data)
		} else if partition == 1 {
			c1 += len(data)
		} else if partition == 2 {
			c2 += len(data)
		}
		return offset, nil
	}
	c.Start(2, cb)

	err = store.PutToQueue(tid, 0, metapb.DefaultGroup, []byte("1"))
	assert.NoError(t, err, "TestConsumer failed")

	err = store.PutToQueue(tid, 1, metapb.DefaultGroup, []byte("1"), []byte("1"))
	assert.NoError(t, err, "TestConsumer failed")

	err = store.PutToQueue(tid, 2, metapb.DefaultGroup, []byte("1"), []byte("1"), []byte("1"))
	assert.NoError(t, err, "TestConsumer failed")

	time.Sleep(time.Second * 2)

	assert.Equal(t, 1, c0, "TestConsumer failed")
	assert.Equal(t, 2, c1, "TestConsumer failed")
	assert.Equal(t, 3, c2, "TestConsumer failed")
}

func TestConsumerRemovePartition(t *testing.T) {
	store, deferFunc := storage.NewTestStorage(t, true)
	defer deferFunc()

	buf := goetty.NewByteBuf(32)
	tid := uint64(10000)
	g1 := []byte("g1")

	err := store.Set(storage.TenantMetadataKey(tid), protoc.MustMarshal(&metapb.Tenant{}))
	assert.NoError(t, err, "TestConsumerRemovePartition failed")

	err = store.Set(storage.QueueMetaKey(tid, buf), protoc.MustMarshal(&metapb.QueueState{
		Partitions: 2,
		Timeout:    60,
		States:     make([]metapb.Partiton, 2, 2),
	}))
	assert.NoError(t, err, "TestConsumerRemovePartition failed")

	cb := func(uint32, uint64, ...[]byte) (uint64, error) {
		return 0, nil
	}

	c, err := newConsumerWithGroup(tid, store, g1, metapb.DefaultGroup)
	assert.NoError(t, err, "TestConsumerRemovePartition failed")
	assert.NotNil(t, c, "TestConsumerRemovePartition failed")

	c.Start(16, cb)
	time.Sleep(time.Second)
	assert.Equal(t, 2, len(c.(*consumer).partitions), "TestConsumerStartAndStop failed")

	c2, err := newConsumerWithGroup(tid, store, g1, metapb.DefaultGroup)
	assert.NoError(t, err, "TestConsumerRemovePartition failed")
	assert.NotNil(t, c, "TestConsumerRemovePartition failed")

	c2.Start(16, cb)
	time.Sleep(time.Second * 2)
	assert.Equal(t, 1, len(c2.(*consumer).partitions), "TestConsumerStartAndStop failed")

	assert.Equal(t, 1, len(c.(*consumer).partitions), "TestConsumerStartAndStop failed")
}
