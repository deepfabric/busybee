package notify

import (
	"testing"

	"github.com/deepfabric/busybee/pkg/pb/metapb"
	"github.com/deepfabric/busybee/pkg/pb/rpcpb"
	"github.com/deepfabric/busybee/pkg/storage"
	"github.com/fagongzi/goetty"
	"github.com/fagongzi/util/protoc"
	"github.com/stretchr/testify/assert"
)

func TestNotify(t *testing.T) {
	s, deferFunc := storage.NewTestStorage(t, true)
	defer deferFunc()
	buf := goetty.NewByteBuf(256)

	tenantID := uint64(1)
	s.Set(storage.QueueMetaKey(tenantID, 0, buf), protoc.MustMarshal(&metapb.QueueState{
		Partitions: 2,
	}))

	n := NewQueueBasedNotifierWithGroup(s, metapb.DefaultGroup)
	assert.NoError(t, n.Notify(tenantID, buf, []metapb.Notify{{
		UserID: 1,
	}}, nil), "TestNotify failed")

	data, err := s.Get(storage.QueueItemKey(storage.PartitionKey(tenantID, 0), 1, buf))
	assert.NoError(t, err, "TestNotify failed")
	assert.NotEmpty(t, data, "TestNotify failed")
}

func TestNotifyWithConditionNotExist(t *testing.T) {
	s, deferFunc := storage.NewTestStorage(t, true)
	defer deferFunc()

	buf := goetty.NewByteBuf(256)
	tenantID := uint64(1)
	s.Set(storage.QueueMetaKey(tenantID, 0, buf), protoc.MustMarshal(&metapb.QueueState{
		Partitions: 1,
	}))

	n := NewQueueBasedNotifierWithGroup(s, metapb.DefaultGroup)

	conditionKey := []byte("ckey")
	conditionValue := []byte("cvalue")

	for i := 0; i < 2; i++ {
		assert.NoError(t, n.Notify(tenantID, buf, []metapb.Notify{{
			UserID: 1,
		}}, &rpcpb.Condition{Key: conditionKey, Cmp: rpcpb.NotExists},
			conditionKey, conditionValue), "TestNotifyWithConditionNotExist failed")

		data, err := s.Get(storage.QueueItemKey(storage.PartitionKey(tenantID, 0), 1, buf))
		assert.NoError(t, err, "TestNotifyWithConditionNotExist failed")
		assert.NotEmpty(t, data, "TestNotifyWithConditionNotExist failed")

		data, err = s.Get(storage.QueueItemKey(storage.PartitionKey(tenantID, 0), 2, buf))
		assert.NoError(t, err, "TestNotifyWithConditionNotExist failed")
		assert.Empty(t, data, "TestNotifyWithConditionNotExist failed")
	}
}

func TestNotifyWithConditionExist(t *testing.T) {
	s, deferFunc := storage.NewTestStorage(t, true)
	defer deferFunc()

	buf := goetty.NewByteBuf(256)
	tenantID := uint64(1)
	s.Set(storage.QueueMetaKey(tenantID, 0, buf), protoc.MustMarshal(&metapb.QueueState{
		Partitions: 1,
	}))
	n := NewQueueBasedNotifierWithGroup(s, metapb.DefaultGroup)

	conditionKey := []byte("ckey")
	conditionValue := []byte("cvalue")

	assert.NoError(t, n.Notify(tenantID, buf, []metapb.Notify{{
		UserID: 1,
	}}, &rpcpb.Condition{Key: conditionKey, Cmp: rpcpb.Exists},
		conditionKey, conditionValue), "TestNotifyWithConditionExist failed")

	data, err := s.Get(storage.QueueItemKey(storage.PartitionKey(tenantID, 0), 1, buf))
	assert.NoError(t, err, "TestNotifyWithConditionExist failed")
	assert.Empty(t, data, "TestNotifyWithConditionExist failed")
}

func TestNotifyWithConditionEqual(t *testing.T) {
	s, deferFunc := storage.NewTestStorage(t, true)
	defer deferFunc()

	buf := goetty.NewByteBuf(256)
	tenantID := uint64(1)
	s.Set(storage.QueueMetaKey(tenantID, 0, buf), protoc.MustMarshal(&metapb.QueueState{
		Partitions: 1,
	}))
	n := NewQueueBasedNotifierWithGroup(s, metapb.DefaultGroup)

	conditionKey := []byte("ckey")
	conditionValue := []byte("v2")
	values := [][]byte{[]byte("v1"), []byte("v2")}
	results := []int{0, 1}

	for i := 0; i < 1; i++ {
		err := s.Set(storage.QueueKVKey(tenantID, conditionKey), values[i])
		assert.NoError(t, err, "TestNotifyWithConditionEqual failed")

		assert.NoError(t, n.Notify(tenantID, buf, []metapb.Notify{{
			UserID: 1,
		}}, &rpcpb.Condition{Key: conditionKey, Cmp: rpcpb.Equal, Value: conditionValue},
			conditionKey, values[i]), "TestNotifyWithConditionEqual failed")

		keys, _, err := s.Scan(storage.QueueItemKey(storage.PartitionKey(tenantID, 0), 1, buf), storage.QueueItemKey(storage.PartitionKey(tenantID, 0), 100, buf), 100)
		assert.NoError(t, err, "TestNotifyWithConditionEqual failed")
		assert.Equal(t, results[i], len(keys), "TestNotifyWithConditionEqual failed")
	}
}

func TestNotifyWithConditionGE(t *testing.T) {
	s, deferFunc := storage.NewTestStorage(t, true)
	defer deferFunc()

	buf := goetty.NewByteBuf(256)
	tenantID := uint64(1)
	s.Set(storage.QueueMetaKey(tenantID, 0, buf), protoc.MustMarshal(&metapb.QueueState{
		Partitions: 1,
	}))

	n := NewQueueBasedNotifierWithGroup(s, metapb.DefaultGroup)

	conditionKey := []byte("ckey")
	conditionValue := goetty.Uint64ToBytes(1)
	values := [][]byte{goetty.Uint64ToBytes(2), goetty.Uint64ToBytes(1), goetty.Uint64ToBytes(0)}
	results := []int{1, 2, 2}

	for i := 0; i < 2; i++ {
		err := s.Set(storage.QueueKVKey(tenantID, conditionKey), values[i])
		assert.NoError(t, err, "TestNotifyWithConditionGE failed")

		assert.NoError(t, n.Notify(tenantID, buf, []metapb.Notify{{
			UserID: 1,
		}}, &rpcpb.Condition{Key: conditionKey, Cmp: rpcpb.GE, Value: conditionValue},
			conditionKey, values[i]), "TestNotifyWithConditionGE failed")

		keys, _, err := s.Scan(storage.QueueItemKey(storage.PartitionKey(tenantID, 0), 1, buf), storage.QueueItemKey(storage.PartitionKey(tenantID, 0), 100, buf), 100)
		assert.NoError(t, err, "TestNotifyWithConditionGE failed")
		assert.Equal(t, results[i], len(keys), "TestNotifyWithConditionGE failed")
	}
}

func TestNotifyWithConditionGT(t *testing.T) {
	s, deferFunc := storage.NewTestStorage(t, true)
	defer deferFunc()

	buf := goetty.NewByteBuf(256)
	tenantID := uint64(1)
	s.Set(storage.QueueMetaKey(tenantID, 0, buf), protoc.MustMarshal(&metapb.QueueState{
		Partitions: 1,
	}))
	n := NewQueueBasedNotifierWithGroup(s, metapb.DefaultGroup)

	conditionKey := []byte("ckey")
	conditionValue := goetty.Uint64ToBytes(1)
	values := [][]byte{goetty.Uint64ToBytes(2), goetty.Uint64ToBytes(1), goetty.Uint64ToBytes(0)}
	results := []int{1, 1, 1}

	for i := 0; i < 2; i++ {
		err := s.Set(storage.QueueKVKey(tenantID, conditionKey), values[i])
		assert.NoError(t, err, "TestNotifyWithConditionGT failed")

		assert.NoError(t, n.Notify(tenantID, buf, []metapb.Notify{{
			UserID: 1,
		}}, &rpcpb.Condition{Key: conditionKey, Cmp: rpcpb.GT, Value: conditionValue},
			conditionKey, values[i]), "TestNotifyWithConditionGT failed")

		keys, _, err := s.Scan(storage.QueueItemKey(storage.PartitionKey(tenantID, 0), 1, buf), storage.QueueItemKey(storage.PartitionKey(tenantID, 0), 100, buf), 100)
		assert.NoError(t, err, "TestNotifyWithConditionGE failed")
		assert.Equal(t, results[i], len(keys), "TestNotifyWithConditionGE failed")
	}
}

func TestNotifyWithConditionLE(t *testing.T) {
	s, deferFunc := storage.NewTestStorage(t, true)
	defer deferFunc()

	buf := goetty.NewByteBuf(256)
	tenantID := uint64(1)
	s.Set(storage.QueueMetaKey(tenantID, 0, buf), protoc.MustMarshal(&metapb.QueueState{
		Partitions: 1,
	}))
	n := NewQueueBasedNotifierWithGroup(s, metapb.DefaultGroup)

	conditionKey := []byte("ckey")
	conditionValue := goetty.Uint64ToBytes(1)
	values := [][]byte{goetty.Uint64ToBytes(1), goetty.Uint64ToBytes(1), goetty.Uint64ToBytes(2)}
	results := []int{1, 2, 2}

	for i := 0; i < 2; i++ {
		err := s.Set(storage.QueueKVKey(tenantID, conditionKey), values[i])
		assert.NoError(t, err, "TestNotifyWithConditionLE failed")

		assert.NoError(t, n.Notify(tenantID, buf, []metapb.Notify{{
			UserID: 1,
		}}, &rpcpb.Condition{Key: conditionKey, Cmp: rpcpb.LE, Value: conditionValue},
			conditionKey, values[i]), "TestNotifyWithConditionLE failed")

		keys, _, err := s.Scan(storage.QueueItemKey(storage.PartitionKey(tenantID, 0), 1, buf), storage.QueueItemKey(storage.PartitionKey(tenantID, 0), 100, buf), 100)
		assert.NoError(t, err, "TestNotifyWithConditionLE failed")
		assert.Equal(t, results[i], len(keys), "TestNotifyWithConditionLE failed")
	}
}

func TestNotifyWithConditionLT(t *testing.T) {
	s, deferFunc := storage.NewTestStorage(t, true)
	defer deferFunc()

	buf := goetty.NewByteBuf(256)
	tenantID := uint64(1)
	s.Set(storage.QueueMetaKey(tenantID, 0, buf), protoc.MustMarshal(&metapb.QueueState{
		Partitions: 1,
	}))
	n := NewQueueBasedNotifierWithGroup(s, metapb.DefaultGroup)

	conditionKey := []byte("ckey")
	conditionValue := goetty.Uint64ToBytes(1)
	values := [][]byte{goetty.Uint64ToBytes(0), goetty.Uint64ToBytes(1), goetty.Uint64ToBytes(2)}
	results := []int{1, 1, 1}

	for i := 0; i < 2; i++ {
		err := s.Set(storage.QueueKVKey(tenantID, conditionKey), values[i])
		assert.NoError(t, err, "TestNotifyWithConditionLT failed")

		assert.NoError(t, n.Notify(tenantID, buf, []metapb.Notify{{
			UserID: 1,
		}}, &rpcpb.Condition{Key: conditionKey, Cmp: rpcpb.LT, Value: conditionValue},
			conditionKey, values[i]), "TestNotifyWithConditionLT failed")

		keys, _, err := s.Scan(storage.QueueItemKey(storage.PartitionKey(tenantID, 0), 1, buf), storage.QueueItemKey(storage.PartitionKey(tenantID, 0), 100, buf), 100)
		assert.NoError(t, err, "TestNotifyWithConditionLT failed")
		assert.Equal(t, results[i], len(keys), "TestNotifyWithConditionLT failed")
	}
}
