package notify

import (
	"testing"
	"time"

	hbmetapb "github.com/deepfabric/beehive/pb/metapb"
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
	assert.NoError(t, s.RaftStore().AddShards(hbmetapb.Shard{
		Group: uint64(metapb.TenantOutputGroup),
		Start: storage.PartitionKey(tenantID, 0),
		End:   storage.PartitionKey(tenantID, 1),
	}), "TestNotify failed")

	time.Sleep(time.Second)

	n := NewQueueBasedNotifier(s)
	assert.NoError(t, n.Notify(tenantID, buf, []metapb.Notify{metapb.Notify{
		UserID: 1,
	}}, nil), "TestNotify failed")

	req := rpcpb.AcquireQueueFetchRequest()
	req.Key = storage.PartitionKey(tenantID, 0)
	req.CompletedOffset = 0
	req.Consumer = []byte("c")
	req.Count = 1

	data, err := s.ExecCommandWithGroup(req, metapb.TenantOutputGroup)
	assert.NoError(t, err, "TestNotify failed")

	resp := rpcpb.AcquireBytesSliceResponse()
	protoc.MustUnmarshal(resp, data)
	assert.Equal(t, 1, len(resp.Values), "TestNotify failed")
}

func TestNotifyWithConditionNotExist(t *testing.T) {
	s, deferFunc := storage.NewTestStorage(t, true)
	defer deferFunc()
	buf := goetty.NewByteBuf(256)

	tenantID := uint64(1)
	assert.NoError(t, s.RaftStore().AddShards(hbmetapb.Shard{
		Group: uint64(metapb.TenantOutputGroup),
		Start: storage.PartitionKey(tenantID, 0),
		End:   storage.PartitionKey(tenantID, 1),
	}), "TestNotifyWithConditionNotExist failed")
	time.Sleep(time.Second)

	conditionKey := []byte("ckey")
	conditionValue := []byte("cvalue")

	for i := 0; i < 2; i++ {
		n := NewQueueBasedNotifier(s)
		assert.NoError(t, n.Notify(tenantID, buf, []metapb.Notify{metapb.Notify{
			UserID: 1,
		}}, &rpcpb.Condition{Key: conditionKey, Cmp: rpcpb.NotExists},
			conditionKey, conditionValue), "TestNotifyWithConditionNotExist failed")

		req := rpcpb.AcquireQueueFetchRequest()
		req.Key = storage.PartitionKey(tenantID, 0)
		req.CompletedOffset = 0
		req.Consumer = []byte("c")
		req.Count = 10

		data, err := s.ExecCommandWithGroup(req, metapb.TenantOutputGroup)
		assert.NoError(t, err, "TestNotifyWithConditionNotExist failed")
		resp := rpcpb.AcquireBytesSliceResponse()
		protoc.MustUnmarshal(resp, data)
		assert.Equal(t, 1, len(resp.Values), "TestNotifyWithConditionNotExist failed")
	}
}

func TestNotifyWithConditionExist(t *testing.T) {
	s, deferFunc := storage.NewTestStorage(t, true)
	defer deferFunc()
	buf := goetty.NewByteBuf(256)

	tenantID := uint64(1)
	assert.NoError(t, s.RaftStore().AddShards(hbmetapb.Shard{
		Group: uint64(metapb.TenantOutputGroup),
		Start: storage.PartitionKey(tenantID, 0),
		End:   storage.PartitionKey(tenantID, 1),
	}), "TestNotifyWithConditionExist failed")
	time.Sleep(time.Second)

	conditionKey := []byte("ckey")
	conditionValue := []byte("cvalue")

	n := NewQueueBasedNotifier(s)
	assert.NoError(t, n.Notify(tenantID, buf, []metapb.Notify{metapb.Notify{
		UserID: 1,
	}}, &rpcpb.Condition{Key: conditionKey, Cmp: rpcpb.Exists},
		conditionKey, conditionValue), "TestNotifyWithConditionExist failed")

	req := rpcpb.AcquireQueueFetchRequest()
	req.Key = storage.PartitionKey(tenantID, 0)
	req.CompletedOffset = 0
	req.Consumer = []byte("c")
	req.Count = 10

	data, err := s.ExecCommandWithGroup(req, metapb.TenantOutputGroup)
	assert.NoError(t, err, "TestNotifyWithConditionExist failed")
	resp := rpcpb.AcquireBytesSliceResponse()
	protoc.MustUnmarshal(resp, data)
	assert.Equal(t, 0, len(resp.Values), "TestNotifyWithConditionExist failed")
}

func TestNotifyWithConditionEqual(t *testing.T) {
	s, deferFunc := storage.NewTestStorage(t, true)
	defer deferFunc()
	buf := goetty.NewByteBuf(256)

	tenantID := uint64(1)
	assert.NoError(t, s.RaftStore().AddShards(hbmetapb.Shard{
		Group: uint64(metapb.TenantOutputGroup),
		Start: storage.PartitionKey(tenantID, 0),
		End:   storage.PartitionKey(tenantID, 1),
	}), "TestNotifyWithConditionEqual failed")
	time.Sleep(time.Second)

	conditionKey := []byte("ckey")
	conditionValue := []byte("v2")
	values := [][]byte{[]byte("v1"), []byte("v2")}
	results := []int{0, 1}

	for i := 0; i < 1; i++ {
		_, err := s.ExecCommandWithGroup(&rpcpb.SetRequest{
			Key:   storage.PartitionKVKey(tenantID, 0, conditionKey),
			Value: values[i],
		}, metapb.TenantOutputGroup)
		assert.NoError(t, err, "TestNotifyWithConditionEqual failed")

		n := NewQueueBasedNotifier(s)
		assert.NoError(t, n.Notify(tenantID, buf, []metapb.Notify{metapb.Notify{
			UserID: 1,
		}}, &rpcpb.Condition{Key: conditionKey, Cmp: rpcpb.Equal, Value: conditionValue},
			conditionKey, values[i]), "TestNotifyWithConditionEqual failed")

		req := rpcpb.AcquireQueueFetchRequest()
		req.Key = storage.PartitionKey(tenantID, 0)
		req.CompletedOffset = 0
		req.Consumer = []byte("c")
		req.Count = 10

		data, err := s.ExecCommandWithGroup(req, metapb.TenantOutputGroup)
		assert.NoError(t, err, "TestNotifyWithConditionEqual failed")
		resp := rpcpb.AcquireBytesSliceResponse()
		protoc.MustUnmarshal(resp, data)
		assert.Equal(t, results[i], len(resp.Values), "TestNotifyWithConditionEqual failed")
	}
}

func TestNotifyWithConditionGE(t *testing.T) {
	s, deferFunc := storage.NewTestStorage(t, true)
	defer deferFunc()
	buf := goetty.NewByteBuf(256)

	tenantID := uint64(1)
	assert.NoError(t, s.RaftStore().AddShards(hbmetapb.Shard{
		Group: uint64(metapb.TenantOutputGroup),
		Start: storage.PartitionKey(tenantID, 0),
		End:   storage.PartitionKey(tenantID, 1),
	}), "TestNotifyWithConditionGE failed")
	time.Sleep(time.Second)

	conditionKey := []byte("ckey")
	conditionValue := goetty.Uint64ToBytes(1)
	values := [][]byte{goetty.Uint64ToBytes(0), goetty.Uint64ToBytes(1), goetty.Uint64ToBytes(2)}
	results := []int{1, 2, 2}

	for i := 0; i < 2; i++ {
		_, err := s.ExecCommandWithGroup(&rpcpb.SetRequest{
			Key:   storage.PartitionKVKey(tenantID, 0, conditionKey),
			Value: values[i],
		}, metapb.TenantOutputGroup)
		assert.NoError(t, err, "TestNotifyWithConditionGE failed")

		n := NewQueueBasedNotifier(s)
		assert.NoError(t, n.Notify(tenantID, buf, []metapb.Notify{metapb.Notify{
			UserID: 1,
		}}, &rpcpb.Condition{Key: conditionKey, Cmp: rpcpb.GE, Value: conditionValue},
			conditionKey, values[i]), "TestNotifyWithConditionGE failed")

		req := rpcpb.AcquireQueueFetchRequest()
		req.Key = storage.PartitionKey(tenantID, 0)
		req.CompletedOffset = 0
		req.Consumer = []byte("c")
		req.Count = 10

		data, err := s.ExecCommandWithGroup(req, metapb.TenantOutputGroup)
		assert.NoError(t, err, "TestNotifyWithConditionGE failed")
		resp := rpcpb.AcquireBytesSliceResponse()
		protoc.MustUnmarshal(resp, data)
		assert.Equal(t, results[i], len(resp.Values), "TestNotifyWithConditionGE failed")
	}
}

func TestNotifyWithConditionGT(t *testing.T) {
	s, deferFunc := storage.NewTestStorage(t, true)
	defer deferFunc()
	buf := goetty.NewByteBuf(256)

	tenantID := uint64(1)
	assert.NoError(t, s.RaftStore().AddShards(hbmetapb.Shard{
		Group: uint64(metapb.TenantOutputGroup),
		Start: storage.PartitionKey(tenantID, 0),
		End:   storage.PartitionKey(tenantID, 1),
	}), "TestNotifyWithConditionGT failed")
	time.Sleep(time.Second)

	conditionKey := []byte("ckey")
	conditionValue := goetty.Uint64ToBytes(1)
	values := [][]byte{goetty.Uint64ToBytes(0), goetty.Uint64ToBytes(1), goetty.Uint64ToBytes(2)}
	results := []int{1, 1, 1}

	for i := 0; i < 2; i++ {
		_, err := s.ExecCommandWithGroup(&rpcpb.SetRequest{
			Key:   storage.PartitionKVKey(tenantID, 0, conditionKey),
			Value: values[i],
		}, metapb.TenantOutputGroup)
		assert.NoError(t, err, "TestNotifyWithConditionGT failed")

		n := NewQueueBasedNotifier(s)
		assert.NoError(t, n.Notify(tenantID, buf, []metapb.Notify{metapb.Notify{
			UserID: 1,
		}}, &rpcpb.Condition{Key: conditionKey, Cmp: rpcpb.GT, Value: conditionValue},
			conditionKey, values[i]), "TestNotifyWithConditionGT failed")

		req := rpcpb.AcquireQueueFetchRequest()
		req.Key = storage.PartitionKey(tenantID, 0)
		req.CompletedOffset = 0
		req.Consumer = []byte("c")
		req.Count = 10

		data, err := s.ExecCommandWithGroup(req, metapb.TenantOutputGroup)
		assert.NoError(t, err, "TestNotifyWithConditionGT failed")
		resp := rpcpb.AcquireBytesSliceResponse()
		protoc.MustUnmarshal(resp, data)
		assert.Equal(t, results[i], len(resp.Values), "TestNotifyWithConditionGT failed")
	}
}

func TestNotifyWithConditionLE(t *testing.T) {
	s, deferFunc := storage.NewTestStorage(t, true)
	defer deferFunc()
	buf := goetty.NewByteBuf(256)

	tenantID := uint64(1)
	assert.NoError(t, s.RaftStore().AddShards(hbmetapb.Shard{
		Group: uint64(metapb.TenantOutputGroup),
		Start: storage.PartitionKey(tenantID, 0),
		End:   storage.PartitionKey(tenantID, 1),
	}), "TestNotifyWithConditionLE failed")
	time.Sleep(time.Second)

	conditionKey := []byte("ckey")
	conditionValue := goetty.Uint64ToBytes(1)
	values := [][]byte{goetty.Uint64ToBytes(2), goetty.Uint64ToBytes(1), goetty.Uint64ToBytes(0)}
	results := []int{1, 2, 2}

	for i := 0; i < 2; i++ {
		_, err := s.ExecCommandWithGroup(&rpcpb.SetRequest{
			Key:   storage.PartitionKVKey(tenantID, 0, conditionKey),
			Value: values[i],
		}, metapb.TenantOutputGroup)
		assert.NoError(t, err, "TestNotifyWithConditionLE failed")

		n := NewQueueBasedNotifier(s)
		assert.NoError(t, n.Notify(tenantID, buf, []metapb.Notify{metapb.Notify{
			UserID: 1,
		}}, &rpcpb.Condition{Key: conditionKey, Cmp: rpcpb.LE, Value: conditionValue},
			conditionKey, values[i]), "TestNotifyWithConditionLE failed")

		req := rpcpb.AcquireQueueFetchRequest()
		req.Key = storage.PartitionKey(tenantID, 0)
		req.CompletedOffset = 0
		req.Consumer = []byte("c")
		req.Count = 10

		data, err := s.ExecCommandWithGroup(req, metapb.TenantOutputGroup)
		assert.NoError(t, err, "TestNotifyWithConditionLE failed")
		resp := rpcpb.AcquireBytesSliceResponse()
		protoc.MustUnmarshal(resp, data)
		assert.Equal(t, results[i], len(resp.Values), "TestNotifyWithConditionLE failed")
	}
}

func TestNotifyWithConditionLT(t *testing.T) {
	s, deferFunc := storage.NewTestStorage(t, true)
	defer deferFunc()
	buf := goetty.NewByteBuf(256)

	tenantID := uint64(1)
	assert.NoError(t, s.RaftStore().AddShards(hbmetapb.Shard{
		Group: uint64(metapb.TenantOutputGroup),
		Start: storage.PartitionKey(tenantID, 0),
		End:   storage.PartitionKey(tenantID, 1),
	}), "TestNotifyWithConditionLT failed")
	time.Sleep(time.Second)

	conditionKey := []byte("ckey")
	conditionValue := goetty.Uint64ToBytes(1)
	values := [][]byte{goetty.Uint64ToBytes(2), goetty.Uint64ToBytes(1), goetty.Uint64ToBytes(0)}
	results := []int{1, 1, 1}

	for i := 0; i < 2; i++ {
		_, err := s.ExecCommandWithGroup(&rpcpb.SetRequest{
			Key:   storage.PartitionKVKey(tenantID, 0, conditionKey),
			Value: values[i],
		}, metapb.TenantOutputGroup)
		assert.NoError(t, err, "TestNotifyWithConditionLT failed")

		n := NewQueueBasedNotifier(s)
		assert.NoError(t, n.Notify(tenantID, buf, []metapb.Notify{metapb.Notify{
			UserID: 1,
		}}, &rpcpb.Condition{Key: conditionKey, Cmp: rpcpb.LT, Value: conditionValue},
			conditionKey, values[i]), "TestNotifyWithConditionLT failed")

		req := rpcpb.AcquireQueueFetchRequest()
		req.Key = storage.PartitionKey(tenantID, 0)
		req.CompletedOffset = 0
		req.Consumer = []byte("c")
		req.Count = 10

		data, err := s.ExecCommandWithGroup(req, metapb.TenantOutputGroup)
		assert.NoError(t, err, "TestNotifyWithConditionLT failed")
		resp := rpcpb.AcquireBytesSliceResponse()
		protoc.MustUnmarshal(resp, data)
		assert.Equal(t, results[i], len(resp.Values), "TestNotifyWithConditionLT failed")
	}
}
