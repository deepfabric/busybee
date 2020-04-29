package storage

import (
	"fmt"
	"testing"
	"time"

	bhmetapb "github.com/deepfabric/beehive/pb/metapb"
	"github.com/deepfabric/busybee/pkg/pb/metapb"
	"github.com/deepfabric/busybee/pkg/pb/rpcpb"
	"github.com/deepfabric/busybee/pkg/util"
	"github.com/fagongzi/goetty"
	"github.com/fagongzi/util/protoc"
	"github.com/stretchr/testify/assert"
)

func TestSetAndGet(t *testing.T) {
	store, deferFunc := NewTestStorage(t, true)
	defer deferFunc()

	key := []byte("key1")
	value := []byte("value1")

	err := store.Set(key, value)
	assert.NoError(t, err, "TestSetAndGet failed")

	data, err := store.Get(key)
	assert.NoError(t, err, "TestSetAndGet failed")
	assert.Equal(t, string(value), string(data), "TestSetAndGet failed")
}

func TestSetIf(t *testing.T) {
	store, deferFunc := NewTestStorage(t, true)
	defer deferFunc()

	key := []byte("key1")
	value := []byte("value1")
	value2 := []byte("value2")

	data, err := store.ExecCommand(&rpcpb.SetIfRequest{
		Key:   key,
		Value: value,
		Conditions: []rpcpb.ConditionGroup{
			{
				Conditions: []rpcpb.Condition{
					{
						Cmp: rpcpb.Exists,
					},
				},
			},
			{
				Conditions: []rpcpb.Condition{
					{
						Cmp: rpcpb.NotExists,
					},
					{
						Cmp:   rpcpb.Equal,
						Value: value,
					},
				},
			},
		},
	})

	assert.NoError(t, err, "TestSetIf failed")
	result := &rpcpb.BoolResponse{}
	protoc.MustUnmarshal(result, data)
	assert.False(t, result.Value, "TestSetIf failed")

	data, err = store.Get(key)
	assert.NoError(t, err, "TestSetIf failed")
	assert.Empty(t, data, "TestSetIf failed")

	data, err = store.ExecCommand(&rpcpb.SetIfRequest{
		Key:   key,
		Value: value2,
		Conditions: []rpcpb.ConditionGroup{
			{
				Conditions: []rpcpb.Condition{
					{
						Cmp: rpcpb.NotExists,
					},
				},
			},
		},
	})
	assert.NoError(t, err, "TestSetIf failed")
	result.Reset()
	protoc.MustUnmarshal(result, data)
	assert.True(t, result.Value, "TestSetIf failed")

	data, err = store.Get(key)
	assert.NoError(t, err, "TestSetIf failed")
	assert.Equal(t, string(value2), string(data), "TestSetIf failed")
}

func TestDelete(t *testing.T) {
	store, deferFunc := NewTestStorage(t, true)
	defer deferFunc()

	key := []byte("key1")
	value := []byte("value1")

	err := store.Set(key, value)
	assert.NoError(t, err, "TestDelete failed")

	err = store.Delete(key)
	assert.NoError(t, err, "TestDelete failed")

	data, err := store.Get(key)
	assert.NoError(t, err, "TestDelete failed")
	assert.Empty(t, data, "TestDelete failed")
}

func TestDeleteIf(t *testing.T) {
	store, deferFunc := NewTestStorage(t, true)
	defer deferFunc()

	key := []byte("key1")
	value := []byte("value1")
	value2 := []byte("value2")

	err := store.Set(key, value)
	assert.NoError(t, err, "TestDeleteIf failed")

	data, err := store.ExecCommand(&rpcpb.DeleteIfRequest{
		Key: key,
		Conditions: []rpcpb.ConditionGroup{
			{
				Conditions: []rpcpb.Condition{
					{
						Cmp:   rpcpb.Equal,
						Value: value2,
					},
				},
			},
		},
	})
	assert.NoError(t, err, "TestDeleteIf failed")
	result := &rpcpb.BoolResponse{}
	protoc.MustUnmarshal(result, data)
	assert.False(t, result.Value, "TestDeleteIf failed")

	data, err = store.Get(key)
	assert.NoError(t, err, "TestDeleteIf failed")
	assert.NotEmpty(t, data, "TestDeleteIf failed")

	data, err = store.ExecCommand(&rpcpb.DeleteIfRequest{
		Key: key,
		Conditions: []rpcpb.ConditionGroup{
			{
				Conditions: []rpcpb.Condition{
					{
						Cmp: rpcpb.Exists,
					},
				},
			},
		},
	})
	assert.NoError(t, err, "TestDeleteIf failed")
	result.Reset()
	protoc.MustUnmarshal(result, data)
	assert.True(t, result.Value, "TestDeleteIf failed")

	data, err = store.Get(key)
	assert.NoError(t, err, "TestDeleteIf failed")
	assert.Empty(t, data, "TestDeleteIf failed")
}

func TestScan(t *testing.T) {
	store, deferFunc := NewTestStorage(t, true)
	defer deferFunc()

	err := store.RaftStore().AddShards(bhmetapb.Shard{
		Group: 1,
		Start: []byte("k1"),
		End:   []byte("k5"),
	}, bhmetapb.Shard{
		Group: 1,
		Start: []byte("k5"),
		End:   []byte("k9"),
	})
	assert.NoError(t, err, "TestScan failed")

	time.Sleep(time.Second * 2)

	for i := 1; i < 9; i++ {
		_, err = store.ExecCommandWithGroup(&rpcpb.SetRequest{
			Key:   []byte(fmt.Sprintf("k%d", i)),
			Value: []byte(fmt.Sprintf("v%d", i)),
		}, metapb.Group(1))
		assert.NoErrorf(t, err, "TestScan failed %d", i)
	}

	req := rpcpb.AcquireScanRequest()
	req.Start = []byte("k1")
	req.End = []byte("k9")
	req.Limit = 9
	data, err := store.ExecCommandWithGroup(req, metapb.Group(1))
	assert.NoError(t, err, "TestScan failed")
	resp := rpcpb.AcquireBytesSliceResponse()
	protoc.MustUnmarshal(resp, data)
	assert.Equal(t, 4, len(resp.Values), "TestScan failed")

	req = rpcpb.AcquireScanRequest()
	req.Start = []byte("k1")
	req.End = []byte("k9")
	req.Limit = 2
	data, err = store.ExecCommandWithGroup(req, metapb.Group(1))
	assert.NoError(t, err, "TestScan failed")
	resp = rpcpb.AcquireBytesSliceResponse()
	protoc.MustUnmarshal(resp, data)
	assert.Equal(t, 2, len(resp.Values), "TestScan failed")

	req = rpcpb.AcquireScanRequest()
	req.Start = []byte("k4")
	req.End = []byte("k9")
	req.Limit = 2
	data, err = store.ExecCommandWithGroup(req, metapb.Group(1))
	assert.NoError(t, err, "TestScan failed")
	resp = rpcpb.AcquireBytesSliceResponse()
	protoc.MustUnmarshal(resp, data)
	assert.Equal(t, 1, len(resp.Values), "TestScan failed")

	req = rpcpb.AcquireScanRequest()
	req.Start = []byte("k5")
	req.End = []byte("k9")
	req.Limit = 2
	data, err = store.ExecCommandWithGroup(req, metapb.Group(1))
	assert.NoError(t, err, "TestScan failed")
	resp = rpcpb.AcquireBytesSliceResponse()
	protoc.MustUnmarshal(resp, data)
	assert.Equal(t, 2, len(resp.Values), "TestScan failed")

	req = rpcpb.AcquireScanRequest()
	req.Start = []byte("k5")
	req.End = []byte("k9")
	req.Limit = 10
	data, err = store.ExecCommandWithGroup(req, metapb.Group(1))
	assert.NoError(t, err, "TestScan failed")
	resp = rpcpb.AcquireBytesSliceResponse()
	protoc.MustUnmarshal(resp, data)
	assert.Equal(t, 4, len(resp.Values), "TestScan failed")
}

func TestAllocIDAndReset(t *testing.T) {
	store, deferFunc := NewTestStorage(t, true)
	defer deferFunc()

	key := []byte("key1")
	data, err := store.ExecCommand(&rpcpb.AllocIDRequest{
		Key:   key,
		Batch: 1,
	})
	assert.NoError(t, err, "TestAllocID failed")
	assert.NotEmpty(t, data, "TestAllocID failed")

	resp := rpcpb.AcquireUint32RangeResponse()
	protoc.MustUnmarshal(resp, data)
	assert.Equal(t, uint32(1), resp.From, "TestAllocID failed")
	assert.Equal(t, uint32(1), resp.To, "TestAllocID failed")

	data, err = store.ExecCommand(&rpcpb.AllocIDRequest{
		Key:   key,
		Batch: 2,
	})
	assert.NoError(t, err, "TestAllocID failed")
	assert.NotEmpty(t, data, "TestAllocID failed")

	protoc.MustUnmarshal(resp, data)
	assert.Equal(t, uint32(2), resp.From, "TestAllocID failed")
	assert.Equal(t, uint32(3), resp.To, "TestAllocID failed")

	_, err = store.ExecCommand(&rpcpb.ResetIDRequest{
		Key:       key,
		StartWith: 0,
	})
	data, _ = store.ExecCommand(&rpcpb.AllocIDRequest{
		Key:   key,
		Batch: 2,
	})
	protoc.MustUnmarshal(resp, data)
	assert.Equal(t, uint32(1), resp.From, "TestAllocID failed")
	assert.Equal(t, uint32(2), resp.To, "TestAllocID failed")
}

func TestBMCreate(t *testing.T) {
	store, deferFunc := NewTestStorage(t, true)
	defer deferFunc()

	key := []byte("key1")
	value := []uint32{1, 2, 3, 4, 5}

	data, err := store.ExecCommand(&rpcpb.BMCreateRequest{
		Key:   key,
		Value: value,
	})
	assert.NoError(t, err, "TestBMCreate failed")

	data, err = store.Get(key)
	assert.NoError(t, err, "TestBMCreate failed")
	assert.NotEmpty(t, data, "TestBMCreate failed")

	bm := util.MustParseBM(data)
	assert.Equal(t, uint64(len(value)), bm.GetCardinality(), "TestBMCreate failed")
}

func TestBMAdd(t *testing.T) {
	store, deferFunc := NewTestStorage(t, true)
	defer deferFunc()

	key := []byte("key1")
	value := []uint32{1, 2, 3, 4, 5}
	value2 := []uint32{6, 7, 8, 9, 10}

	data, err := store.ExecCommand(&rpcpb.BMCreateRequest{
		Key:   key,
		Value: value,
	})
	assert.NoError(t, err, "TestBMAdd failed")

	data, err = store.ExecCommand(&rpcpb.BMAddRequest{
		Key:   key,
		Value: value2,
	})
	assert.NoError(t, err, "TestBMAdd failed")

	data, err = store.Get(key)
	assert.NoError(t, err, "TestBMAdd failed")
	assert.NotEmpty(t, data, "TestBMAdd failed")

	bm := util.MustParseBM(data)
	assert.Equal(t, uint64(len(value)+len(value2)), bm.GetCardinality(), "TestBMAdd failed")
}

func TestBMRemove(t *testing.T) {
	store, deferFunc := NewTestStorage(t, true)
	defer deferFunc()
	key := []byte("key1")
	value := []uint32{1, 2, 3, 4, 5}

	data, err := store.ExecCommand(&rpcpb.BMCreateRequest{
		Key:   key,
		Value: value,
	})
	assert.NoError(t, err, "TestBMRemove failed")

	data, err = store.ExecCommand(&rpcpb.BMRemoveRequest{
		Key:   key,
		Value: value[2:],
	})
	assert.NoError(t, err, "TestBMRemove failed")

	data, err = store.Get(key)
	assert.NoError(t, err, "TestBMRemove failed")
	assert.NotEmpty(t, data, "TestBMRemove failed")

	bm := util.MustParseBM(data)
	assert.Equal(t, uint64(2), bm.GetCardinality(), "TestBMRemove failed")
}

func TestBMClear(t *testing.T) {
	store, deferFunc := NewTestStorage(t, true)
	defer deferFunc()
	key := []byte("key1")
	value := []uint32{1, 2, 3, 4, 5}

	data, err := store.ExecCommand(&rpcpb.BMCreateRequest{
		Key:   key,
		Value: value,
	})
	assert.NoError(t, err, "TestBMClear failed")

	data, err = store.ExecCommand(&rpcpb.BMClearRequest{
		Key: key,
	})
	assert.NoError(t, err, "TestBMClear failed")

	data, err = store.ExecCommand(&rpcpb.BMCountRequest{
		Key: key,
	})
	assert.NoError(t, err, "TestBMClear failed")
	assert.Empty(t, data, "TestBMClear failed")

	resp := &rpcpb.Uint64Response{}
	protoc.MustUnmarshal(resp, data)
	assert.Equal(t, uint64(0), resp.Value, "TestBMClear failed")
}

func TestBMContains(t *testing.T) {
	store, deferFunc := NewTestStorage(t, true)
	defer deferFunc()

	key := []byte("key1")
	value := []uint32{1, 2, 3, 4, 5}

	data, err := store.ExecCommand(&rpcpb.BMCreateRequest{
		Key:   key,
		Value: value,
	})
	assert.NoError(t, err, "TestBMContains failed")

	data, err = store.ExecCommand(&rpcpb.BMContainsRequest{
		Key:   key,
		Value: value,
	})
	assert.NoError(t, err, "TestBMContains failed")

	resp := &rpcpb.BoolResponse{}
	protoc.MustUnmarshal(resp, data)
	assert.True(t, resp.Value, "TestBMContains failed")
}

func TestBMCount(t *testing.T) {
	store, deferFunc := NewTestStorage(t, true)
	defer deferFunc()

	key := []byte("key1111")
	value := []uint32{1, 2, 3, 4, 5}

	data, err := store.ExecCommand(&rpcpb.BMCreateRequest{
		Key:   key,
		Value: value,
	})
	assert.NoError(t, err, "TestBMCount failed")

	data, err = store.ExecCommand(&rpcpb.BMCountRequest{
		Key: key,
	})
	assert.NoError(t, err, "TestBMCount failed")

	resp := &rpcpb.Uint64Response{}
	protoc.MustUnmarshal(resp, data)
	assert.Equal(t, uint64(len(value)), resp.Value, "TestBMCount failed")
}

func TestBMRange(t *testing.T) {
	store, deferFunc := NewTestStorage(t, true)
	defer deferFunc()

	key := []byte("key1")
	value := []uint32{1, 2, 3, 4, 5}

	data, err := store.ExecCommand(&rpcpb.BMCreateRequest{
		Key:   key,
		Value: value,
	})
	assert.NoError(t, err, "TestBMRange failed")

	data, err = store.ExecCommand(&rpcpb.BMRangeRequest{
		Key:   key,
		Start: 1,
		Limit: 2,
	})
	assert.NoError(t, err, "TestBMRange failed")

	resp := &rpcpb.Uint32SliceResponse{}
	protoc.MustUnmarshal(resp, data)
	assert.Equal(t, uint32(1), resp.Values[0], "TestBMCount failed")
	assert.Equal(t, uint32(2), resp.Values[1], "TestBMCount failed")

	data, err = store.ExecCommand(&rpcpb.BMRangeRequest{
		Key:   key,
		Start: 0,
		Limit: 2,
	})
	assert.NoError(t, err, "TestBMRange failed")

	resp = &rpcpb.Uint32SliceResponse{}
	protoc.MustUnmarshal(resp, data)
	assert.Equal(t, uint32(1), resp.Values[0], "TestBMCount failed")
	assert.Equal(t, uint32(2), resp.Values[1], "TestBMCount failed")
}

func TestUpdateMapping(t *testing.T) {
	store, deferFunc := NewTestStorage(t, true)
	defer deferFunc()

	req := rpcpb.AcquireUpdateMappingRequest()
	req.Set.Values = append(req.Set.Values, metapb.IDValue{
		Type:  "c0",
		Value: "id0-v1",
	}, metapb.IDValue{
		Type:  "c1",
		Value: "id1-v1",
	})
	data, err := store.ExecCommand(req)
	assert.NoError(t, err, "TestUpdateMapping failed")
	assert.NotEmpty(t, data, "TestUpdateMapping failed")
	resp := &metapb.IDSet{}
	protoc.MustUnmarshal(resp, data)
	assert.Equal(t, 2, len(resp.Values), "TestUpdateMapping failed")

	req = rpcpb.AcquireUpdateMappingRequest()
	req.Set.Values = append(req.Set.Values, metapb.IDValue{
		Type:  "c0",
		Value: "id0-v2",
	}, metapb.IDValue{
		Type:  "c2",
		Value: "id2-v1",
	})
	data, err = store.ExecCommand(req)
	assert.NoError(t, err, "TestUpdateMapping failed")
	assert.NotEmpty(t, data, "TestUpdateMapping failed")
	resp.Reset()
	protoc.MustUnmarshal(resp, data)
	assert.Equal(t, 3, len(resp.Values), "TestUpdateMapping failed")
	for _, value := range resp.Values {
		if value.Type == "c0" {
			assert.Equal(t, "id0-v2", value.Value, "TestUpdateMapping failed")
		} else if value.Type == "c1" {
			assert.Equal(t, "id1-v1", value.Value, "TestUpdateMapping failed")
		} else if value.Type == "c2" {
			assert.Equal(t, "id2-v1", value.Value, "TestUpdateMapping failed")
		}
	}
}

func TestPutToQueueWithClean(t *testing.T) {
	backup := countToClean
	defer func() {
		countToClean = backup
	}()

	store, deferFunc := NewTestStorage(t, true)
	defer deferFunc()

	buf := goetty.NewByteBuf(32)
	store.Set(QueueMetaKey(10, buf), protoc.MustMarshal(&metapb.QueueState{
		Partitions: 2,
	}))

	values := [][]byte{[]byte("1"), []byte("2"), []byte("3")}
	err := store.PutToQueue(10, 0, metapb.DefaultGroup, values...)
	assert.NoError(t, err, "TestPutToQueueWithClean failed")

	value, err := store.Get(maxAndCleanOffsetKey(PartitionKey(10, 0), buf))
	assert.NoError(t, err, "TestPutToQueueWithClean failed")
	assert.Equal(t, uint64(3), goetty.Byte2UInt64(value), "TestPutToQueueWithClean failed")
	assert.Equal(t, uint64(0), goetty.Byte2UInt64(value[8:]), "TestPutToQueueWithClean failed")

	now := time.Now().Unix()
	g1 := []byte("g1")
	g1Value := make([]byte, 16, 16)
	g2 := []byte("g2")
	g2Value := make([]byte, 16, 16)

	goetty.Uint64ToBytesTo(1, g1Value)
	goetty.Int64ToBytesTo(now, g1Value[8:])
	err = store.Set(committedOffsetKey(PartitionKey(10, 0), g1, buf), g1Value)
	assert.NoError(t, err, "TestPutToQueueWithClean failed")

	goetty.Uint64ToBytesTo(2, g2Value)
	goetty.Int64ToBytesTo(now, g2Value[8:])
	err = store.Set(committedOffsetKey(PartitionKey(10, 0), g2, buf), g2Value)
	assert.NoError(t, err, "TestPutToQueueWithClean failed")

	countToClean = 3
	err = store.PutToQueue(10, 0, metapb.DefaultGroup, values[0])
	assert.NoError(t, err, "TestPutToQueueWithClean failed")
	value, err = store.Get(maxAndCleanOffsetKey(PartitionKey(10, 0), buf))
	assert.NoError(t, err, "TestPutToQueueWithClean failed")
	assert.Equal(t, uint64(4), goetty.Byte2UInt64(value), "TestPutToQueueWithClean failed")
	assert.Equal(t, uint64(0), goetty.Byte2UInt64(value[8:]), "TestPutToQueueWithClean failed")

	goetty.Uint64ToBytesTo(1, g1Value)
	goetty.Int64ToBytesTo(now-maxConsumerAlive, g1Value[8:])
	err = store.Set(committedOffsetKey(PartitionKey(10, 0), g1, buf), g1Value)
	assert.NoError(t, err, "TestPutToQueueWithClean failed")

	goetty.Uint64ToBytesTo(2, g2Value)
	goetty.Int64ToBytesTo(now-maxConsumerAlive, g2Value[8:])
	err = store.Set(committedOffsetKey(PartitionKey(10, 0), g2, buf), g2Value)
	assert.NoError(t, err, "TestPutToQueueWithClean failed")

	err = store.PutToQueue(10, 0, metapb.DefaultGroup, values[0])
	assert.NoError(t, err, "TestPutToQueueWithClean failed")
	value, err = store.Get(maxAndCleanOffsetKey(PartitionKey(10, 0), buf))
	assert.NoError(t, err, "TestPutToQueueWithClean failed")
	assert.Equal(t, uint64(5), goetty.Byte2UInt64(value), "TestPutToQueueWithClean failed")
	assert.Equal(t, uint64(1), goetty.Byte2UInt64(value[8:]), "TestPutToQueueWithClean failed")

	keys, _, err := store.Scan(QueueItemKey(PartitionKey(10, 0), 0, buf),
		QueueItemKey(PartitionKey(10, 0), 100, buf),
		100)
	assert.NoError(t, err, "TestPutToQueueWithClean failed")
	assert.Equal(t, 4, len(keys), "TestPutToQueueWithClean failed")
}

func TestPutToQueue(t *testing.T) {
	store, deferFunc := NewTestStorage(t, true)
	defer deferFunc()

	buf := goetty.NewByteBuf(32)
	store.Set(QueueMetaKey(10, buf), protoc.MustMarshal(&metapb.QueueState{
		Partitions: 2,
	}))

	err := store.PutToQueue(10, 0, metapb.DefaultGroup, protoc.MustMarshal(&metapb.Event{
		Type: metapb.UserType,
		User: &metapb.UserEvent{
			TenantID: 10000,
			UserID:   1,
			Data: []metapb.KV{
				{
					Key:   []byte("uid"),
					Value: []byte("1"),
				},
			},
		},
	}), protoc.MustMarshal(&metapb.Event{
		Type: metapb.UserType,
		User: &metapb.UserEvent{
			TenantID: 10000,
			UserID:   2,
			Data: []metapb.KV{
				{
					Key:   []byte("uid"),
					Value: []byte("2"),
				},
			},
		},
	}), protoc.MustMarshal(&metapb.Event{
		Type: metapb.UserType,
		User: &metapb.UserEvent{
			TenantID: 10000,
			UserID:   3,
			Data: []metapb.KV{
				{
					Key:   []byte("uid"),
					Value: []byte("3"),
				},
			},
		},
	}))
	assert.NoError(t, err, "TestPutToQueue failed")

	for i := uint64(1); i < 4; i++ {
		data, err := store.Get(QueueItemKey(PartitionKey(10, 0), i, buf))
		assert.NoError(t, err, "TestPutToQueue failed")

		evt := &metapb.Event{}
		protoc.MustUnmarshal(evt, data)

		assert.Equal(t, metapb.UserType, evt.Type, "TestPutToQueue failed")
		assert.Equal(t, fmt.Sprintf("%d", i), string(evt.User.Data[0].Value), "TestPutToQueue failed")
	}
}

func TestPutToQueueWithAlloc(t *testing.T) {
	store, deferFunc := NewTestStorage(t, true)
	defer deferFunc()

	buf := goetty.NewByteBuf(32)
	store.Set(QueueMetaKey(10, buf), protoc.MustMarshal(&metapb.QueueState{
		Partitions: 3,
	}))

	values := [][]byte{[]byte("1"), []byte("2"), []byte("3")}
	err := store.PutToQueueWithAlloc(10, metapb.DefaultGroup, values...)
	assert.NoError(t, err, "TestPutToQueueWithAlloc failed")

	for i := uint32(0); i < 3; i++ {
		data, err := store.Get(QueueItemKey(PartitionKey(10, i), 1, buf))
		assert.NoError(t, err, "TestPutToQueueWithAlloc failed")
		assert.Equal(t, values[i], data, "TestPutToQueue failed")
	}
}

func TestPutToQueueWithKV(t *testing.T) {
	store, deferFunc := NewTestStorage(t, true)
	defer deferFunc()

	buf := goetty.NewByteBuf(32)
	store.Set(QueueMetaKey(10, buf), protoc.MustMarshal(&metapb.QueueState{
		Partitions: 2,
	}))

	key := []byte("key1")
	value := []byte("value")
	err := store.PutToQueueWithKV(10, 0, metapb.DefaultGroup, [][]byte{
		protoc.MustMarshal(&metapb.Event{
			Type: metapb.UserType,
			User: &metapb.UserEvent{
				TenantID: 10,
				UserID:   1,
				Data: []metapb.KV{
					{
						Key:   []byte("uid"),
						Value: []byte("1"),
					},
				},
			},
		}),
	}, key, value)
	assert.NoError(t, err, "TestPutToQueueWithKV failed")

	data, err := store.Get(QueueItemKey(PartitionKey(10, 0), 1, buf))
	assert.NoError(t, err, "TestPutToQueueWithKV failed")

	evt := &metapb.Event{}
	protoc.MustUnmarshal(evt, data)
	assert.Equal(t, metapb.UserType, evt.Type, "TestPutToQueueWithKV failed")
	assert.Equal(t, 1, len(evt.User.Data), "TestPutToQueueWithKV failed")

	v, err := store.Get(QueueKVKey(10, key))
	assert.NoError(t, err, "TestPutToQueueWithKV failed")
	assert.NotEmpty(t, v, "TestPutToQueueWithKV failed")
	assert.Equal(t, string(value), string(v), "TestPutToQueueWithKV failed")
}

func TestPutToQueueWithAllocAndKV(t *testing.T) {
	store, deferFunc := NewTestStorage(t, true)
	defer deferFunc()

	buf := goetty.NewByteBuf(32)
	store.Set(QueueMetaKey(10, buf), protoc.MustMarshal(&metapb.QueueState{
		Partitions: 3,
	}))

	key := []byte("key")
	value := []byte("value")
	values := [][]byte{[]byte("1"), []byte("2"), []byte("3")}
	err := store.PutToQueueWithAllocAndKV(10, metapb.DefaultGroup, values, key, value)
	assert.NoError(t, err, "TestPutToQueueWithAllocAndKV failed")

	for i := uint32(0); i < 3; i++ {
		data, err := store.Get(QueueItemKey(PartitionKey(10, i), 1, buf))
		assert.NoError(t, err, "TestPutToQueueWithAllocAndKV failed")
		assert.Equal(t, values[i], data, "TestPutToQueueWithAllocAndKV failed")
	}

	data, err := store.Get(QueueKVKey(10, key))
	assert.NoError(t, err, "TestPutToQueueWithAllocAndKV failed")
	assert.Equal(t, value, data, "TestPutToQueueWithAllocAndKV failed")
}

func TestQueueFetchWithNoConsumers(t *testing.T) {
	store, deferFunc := NewTestStorage(t, true)
	defer deferFunc()

	tid := uint64(1)
	g1 := []byte("g1")
	state := &metapb.QueueState{
		Partitions: 2,
		States: []metapb.Partiton{
			{}, {},
		},
		Timeout: 60,
	}
	buf := goetty.NewByteBuf(32)
	key := QueueMetaKey(tid, buf)
	err := store.Set(key, protoc.MustMarshal(state))
	assert.NoError(t, err, "TestQueueFetchWithNoConsumers failed")

	req := rpcpb.AcquireQueueFetchRequest()
	req.ID = tid
	req.Group = g1
	resp := getTestFetchResp(t, store, req)
	assert.True(t, resp.Removed, "TestQueueFetchWithNoConsumers failed")
}

func TestQueueFetchWithInvalidConsumer(t *testing.T) {
	store, deferFunc := NewTestStorage(t, true)
	defer deferFunc()

	tid := uint64(1)
	g1 := []byte("g1")
	state := &metapb.QueueState{
		Consumers:  1,
		Partitions: 2,
		States: []metapb.Partiton{
			{}, {},
		},
		Timeout: 60,
	}
	buf := goetty.NewByteBuf(32)
	key := QueueStateKey(tid, g1, buf)
	err := store.Set(key, protoc.MustMarshal(state))
	assert.NoError(t, err, "TestQueueFetchWithInvalidConsumer failed")

	req := rpcpb.AcquireQueueFetchRequest()
	req.ID = tid
	req.Group = g1
	req.Consumer = 1
	resp := getTestFetchResp(t, store, req)
	assert.True(t, resp.Removed, "TestQueueFetchWithInvalidConsumer failed")
}

func TestQueueFetchWithInvalidPartition(t *testing.T) {
	store, deferFunc := NewTestStorage(t, true)
	defer deferFunc()

	tid := uint64(1)
	g1 := []byte("g1")
	state := &metapb.QueueState{
		Consumers:  1,
		Partitions: 2,
		States: []metapb.Partiton{
			{}, {},
		},
		Timeout: 60,
	}
	buf := goetty.NewByteBuf(32)
	key := QueueStateKey(tid, g1, buf)
	err := store.Set(key, protoc.MustMarshal(state))
	assert.NoError(t, err, "TestQueueFetchWithInvalidPartition failed")

	req := rpcpb.AcquireQueueFetchRequest()
	req.ID = tid
	req.Group = g1
	req.Consumer = 0
	req.Partition = 2
	resp := getTestFetchResp(t, store, req)
	assert.True(t, resp.Removed, "TestQueueFetchWithInvalidPartition failed")
}

func TestQueueJoin(t *testing.T) {
	store, deferFunc := NewTestStorage(t, true)
	defer deferFunc()

	tid := uint64(1)
	g1 := []byte("g1")
	state := &metapb.QueueState{
		Partitions: 2,
		States: []metapb.Partiton{
			{}, {},
		},
		Timeout: 60,
	}
	buf := goetty.NewByteBuf(32)
	key := QueueMetaKey(tid, buf)
	err := store.Set(key, protoc.MustMarshal(state))
	assert.NoError(t, err, "TestQueueJoin failed")

	req := rpcpb.AcquireQueueJoinGroupRequest()
	req.ID = tid
	req.Group = g1

	resp := getTestJoinResp(t, store, req)
	assert.Equal(t, uint32(0), resp.Index, "TestQueueJoin failed")
	assert.Equal(t, 2, len(resp.Partitions), "TestQueueJoin failed")
	assert.Equal(t, 2, len(resp.Versions), "TestQueueJoin failed")
	pvs := []uint32{0, 1}
	vs := []uint64{0, 0}
	for idx := range resp.Partitions {
		assert.Equal(t, pvs[idx], resp.Partitions[idx], "TestQueueJoin failed")
		assert.Equal(t, vs[idx], resp.Versions[idx], "TestQueueJoin failed")
	}

	state = getTestQueueuState(t, store, tid, g1)
	assert.Equal(t, uint32(1), state.Consumers, "TestQueueJoin failed")

	versionValues := []uint64{0, 0}
	consumerValues := []uint32{0, 0}
	stateValues := []metapb.PartitonState{metapb.PSRebalancing, metapb.PSRebalancing}
	for i := 0; i < 2; i++ {
		assert.Equal(t, versionValues[i], state.States[i].Version, "TestQueueJoin failed")
		assert.Equal(t, consumerValues[i], state.States[i].Consumer, "TestQueueJoin failed")
		assert.Equal(t, stateValues[i], state.States[i].State, "TestQueueJoin failed")
		assert.Equal(t, uint64(0), state.States[i].Completed, "TestQueueJoin failed")
		assert.Equal(t, uint64(0), state.States[i].LastFetchCount, "TestQueueJoin failed")
		assert.True(t, state.States[i].LastFetchTS > 0, "TestQueueJoin failed")
	}
}

func TestQueueFetchAfterJoin(t *testing.T) {
	store, deferFunc := NewTestStorage(t, true)
	defer deferFunc()

	tid := uint64(1)
	g1 := []byte("g1")
	state := &metapb.QueueState{
		Partitions: 2,
		States: []metapb.Partiton{
			{}, {},
		},
		Timeout: 60,
	}
	buf := goetty.NewByteBuf(32)
	key := QueueMetaKey(tid, buf)
	err := store.Set(key, protoc.MustMarshal(state))
	assert.NoError(t, err, "TestQueueFetchAfterJoin failed")

	values := [][]byte{[]byte("1"), []byte("2"), []byte("3")}
	err = store.PutToQueue(tid, 0, metapb.DefaultGroup, values...)
	assert.NoError(t, err, "TestQueueFetchAfterJoin failed")

	req := rpcpb.AcquireQueueJoinGroupRequest()
	req.ID = tid
	req.Group = g1

	resp := getTestJoinResp(t, store, req)
	fetch := rpcpb.AcquireQueueFetchRequest()
	fetch.ID = tid
	fetch.Group = g1
	fetch.Consumer = resp.Index
	fetch.Partition = resp.Partitions[0]
	fetch.Count = 3
	fetch.MaxBytes = 2
	fetchResp := getTestFetchResp(t, store, fetch)
	assert.False(t, fetchResp.Removed, "TestQueueFetchAfterJoin failed")
	assert.Equal(t, 2, len(fetchResp.Items), "TestQueueFetchAfterJoin failed")

	state = getTestQueueuState(t, store, tid, g1)
	assert.Equal(t, uint32(1), state.Consumers, "TestQueueFetchAfterJoin failed")

	versionValues := []uint64{0, 0}
	consumerValues := []uint32{0, 0}
	countValues := []uint64{2, 0}
	stateValues := []metapb.PartitonState{metapb.PSRunning, metapb.PSRebalancing}
	for i := 0; i < 2; i++ {
		assert.Equal(t, versionValues[i], state.States[i].Version, "TestQueueFetchAfterJoin failed")
		assert.Equal(t, consumerValues[i], state.States[i].Consumer, "TestQueueFetchAfterJoin failed")
		assert.Equal(t, stateValues[i], state.States[i].State, "TestQueueFetchAfterJoin failed")
		assert.Equal(t, uint64(0), state.States[i].Completed, "TestQueueFetchAfterJoin failed")
		assert.Equal(t, countValues[i], state.States[i].LastFetchCount, "TestQueueFetchAfterJoin failed")
		assert.True(t, state.States[i].LastFetchTS > 0, "TestQueueFetchAfterJoin failed")
	}
}

func TestQueueFetchWithRebalancing(t *testing.T) {
	store, deferFunc := NewTestStorage(t, true)
	defer deferFunc()

	tid := uint64(1)
	g1 := []byte("g1")
	state := &metapb.QueueState{
		Consumers:  1,
		Partitions: 1,
		States: []metapb.Partiton{
			{
				Consumer:       0,
				Version:        1,
				State:          metapb.PSRebalancing,
				LastFetchCount: 10,
				LastFetchTS:    time.Now().Unix(),
			},
		},
		Timeout: 60,
	}
	buf := goetty.NewByteBuf(32)
	key := QueueStateKey(tid, g1, buf)
	err := store.Set(key, protoc.MustMarshal(state))
	assert.NoError(t, err, "TestQueueFetchWithRebalancing failed")

	store.Set(QueueMetaKey(tid, buf), protoc.MustMarshal(state))

	err = store.PutToQueue(tid, 0, metapb.DefaultGroup, []byte("1"))
	assert.NoError(t, err, "TestQueueFetchWithRebalancing failed")

	req := rpcpb.AcquireQueueFetchRequest()
	req.Reset()
	req.ID = tid
	req.Group = g1
	req.Consumer = 0
	req.Version = 1
	req.Partition = 0

	resp := getTestFetchResp(t, store, req)
	assert.Equal(t, uint64(0), resp.LastOffset, "TestQueueFetchWithRebalancing failed")
	assert.Empty(t, resp.Items, "TestQueueFetchWithRebalancing failed")
	assert.False(t, resp.Removed, "TestQueueFetchWithRebalancing failed")

	state.States[0].LastFetchCount = 0
	err = store.Set(key, protoc.MustMarshal(state))
	assert.NoError(t, err, "TestQueueFetchWithRebalancing failed")

	req.Reset()
	req.ID = tid
	req.Group = g1
	req.Consumer = 0
	req.Version = 1
	req.Partition = 0
	req.Count = 10
	resp = getTestFetchResp(t, store, req)
	assert.Equal(t, uint64(1), resp.LastOffset, "TestQueueFetchWithRebalancing failed")
	assert.Equal(t, 1, len(resp.Items), "TestQueueFetchWithRebalancing failed")
	assert.False(t, resp.Removed, "TestQueueFetchWithRebalancing failed")
}

func TestQueueFetchWithRemoveTimeoutConsumer(t *testing.T) {
	store, deferFunc := NewTestStorage(t, true)
	defer deferFunc()

	tid := uint64(1)
	g1 := []byte("g1")

	state := &metapb.QueueState{
		Consumers:  2,
		Partitions: 2,
		States: []metapb.Partiton{
			{
				Consumer:    0,
				Version:     0,
				State:       metapb.PSRunning,
				LastFetchTS: time.Now().Unix() - 60 - 1,
			},
			{
				Consumer:    1,
				Version:     1,
				State:       metapb.PSRunning,
				LastFetchTS: time.Now().Unix(),
			},
		},
		Timeout: 60,
	}
	buf := goetty.NewByteBuf(32)
	key := QueueMetaKey(tid, buf)
	err := store.Set(key, protoc.MustMarshal(state))
	assert.NoError(t, err, "TestQueueConcurrencyFetchWithRemoveTimeoutConsumer failed")

	req := rpcpb.AcquireQueueFetchRequest()
	for i := 0; i < 2; i++ {
		req.Reset()
		req.ID = tid
		req.Group = g1
		req.Consumer = 0
		req.Version = 0

		resp := getTestFetchResp(t, store, req)
		assert.True(t, resp.Removed, "TestQueueConcurrencyFetchWithRemoveTimeoutConsumer failed")

		state = getTestQueueuState(t, store, tid, g1)
		assert.Equal(t, uint32(0), state.Consumers, "TestQueueConcurrencyFetchWithRemoveTimeoutConsumer failed")

		versionValues := []uint64{1, 2}
		consumerValues := []uint32{0, 0}
		stateValues := []metapb.PartitonState{metapb.PSRebalancing, metapb.PSRebalancing}
		for i := 0; i < 2; i++ {
			assert.Equal(t, versionValues[i], state.States[i].Version, "TestQueueConcurrencyFetchWithRemoveTimeoutConsumer failed")
			assert.Equal(t, consumerValues[i], state.States[i].Consumer, "TestQueueConcurrencyFetchWithRemoveTimeoutConsumer failed")
			assert.Equal(t, stateValues[i], state.States[i].State, "TestQueueConcurrencyFetchWithRemoveTimeoutConsumer failed")
			assert.Equal(t, uint64(0), state.States[i].Completed, "TestQueueConcurrencyFetchWithRemoveTimeoutConsumer failed")
			assert.Equal(t, uint64(0), state.States[i].LastFetchCount, "TestQueueConcurrencyFetchWithRemoveTimeoutConsumer failed")
			assert.True(t, state.States[i].LastFetchTS > 0, "TestQueueConcurrencyFetchWithRemoveTimeoutConsumer failed")
		}
	}

	req.Reset()
	req.ID = tid
	req.Group = g1
	req.Consumer = 0
	req.Version = 0
	resp := getTestFetchResp(t, store, req)
	assert.True(t, resp.Removed, "TestQueueConcurrencyFetchWithRemoveTimeoutConsumer failed")

	req.Reset()
	req.ID = tid
	req.Group = g1
	req.Consumer = 1
	req.Version = 1
	resp = getTestFetchResp(t, store, req)
	assert.True(t, resp.Removed, "TestQueueConcurrencyFetchWithRemoveTimeoutConsumer failed")
}

func TestQueueJoinAfterClearConsumers(t *testing.T) {
	store, deferFunc := NewTestStorage(t, true)
	defer deferFunc()

	tid := uint64(1)
	g1 := []byte("g1")

	now := time.Now().Unix()
	state := &metapb.QueueState{
		Consumers:  0,
		Partitions: 2,
		States: []metapb.Partiton{
			{
				Consumer:    0,
				Version:     1,
				State:       metapb.PSRebalancing,
				LastFetchTS: now - 60 - 1,
			},
			{
				Consumer:    0,
				Version:     2,
				State:       metapb.PSRebalancing,
				LastFetchTS: now,
			},
		},
		Timeout: 60,
	}
	buf := goetty.NewByteBuf(32)
	key := QueueStateKey(tid, g1, buf)
	err := store.Set(key, protoc.MustMarshal(state))
	assert.NoError(t, err, "TestQueueJoinAfterClearConsumers failed")

	req := rpcpb.AcquireQueueJoinGroupRequest()
	req.ID = tid
	req.Group = g1

	resp := getTestJoinResp(t, store, req)
	assert.Equal(t, uint32(0), resp.Index, "TestQueueJoin failed")
	assert.Equal(t, 2, len(resp.Partitions), "TestQueueJoin failed")
	assert.Equal(t, 2, len(resp.Versions), "TestQueueJoin failed")
	pvs := []uint32{0, 1}
	vs := []uint64{1, 2}
	for idx := range resp.Partitions {
		assert.Equal(t, pvs[idx], resp.Partitions[idx], "TestQueueJoin failed")
		assert.Equal(t, vs[idx], resp.Versions[idx], "TestQueueJoin failed")
	}

	state = getTestQueueuState(t, store, tid, g1)
	assert.Equal(t, uint32(1), state.Consumers, "TestQueueJoin failed")

	versionValues := []uint64{1, 2}
	consumerValues := []uint32{0, 0}
	stateValues := []metapb.PartitonState{metapb.PSRebalancing, metapb.PSRebalancing}
	for i := 0; i < 2; i++ {
		assert.Equal(t, versionValues[i], state.States[i].Version, "TestQueueJoin failed")
		assert.Equal(t, consumerValues[i], state.States[i].Consumer, "TestQueueJoin failed")
		assert.Equal(t, stateValues[i], state.States[i].State, "TestQueueJoin failed")
		assert.Equal(t, uint64(0), state.States[i].Completed, "TestQueueJoin failed")
		assert.Equal(t, uint64(0), state.States[i].LastFetchCount, "TestQueueJoin failed")
		assert.True(t, state.States[i].LastFetchTS >= now, "TestQueueJoin failed")
	}
}

func TestQueueFetchWithCommitOffset(t *testing.T) {
	store, deferFunc := NewTestStorage(t, true)
	defer deferFunc()

	tid := uint64(1)
	g1 := []byte("g1")

	state := &metapb.QueueState{
		Consumers:  2,
		Partitions: 2,
		States: []metapb.Partiton{
			{
				Consumer:    0,
				Version:     0,
				State:       metapb.PSRunning,
				LastFetchTS: time.Now().Unix(),
			},
			{
				Consumer:    1,
				Version:     1,
				State:       metapb.PSRunning,
				LastFetchTS: time.Now().Unix(),
			},
		},
		Timeout: 60,
	}
	buf := goetty.NewByteBuf(32)
	key := QueueStateKey(tid, g1, buf)
	err := store.Set(key, protoc.MustMarshal(state))
	assert.NoError(t, err, "TestQueueConcurrencyFetchWithCommitOffset failed")

	req := rpcpb.AcquireQueueFetchRequest()
	req.ID = tid
	req.Group = g1
	req.Partition = 0
	req.Consumer = 0
	req.Version = 0
	req.CompletedOffset = 10
	resp := getTestFetchResp(t, store, req)
	assert.False(t, resp.Removed, "TestQueueConcurrencyFetchWithCommitOffset failed")

	req.Reset()
	req.ID = tid
	req.Group = g1
	req.Partition = 1
	req.Consumer = 1
	req.Version = 1
	req.CompletedOffset = 20
	resp = getTestFetchResp(t, store, req)
	assert.False(t, resp.Removed, "TestQueueConcurrencyFetchWithCommitOffset failed")

	state = getTestQueueuState(t, store, tid, g1)
	assert.Equal(t, uint32(2), state.Consumers, "TestQueueConcurrencyFetchWithCommitOffset failed")

	offsetValues := []uint64{10, 20}
	versionValues := []uint64{0, 1}
	consumerValues := []uint32{0, 1}
	stateValues := []metapb.PartitonState{metapb.PSRunning, metapb.PSRunning}
	for i := 0; i < 2; i++ {
		assert.Equal(t, versionValues[i], state.States[i].Version, "TestQueueConcurrencyFetchWithCommitOffset failed")
		assert.Equal(t, consumerValues[i], state.States[i].Consumer, "TestQueueConcurrencyFetchWithCommitOffset failed")
		assert.Equal(t, stateValues[i], state.States[i].State, "TestQueueConcurrencyFetchWithCommitOffset failed")
		assert.Equal(t, offsetValues[i], state.States[i].Completed, "TestQueueConcurrencyFetchWithCommitOffset failed")
		assert.Equal(t, uint64(0), state.States[i].LastFetchCount, "TestQueueConcurrencyFetchWithCommitOffset failed")
		assert.True(t, state.States[i].LastFetchTS > 0, "TestQueueConcurrencyFetchWithCommitOffset failed")
	}
}

func TestQueueFetchWithStaleCommitOffset(t *testing.T) {
	store, deferFunc := NewTestStorage(t, true)
	defer deferFunc()

	tid := uint64(1)
	g1 := []byte("g1")

	state := &metapb.QueueState{
		Consumers:  3,
		Partitions: 3,
		States: []metapb.Partiton{
			{
				Consumer:       0,
				Version:        1,
				State:          metapb.PSRunning,
				LastFetchCount: 0,
				LastFetchTS:    time.Now().Unix(),
			},
			{
				Consumer:       1,
				Version:        1,
				State:          metapb.PSRebalancing,
				LastFetchCount: 0,
				LastFetchTS:    time.Now().Unix(),
			},
			{
				Consumer:       2,
				Version:        1,
				State:          metapb.PSRebalancing,
				LastFetchCount: 10,
				LastFetchTS:    time.Now().Unix(),
			},
		},
		Timeout: 60,
	}
	buf := goetty.NewByteBuf(32)
	key := QueueStateKey(tid, g1, buf)
	err := store.Set(key, protoc.MustMarshal(state))
	assert.NoError(t, err, "TestQueueConcurrencyFetchWithStaleCommitOffset failed")

	req := rpcpb.AcquireQueueFetchRequest()
	req.ID = tid
	req.Group = g1
	req.Partition = 0
	req.Consumer = 0
	req.Version = 0
	req.CompletedOffset = 10
	resp := getTestFetchResp(t, store, req)
	assert.True(t, resp.Removed, "TestQueueConcurrencyFetchWithStaleCommitOffset failed")

	req.Reset()
	req.ID = tid
	req.Group = g1
	req.Partition = 1
	req.Consumer = 1
	req.Version = 0
	req.CompletedOffset = 20
	resp = getTestFetchResp(t, store, req)
	assert.True(t, resp.Removed, "TestQueueConcurrencyFetchWithStaleCommitOffset failed")

	req.Reset()
	req.ID = tid
	req.Group = g1
	req.Partition = 2
	req.Consumer = 2
	req.Version = 0
	req.CompletedOffset = 30
	resp = getTestFetchResp(t, store, req)
	assert.True(t, resp.Removed, "TestQueueConcurrencyFetchWithStaleCommitOffset failed")

	state = getTestQueueuState(t, store, tid, g1)
	assert.Equal(t, uint32(3), state.Consumers, "TestQueueConcurrencyFetchWithStaleCommitOffset failed")

	offsetValues := []uint64{0, 0, 30}
	versionValues := []uint64{1, 1, 1}
	consumerValues := []uint32{0, 1, 2}
	stateValues := []metapb.PartitonState{metapb.PSRunning, metapb.PSRebalancing, metapb.PSRunning}
	for i := 0; i < 1; i++ {
		assert.Equal(t, versionValues[i], state.States[i].Version, "TestQueueConcurrencyFetchWithStaleCommitOffset failed")
		assert.Equal(t, consumerValues[i], state.States[i].Consumer, "TestQueueConcurrencyFetchWithStaleCommitOffset failed")
		assert.Equal(t, stateValues[i], state.States[i].State, "TestQueueConcurrencyFetchWithStaleCommitOffset failed")
		assert.Equal(t, offsetValues[i], state.States[i].Completed, "TestQueueConcurrencyFetchWithStaleCommitOffset failed")
		assert.Equal(t, uint64(0), state.States[i].LastFetchCount, "TestQueueConcurrencyFetchWithStaleCommitOffset failed")
		assert.True(t, state.States[i].LastFetchTS > 0, "TestQueueConcurrencyFetchWithStaleCommitOffset failed")
	}
}

func TestQueueScan(t *testing.T) {
	store, deferFunc := NewTestStorage(t, true)
	defer deferFunc()

	tid := uint64(1)
	c1 := []byte("consumer-01")

	buf := goetty.NewByteBuf(32)
	store.Set(QueueMetaKey(tid, buf), protoc.MustMarshal(&metapb.QueueState{
		Partitions: 1,
	}))

	values := [][]byte{[]byte("1"), []byte("2"), []byte("3")}
	err := store.PutToQueue(tid, 0, metapb.DefaultGroup, values...)
	assert.NoError(t, err, "TestQueueScan failed")

	for i := 0; i < 2; i++ {
		req := rpcpb.AcquireQueueScanRequest()
		req.ID = tid
		req.Partition = 0
		req.Consumer = c1
		req.CompletedOffset = 0
		req.Count = 3

		value, err := store.ExecCommand(req)
		assert.NoError(t, err, "TestQueueScan failed")
		assert.NotEmpty(t, value, "TestQueueScan failed")

		resp := &rpcpb.QueueFetchResponse{}
		protoc.MustUnmarshal(resp, value)
		assert.Equal(t, 3, len(resp.Items), "TestQueueScan failed")
	}
}

func TestQueueScanWithCompletedOffset(t *testing.T) {
	store, deferFunc := NewTestStorage(t, true)
	defer deferFunc()

	tid := uint64(1)
	c1 := []byte("consumer-01")

	buf := goetty.NewByteBuf(32)
	store.Set(QueueMetaKey(tid, buf), protoc.MustMarshal(&metapb.QueueState{
		Partitions: 1,
	}))

	values := [][]byte{[]byte("1"), []byte("2"), []byte("3")}
	err := store.PutToQueue(tid, 0, metapb.DefaultGroup, values...)
	assert.NoError(t, err, "TestQueueScan failed")

	now := time.Now().Unix()
	value := make([]byte, 16, 16)
	goetty.Uint64ToBytesTo(1, value)
	goetty.Int64ToBytesTo(now, value[8:])
	err = store.Set(committedOffsetKey(PartitionKey(tid, 0), c1, buf), value)
	assert.NoError(t, err, "TestQueueScan failed")

	req := rpcpb.AcquireQueueScanRequest()
	req.ID = tid
	req.Partition = 0
	req.Consumer = c1
	req.CompletedOffset = 0
	req.Count = 3

	value, err = store.ExecCommand(req)
	assert.NoError(t, err, "TestQueueScan failed")
	assert.NotEmpty(t, value, "TestQueueScan failed")

	resp := &rpcpb.QueueFetchResponse{}
	protoc.MustUnmarshal(resp, value)
	assert.Equal(t, 2, len(resp.Items), "TestQueueScan failed")
}

func TestQueueCommit(t *testing.T) {
	store, deferFunc := NewTestStorage(t, true)
	defer deferFunc()

	tid := uint64(1)
	c1 := []byte("consumer-01")

	buf := goetty.NewByteBuf(32)
	store.Set(QueueMetaKey(tid, buf), protoc.MustMarshal(&metapb.QueueState{
		Partitions: 1,
	}))

	key := committedOffsetKey(PartitionKey(tid, 0), c1, buf)

	req := rpcpb.AcquireQueueCommitRequest()
	req.ID = tid
	req.Partition = 0
	req.Consumer = c1
	req.CompletedOffset = 10
	_, err := store.ExecCommand(req)
	assert.NoError(t, err, "TestQueueCommit failed")

	value, err := store.Get(key)
	assert.NoError(t, err, "TestQueueCommit failed")
	assert.NotEmpty(t, value, "TestQueueCommit failed")
	assert.Equal(t, uint64(10), goetty.Byte2UInt64(value), "TestQueueCommit failed")

	req = rpcpb.AcquireQueueCommitRequest()
	req.ID = tid
	req.Partition = 0
	req.Consumer = c1
	req.CompletedOffset = 2
	_, err = store.ExecCommand(req)
	assert.NoError(t, err, "TestQueueCommit failed")

	value, err = store.Get(key)
	assert.NoError(t, err, "TestQueueCommit failed")
	assert.NotEmpty(t, value, "TestQueueCommit failed")
	assert.Equal(t, uint64(2), goetty.Byte2UInt64(value), "TestQueueCommit failed")
}

func getTestQueueuState(t *testing.T, store Storage, tid uint64, g []byte) *metapb.QueueState {
	buf := goetty.NewByteBuf(32)
	key := QueueStateKey(tid, g, buf)
	data, err := store.Get(key)
	assert.NoError(t, err, "getQueueuState failed")

	state := &metapb.QueueState{}
	protoc.MustUnmarshal(state, data)
	return state
}

func getTestFetchResp(t *testing.T, store Storage, req *rpcpb.QueueFetchRequest) *rpcpb.QueueFetchResponse {
	data, err := store.ExecCommand(req)
	assert.NoError(t, err, "getTestConcurrencyResp failed")

	resp := &rpcpb.QueueFetchResponse{}
	protoc.MustUnmarshal(resp, data)
	return resp
}

func getTestJoinResp(t *testing.T, store Storage, req *rpcpb.QueueJoinGroupRequest) *rpcpb.QueueJoinGroupResponse {
	data, err := store.ExecCommand(req)
	assert.NoError(t, err, "getTestJoinResp failed")

	resp := &rpcpb.QueueJoinGroupResponse{}
	protoc.MustUnmarshal(resp, data)
	return resp
}
