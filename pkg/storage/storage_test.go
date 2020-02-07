package storage

import (
	"fmt"
	"testing"
	"time"

	bhmetapb "github.com/deepfabric/beehive/pb/metapb"
	"github.com/deepfabric/busybee/pkg/pb/metapb"
	"github.com/deepfabric/busybee/pkg/pb/rpcpb"
	"github.com/deepfabric/busybee/pkg/util"
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
	assert.Equal(t, uint64(1), resp.From, "TestAllocID failed")
	assert.Equal(t, uint64(1), resp.To, "TestAllocID failed")

	data, err = store.ExecCommand(&rpcpb.AllocIDRequest{
		Key:   key,
		Batch: 2,
	})
	assert.NoError(t, err, "TestAllocID failed")
	assert.NotEmpty(t, data, "TestAllocID failed")

	protoc.MustUnmarshal(resp, data)
	assert.Equal(t, uint64(2), resp.From, "TestAllocID failed")
	assert.Equal(t, uint64(3), resp.To, "TestAllocID failed")

	_, err = store.ExecCommand(&rpcpb.ResetIDRequest{
		Key:       key,
		StartWith: 0,
	})
	data, _ = store.ExecCommand(&rpcpb.AllocIDRequest{
		Key:   key,
		Batch: 2,
	})
	protoc.MustUnmarshal(resp, data)
	assert.Equal(t, uint64(1), resp.From, "TestAllocID failed")
	assert.Equal(t, uint64(2), resp.To, "TestAllocID failed")
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

	data, err = store.ExecCommand(&rpcpb.GetRequest{
		Key: key,
	})
	assert.NoError(t, err, "TestBMCreate failed")
	assert.NotEmpty(t, data, "TestBMCreate failed")

	resp := &rpcpb.BytesResponse{}
	protoc.MustUnmarshal(resp, data)
	assert.NotEmpty(t, resp.Value, "TestBMCreate failed")

	bm := util.MustParseBM(resp.Value)
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

	data, err = store.ExecCommand(&rpcpb.GetRequest{
		Key: key,
	})
	assert.NoError(t, err, "TestBMAdd failed")
	assert.NotEmpty(t, data, "TestBMAdd failed")

	resp := &rpcpb.BytesResponse{}
	protoc.MustUnmarshal(resp, data)
	assert.NotEmpty(t, resp.Value, "TestBMAdd failed")

	bm := util.MustParseBM(resp.Value)
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

	data, err = store.ExecCommand(&rpcpb.GetRequest{
		Key: key,
	})
	assert.NoError(t, err, "TestBMRemove failed")
	assert.NotEmpty(t, data, "TestBMRemove failed")

	resp := &rpcpb.BytesResponse{}
	protoc.MustUnmarshal(resp, data)
	assert.NotEmpty(t, resp.Value, "TestBMRemove failed")

	bm := util.MustParseBM(resp.Value)
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
		Type:  0,
		Value: "id0-v1",
	}, metapb.IDValue{
		Type:  1,
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
		Type:  0,
		Value: "id0-v2",
	}, metapb.IDValue{
		Type:  2,
		Value: "id2-v1",
	})
	data, err = store.ExecCommand(req)
	assert.NoError(t, err, "TestUpdateMapping failed")
	assert.NotEmpty(t, data, "TestUpdateMapping failed")
	resp.Reset()
	protoc.MustUnmarshal(resp, data)
	assert.Equal(t, 3, len(resp.Values), "TestUpdateMapping failed")
	for _, value := range resp.Values {
		if value.Type == 0 {
			assert.Equal(t, "id0-v2", value.Value, "TestUpdateMapping failed")
		} else if value.Type == 1 {
			assert.Equal(t, "id1-v1", value.Value, "TestUpdateMapping failed")
		} else if value.Type == 2 {
			assert.Equal(t, "id2-v1", value.Value, "TestUpdateMapping failed")
		}
	}
}
