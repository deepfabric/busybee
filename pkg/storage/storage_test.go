package storage

import (
	"testing"

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
