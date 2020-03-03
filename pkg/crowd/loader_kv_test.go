package crowd

import (
	"testing"

	"github.com/deepfabric/busybee/pkg/pb/rpcpb"
	"github.com/deepfabric/busybee/pkg/storage"
	"github.com/stretchr/testify/assert"
)

func TestLoadFromKV(t *testing.T) {
	store, deferFunc := storage.NewTestStorage(t, true)
	defer deferFunc()

	key := []byte("bm1")
	_, err := store.ExecCommand(&rpcpb.BMCreateRequest{
		Key:   key,
		Value: []uint32{1, 2, 3},
	})
	assert.NoError(t, err, "TestLoadFromKV failed")

	ld := NewKVLoader(store)
	bm, err := ld.Get(key)
	assert.NoError(t, err, "TestLoadFromKV failed")
	assert.Equal(t, uint64(3), bm.GetCardinality(), "TestLoadFromKV failed")
	assert.Equal(t, uint32(1), bm.Minimum(), "TestLoadFromKV failed")
	assert.Equal(t, uint32(3), bm.Maximum(), "TestLoadFromKV failed")
}
