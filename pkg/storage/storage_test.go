package storage

import (
	"flag"
	"io/ioutil"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/deepfabric/beehive/storage"
	"github.com/deepfabric/beehive/storage/nemo"
	"github.com/deepfabric/busybee/pkg/pb/metapb"
	"github.com/deepfabric/busybee/pkg/pb/rpcpb"
	"github.com/fagongzi/util/protoc"
	"github.com/stretchr/testify/assert"
)

var (
	tmp = "/tmp/busy"
)

var (
	beehiveCfg = `
	# The beehive example configuration

# The node name in the cluster
name = "node1"

# The RPC address to serve requests
raftAddr = "127.0.0.1:10001"

# The RPC address to serve requests
rpcAddr = "127.0.0.1:10002"

[prophet]
# The application and prophet RPC address, send heartbeats, alloc id, watch event, etc. required
rpcAddr = "127.0.0.1:9527"

# Store cluster metedata
storeMetadata = true

# The embed etcd client address, required while storeMetadata is true
clientAddr = "127.0.0.1:2371"

# The embed etcd peer address, required while storeMetadata is true
peerAddr = "127.0.0.1:2381"
	`
)

func TestSetAndGet(t *testing.T) {
	os.RemoveAll(tmp)
	s, err := nemo.NewStorage(filepath.Join(tmp, "nemo"))
	assert.NoError(t, err, "TestSetAndGet failed")
	defer s.Close()

	err = ioutil.WriteFile(filepath.Join(tmp, "cfg.toml"), []byte(beehiveCfg), os.ModeAppend)
	assert.NoError(t, err, "TestSetAndGet failed")
	flag.Set("beehive-cfg", filepath.Join(tmp, "cfg.toml"))

	store, err := NewStorage("127.0.0.1:12345", tmp, []storage.MetadataStorage{s}, []storage.DataStorage{s})
	assert.NoError(t, err, "TestSetAndGet failed")
	defer store.Close()

	key := []byte("key1")
	value := []byte("value1")

	data, err := store.ExecCommand(&rpcpb.SetRequest{
		Key:   key,
		Value: value,
	})
	assert.NoError(t, err, "TestSetAndGet failed")
	assert.NotEmpty(t, data, "TestSetAndGet failed")

	data, err = store.ExecCommand(&rpcpb.GetRequest{
		Key: key,
	})
	assert.NoError(t, err, "TestSetAndGet failed")
	assert.NotEmpty(t, data, "TestSetAndGet failed")
	resp := &rpcpb.GetResponse{}
	protoc.MustUnmarshal(resp, data)
	assert.Equal(t, string(value), string(resp.Value), "TestSetAndGet failed")
}

func TestDelete(t *testing.T) {
	os.RemoveAll(tmp)
	s, err := nemo.NewStorage(filepath.Join(tmp, "nemo"))
	assert.NoError(t, err, "TestDelete failed")
	defer s.Close()

	err = ioutil.WriteFile(filepath.Join(tmp, "cfg.toml"), []byte(beehiveCfg), os.ModeAppend)
	assert.NoError(t, err, "TestDelete failed")
	flag.Set("beehive-cfg", filepath.Join(tmp, "cfg.toml"))

	store, err := NewStorage("127.0.0.1:12345", tmp, []storage.MetadataStorage{s}, []storage.DataStorage{s})
	assert.NoError(t, err, "TestDelete failed")
	defer store.Close()

	key := []byte("key1")
	value := []byte("value1")

	data, err := store.ExecCommand(&rpcpb.SetRequest{
		Key:   key,
		Value: value,
	})
	assert.NoError(t, err, "TestDelete failed")

	data, err = store.ExecCommand(&rpcpb.DeleteRequest{
		Key: key,
	})
	assert.NoError(t, err, "TestDelete failed")
	assert.NotEmpty(t, data, "TestDelete failed")

	data, err = store.ExecCommand(&rpcpb.GetRequest{
		Key: key,
	})
	assert.NoError(t, err, "TestDelete failed")
	assert.NotEmpty(t, data, "TestDelete failed")
	resp := &rpcpb.GetResponse{}
	protoc.MustUnmarshal(resp, data)
	assert.Empty(t, resp.Value, "TestDelete failed")
}

func TestBMCreate(t *testing.T) {
	os.RemoveAll(tmp)
	s, err := nemo.NewStorage(filepath.Join(tmp, "nemo"))
	assert.NoError(t, err, "TestBMCreate failed")
	defer s.Close()

	err = ioutil.WriteFile(filepath.Join(tmp, "cfg.toml"), []byte(beehiveCfg), os.ModeAppend)
	assert.NoError(t, err, "TestBMCreate failed")
	flag.Set("beehive-cfg", filepath.Join(tmp, "cfg.toml"))

	store, err := NewStorage("127.0.0.1:12345", tmp, []storage.MetadataStorage{s}, []storage.DataStorage{s})
	assert.NoError(t, err, "TestBMCreate failed")
	defer store.Close()

	key := []byte("key1")
	value := []uint64{1, 2, 3, 4, 5}

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

	resp := &rpcpb.GetResponse{}
	protoc.MustUnmarshal(resp, data)
	assert.NotEmpty(t, resp.Value, "TestBMCreate failed")

	bm := mustParseBitmap(resp.Value)
	assert.Equal(t, uint64(len(value)), bm.Count(), "TestBMCreate failed")
}

func TestBMAdd(t *testing.T) {
	os.RemoveAll(tmp)
	s, err := nemo.NewStorage(filepath.Join(tmp, "nemo"))
	assert.NoError(t, err, "TestBMAdd failed")
	defer s.Close()

	err = ioutil.WriteFile(filepath.Join(tmp, "cfg.toml"), []byte(beehiveCfg), os.ModeAppend)
	assert.NoError(t, err, "TestBMAdd failed")
	flag.Set("beehive-cfg", filepath.Join(tmp, "cfg.toml"))

	store, err := NewStorage("127.0.0.1:12345", tmp, []storage.MetadataStorage{s}, []storage.DataStorage{s})
	assert.NoError(t, err, "TestBMAdd failed")
	defer store.Close()

	key := []byte("key1")
	value := []uint64{1, 2, 3, 4, 5}
	value2 := []uint64{6, 7, 8, 9, 10}

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

	resp := &rpcpb.GetResponse{}
	protoc.MustUnmarshal(resp, data)
	assert.NotEmpty(t, resp.Value, "TestBMAdd failed")

	bm := mustParseBitmap(resp.Value)
	assert.Equal(t, uint64(len(value)+len(value2)), bm.Count(), "TestBMAdd failed")
}

func TestBMRemove(t *testing.T) {
	os.RemoveAll(tmp)
	s, err := nemo.NewStorage(filepath.Join(tmp, "nemo"))
	assert.NoError(t, err, "TestBMRemove failed")
	defer s.Close()

	err = ioutil.WriteFile(filepath.Join(tmp, "cfg.toml"), []byte(beehiveCfg), os.ModeAppend)
	assert.NoError(t, err, "TestBMRemove failed")
	flag.Set("beehive-cfg", filepath.Join(tmp, "cfg.toml"))

	store, err := NewStorage("127.0.0.1:12345", tmp, []storage.MetadataStorage{s}, []storage.DataStorage{s})
	assert.NoError(t, err, "TestBMRemove failed")
	defer store.Close()

	key := []byte("key1")
	value := []uint64{1, 2, 3, 4, 5}

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

	resp := &rpcpb.GetResponse{}
	protoc.MustUnmarshal(resp, data)
	assert.NotEmpty(t, resp.Value, "TestBMRemove failed")

	bm := mustParseBitmap(resp.Value)
	assert.Equal(t, uint64(2), bm.Count(), "TestBMRemove failed")
}

func TestBMClear(t *testing.T) {
	os.RemoveAll(tmp)
	s, err := nemo.NewStorage(filepath.Join(tmp, "nemo"))
	assert.NoError(t, err, "TestBMClear failed")
	defer s.Close()

	err = ioutil.WriteFile(filepath.Join(tmp, "cfg.toml"), []byte(beehiveCfg), os.ModeAppend)
	assert.NoError(t, err, "TestBMClear failed")
	flag.Set("beehive-cfg", filepath.Join(tmp, "cfg.toml"))

	store, err := NewStorage("127.0.0.1:12345", tmp, []storage.MetadataStorage{s}, []storage.DataStorage{s})
	assert.NoError(t, err, "TestBMClear failed")
	defer store.Close()

	key := []byte("key1")
	value := []uint64{1, 2, 3, 4, 5}

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
	assert.NotEmpty(t, data, "TestBMClear failed")

	resp := &rpcpb.BMCountResponse{}
	protoc.MustUnmarshal(resp, data)
	assert.Equal(t, uint64(0), resp.Count, "TestBMClear failed")
}

func TestBMContains(t *testing.T) {
	os.RemoveAll(tmp)
	s, err := nemo.NewStorage(filepath.Join(tmp, "nemo"))
	assert.NoError(t, err, "TestBMContains failed")
	defer s.Close()

	err = ioutil.WriteFile(filepath.Join(tmp, "cfg.toml"), []byte(beehiveCfg), os.ModeAppend)
	assert.NoError(t, err, "TestBMContains failed")
	flag.Set("beehive-cfg", filepath.Join(tmp, "cfg.toml"))

	store, err := NewStorage("127.0.0.1:12345", tmp, []storage.MetadataStorage{s}, []storage.DataStorage{s})
	assert.NoError(t, err, "TestBMContains failed")
	defer store.Close()

	key := []byte("key1")
	value := []uint64{1, 2, 3, 4, 5}

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

	resp := &rpcpb.BMContainsResponse{}
	protoc.MustUnmarshal(resp, data)
	assert.True(t, resp.Contains, "TestBMContains failed")
}

func TestBMDel(t *testing.T) {
	os.RemoveAll(tmp)
	s, err := nemo.NewStorage(filepath.Join(tmp, "nemo"))
	assert.NoError(t, err, "TestBMDel failed")
	defer s.Close()

	err = ioutil.WriteFile(filepath.Join(tmp, "cfg.toml"), []byte(beehiveCfg), os.ModeAppend)
	assert.NoError(t, err, "TestBMDel failed")
	flag.Set("beehive-cfg", filepath.Join(tmp, "cfg.toml"))

	store, err := NewStorage("127.0.0.1:12345", tmp, []storage.MetadataStorage{s}, []storage.DataStorage{s})
	assert.NoError(t, err, "TestBMDel failed")
	defer store.Close()

	key := []byte("key1")
	value := []uint64{1, 2, 3, 4, 5}

	data, err := store.ExecCommand(&rpcpb.BMCreateRequest{
		Key:   key,
		Value: value,
	})
	assert.NoError(t, err, "TestBMDel failed")

	data, err = store.ExecCommand(&rpcpb.BMDelRequest{
		Key: key,
	})
	assert.NoError(t, err, "TestBMDel failed")

	data, err = store.ExecCommand(&rpcpb.GetRequest{
		Key: key,
	})
	assert.NoError(t, err, "TestBMDel failed")

	resp := &rpcpb.GetResponse{}
	protoc.MustUnmarshal(resp, data)
	assert.Empty(t, resp.Value, "TestBMDel failed")
}

func TestBMCount(t *testing.T) {
	os.RemoveAll(tmp)
	s, err := nemo.NewStorage(filepath.Join(tmp, "nemo"))
	assert.NoError(t, err, "TestBMCount failed")
	defer s.Close()

	err = ioutil.WriteFile(filepath.Join(tmp, "cfg.toml"), []byte(beehiveCfg), os.ModeAppend)
	assert.NoError(t, err, "TestBMCount failed")
	flag.Set("beehive-cfg", filepath.Join(tmp, "cfg.toml"))

	store, err := NewStorage("127.0.0.1:12345", tmp, []storage.MetadataStorage{s}, []storage.DataStorage{s})
	assert.NoError(t, err, "TestBMCount failed")
	defer store.Close()

	key := []byte("key1")
	value := []uint64{1, 2, 3, 4, 5}

	data, err := store.ExecCommand(&rpcpb.BMCreateRequest{
		Key:   key,
		Value: value,
	})
	assert.NoError(t, err, "TestBMCount failed")

	data, err = store.ExecCommand(&rpcpb.BMCountRequest{
		Key: key,
	})
	assert.NoError(t, err, "TestBMCount failed")

	resp := &rpcpb.BMCountResponse{}
	protoc.MustUnmarshal(resp, data)
	assert.Equal(t, uint64(len(value)), resp.Count, "TestBMCount failed")
}

func TestBMRange(t *testing.T) {
	os.RemoveAll(tmp)
	s, err := nemo.NewStorage(filepath.Join(tmp, "nemo"))
	assert.NoError(t, err, "TestBMRange failed")
	defer s.Close()

	err = ioutil.WriteFile(filepath.Join(tmp, "cfg.toml"), []byte(beehiveCfg), os.ModeAppend)
	assert.NoError(t, err, "TestBMRange failed")
	flag.Set("beehive-cfg", filepath.Join(tmp, "cfg.toml"))

	store, err := NewStorage("127.0.0.1:12345", tmp, []storage.MetadataStorage{s}, []storage.DataStorage{s})
	assert.NoError(t, err, "TestBMRange failed")
	defer store.Close()

	key := []byte("key1")
	value := []uint64{1, 2, 3, 4, 5}

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

	resp := &rpcpb.BMRangeResponse{}
	protoc.MustUnmarshal(resp, data)
	assert.Equal(t, uint64(1), resp.Values[0], "TestBMCount failed")
	assert.Equal(t, uint64(2), resp.Values[1], "TestBMCount failed")

	data, err = store.ExecCommand(&rpcpb.BMRangeRequest{
		Key:   key,
		Start: 0,
		Limit: 2,
	})
	assert.NoError(t, err, "TestBMRange failed")

	resp = &rpcpb.BMRangeResponse{}
	protoc.MustUnmarshal(resp, data)
	assert.Equal(t, uint64(1), resp.Values[0], "TestBMCount failed")
	assert.Equal(t, uint64(2), resp.Values[1], "TestBMCount failed")
}

func TestStartWF(t *testing.T) {
	os.RemoveAll(tmp)
	s, err := nemo.NewStorage(filepath.Join(tmp, "nemo"))
	assert.NoError(t, err, "TestStartWF failed")

	err = ioutil.WriteFile(filepath.Join(tmp, "cfg.toml"), []byte(beehiveCfg), os.ModeAppend)
	assert.NoError(t, err, "TestStartWF failed")
	flag.Set("beehive-cfg", filepath.Join(tmp, "cfg.toml"))

	store, err := NewStorage("127.0.0.1:12345", tmp, []storage.MetadataStorage{s}, []storage.DataStorage{s})
	assert.NoError(t, err, "TestStartWF failed")
	_, err = store.ExecCommand(&rpcpb.StartWFRequest{
		Instance: metapb.WorkflowInstance{
			ID:       1,
			Snapshot: metapb.Workflow{},
		},
	})
	assert.NoError(t, err, "TestStartWF failed")
	assert.True(t, len(store.WatchEvent()) > 0, "TestStartWF failed")

	store.Close()
	s.Close()

	s, err = nemo.NewStorage(filepath.Join(tmp, "nemo"))
	assert.NoError(t, err, "TestStartWF failed")
	defer s.Close()

	store, err = NewStorage("127.0.0.1:12345", tmp, []storage.MetadataStorage{s}, []storage.DataStorage{s})
	assert.NoError(t, err, "TestStartWF failed")
	defer store.Close()

	c := store.WatchEvent()
	time.Sleep(time.Second * 1)
	assert.True(t, len(c) > 0, "TestStartWF failed")
}