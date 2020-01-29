package storage

import (
	"time"

	"github.com/deepfabric/beehive"
	"github.com/deepfabric/beehive/raftstore"
	"github.com/deepfabric/beehive/server"
	beehiveStorage "github.com/deepfabric/beehive/storage"
	bhstorage "github.com/deepfabric/beehive/storage"
	"github.com/deepfabric/busybee/pkg/pb/metapb"
	"github.com/deepfabric/busybee/pkg/pb/rpcpb"
	"github.com/fagongzi/util/protoc"
	"github.com/fagongzi/util/task"
)

const (
	defaultRPCTimeout = time.Second * 10
)

// Storage storage
type Storage interface {
	// Start the storage
	Start() error
	// Close close the storage
	Close()
	// WatchInstance watch instance
	WatchEvent() chan Event
	// Set set key value
	Set([]byte, []byte) error
	// Get returns the value of key
	Get([]byte) ([]byte, error)
	// Delete remove the key from the store
	Delete([]byte) error
	// ExecCommand exec command
	ExecCommand(cmd interface{}) ([]byte, error)
	// AsyncExecCommand async exec command
	AsyncExecCommand(interface{}, func(interface{}, []byte, error), interface{})
	// ExecCommandWithGroup exec command with group
	ExecCommandWithGroup(interface{}, metapb.Group) ([]byte, error)
	// AsyncExecCommandWithGroup async exec command with group
	AsyncExecCommandWithGroup(interface{}, metapb.Group, func(interface{}, []byte, error), interface{})
	// RaftStore returns the raft store
	RaftStore() raftstore.Store
}

type beeStorage struct {
	app    *server.Application
	store  raftstore.Store
	eventC chan Event
	shardC chan shardCycle
	runner *task.Runner
}

// NewStorage returns a beehive request handler
func NewStorage(dataPath string,
	metadataStorages []beehiveStorage.MetadataStorage,
	dataStorages []beehiveStorage.DataStorage) (Storage, error) {

	h := &beeStorage{
		eventC: make(chan Event, 1024),
		shardC: make(chan shardCycle, 1024),
		runner: task.NewRunner(),
	}

	store, err := beehive.CreateRaftStoreFromFile(dataPath,
		metadataStorages,
		dataStorages,
		raftstore.WithShardStateAware(h),
		raftstore.WithWriteBatchFunc(h.WriteBatch))
	if err != nil {
		return nil, err
	}

	h.store = store
	return h, nil
}

func (h *beeStorage) Start() error {
	h.init()

	app := server.NewApplication(server.Cfg{
		Store:          h.store,
		Handler:        h,
		ExternalServer: true,
	})
	err := app.Start()
	if err != nil {
		return err
	}

	h.app = app
	return nil
}

func (h *beeStorage) Close() {
	h.runner.Stop()
	h.app.Stop()
	close(h.shardC)
	close(h.eventC)
	h.store.Stop()
}

func (h *beeStorage) Set(key, value []byte) error {
	req := rpcpb.AcquireSetRequest()
	req.Key = key
	req.Value = value
	_, err := h.ExecCommand(req)
	return err
}

func (h *beeStorage) Get(key []byte) ([]byte, error) {
	req := rpcpb.AcquireGetRequest()
	req.Key = key
	value, err := h.ExecCommand(req)
	if err != nil {
		return nil, err
	}

	resp := rpcpb.BytesResponse{}
	protoc.MustUnmarshal(&resp, value)
	return resp.Value, nil
}

func (h *beeStorage) Delete(key []byte) error {
	req := rpcpb.AcquireDeleteRequest()
	req.Key = key

	_, err := h.ExecCommand(req)
	return err
}

func (h *beeStorage) ExecCommand(cmd interface{}) ([]byte, error) {
	return h.app.Exec(cmd, defaultRPCTimeout)
}

func (h *beeStorage) AsyncExecCommand(cmd interface{}, cb func(interface{}, []byte, error), arg interface{}) {
	h.app.AsyncExecWithTimeout(cmd, cb, defaultRPCTimeout, arg)
}

func (h *beeStorage) AsyncExecCommandWithGroup(cmd interface{}, group metapb.Group, cb func(interface{}, []byte, error), arg interface{}) {
	h.app.AsyncExecWithGroupAndTimeout(cmd, uint64(group), cb, defaultRPCTimeout, arg)
}

func (h *beeStorage) ExecCommandWithGroup(cmd interface{}, group metapb.Group) ([]byte, error) {
	return h.app.ExecWithGroup(cmd, uint64(group), defaultRPCTimeout)
}

func (h *beeStorage) WatchEvent() chan Event {
	return h.eventC
}

func (h *beeStorage) RaftStore() raftstore.Store {
	return h.store
}

func (h *beeStorage) getStore(shard uint64) bhstorage.DataStorage {
	return h.store.DataStorage(shard)
}
