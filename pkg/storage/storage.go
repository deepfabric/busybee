package storage

import (
	"github.com/deepfabric/beehive"
	"github.com/deepfabric/beehive/raftstore"
	"github.com/deepfabric/beehive/server"
	beehiveStorage "github.com/deepfabric/beehive/storage"
	"github.com/fagongzi/util/task"
	"time"
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

	// ExecCommand exec command
	ExecCommand(cmd interface{}) ([]byte, error)
}

type beeStorage struct {
	addr   string
	app    *server.Application
	store  raftstore.Store
	eventC chan Event
	shardC chan shardCycle
	runner *task.Runner
}

// NewStorage returns a beehive request handler
func NewStorage(addr string, dataPath string,
	metadataStorages []beehiveStorage.MetadataStorage,
	dataStorages []beehiveStorage.DataStorage) (Storage, error) {

	h := &beeStorage{
		addr:   addr,
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
		Addr:    h.addr,
		Store:   h.store,
		Handler: h,
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

func (h *beeStorage) ExecCommand(cmd interface{}) ([]byte, error) {
	return h.app.Exec(cmd, defaultRPCTimeout)
}

func (h *beeStorage) WatchEvent() chan Event {
	return h.eventC
}
