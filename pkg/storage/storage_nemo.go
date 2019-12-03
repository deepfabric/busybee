package storage

import (
	"github.com/deepfabric/beehive/storage"
	"github.com/deepfabric/beehive/storage/nemo"
)

func (h *beeStorage) getStore(shard uint64) storage.MetadataStorage {
	return h.store.DataStorage(shard).(*nemo.Storage)
}
