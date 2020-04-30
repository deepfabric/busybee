package storage

import (
	"flag"
	"io/ioutil"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/deepfabric/beehive/raftstore"
	"github.com/deepfabric/beehive/storage"
	"github.com/deepfabric/beehive/storage/nemo"
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

// NewTestStorage returns test storage
func NewTestStorage(t *testing.T, start bool) (Storage, func()) {
	os.RemoveAll(tmp)
	s, err := nemo.NewStorage(filepath.Join(tmp, "nemo"))
	assert.NoError(t, err, "NewTestStorage failed")

	err = ioutil.WriteFile(filepath.Join(tmp, "cfg.toml"), []byte(beehiveCfg), os.ModeAppend)
	assert.NoError(t, err, "NewTestStorage failed")
	flag.Set("beehive-cfg", filepath.Join(tmp, "cfg.toml"))

	store, err := NewStorageWithOptions(tmp, []storage.MetadataStorage{s},
		[]storage.DataStorage{s},
		raftstore.WithEnsureNewShardInterval(time.Millisecond*200))
	assert.NoError(t, err, "NewTestStorage failed")
	if start {
		assert.NoError(t, store.Start(), "NewTestStorage failed")
	}

	return store, func() {
		store.Close()
		s.Close()
	}
}
