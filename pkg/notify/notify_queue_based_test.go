package notify

import (
	"testing"
	"time"

	hbmetapb "github.com/deepfabric/beehive/pb/metapb"
	"github.com/deepfabric/busybee/pkg/pb/metapb"
	"github.com/deepfabric/busybee/pkg/pb/rpcpb"
	"github.com/deepfabric/busybee/pkg/storage"
	"github.com/fagongzi/util/protoc"
	"github.com/stretchr/testify/assert"
)

func TestNotify(t *testing.T) {
	s, deferFunc := storage.NewTestStorage(t, true)
	defer deferFunc()

	tenantID := uint64(1)
	assert.NoError(t, s.RaftStore().AddShards(hbmetapb.Shard{
		Group: uint64(metapb.TenantOutputGroup),
		Start: storage.PartitionKey(tenantID, 0),
		End:   storage.PartitionKey(tenantID, 1),
	}), "TestNotify failed")

	time.Sleep(time.Second * 2)

	n := NewQueueBasedNotifier(s)
	assert.NoError(t, n.Notify(tenantID, metapb.Notify{
		UserID: 1,
	}), "TestNotify failed")

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
