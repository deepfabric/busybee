package storage

import (
	"github.com/deepfabric/beehive/pb"
	"github.com/deepfabric/beehive/pb/raftcmdpb"
	"github.com/deepfabric/busybee/pkg/pb/rpcpb"
	"github.com/fagongzi/log"
	"github.com/fagongzi/util/protoc"
)

type batchReader struct {
	shard uint64
	group uint64
	bs    *beeStorage
	resp  *rpcpb.BytesResponse
	keys  [][]byte
	resps []*raftcmdpb.Response
}

func newBatchReader(bs *beeStorage) *batchReader {
	return &batchReader{
		bs:   bs,
		resp: &rpcpb.BytesResponse{},
	}
}

func (b *batchReader) Add(shard uint64, req *raftcmdpb.Request, attrs map[string]interface{}) (bool, error) {
	if b.shard != 0 && b.shard != shard {
		log.Fatalf("BUG: diffent shard opts in a read batch, %d, %d",
			b.shard,
			shard)
	}

	if req.CustemType != uint64(rpcpb.Get) {
		return false, nil
	}

	b.shard = shard
	b.group = req.Group
	b.keys = append(b.keys, req.Key)
	return true, nil
}

func (b *batchReader) Execute() ([]*raftcmdpb.Response, error) {
	s := b.bs.getStoreByGroup(b.group)

	values, err := s.MGet(b.keys...)
	if err != nil {
		return nil, err
	}

	for _, value := range values {
		b.resp.Value = value
		resp := pb.AcquireResponse()
		resp.Value = protoc.MustMarshal(b.resp)
		b.resps = append(b.resps, resp)
	}

	return b.resps, nil
}

func (b *batchReader) Reset() {
	b.shard = 0
	b.keys = b.keys[:0]
	b.resps = b.resps[:0]
}
