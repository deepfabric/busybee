package crm

import (
	"sync/atomic"

	"github.com/deepfabric/busybee/pkg/pb/metapb"
	"github.com/deepfabric/busybee/pkg/pb/rpcpb"
	"github.com/deepfabric/busybee/pkg/storage"
	"github.com/deepfabric/busybee/pkg/util"
	"github.com/fagongzi/util/hack"
	"github.com/fagongzi/util/protoc"
)

// Service crm service
type Service interface {
	UpdateMapping(*rpcpb.UpdateMappingRequest, func(interface{}, []byte, error), interface{})
	GetIDValue(uint64, metapb.IDValue, string, func(interface{}, []byte, error), interface{})
	UpdateProfile(uint64, uint32, []byte, func(interface{}, []byte, error), interface{})
	GetProfileField(uint64, uint32, string, func(interface{}, []byte, error), interface{})
}

// NewService returns crm service
func NewService(store storage.Storage) Service {
	return &service{
		store: store,
	}
}

type batchUpdated struct {
	total     uint64
	completed uint64
	err       error
	cb        func(interface{}, []byte, error)
	arg       interface{}
}

type service struct {
	store storage.Storage
}

func (s *service) UpdateMapping(req *rpcpb.UpdateMappingRequest, cb func(interface{}, []byte, error), arg interface{}) {
	tid := req.ID

	s.store.AsyncExecCommand(req, func(arg interface{}, value []byte, err error) {
		if err != nil {
			cb(arg, nil, err)
			return
		}

		resp := metapb.IDSet{}
		protoc.MustUnmarshal(&resp, value)

		var keys []int
		var values []int
		n := len(resp.Values)
		for i := 0; i < n; i++ {
			for j := 0; j < n; j++ {
				if i == j {
					continue
				}
				keys = append(keys, i)
				values = append(values, j)
			}
		}

		if len(keys) == 0 {
			cb(arg, nil, nil)
			return
		}

		n = len(keys)
		b := &batchUpdated{uint64(n), 0, nil, cb, arg}
		for i := 0; i < n; i++ {
			req := rpcpb.AcquireSetRequest()
			req.Key = storage.MappingKey(tid, resp.Values[keys[i]], resp.Values[values[i]].Type)
			req.Value = hack.StringToSlice(resp.Values[values[i]].Value)

			s.store.AsyncExecCommand(req, s.batchUpdateCB, b)
		}
	}, arg)

}

func (s *service) GetIDValue(tid uint64, from metapb.IDValue, to string, cb func(interface{}, []byte, error), arg interface{}) {
	req := rpcpb.AcquireGetRequest()
	req.Key = storage.MappingKey(tid, from, to)
	s.store.AsyncExecCommand(req, cb, arg)
}

func (s *service) UpdateProfile(tid uint64, uid uint32, value []byte, cb func(interface{}, []byte, error), arg interface{}) {
	req := rpcpb.AcquireSetRequest()
	req.Key = storage.ProfileKey(tid, uid)
	req.Value = value

	s.store.AsyncExecCommand(req, cb, arg)
}

func (s *service) GetProfileField(tid uint64, uid uint32, field string, cb func(interface{}, []byte, error), arg interface{}) {
	req := rpcpb.AcquireGetRequest()
	req.Key = storage.ProfileKey(tid, uid)
	s.store.AsyncExecCommand(req, func(arg interface{}, value []byte, err error) {
		if err != nil {
			cb(arg, nil, err)
			return
		}

		if len(value) == 0 {
			cb(arg, nil, nil)
			return
		}

		resp := rpcpb.AcquireBytesResponse()
		protoc.MustUnmarshal(resp, value)

		if field == "" {
			cb(arg, resp.Value, nil)
			return
		}

		cb(arg, util.ExtractJSONField(resp.Value, field), nil)
	}, arg)
}

func (s *service) batchUpdateCB(arg interface{}, value []byte, err error) {
	b := arg.(*batchUpdated)
	n := atomic.AddUint64(&b.completed, 1)

	if n == b.total {
		if err != nil {
			b.err = err
		}

		b.cb(b.arg, nil, b.err)
	}
}
