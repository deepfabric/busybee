package storage

import (
	"time"

	"github.com/deepfabric/beehive/util"
	"github.com/deepfabric/busybee/pkg/pb/rpcpb"
	"github.com/fagongzi/util/protoc"
)

type distributedLock struct {
	key       []byte
	expect    []byte
	ttl       int
	keepalive time.Duration
}

func (h *beeStorage) Lock(key []byte, expect []byte, ttl int, keepalive time.Duration, wait bool) (bool, error) {
	lock := &distributedLock{key, expect, ttl, keepalive}
	_, loaded := h.locks.LoadOrStore(string(key), lock)

	for {
		ok, err := h.doLock(lock)
		if err != nil {
			return false, err
		}

		if ok {
			if !loaded {
				h.doKeepalive(string(key))
			}
			break
		}

		if !wait {
			return false, nil
		}
		time.Sleep(time.Second * time.Duration(ttl))
	}

	return true, nil
}

func (h *beeStorage) Unlock(key []byte, expect []byte) error {
	if lock, ok := h.locks.Load(string(key)); ok {
		return h.doUnlock(lock.(*distributedLock))
	}

	return nil
}

func (h *beeStorage) doKeepalive(arg interface{}) {
	key := arg.(string)
	if lock, ok := h.locks.Load(key); ok {
		h.AsyncExecCommand(lockRequest(lock.(*distributedLock)), h.doKeepaliveResp, arg)
	}
}

func (h *beeStorage) doKeepaliveResp(arg interface{}, value []byte, err error) {
	if err != nil {
		h.doKeepalive(arg)
		return
	}

	resp := rpcpb.AcquireBoolResponse()
	protoc.MustUnmarshal(resp, value)
	if !resp.Value {
		return
	}

	key := arg.(string)
	if lock, ok := h.locks.Load(key); ok {
		util.DefaultTimeoutWheel().Schedule(lock.(*distributedLock).keepalive, h.doKeepalive, arg)
	}
}

func (h *beeStorage) doLock(lock *distributedLock) (bool, error) {
	value, err := h.ExecCommand(lockRequest(lock))
	if err != nil {
		return false, err
	}

	resp := rpcpb.AcquireBoolResponse()
	protoc.MustUnmarshal(resp, value)
	return resp.Value, nil
}

func (h *beeStorage) doUnlock(lock *distributedLock) error {
	h.locks.Delete(string(lock.key))

	req := rpcpb.AcquireDeleteIfRequest()
	req.Key = lock.key
	req.Conditions = append(req.Conditions, rpcpb.ConditionGroup{
		Conditions: []rpcpb.Condition{{Cmp: rpcpb.Equal, Value: lock.expect}},
	})

	_, err := h.ExecCommand(req)
	return err
}

func lockRequest(lock *distributedLock) *rpcpb.SetIfRequest {
	req := rpcpb.AcquireSetIfRequest()
	req.Key = lock.key
	req.TTL = int64(lock.ttl)
	req.Value = lock.expect
	req.Conditions = append(req.Conditions, rpcpb.ConditionGroup{
		Conditions: []rpcpb.Condition{{Cmp: rpcpb.NotExists}},
	})
	req.Conditions = append(req.Conditions, rpcpb.ConditionGroup{
		Conditions: []rpcpb.Condition{{Cmp: rpcpb.Equal, Value: lock.expect}},
	})
	return req
}
