package storage

import (
	"encoding/hex"
	"time"

	"github.com/deepfabric/beehive/util"
	"github.com/deepfabric/busybee/pkg/pb/metapb"
	"github.com/deepfabric/busybee/pkg/pb/rpcpb"
	"github.com/fagongzi/log"
	"github.com/fagongzi/util/protoc"
)

type distributedLock struct {
	key            []byte
	expect         []byte
	ttl            int
	keepalive      time.Duration
	keepfailedFunc func()
	group          metapb.Group
}

func (h *beeStorage) Lock(key []byte, expect []byte, ttl int, keepalive time.Duration, wait bool, keepfailedFunc func(), group metapb.Group) (bool, error) {
	lock := &distributedLock{key, expect, ttl, keepalive, keepfailedFunc, group}
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
	key := arg.(string)

	v, ok := h.locks.Load(key)
	if !ok {
		return
	}

	lock := v.(*distributedLock)
	if err == nil {
		resp := rpcpb.AcquireBoolResponse()
		protoc.MustUnmarshal(resp, value)
		if !resp.Value {
			log.Errorf("%s lock %s keepalive failed with another has locked",
				key[18:],
				hex.EncodeToString(lock.expect))
			if lock.keepfailedFunc != nil {
				lock.keepfailedFunc()
			}
			return
		}
	}

	if err != nil {
		log.Errorf("%s lock %s keepalive failed with %+v",
			key[18:],
			hex.EncodeToString(lock.expect),
			err)
	}

	util.DefaultTimeoutWheel().Schedule(lock.keepalive, h.doKeepalive, arg)
}

func (h *beeStorage) doLock(lock *distributedLock) (bool, error) {
	value, err := h.ExecCommandWithGroup(lockRequest(lock), lock.group)
	if err != nil {
		return false, err
	}

	resp := rpcpb.AcquireBoolResponse()
	protoc.MustUnmarshal(resp, value)
	return resp.Value, nil
}

func (h *beeStorage) doUnlock(lock *distributedLock) error {
	h.locks.Delete(string(lock.key))
	log.Errorf("%s lock %s unlocked",
		lock.key[18:],
		hex.EncodeToString(lock.expect))

	req := rpcpb.AcquireUnlockRequest()
	req.Key = lock.key
	req.Value = lock.expect

	_, err := h.ExecCommandWithGroup(req, lock.group)
	return err
}

func lockRequest(lock *distributedLock) *rpcpb.LockRequest {
	req := rpcpb.AcquireLockRequest()
	req.Key = lock.key
	req.ExpireAt = time.Now().Unix() + int64(lock.ttl)
	req.Value = lock.expect
	return req
}
