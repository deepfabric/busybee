package storage

import (
	"github.com/deepfabric/busybee/pkg/pb/metapb"
	"github.com/fagongzi/goetty"
	"github.com/fagongzi/util/hack"
)

const (
	kvPrefix             byte = 0
	mappingPrefix        byte = 1
	profilePrefix        byte = 2
	queueMetadataPrefix  byte = 3
	workflowMetataPrefix byte = 4

	instance      byte = 1
	instanceShard byte = 2
)

// KVKey returns a kv key
func KVKey(src []byte) []byte {
	n := len(src) + 1
	key := make([]byte, n, n)
	key[0] = kvPrefix
	copy(key[1:], src)
	return key
}

// QueueMetadataKey returns queue metadata key
func QueueMetadataKey(id uint64, group metapb.Group) []byte {
	key := make([]byte, 13, 13)
	key[0] = queueMetadataPrefix
	goetty.Uint64ToBytesTo(id, key[1:])
	goetty.Uint32ToBytesTo(uint32(group), key[9:])
	return key
}

// StartedInstanceKey instance key
func StartedInstanceKey(id uint64) []byte {
	key := make([]byte, 10, 10)
	key[0] = workflowMetataPrefix
	key[1] = instance
	goetty.Uint64ToBytesTo(id, key[2:])
	return key
}

// InstanceShardKey instance shard key
func InstanceShardKey(id uint64, start uint32, end uint32) []byte {
	key := make([]byte, 18, 18)
	key[0] = workflowMetataPrefix
	key[1] = instanceShard
	goetty.Uint64ToBytesTo(id, key[2:])
	goetty.Uint32ToBytesTo(start, key[10:])
	goetty.Uint32ToBytesTo(end, key[14:])
	return key
}

// MappingKey returns a mapping key
func MappingKey(tenantID uint64, from metapb.IDValue, to uint32) []byte {
	size := 17 + len(from.Value)
	key := make([]byte, size, size)
	key[0] = mappingPrefix
	goetty.Uint64ToBytesTo(tenantID, key[1:])
	goetty.Uint32ToBytesTo(from.Type, key[9:])
	goetty.Uint32ToBytesTo(to, key[13:])
	copy(key[17:], hack.StringToSlice(from.Value))
	return key
}

// ProfileKey returns a profile key
func ProfileKey(tenantID uint64, uid uint32) []byte {
	key := make([]byte, 13, 13)
	key[0] = profilePrefix
	goetty.Uint64ToBytesTo(tenantID, key[1:])
	goetty.Uint32ToBytesTo(uid, key[9:])
	return key
}

func maxAndCleanOffsetKey(src []byte) []byte {
	n := len(src) + 1
	key := make([]byte, n, n)
	copy(key, src)
	key[len(src)] = 0x00
	return key
}

func itemKey(src []byte, offset uint64) []byte {
	n := len(src) + 9
	key := make([]byte, n, n)
	copy(key, src)
	key[len(src)] = 0x01
	goetty.Uint64ToBytesTo(offset, key[len(src)+1:])
	return key
}

func committedOffsetKey(src []byte, consumer []byte) []byte {
	n := len(src) + len(consumer)
	key := make([]byte, n, n)
	copy(key, src)
	key[len(src)] = 0x02
	copy(key[len(src)+1:], consumer)
	return key
}

func committedOffsetKeyRange(src []byte) ([]byte, []byte) {
	n := len(src)
	start := make([]byte, n, n)
	copy(start, src)
	start[n-1] = 0x02

	end := make([]byte, n, n)
	copy(end, src)
	end[n-1] = 0x03

	return start, end
}

func removedOffsetKeyRange(src []byte, from, to uint64) ([]byte, []byte) {
	n := len(src) + 8
	start := make([]byte, n, n)
	copy(start, src)
	start[n-1] = 0x01
	goetty.Uint64ToBytesTo(from, start[n:])

	end := make([]byte, n, n)
	copy(start, src)
	end[n-1] = 0x01
	goetty.Uint64ToBytesTo(to, end[n:])
	return start, end
}
