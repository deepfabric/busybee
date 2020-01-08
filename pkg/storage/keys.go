package storage

import (
	"github.com/deepfabric/busybee/pkg/pb/metapb"
	"github.com/fagongzi/goetty"
	"github.com/fagongzi/util/hack"
)

const (
	mappingPrefix byte = 1
	profilePrefix byte = 2
)

// InstanceStartKey instance key
func InstanceStartKey(id uint64) []byte {
	key := make([]byte, 9, 9)
	key[8] = 0
	goetty.Uint64ToBytesTo(id, key)
	return key
}

// InstanceStateKey instance state key
func InstanceStateKey(id uint64, start uint32, end uint32) []byte {
	key := make([]byte, 16, 16)
	goetty.Uint32ToBytesTo(end, key)
	goetty.Uint32ToBytesTo(start, key[4:])
	goetty.Uint64ToBytesTo(id, key[8:])
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

func queueLastOffsetKey(key []byte) []byte {
	size := len(key) + 1
	value := make([]byte, size, size)
	copy(value, key)
	value[len(key)] = 'c'
	return value
}

func queueItemKey(key []byte, offset uint64) []byte {
	size := len(key) + 9
	value := make([]byte, size, size)
	copy(value, key)
	value[len(key)] = 'd'
	goetty.Uint64ToBytesTo(offset, value[len(key)+1:])
	return value
}
