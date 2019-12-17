package storage

import (
	"github.com/fagongzi/goetty"
)

// InstanceKey instance key
func InstanceKey(id uint64) []byte {
	return goetty.Uint64ToBytes(id)
}

// InstanceStateKey instance state key
func InstanceStateKey(id uint64, start uint64, end uint64) []byte {
	key := make([]byte, 24, 24)
	goetty.Uint64ToBytesTo(end, key)
	goetty.Uint64ToBytesTo(start, key[8:])
	goetty.Uint64ToBytesTo(id, key[16:])
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
