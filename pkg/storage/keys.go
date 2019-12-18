package storage

import (
	"github.com/fagongzi/goetty"
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
