package util

import (
	"bytes"
	"reflect"
	"unsafe"

	"github.com/fagongzi/goetty"
	"github.com/pilosa/pilosa/roaring"
)

const (
	bsiExistsBit = 0
	bsiSignBit   = 1
	bsiOffsetBit = 2
)

// BSI bitmap based kv
type BSI struct {
	bitSize int
	ms      []*roaring.Bitmap
}

// NewBSI creates BSI
func NewBSI(bitSize int) *BSI {
	ms := make([]*roaring.Bitmap, bitSize+2)
	for i, j := 0, bitSize+2; i < j; i++ {
		ms[i] = roaring.NewBitmap()
	}
	return &BSI{
		ms:      ms,
		bitSize: bitSize,
	}
}

// Clone returns BSI copy from current
func (b *BSI) Clone() *BSI {
	ms := make([]*roaring.Bitmap, b.bitSize+2)
	for i, j := 0, b.bitSize+2; i < j; i++ {
		ms[i] = b.ms[i].Clone()
	}
	return &BSI{
		ms:      ms,
		bitSize: b.bitSize,
	}
}

// Marshal marshal
func (b *BSI) Marshal() ([]byte, error) {
	var body []byte
	buf := goetty.NewByteBuf(1024)
	defer buf.Release()

	os := make([]uint32, 0, len(b.ms))
	buf.WriteByte(byte(b.bitSize & 0xFF))
	for _, m := range b.ms {
		data, err := marshalBM(m)
		if err != nil {
			return nil, err
		}
		os = append(os, uint32(len(body)))
		body = append(body, data...)
	}
	{
		data := encodeUint32Slice(os)
		buf.WriteUInt32(uint32(len(data)))
		buf.Write(data)
	}

	buf.Write(body)
	_, v, err := buf.ReadAll()
	return v, err
}

// Unmarshal un marshal
func (b *BSI) Unmarshal(data []byte) error {
	b.bitSize = int(data[0])
	data = data[1:]
	n := goetty.Byte2UInt32(data[:4])
	data = data[4:]
	os := decodeUint32Slice(data[:n])
	data = data[n:]
	b.ms = make([]*roaring.Bitmap, b.bitSize+2)
	for i, j := 0, b.bitSize+2; i < j; i++ {
		b.ms[i] = roaring.NewBitmap()
		if i < j-1 {
			if err := b.ms[i].UnmarshalBinary(data[os[i]:os[i+1]]); err != nil {
				return err
			}
		} else {
			if err := b.ms[i].UnmarshalBinary(data[os[i]:]); err != nil {
				return err
			}
		}
	}
	return nil
}

// Get returns the value of k
func (b *BSI) Get(k uint32) (int64, bool) {
	var v int64

	if !b.bit(bsiExistsBit, k) {
		return -1, false
	}
	for i, j := uint(0), uint(b.bitSize); i < j; i++ {
		if b.bit(uint32(bsiOffsetBit+i), k) {
			v |= (1 << i)
		}
	}
	if b.bit(uint32(bsiSignBit), k) {
		v = -v
	}
	return v, true
}

// Set set int64 to bsi
func (b *BSI) Set(k uint32, v int64) error {
	uv := uint64(v)
	if v < 0 {
		uv = uint64(-v)
	}
	for i, j := uint(0), uint(b.bitSize); i < j; i++ {
		if uv&(1<<i) != 0 {
			b.setBit(uint32(bsiOffsetBit+i), k)
		} else {
			b.clearBit(uint32(bsiOffsetBit+i), k)
		}
	}
	b.setBit(uint32(bsiExistsBit), k)
	if v < 0 {
		b.setBit(uint32(bsiSignBit), k)
	} else {
		b.clearBit(uint32(bsiSignBit), k)
	}
	return nil
}

// Del remove k from bsi
func (b *BSI) Del(k uint32) error {
	b.clearBit(uint32(bsiExistsBit), k)
	return nil
}

func (b *BSI) setBit(x, y uint32)   { b.ms[x].Add(uint64(y)) }
func (b *BSI) clearBit(x, y uint32) { b.ms[x].Remove(uint64(y)) }
func (b *BSI) bit(x, y uint32) bool { return b.ms[x].Contains(uint64(y)) }

func marshalBM(bm *roaring.Bitmap) ([]byte, error) {
	var buf bytes.Buffer

	if _, err := bm.WriteTo(&buf); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func encodeUint32Slice(v []uint32) []byte {
	hp := *(*reflect.SliceHeader)(unsafe.Pointer(&v))
	hp.Len *= 4
	hp.Cap *= 4
	return *(*[]byte)(unsafe.Pointer(&hp))
}

func decodeUint32Slice(v []byte) []uint32 {
	hp := *(*reflect.SliceHeader)(unsafe.Pointer(&v))
	hp.Len /= 4
	hp.Cap /= 4
	return *(*[]uint32)(unsafe.Pointer(&hp))
}
