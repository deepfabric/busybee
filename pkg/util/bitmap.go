package util

import (
	"runtime"

	"github.com/RoaringBitmap/roaring"
	"github.com/fagongzi/log"
)

// MustParseBM parse a bitmap
func MustParseBM(data []byte) *roaring.Bitmap {
	bm := AcquireBitmap()
	MustParseBMTo(data, bm)
	return bm
}

// MustParseBMTo parse a bitmap
func MustParseBMTo(data []byte, bm *roaring.Bitmap) {
	err := bm.UnmarshalBinary(data)
	if err != nil {
		buf := make([]byte, 4096)
		n := runtime.Stack(buf, true)
		log.Fatalf("BUG: parse bm failed with %+v \n %s", err, string(buf[:n]))
	}
}

// MustMarshalBM must marshal BM
func MustMarshalBM(bm *roaring.Bitmap) []byte {
	data, err := bm.MarshalBinary()
	if err != nil {
		log.Fatalf("BUG: write bm failed with %+v", err)
	}

	return data
}

// BMAnd bitmap and
func BMAnd(bms ...*roaring.Bitmap) *roaring.Bitmap {
	value := bms[0].Clone()
	for idx, bm := range bms {
		if idx > 0 {
			value.And(bm)
		}
	}

	return value
}

// BMAndInterface bm and using interface{}
func BMAndInterface(bms ...interface{}) *roaring.Bitmap {
	value := bms[0].(*roaring.Bitmap).Clone()
	for idx, bm := range bms {
		if idx > 0 {
			value.And(bm.(*roaring.Bitmap))
		}
	}

	return value
}

// BMOr bitmap or
func BMOr(bms ...*roaring.Bitmap) *roaring.Bitmap {
	value := AcquireBitmap()
	for _, bm := range bms {
		value.Or(bm)
	}

	return value
}

// BMOrInterface bitmap or using interface{}
func BMOrInterface(bms ...interface{}) *roaring.Bitmap {
	value := AcquireBitmap()
	for _, bm := range bms {
		value.Or(bm.(*roaring.Bitmap))
	}

	return value
}

// BMXOr bitmap xor (A union B) - (A and B)
func BMXOr(bms ...*roaring.Bitmap) *roaring.Bitmap {
	value := bms[0].Clone()
	for idx, bm := range bms {
		if idx > 0 {
			value.Xor(bm)
		}
	}

	return value
}

// BMXOrInterface bitmap xor using interface{}
func BMXOrInterface(bms ...interface{}) *roaring.Bitmap {
	value := bms[0].(*roaring.Bitmap).Clone()
	for idx, bm := range bms {
		if idx > 0 {
			value.Xor(bm.(*roaring.Bitmap))
		}
	}

	return value
}

// BMAndnot bitmap andnot A - (A and B)
func BMAndnot(bms ...*roaring.Bitmap) *roaring.Bitmap {
	and := BMAnd(bms...)
	value := bms[0].Clone()
	value.Xor(and)
	return value
}

// BMAndnotInterface bitmap andnot using interface{}
func BMAndnotInterface(bms ...interface{}) *roaring.Bitmap {
	and := BMAndInterface(bms...)
	value := bms[0].(*roaring.Bitmap).Clone()
	value.Xor(and)
	return value
}

// BMMinus bm1 - bm2
func BMMinus(bm1, bm2 *roaring.Bitmap) *roaring.Bitmap {
	v := bm1.Clone()
	v.And(bm2)
	return BMXOr(bm1, v)
}

// BMRemove bm1 - bm2
func BMRemove(bm1, bm2 *roaring.Bitmap) {
	v := bm1.Clone()
	v.And(bm2)
	bm1.Xor(v)
}

// AcquireBitmap create a bitmap
func AcquireBitmap() *roaring.Bitmap {
	return roaring.NewBitmap()
}

// BMAlloc alloc bm
func BMAlloc(new *roaring.Bitmap, shards ...*roaring.Bitmap) {
	old := BMOr(shards...)
	// in new and not in old
	added := BMMinus(new, old)
	// in old not in new
	removed := BMMinus(old, new)

	totalRemoved := float64(0)
	removes := make([]float64, len(shards), len(shards))
	if removed.GetCardinality() > 0 {
		for idx, shard := range shards {
			n := shard.GetCardinality()
			BMRemove(shard, removed)
			n = n - shard.GetCardinality()
			totalRemoved += float64(n)
			removes[idx] = float64(n)
		}
	}

	totalAdded := float64(added.GetCardinality())
	if totalAdded > 0 {
		for i := 0; i < len(removes); i++ {
			if removes[i] == 0 {
				continue
			}

			removes[i] = (totalAdded * removes[i] / totalRemoved)
		}

		op := 0
		itr := added.Iterator()
		for {
			if !itr.HasNext() {
				break
			}

			if removes[op] > 0 {
				shards[op].Add(itr.Next())
				removes[op]--
				continue
			}

			op++
		}
	}
}

// BMSplit split the bitmap
func BMSplit(bm *roaring.Bitmap, ranges uint64) []*roaring.Bitmap {
	if ranges <= 1 ||
		bm.GetCardinality() == 0 ||
		ranges > bm.GetCardinality() {
		return []*roaring.Bitmap{bm.Clone()}
	}

	countPerRange := bm.GetCardinality() / ranges
	countMorePerRange := countPerRange + 1
	mod := bm.GetCardinality() % ranges

	values := make([]*roaring.Bitmap, 0, ranges)
	c := uint64(0)
	tmp := AcquireBitmap()
	itr := bm.Iterator()
	check := countPerRange
	for {
		if !itr.HasNext() {
			break
		}

		value := itr.Next()
		tmp.Add(value)
		c++

		check = countPerRange
		if uint64(len(values)) < mod {
			check = countMorePerRange
		}

		if c == check {
			values = append(values, tmp)
			tmp = AcquireBitmap()
			c = 0
		}
	}

	if tmp.GetCardinality() > 0 {
		values = append(values, tmp)
	}

	return values
}
