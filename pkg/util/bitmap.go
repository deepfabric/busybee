package util

import (
	"github.com/RoaringBitmap/roaring"
	"log"
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
		log.Fatalf("BUG: parse bm failed with %+v", err)
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

// AcquireBitmap create a bitmap
func AcquireBitmap() *roaring.Bitmap {
	return roaring.NewBitmap()
}
