package util

import (
	"bytes"
	"github.com/pilosa/pilosa/roaring"
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
	_, _, err := bm.ImportRoaringBits(data, false, false, 0)
	if err != nil {
		log.Fatalf("BUG: parse bm failed with %+v", err)
	}
}

// MustWriteTo must write bitmap to buf
func MustWriteTo(bm *roaring.Bitmap, buf *bytes.Buffer) {
	_, err := bm.WriteTo(buf)
	if err != nil {
		log.Fatalf("BUG: write bm failed with %+v", err)
	}
}

// BMAnd bitmap and
func BMAnd(bms ...*roaring.Bitmap) *roaring.Bitmap {
	var value *roaring.Bitmap
	for _, bm := range bms {
		if value == nil {
			value = bm
		} else {
			value = value.Intersect(bm)
		}
	}

	return value
}

// BMAndInterface bm and using interface{}
func BMAndInterface(bms ...interface{}) *roaring.Bitmap {
	var value *roaring.Bitmap
	for _, bm := range bms {
		if value == nil {
			value = bm.(*roaring.Bitmap)
		} else {
			value = value.Intersect(bm.(*roaring.Bitmap))
		}
	}

	return value
}

// BMOr bitmap or
func BMOr(bms ...*roaring.Bitmap) *roaring.Bitmap {
	var value *roaring.Bitmap
	for _, bm := range bms {
		if value == nil {
			value = bm
		} else {
			value = value.Union(bm)
		}
	}

	return value
}

// BMOrInterface bitmap or using interface{}
func BMOrInterface(bms ...interface{}) *roaring.Bitmap {
	var value *roaring.Bitmap
	for _, bm := range bms {
		if value == nil {
			value = bm.(*roaring.Bitmap)
		} else {
			value = value.Union(bm.(*roaring.Bitmap))
		}
	}

	return value
}

// BMXOr bitmap xor (A union B) - (A and B)
func BMXOr(bms ...*roaring.Bitmap) *roaring.Bitmap {
	var value *roaring.Bitmap
	for _, bm := range bms {
		if value == nil {
			value = bm
		} else {
			value = value.Xor(bm)
		}
	}

	return value
}

// BMXOrInterface bitmap xor using interface{}
func BMXOrInterface(bms ...interface{}) *roaring.Bitmap {
	var value *roaring.Bitmap
	for _, bm := range bms {
		if value == nil {
			value = bm.(*roaring.Bitmap)
		} else {
			value = value.Xor(bm.(*roaring.Bitmap))
		}
	}

	return value
}

// BMAndnot bitmap andnot A - (A and B)
func BMAndnot(bms ...*roaring.Bitmap) *roaring.Bitmap {
	var value *roaring.Bitmap
	var and *roaring.Bitmap
	for idx, bm := range bms {
		if idx == 0 {
			value = bm
			and = bm
		} else {
			and = and.Intersect(bm)
		}
	}

	return value.Xor(and)
}

// BMAndnotInterface bitmap andnot using interface{}
func BMAndnotInterface(bms ...interface{}) *roaring.Bitmap {
	var value *roaring.Bitmap
	var and *roaring.Bitmap
	for idx, bm := range bms {
		if idx == 0 {
			value = bm.(*roaring.Bitmap)
			and = value
		} else {
			and = and.Intersect(bm.(*roaring.Bitmap))
		}
	}

	return value.Xor(and)
}

// AcquireBitmap create a bitmap
func AcquireBitmap() *roaring.Bitmap {
	return roaring.NewBTreeBitmap()
}
