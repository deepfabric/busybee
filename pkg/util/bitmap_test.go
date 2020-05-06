package util

import (
	"testing"

	"github.com/RoaringBitmap/roaring"
	"github.com/stretchr/testify/assert"
)

func TestAnd(t *testing.T) {
	bm := BMAnd(roaring.BitmapOf(1, 2, 3), roaring.BitmapOf(3, 4, 5), roaring.BitmapOf(3, 5, 6, 7))
	assert.Equal(t, uint64(1), bm.GetCardinality(), "test and failed")

	bm = BMAndInterface(roaring.BitmapOf(1, 2, 3), roaring.BitmapOf(3, 4, 5), roaring.BitmapOf(3, 5, 6, 7))
	assert.Equal(t, uint64(1), bm.GetCardinality(), "test and failed")
}

func TestOr(t *testing.T) {
	bm := BMOr(roaring.BitmapOf(1, 2, 3), roaring.BitmapOf(3, 4, 5), roaring.BitmapOf(5, 6, 7))
	assert.Equal(t, uint64(7), bm.GetCardinality(), "test and failed")

	bm = BMOrInterface(roaring.BitmapOf(1, 2, 3), roaring.BitmapOf(3, 4, 5), roaring.BitmapOf(5, 6, 7))
	assert.Equal(t, uint64(7), bm.GetCardinality(), "test and failed")
}

func TestXor(t *testing.T) {
	bm := BMXOr(roaring.BitmapOf(1, 2, 3), roaring.BitmapOf(3, 4, 5), roaring.BitmapOf(3, 5, 6, 7))
	assert.Equal(t, uint64(6), bm.GetCardinality(), "test and failed")

	bm = BMXOrInterface(roaring.BitmapOf(1, 2, 3), roaring.BitmapOf(3, 4, 5), roaring.BitmapOf(3, 5, 6, 7))
	assert.Equal(t, uint64(6), bm.GetCardinality(), "test and failed")
}

func TestAndnot(t *testing.T) {
	bm := BMAndnot(roaring.BitmapOf(1, 2, 3), roaring.BitmapOf(3, 4, 5), roaring.BitmapOf(3, 5, 6, 7))
	assert.Equal(t, uint64(2), bm.GetCardinality(), "test and failed")

	bm = BMAndnotInterface(roaring.BitmapOf(1, 2, 3), roaring.BitmapOf(3, 4, 5), roaring.BitmapOf(3, 5, 6, 7))
	assert.Equal(t, uint64(2), bm.GetCardinality(), "test and failed")
}

func TestBMSplit(t *testing.T) {
	bm := AcquireBitmap()
	bm.Flip(1, 100+1)

	assert.Equal(t, uint64(100), bm.GetCardinality(), "TestBMSplit failed")

	bms := BMSplit(bm, 101)
	assert.Equal(t, 1, len(bms), "TestBMSplit failed")

	bms = BMSplit(bm, 100)
	assert.Equal(t, 1, len(bms), "TestBMSplit failed")

	bms = BMSplit(bm, 50)
	assert.Equal(t, 2, len(bms), "TestBMSplit failed")
	assert.Equal(t, 1, int(bms[0].Minimum()), "TestBMSplit failed")
	assert.Equal(t, 51, int(bms[1].Minimum()), "TestBMSplit failed")
}
