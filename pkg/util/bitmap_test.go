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
