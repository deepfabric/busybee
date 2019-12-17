package util

import (
	"github.com/pilosa/pilosa/roaring"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestAnd(t *testing.T) {
	bm := BMAnd(roaring.NewBitmap(1, 2, 3), roaring.NewBitmap(3, 4, 5), roaring.NewBitmap(3, 5, 6, 7))
	assert.Equal(t, uint64(1), bm.Count(), "test and failed")

	bm = BMAndInterface(roaring.NewBitmap(1, 2, 3), roaring.NewBitmap(3, 4, 5), roaring.NewBitmap(3, 5, 6, 7))
	assert.Equal(t, uint64(1), bm.Count(), "test and failed")
}

func TestOr(t *testing.T) {
	bm := BMOr(roaring.NewBitmap(1, 2, 3), roaring.NewBitmap(3, 4, 5), roaring.NewBitmap(5, 6, 7))
	assert.Equal(t, uint64(7), bm.Count(), "test and failed")

	bm = BMOrInterface(roaring.NewBitmap(1, 2, 3), roaring.NewBitmap(3, 4, 5), roaring.NewBitmap(5, 6, 7))
	assert.Equal(t, uint64(7), bm.Count(), "test and failed")
}

func TestXor(t *testing.T) {
	bm := BMXOr(roaring.NewBitmap(1, 2, 3), roaring.NewBitmap(3, 4, 5), roaring.NewBitmap(3, 5, 6, 7))
	assert.Equal(t, uint64(6), bm.Count(), "test and failed")

	bm = BMXOrInterface(roaring.NewBitmap(1, 2, 3), roaring.NewBitmap(3, 4, 5), roaring.NewBitmap(3, 5, 6, 7))
	assert.Equal(t, uint64(6), bm.Count(), "test and failed")
}

func TestAndnot(t *testing.T) {
	bm := BMAndnot(roaring.NewBitmap(1, 2, 3), roaring.NewBitmap(3, 4, 5), roaring.NewBitmap(3, 5, 6, 7))
	assert.Equal(t, uint64(2), bm.Count(), "test and failed")

	bm = BMAndnotInterface(roaring.NewBitmap(1, 2, 3), roaring.NewBitmap(3, 4, 5), roaring.NewBitmap(3, 5, 6, 7))
	assert.Equal(t, uint64(2), bm.Count(), "test and failed")
}
