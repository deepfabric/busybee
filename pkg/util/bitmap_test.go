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
	assert.Equal(t, 1, len(BMSplit(roaring.BitmapOf(1, 2), 1)), "TestBMSplit failed")
	assert.Equal(t, 1, len(BMSplit(roaring.BitmapOf(1, 2), 3)), "TestBMSplit failed")
	assert.Equal(t, 1, len(BMSplit(roaring.BitmapOf(), 3)), "TestBMSplit failed")

	assert.Equal(t, 2, len(BMSplit(roaring.BitmapOf(1, 2), 2)), "TestBMSplit failed")
	assert.Equal(t, 2, len(BMSplit(roaring.BitmapOf(1, 2, 3), 2)), "TestBMSplit failed")

	assert.Equal(t, 5, len(BMSplit(roaring.BitmapOf(1, 2, 3, 4, 5), 5)), "TestBMSplit failed")
	assert.Equal(t, 5, len(BMSplit(roaring.BitmapOf(1, 2, 3, 4, 5, 6, 7, 8, 9), 5)), "TestBMSplit failed")

	bms := BMSplit(roaring.BitmapOf(1, 2, 3, 4, 5, 6, 7, 8, 9), 5)
	assert.Equal(t, uint64(2), bms[0].GetCardinality(), "TestBMSplit failed")
	assert.Equal(t, uint32(1), bms[0].Minimum(), "TestBMSplit failed")
	assert.Equal(t, uint32(2), bms[0].Maximum(), "TestBMSplit failed")

	assert.Equal(t, uint64(2), bms[1].GetCardinality(), "TestBMSplit failed")
	assert.Equal(t, uint32(3), bms[1].Minimum(), "TestBMSplit failed")
	assert.Equal(t, uint32(4), bms[1].Maximum(), "TestBMSplit failed")

	assert.Equal(t, uint64(2), bms[2].GetCardinality(), "TestBMSplit failed")
	assert.Equal(t, uint32(5), bms[2].Minimum(), "TestBMSplit failed")
	assert.Equal(t, uint32(6), bms[2].Maximum(), "TestBMSplit failed")

	assert.Equal(t, uint64(2), bms[3].GetCardinality(), "TestBMSplit failed")
	assert.Equal(t, uint32(7), bms[3].Minimum(), "TestBMSplit failed")
	assert.Equal(t, uint32(8), bms[3].Maximum(), "TestBMSplit failed")

	assert.Equal(t, uint64(1), bms[4].GetCardinality(), "TestBMSplit failed")
	assert.Equal(t, uint32(9), bms[4].Minimum(), "TestBMSplit failed")
	assert.Equal(t, uint32(9), bms[4].Maximum(), "TestBMSplit failed")
}

func TestBMAlloc(t *testing.T) {
	bm1 := roaring.BitmapOf(1, 2, 3)
	bm2 := roaring.BitmapOf(4, 5, 6)
	bm3 := roaring.BitmapOf(7, 8, 9)

	new := roaring.BitmapOf(4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21)
	BMAlloc(new, bm1, bm2, bm3)
	assert.Equal(t, uint64(6), bm1.GetCardinality(), "TestBMAlloc failed")
	assert.Equal(t, uint64(6), bm2.GetCardinality(), "TestBMAlloc failed")
	assert.Equal(t, uint64(6), bm3.GetCardinality(), "TestBMAlloc failed")
}

func TestBMAllocWithBig(t *testing.T) {
	bm := AcquireBitmap()
	for i := uint32(0); i < 400000000; i++ {
		if i%333 == 0 {
			bm.Add(i)
		}
	}

	bmNew := AcquireBitmap()
	bmNew.AddMany([]uint32{1, 2, 3, 4})
	shards := BMSplit(bm, 128)
	BMAlloc(bmNew, shards...)
}

func TestBMSplitAndAlloc(t *testing.T) {
	shards := BMSplit(roaring.BitmapOf(2, 3, 4), 3)
	assert.Equal(t, 3, len(shards), "TestBMSplitAndAlloc failed")
	assert.Equal(t, uint64(1), shards[0].GetCardinality(), "TestBMSplitAndAlloc failed")
	assert.Equal(t, uint64(1), shards[1].GetCardinality(), "TestBMSplitAndAlloc failed")
	assert.Equal(t, uint64(1), shards[2].GetCardinality(), "TestBMSplitAndAlloc failed")

	assert.Equal(t, uint32(2), shards[0].Minimum(), "TestBMSplitAndAlloc failed")
	assert.Equal(t, uint32(3), shards[1].Minimum(), "TestBMSplitAndAlloc failed")
	assert.Equal(t, uint32(4), shards[2].Minimum(), "TestBMSplitAndAlloc failed")

	BMAlloc(roaring.BitmapOf(1, 2, 3, 5), shards...)

	assert.Equal(t, uint32(2), shards[0].Minimum(), "TestBMSplitAndAlloc failed")
	assert.Equal(t, uint32(3), shards[1].Minimum(), "TestBMSplitAndAlloc failed")
	assert.Equal(t, uint32(1), shards[2].Minimum(), "TestBMSplitAndAlloc failed")
}
