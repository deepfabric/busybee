package crowd

import (
	"testing"

	"github.com/RoaringBitmap/roaring"
	"github.com/deepfabric/busybee/pkg/util"
	"github.com/stretchr/testify/assert"
)

func TestLoadFromRaw(t *testing.T) {
	bm := roaring.BitmapOf(1, 2, 3)
	data := util.MustMarshalBM(bm)

	ld := NewRawLoader()
	bm, err := ld.Get(data)
	assert.NoError(t, err, "TestLoadFromKV failed")
	assert.Equal(t, uint64(3), bm.GetCardinality(), "TestLoadFromKV failed")
	assert.Equal(t, uint32(1), bm.Minimum(), "TestLoadFromKV failed")
	assert.Equal(t, uint32(3), bm.Maximum(), "TestLoadFromKV failed")
}
