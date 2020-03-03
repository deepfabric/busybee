package crowd

import (
	"fmt"

	"github.com/RoaringBitmap/roaring"
	"github.com/deepfabric/busybee/pkg/util"
)

type rawLoader struct {
}

// NewRawLoader returns raw loader
func NewRawLoader() Loader {
	return &rawLoader{}
}

func (l *rawLoader) Get(value []byte) (*roaring.Bitmap, error) {
	bm := util.AcquireBitmap()
	err := bm.UnmarshalBinary(value)
	if err != nil {
		return nil, err
	}

	logger.Infof("load %d crowd from raw bytes",
		bm.GetCardinality())
	return bm, nil
}

func (l *rawLoader) Set(key []byte, data []byte) (uint64, uint32, error) {
	return 0, 0, fmt.Errorf("Raw loader not support Set")
}
