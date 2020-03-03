package crowd

import (
	"github.com/RoaringBitmap/roaring"
	"github.com/fagongzi/log"
)

var (
	logger = log.NewLoggerWithPrefix("[bm-loader]")
)

// Loader loader
type Loader interface {
	// Get get bitmap
	Get([]byte) (*roaring.Bitmap, error)
	// Set bitmap
	Set([]byte, []byte) (uint64, uint32, error)
}
