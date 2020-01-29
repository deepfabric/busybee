package expr

import (
	"github.com/deepfabric/busybee/pkg/pb/apipb"
)

// Ctx expr excution ctx
type Ctx interface {
	Event() apipb.Event
	Profile([]byte) ([]byte, error)
	KV([]byte) ([]byte, error)
}
