package notify

import (
	"github.com/deepfabric/busybee/pkg/pb/metapb"
)

// Notifier service notify
type Notifier interface {
	Notify(uint64, ...metapb.Notify) error
}
