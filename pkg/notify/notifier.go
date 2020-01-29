package notify

import (
	"github.com/deepfabric/busybee/pkg/pb/apipb"
)

// Notifier service notify
type Notifier interface {
	Notify(uint64, ...apipb.Notify) error
}
