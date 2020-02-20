package notify

import (
	"github.com/deepfabric/busybee/pkg/pb/metapb"
	"github.com/fagongzi/goetty"
)

// Notifier service notify
type Notifier interface {
	Notify(uint64, *goetty.ByteBuf, ...metapb.Notify) error
}
