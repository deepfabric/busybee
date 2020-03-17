package notify

import (
	"github.com/deepfabric/busybee/pkg/pb/metapb"
	"github.com/deepfabric/busybee/pkg/pb/rpcpb"
	"github.com/fagongzi/goetty"
	"github.com/fagongzi/log"
)

var (
	logger = log.NewLoggerWithPrefix("[notifier]")
)

// Notifier service notify
type Notifier interface {
	Notify(uint64, *goetty.ByteBuf, []metapb.Notify, *rpcpb.Condition, ...[]byte) error
}
