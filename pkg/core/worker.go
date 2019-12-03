package core

import (
	"github.com/deepfabric/busybee/pkg/pb/metapb"
)

type worker struct {
	id      uint64
	instace *metapb.WorkflowInstance
}
