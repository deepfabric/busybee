package api

import (
	"github.com/deepfabric/busybee/pkg/pb/metapb"
)

// WorkflowAPI work flow api
type WorkflowAPI interface {
	// Create create a work flow definition meta
	Create(meta metapb.Workflow) (uint64, error)
	// Update update a work flow definition meta
	Update(meta metapb.Workflow) error
	// HasInstance returns true if the workflow has actived instance
	HasInstance(workflowID uint64) (bool, error)

	// CreateInstance create a new work flow instance,
	// an instance may contain a lot of people, so an instance will be divided into many shards,
	// each shard handles some people's events.
	CreateInstance(workflowID uint64, initShardCount uint64) (uint64, error)
	// DeleteInstance delete instance
	DeleteInstance(instanceID uint64) error
}
