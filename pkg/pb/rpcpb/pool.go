package rpcpb

import (
	"sync"
)

var (
	setRequestPool                      sync.Pool
	getRequestPool                      sync.Pool
	deleteRequestPool                   sync.Pool
	bmCreateRequestPool                 sync.Pool
	bmAddRequestPool                    sync.Pool
	bmRemoveRequestPool                 sync.Pool
	bmClearRequestPool                  sync.Pool
	bmContainsRequestPool               sync.Pool
	bmDelRequestPool                    sync.Pool
	bmCountRequestPool                  sync.Pool
	bmRangeRequestPool                  sync.Pool
	startingInstanceRequestPool         sync.Pool
	startedInstanceRequestPool          sync.Pool
	createInstanceStateShardRequestPool sync.Pool
	updateInstanceStateShardRequestPool sync.Pool
	removeInstanceStateShardRequestPool sync.Pool
	stepInstanceStateShardRequestPool   sync.Pool
	queueAddRequestPool                 sync.Pool
	queueFetchRequestPool               sync.Pool

	setResponsePool                      sync.Pool
	getResponsePool                      sync.Pool
	deleteResponsePool                   sync.Pool
	bmCreateResponsePool                 sync.Pool
	bmAddResponsePool                    sync.Pool
	bmRemoveResponsePool                 sync.Pool
	bmClearResponsePool                  sync.Pool
	bmContainsResponsePool               sync.Pool
	bmDelResponsePool                    sync.Pool
	bmCountResponsePool                  sync.Pool
	bmRangeResponsePool                  sync.Pool
	startingInstanceResponsePool         sync.Pool
	startedInstanceResponsePool          sync.Pool
	createInstanceStateShardResponsePool sync.Pool
	updateInstanceStateShardResponsePool sync.Pool
	removeInstanceStateShardResponsePool sync.Pool
	stepInstanceStateShardResponsePool   sync.Pool
	queueAddResponsePool                 sync.Pool
	queueFetchResponsePool               sync.Pool
)

// AcquireSetRequest returns value from pool
func AcquireSetRequest() *SetRequest {
	value := setRequestPool.Get()
	if value == nil {
		return &SetRequest{}
	}
	return value.(*SetRequest)
}

// ReleaseSetRequest returns the value to pool
func ReleaseSetRequest(value *SetRequest) {
	value.Reset()
	setRequestPool.Put(value)
}

// AcquireGetRequest returns value from pool
func AcquireGetRequest() *GetRequest {
	value := getRequestPool.Get()
	if value == nil {
		return &GetRequest{}
	}
	return value.(*GetRequest)
}

// ReleaseGetRequest returns the value to pool
func ReleaseGetRequest(value *GetRequest) {
	value.Reset()
	getRequestPool.Put(value)
}

// AcquireDeleteRequest returns value from pool
func AcquireDeleteRequest() *DeleteRequest {
	value := deleteRequestPool.Get()
	if value == nil {
		return &DeleteRequest{}
	}
	return value.(*DeleteRequest)
}

// ReleaseDeleteRequest returns the value to pool
func ReleaseDeleteRequest(value *DeleteRequest) {
	value.Reset()
	deleteRequestPool.Put(value)
}

// AcquireBMCreateRequest returns value from pool
func AcquireBMCreateRequest() *BMCreateRequest {
	value := bmCreateRequestPool.Get()
	if value == nil {
		return &BMCreateRequest{}
	}
	return value.(*BMCreateRequest)
}

// ReleaseBMCreateRequest returns the value to pool
func ReleaseBMCreateRequest(value *BMCreateRequest) {
	value.Reset()
	bmCreateRequestPool.Put(value)
}

// AcquireBMAddRequest returns value from pool
func AcquireBMAddRequest() *BMAddRequest {
	value := bmAddRequestPool.Get()
	if value == nil {
		return &BMAddRequest{}
	}
	return value.(*BMAddRequest)
}

// ReleaseBMAddRequest returns the value to pool
func ReleaseBMAddRequest(value *BMAddRequest) {
	value.Reset()
	bmAddRequestPool.Put(value)
}

// AcquireBMRemoveRequest returns value from pool
func AcquireBMRemoveRequest() *BMRemoveRequest {
	value := bmRemoveRequestPool.Get()
	if value == nil {
		return &BMRemoveRequest{}
	}
	return value.(*BMRemoveRequest)
}

// ReleaseBMRemoveRequest returns the value to pool
func ReleaseBMRemoveRequest(value *BMRemoveRequest) {
	value.Reset()
	bmRemoveRequestPool.Put(value)
}

// AcquireBMClearRequest returns value from pool
func AcquireBMClearRequest() *BMClearRequest {
	value := bmClearRequestPool.Get()
	if value == nil {
		return &BMClearRequest{}
	}
	return value.(*BMClearRequest)
}

// ReleaseBMClearRequest returns the value to pool
func ReleaseBMClearRequest(value *BMClearRequest) {
	value.Reset()
	bmClearRequestPool.Put(value)
}

// AcquireBMContainsRequest returns value from pool
func AcquireBMContainsRequest() *BMContainsRequest {
	value := bmContainsRequestPool.Get()
	if value == nil {
		return &BMContainsRequest{}
	}
	return value.(*BMContainsRequest)
}

// ReleaseBMContainsRequest returns the value to pool
func ReleaseBMContainsRequest(value *BMContainsRequest) {
	value.Reset()
	bmContainsRequestPool.Put(value)
}

// AcquireBMDelRequest returns value from pool
func AcquireBMDelRequest() *BMDelRequest {
	value := bmDelRequestPool.Get()
	if value == nil {
		return &BMDelRequest{}
	}
	return value.(*BMDelRequest)
}

// ReleaseBMDelRequest returns the value to pool
func ReleaseBMDelRequest(value *BMDelRequest) {
	value.Reset()
	bmDelRequestPool.Put(value)
}

// AcquireBMCountRequest returns value from pool
func AcquireBMCountRequest() *BMCountRequest {
	value := bmCountRequestPool.Get()
	if value == nil {
		return &BMCountRequest{}
	}
	return value.(*BMCountRequest)
}

// ReleaseBMCountRequest returns the value to pool
func ReleaseBMCountRequest(value *BMCountRequest) {
	value.Reset()
	bmCountRequestPool.Put(value)
}

// AcquireBMRangeRequest returns value from pool
func AcquireBMRangeRequest() *BMRangeRequest {
	value := bmCountRequestPool.Get()
	if value == nil {
		return &BMRangeRequest{}
	}
	return value.(*BMRangeRequest)
}

// ReleaseBMRangeRequest returns the value to pool
func ReleaseBMRangeRequest(value *BMRangeRequest) {
	value.Reset()
	bmCountRequestPool.Put(value)
}

// AcquireQueueAddRequest returns value from pool
func AcquireQueueAddRequest() *QueueAddRequest {
	value := queueAddRequestPool.Get()
	if value == nil {
		return &QueueAddRequest{}
	}
	return value.(*QueueAddRequest)
}

// ReleaseQueueAddRequest returns the value to pool
func ReleaseQueueAddRequest(value *QueueAddRequest) {
	value.Reset()
	queueAddRequestPool.Put(value)
}

// AcquireQueueFetchRequest returns value from pool
func AcquireQueueFetchRequest() *QueueFetchRequest {
	value := queueFetchRequestPool.Get()
	if value == nil {
		return &QueueFetchRequest{}
	}
	return value.(*QueueFetchRequest)
}

// ReleaseQueueFetchRequest returns the value to pool
func ReleaseQueueFetchRequest(value *QueueFetchRequest) {
	value.Reset()
	queueFetchRequestPool.Put(value)
}

// AcquireSetResponse returns value from pool
func AcquireSetResponse() *SetResponse {
	value := setResponsePool.Get()
	if value == nil {
		return &SetResponse{}
	}
	return value.(*SetResponse)
}

// ReleaseSetResponse returns the value to pool
func ReleaseSetResponse(value *SetResponse) {
	value.Reset()
	setResponsePool.Put(value)
}

// AcquireGetResponse returns value from pool
func AcquireGetResponse() *GetResponse {
	value := getResponsePool.Get()
	if value == nil {
		return &GetResponse{}
	}
	return value.(*GetResponse)
}

// ReleaseGetResponse returns the value to pool
func ReleaseGetResponse(value *GetResponse) {
	value.Reset()
	getResponsePool.Put(value)
}

// AcquireDeleteResponse returns value from pool
func AcquireDeleteResponse() *DeleteResponse {
	value := deleteResponsePool.Get()
	if value == nil {
		return &DeleteResponse{}
	}
	return value.(*DeleteResponse)
}

// ReleaseDeleteResponse returns the value to pool
func ReleaseDeleteResponse(value *DeleteResponse) {
	value.Reset()
	deleteResponsePool.Put(value)
}

// AcquireBMCreateResponse returns value from pool
func AcquireBMCreateResponse() *BMCreateResponse {
	value := bmCreateResponsePool.Get()
	if value == nil {
		return &BMCreateResponse{}
	}
	return value.(*BMCreateResponse)
}

// ReleaseBMCreateResponse returns the value to pool
func ReleaseBMCreateResponse(value *BMCreateResponse) {
	value.Reset()
	bmCreateResponsePool.Put(value)
}

// AcquireBMAddResponse returns value from pool
func AcquireBMAddResponse() *BMAddResponse {
	value := bmAddResponsePool.Get()
	if value == nil {
		return &BMAddResponse{}
	}
	return value.(*BMAddResponse)
}

// ReleaseBMAddResponse returns the value to pool
func ReleaseBMAddResponse(value *BMAddResponse) {
	value.Reset()
	bmAddResponsePool.Put(value)
}

// AcquireBMRemoveResponse returns value from pool
func AcquireBMRemoveResponse() *BMRemoveResponse {
	value := bmRemoveResponsePool.Get()
	if value == nil {
		return &BMRemoveResponse{}
	}
	return value.(*BMRemoveResponse)
}

// ReleaseBMRemoveResponse returns the value to pool
func ReleaseBMRemoveResponse(value *BMRemoveResponse) {
	value.Reset()
	bmRemoveResponsePool.Put(value)
}

// AcquireBMClearResponse returns value from pool
func AcquireBMClearResponse() *BMClearResponse {
	value := bmClearResponsePool.Get()
	if value == nil {
		return &BMClearResponse{}
	}
	return value.(*BMClearResponse)
}

// ReleaseBMClearResponse returns the value to pool
func ReleaseBMClearResponse(value *BMClearResponse) {
	value.Reset()
	bmClearResponsePool.Put(value)
}

// AcquireBMContainsResponse returns value from pool
func AcquireBMContainsResponse() *BMContainsResponse {
	value := bmContainsResponsePool.Get()
	if value == nil {
		return &BMContainsResponse{}
	}
	return value.(*BMContainsResponse)
}

// ReleaseBMContainsResponse returns the value to pool
func ReleaseBMContainsResponse(value *BMContainsResponse) {
	value.Reset()
	bmContainsResponsePool.Put(value)
}

// AcquireBMDelResponse returns value from pool
func AcquireBMDelResponse() *BMDelResponse {
	value := bmDelResponsePool.Get()
	if value == nil {
		return &BMDelResponse{}
	}
	return value.(*BMDelResponse)
}

// ReleaseBMDelResponse returns the value to pool
func ReleaseBMDelResponse(value *BMDelResponse) {
	value.Reset()
	bmDelResponsePool.Put(value)
}

// AcquireBMCountResponse returns value from pool
func AcquireBMCountResponse() *BMCountResponse {
	value := bmCountResponsePool.Get()
	if value == nil {
		return &BMCountResponse{}
	}
	return value.(*BMCountResponse)
}

// ReleaseBMCountResponse returns the value to pool
func ReleaseBMCountResponse(value *BMCountResponse) {
	value.Reset()
	bmCountResponsePool.Put(value)
}

// AcquireBMRangeResponse returns value from pool
func AcquireBMRangeResponse() *BMRangeResponse {
	value := bmRangeResponsePool.Get()
	if value == nil {
		return &BMRangeResponse{}
	}
	return value.(*BMRangeResponse)
}

// ReleaseBMRangeResponse returns the value to pool
func ReleaseBMRangeResponse(value *BMRangeResponse) {
	value.Reset()
	bmRangeResponsePool.Put(value)
}

// AcquireStartingInstanceRequest returns value from pool
func AcquireStartingInstanceRequest() *StartingInstanceRequest {
	value := startingInstanceRequestPool.Get()
	if value == nil {
		return &StartingInstanceRequest{}
	}
	return value.(*StartingInstanceRequest)
}

// ReleaseStartingInstanceRequest returns the value to pool
func ReleaseStartingInstanceRequest(value *StartingInstanceRequest) {
	value.Reset()
	startingInstanceRequestPool.Put(value)
}

// AcquireStartedInstanceRequest returns value from pool
func AcquireStartedInstanceRequest() *StartedInstanceRequest {
	value := startedInstanceRequestPool.Get()
	if value == nil {
		return &StartedInstanceRequest{}
	}
	return value.(*StartedInstanceRequest)
}

// ReleaseStartedInstanceRequest returns the value to pool
func ReleaseStartedInstanceRequest(value *StartedInstanceRequest) {
	value.Reset()
	startedInstanceRequestPool.Put(value)
}

// AcquireStartingInstanceResponse returns value from pool
func AcquireStartingInstanceResponse() *StartingInstanceResponse {
	value := startingInstanceResponsePool.Get()
	if value == nil {
		return &StartingInstanceResponse{}
	}
	return value.(*StartingInstanceResponse)
}

// ReleaseStartingInstanceResponse returns the value to pool
func ReleaseStartingInstanceResponse(value *StartingInstanceResponse) {
	value.Reset()
	startingInstanceResponsePool.Put(value)
}

// AcquireStartedInstanceResponse returns value from pool
func AcquireStartedInstanceResponse() *StartedInstanceResponse {
	value := startedInstanceResponsePool.Get()
	if value == nil {
		return &StartedInstanceResponse{}
	}
	return value.(*StartedInstanceResponse)
}

// ReleaseStartedInstanceResponse returns the value to pool
func ReleaseStartedInstanceResponse(value *StartedInstanceResponse) {
	value.Reset()
	startedInstanceResponsePool.Put(value)
}

// AcquireCreateInstanceStateShardRequest returns value from pool
func AcquireCreateInstanceStateShardRequest() *CreateInstanceStateShardRequest {
	value := createInstanceStateShardRequestPool.Get()
	if value == nil {
		return &CreateInstanceStateShardRequest{}
	}
	return value.(*CreateInstanceStateShardRequest)
}

// ReleaseCreateInstanceStateShardRequest returns the value to pool
func ReleaseCreateInstanceStateShardRequest(value *CreateInstanceStateShardRequest) {
	value.Reset()
	createInstanceStateShardRequestPool.Put(value)
}

// AcquireUpdateInstanceStateShardRequest returns value from pool
func AcquireUpdateInstanceStateShardRequest() *UpdateInstanceStateShardRequest {
	value := updateInstanceStateShardRequestPool.Get()
	if value == nil {
		return &UpdateInstanceStateShardRequest{}
	}
	return value.(*UpdateInstanceStateShardRequest)
}

// ReleaseUpdateInstanceStateShardRequest returns the value to pool
func ReleaseUpdateInstanceStateShardRequest(value *UpdateInstanceStateShardRequest) {
	value.Reset()
	updateInstanceStateShardRequestPool.Put(value)
}

// AcquireRemoveInstanceStateShardRequest returns value from pool
func AcquireRemoveInstanceStateShardRequest() *RemoveInstanceStateShardRequest {
	value := removeInstanceStateShardRequestPool.Get()
	if value == nil {
		return &RemoveInstanceStateShardRequest{}
	}
	return value.(*RemoveInstanceStateShardRequest)
}

// ReleaseRemoveInstanceStateShardRequest returns the value to pool
func ReleaseRemoveInstanceStateShardRequest(value *RemoveInstanceStateShardRequest) {
	value.Reset()
	removeInstanceStateShardRequestPool.Put(value)
}

// AcquireCreateInstanceStateShardResponse returns value from pool
func AcquireCreateInstanceStateShardResponse() *CreateInstanceStateShardResponse {
	value := createInstanceStateShardResponsePool.Get()
	if value == nil {
		return &CreateInstanceStateShardResponse{}
	}
	return value.(*CreateInstanceStateShardResponse)
}

// ReleaseCreateInstanceStateShardResponse returns the value to pool
func ReleaseCreateInstanceStateShardResponse(value *CreateInstanceStateShardResponse) {
	value.Reset()
	createInstanceStateShardResponsePool.Put(value)
}

// AcquireUpdateInstanceStateShardResponse returns value from pool
func AcquireUpdateInstanceStateShardResponse() *UpdateInstanceStateShardResponse {
	value := updateInstanceStateShardResponsePool.Get()
	if value == nil {
		return &UpdateInstanceStateShardResponse{}
	}
	return value.(*UpdateInstanceStateShardResponse)
}

// ReleaseUpdateInstanceStateShardResponse returns the value to pool
func ReleaseUpdateInstanceStateShardResponse(value *UpdateInstanceStateShardResponse) {
	value.Reset()
	updateInstanceStateShardResponsePool.Put(value)
}

// AcquireRemoveInstanceStateShardResponse returns value from pool
func AcquireRemoveInstanceStateShardResponse() *RemoveInstanceStateShardResponse {
	value := removeInstanceStateShardResponsePool.Get()
	if value == nil {
		return &RemoveInstanceStateShardResponse{}
	}
	return value.(*RemoveInstanceStateShardResponse)
}

// ReleaseRemoveInstanceStateShardResponse returns the value to pool
func ReleaseRemoveInstanceStateShardResponse(value *RemoveInstanceStateShardResponse) {
	value.Reset()
	removeInstanceStateShardResponsePool.Put(value)
}

// AcquireQueueAddResponse returns value from pool
func AcquireQueueAddResponse() *QueueAddResponse {
	value := queueAddResponsePool.Get()
	if value == nil {
		return &QueueAddResponse{}
	}
	return value.(*QueueAddResponse)
}

// ReleaseQueueAddResponse returns the value to pool
func ReleaseQueueAddResponse(value *QueueAddResponse) {
	value.Reset()
	queueAddResponsePool.Put(value)
}

// AcquireQueueFetchResponse returns value from pool
func AcquireQueueFetchResponse() *QueueFetchResponse {
	value := queueFetchResponsePool.Get()
	if value == nil {
		return &QueueFetchResponse{}
	}
	return value.(*QueueFetchResponse)
}

// ReleaseQueueFetchResponse returns the value to pool
func ReleaseQueueFetchResponse(value *QueueFetchResponse) {
	value.Reset()
	queueFetchResponsePool.Put(value)
}

// AcquireStepInstanceStateShardRequest returns value from pool
func AcquireStepInstanceStateShardRequest() *StepInstanceStateShardRequest {
	value := stepInstanceStateShardRequestPool.Get()
	if value == nil {
		return &StepInstanceStateShardRequest{}
	}
	return value.(*StepInstanceStateShardRequest)
}

// ReleaseStepInstanceStateShardRequest returns the value to pool
func ReleaseStepInstanceStateShardRequest(value *StepInstanceStateShardRequest) {
	value.Reset()
	stepInstanceStateShardRequestPool.Put(value)
}

// AcquireStepInstanceStateShardResponse returns value from pool
func AcquireStepInstanceStateShardResponse() *StepInstanceStateShardResponse {
	value := stepInstanceStateShardResponsePool.Get()
	if value == nil {
		return &StepInstanceStateShardResponse{}
	}
	return value.(*StepInstanceStateShardResponse)
}

// ReleaseStepInstanceStateShardResponse returns the value to pool
func ReleaseStepInstanceStateShardResponse(value *StepInstanceStateShardResponse) {
	value.Reset()
	stepInstanceStateShardResponsePool.Put(value)
}
