package rpcpb

import (
	"sync"

	"github.com/fagongzi/util/protoc"
)

var (
	// EmptyResp empty resp
	EmptyResp = EmptyResponse{}
	// EmptyRespBytes empty resp bytes
	EmptyRespBytes = protoc.MustMarshal(&EmptyResp)
	// EmptyBytesSliceBytes empty bytes slice response
	EmptyBytesSliceBytes = protoc.MustMarshal(&BytesSliceResponse{})
)

var (
	setPool                      sync.Pool
	getPool                      sync.Pool
	deletePool                   sync.Pool
	scanPool                     sync.Pool
	allocIDPool                  sync.Pool
	resetIDPool                  sync.Pool
	bmCreatePool                 sync.Pool
	bmAddPool                    sync.Pool
	bmRemovePool                 sync.Pool
	bmClearPool                  sync.Pool
	bmContainsPool               sync.Pool
	bmCountPool                  sync.Pool
	bmRangePool                  sync.Pool
	startingInstancePool         sync.Pool
	startedInstancePool          sync.Pool
	stopInstancePool             sync.Pool
	stoppedInstancePool          sync.Pool
	createInstanceStateShardPool sync.Pool
	updateInstanceStateShardPool sync.Pool
	removeInstanceStateShardPool sync.Pool
	queueAddPool                 sync.Pool
	queueFetchPool               sync.Pool
	queueConcurrencyFetchPool    sync.Pool
	queueJoinPool                sync.Pool
	tenantInitPool               sync.Pool
	updateMappingPool            sync.Pool
	scanMappingPool              sync.Pool

	uint64Pool      sync.Pool
	bytesPool       sync.Pool
	boolPool        sync.Pool
	uint32Pool      sync.Pool
	uint32RangePool sync.Pool
	uint32SlicePool sync.Pool
	bytesSlicePool  sync.Pool

	requestPool = sync.Pool{
		New: func() interface{} {
			return &Request{}
		},
	}

	responsePool = sync.Pool{
		New: func() interface{} {
			return &Response{}
		},
	}
)

// AcquireResetIDRequest returns value from pool
func AcquireResetIDRequest() *ResetIDRequest {
	value := resetIDPool.Get()
	if value == nil {
		return &ResetIDRequest{}
	}
	return value.(*ResetIDRequest)
}

// ReleaseResetIDRequest returns the value to pool
func ReleaseResetIDRequest(value *ResetIDRequest) {
	value.Reset()
	resetIDPool.Put(value)
}

// AcquireAllocIDRequest returns value from pool
func AcquireAllocIDRequest() *AllocIDRequest {
	value := allocIDPool.Get()
	if value == nil {
		return &AllocIDRequest{}
	}
	return value.(*AllocIDRequest)
}

// ReleaseAllocIDRequest returns the value to pool
func ReleaseAllocIDRequest(value *AllocIDRequest) {
	value.Reset()
	allocIDPool.Put(value)
}

// AcquireUint32RangeResponse returns value from pool
func AcquireUint32RangeResponse() *Uint32RangeResponse {
	value := uint32RangePool.Get()
	if value == nil {
		return &Uint32RangeResponse{}
	}
	return value.(*Uint32RangeResponse)
}

// ReleaseUint32RangeResponse returns the value to pool
func ReleaseUint32RangeResponse(value *Uint32RangeResponse) {
	value.Reset()
	uint32RangePool.Put(value)
}

// AcquireUint32Response returns value from pool
func AcquireUint32Response() *Uint32Response {
	value := uint32Pool.Get()
	if value == nil {
		return &Uint32Response{}
	}
	return value.(*Uint32Response)
}

// ReleaseUint32Response returns the value to pool
func ReleaseUint32Response(value *Uint32Response) {
	value.Reset()
	uint32Pool.Put(value)
}

// AcquireRequest returns value from pool
func AcquireRequest() *Request {
	return requestPool.Get().(*Request)
}

// ReleaseRequest returns the value to pool
func ReleaseRequest(value *Request) {
	value.Reset()
	requestPool.Put(value)
}

// AcquireResponse returns value from pool
func AcquireResponse() *Response {
	return responsePool.Get().(*Response)
}

// ReleaseResponse returns the value to pool
func ReleaseResponse(value *Response) {
	value.Reset()
	responsePool.Put(value)
}

// AcquireTenantInitRequest returns value from pool
func AcquireTenantInitRequest() *TenantInitRequest {
	value := tenantInitPool.Get()
	if value == nil {
		return &TenantInitRequest{}
	}
	return value.(*TenantInitRequest)
}

// ReleaseTenantInitRequest returns the value to pool
func ReleaseTenantInitRequest(value *TenantInitRequest) {
	value.Reset()
	tenantInitPool.Put(value)
}

// AcquireBytesSliceResponse returns value from pool
func AcquireBytesSliceResponse() *BytesSliceResponse {
	value := bytesSlicePool.Get()
	if value == nil {
		return &BytesSliceResponse{}
	}
	return value.(*BytesSliceResponse)
}

// ReleaseBytesSliceResponse returns the value to pool
func ReleaseBytesSliceResponse(value *BytesSliceResponse) {
	value.Reset()
	bytesSlicePool.Put(value)
}

// AcquireUint32SliceResponse returns value from pool
func AcquireUint32SliceResponse() *Uint32SliceResponse {
	value := uint32SlicePool.Get()
	if value == nil {
		return &Uint32SliceResponse{}
	}
	return value.(*Uint32SliceResponse)
}

// ReleaseUint32SliceResponse returns the value to pool
func ReleaseUint32SliceResponse(value *Uint32SliceResponse) {
	value.Reset()
	uint32SlicePool.Put(value)
}

// AcquireBoolResponse returns value from pool
func AcquireBoolResponse() *BoolResponse {
	value := boolPool.Get()
	if value == nil {
		return &BoolResponse{}
	}
	return value.(*BoolResponse)
}

// ReleaseBoolResponse returns the value to pool
func ReleaseBoolResponse(value *BoolResponse) {
	value.Reset()
	boolPool.Put(value)
}

// AcquireBytesResponse returns value from pool
func AcquireBytesResponse() *BytesResponse {
	value := bytesPool.Get()
	if value == nil {
		return &BytesResponse{}
	}
	return value.(*BytesResponse)
}

// ReleaseBytesResponse returns the value to pool
func ReleaseBytesResponse(value *BytesResponse) {
	value.Reset()
	bytesPool.Put(value)
}

// AcquireUint64Response returns value from pool
func AcquireUint64Response() *Uint64Response {
	value := uint64Pool.Get()
	if value == nil {
		return &Uint64Response{}
	}
	return value.(*Uint64Response)
}

// ReleaseUint64Response returns the value to pool
func ReleaseUint64Response(value *Uint64Response) {
	value.Reset()
	uint64Pool.Put(value)
}

// AcquireSetRequest returns value from pool
func AcquireSetRequest() *SetRequest {
	value := setPool.Get()
	if value == nil {
		return &SetRequest{}
	}
	return value.(*SetRequest)
}

// ReleaseSetRequest returns the value to pool
func ReleaseSetRequest(value *SetRequest) {
	value.Reset()
	setPool.Put(value)
}

// AcquireGetRequest returns value from pool
func AcquireGetRequest() *GetRequest {
	value := getPool.Get()
	if value == nil {
		return &GetRequest{}
	}
	return value.(*GetRequest)
}

// ReleaseGetRequest returns the value to pool
func ReleaseGetRequest(value *GetRequest) {
	value.Reset()
	getPool.Put(value)
}

// AcquireDeleteRequest returns value from pool
func AcquireDeleteRequest() *DeleteRequest {
	value := deletePool.Get()
	if value == nil {
		return &DeleteRequest{}
	}
	return value.(*DeleteRequest)
}

// ReleaseDeleteRequest returns the value to pool
func ReleaseDeleteRequest(value *DeleteRequest) {
	value.Reset()
	deletePool.Put(value)
}

// AcquireScanRequest returns value from pool
func AcquireScanRequest() *ScanRequest {
	value := scanPool.Get()
	if value == nil {
		return &ScanRequest{}
	}
	return value.(*ScanRequest)
}

// ReleaseScanRequest returns the value to pool
func ReleaseScanRequest(value *ScanRequest) {
	value.Reset()
	scanPool.Put(value)
}

// AcquireBMCreateRequest returns value from pool
func AcquireBMCreateRequest() *BMCreateRequest {
	value := bmCreatePool.Get()
	if value == nil {
		return &BMCreateRequest{}
	}
	return value.(*BMCreateRequest)
}

// ReleaseBMCreateRequest returns the value to pool
func ReleaseBMCreateRequest(value *BMCreateRequest) {
	value.Reset()
	bmCreatePool.Put(value)
}

// AcquireBMAddRequest returns value from pool
func AcquireBMAddRequest() *BMAddRequest {
	value := bmAddPool.Get()
	if value == nil {
		return &BMAddRequest{}
	}
	return value.(*BMAddRequest)
}

// ReleaseBMAddRequest returns the value to pool
func ReleaseBMAddRequest(value *BMAddRequest) {
	value.Reset()
	bmAddPool.Put(value)
}

// AcquireBMRemoveRequest returns value from pool
func AcquireBMRemoveRequest() *BMRemoveRequest {
	value := bmRemovePool.Get()
	if value == nil {
		return &BMRemoveRequest{}
	}
	return value.(*BMRemoveRequest)
}

// ReleaseBMRemoveRequest returns the value to pool
func ReleaseBMRemoveRequest(value *BMRemoveRequest) {
	value.Reset()
	bmRemovePool.Put(value)
}

// AcquireBMClearRequest returns value from pool
func AcquireBMClearRequest() *BMClearRequest {
	value := bmClearPool.Get()
	if value == nil {
		return &BMClearRequest{}
	}
	return value.(*BMClearRequest)
}

// ReleaseBMClearRequest returns the value to pool
func ReleaseBMClearRequest(value *BMClearRequest) {
	value.Reset()
	bmClearPool.Put(value)
}

// AcquireBMContainsRequest returns value from pool
func AcquireBMContainsRequest() *BMContainsRequest {
	value := bmContainsPool.Get()
	if value == nil {
		return &BMContainsRequest{}
	}
	return value.(*BMContainsRequest)
}

// ReleaseBMContainsRequest returns the value to pool
func ReleaseBMContainsRequest(value *BMContainsRequest) {
	value.Reset()
	bmContainsPool.Put(value)
}

// AcquireBMCountRequest returns value from pool
func AcquireBMCountRequest() *BMCountRequest {
	value := bmCountPool.Get()
	if value == nil {
		return &BMCountRequest{}
	}
	return value.(*BMCountRequest)
}

// ReleaseBMCountRequest returns the value to pool
func ReleaseBMCountRequest(value *BMCountRequest) {
	value.Reset()
	bmCountPool.Put(value)
}

// AcquireBMRangeRequest returns value from pool
func AcquireBMRangeRequest() *BMRangeRequest {
	value := bmRangePool.Get()
	if value == nil {
		return &BMRangeRequest{}
	}
	return value.(*BMRangeRequest)
}

// ReleaseBMRangeRequest returns the value to pool
func ReleaseBMRangeRequest(value *BMRangeRequest) {
	value.Reset()
	bmRangePool.Put(value)
}

// AcquireQueueAddRequest returns value from pool
func AcquireQueueAddRequest() *QueueAddRequest {
	value := queueAddPool.Get()
	if value == nil {
		return &QueueAddRequest{}
	}
	return value.(*QueueAddRequest)
}

// ReleaseQueueAddRequest returns the value to pool
func ReleaseQueueAddRequest(value *QueueAddRequest) {
	value.Reset()
	queueAddPool.Put(value)
}

// AcquireQueueFetchRequest returns value from pool
func AcquireQueueFetchRequest() *QueueFetchRequest {
	value := queueFetchPool.Get()
	if value == nil {
		return &QueueFetchRequest{}
	}
	return value.(*QueueFetchRequest)
}

// ReleaseQueueFetchRequest returns the value to pool
func ReleaseQueueFetchRequest(value *QueueFetchRequest) {
	value.Reset()
	queueFetchPool.Put(value)
}

// AcquireQueueConcurrencyFetchRequest returns value from pool
func AcquireQueueConcurrencyFetchRequest() *QueueConcurrencyFetchRequest {
	value := queueConcurrencyFetchPool.Get()
	if value == nil {
		return &QueueConcurrencyFetchRequest{}
	}
	return value.(*QueueConcurrencyFetchRequest)
}

// ReleaseQueueConcurrencyFetchRequest returns the value to pool
func ReleaseQueueConcurrencyFetchRequest(value *QueueConcurrencyFetchRequest) {
	value.Reset()
	queueConcurrencyFetchPool.Put(value)
}

// AcquireQueueJoinGroupRequest returns value from pool
func AcquireQueueJoinGroupRequest() *QueueJoinGroupRequest {
	value := queueJoinPool.Get()
	if value == nil {
		return &QueueJoinGroupRequest{}
	}
	return value.(*QueueJoinGroupRequest)
}

// ReleaseQueueJoinGroupRequest returns the value to pool
func ReleaseQueueJoinGroupRequest(value *QueueJoinGroupRequest) {
	value.Reset()
	queueJoinPool.Put(value)
}

// AcquireUpdateMappingRequest returns value from pool
func AcquireUpdateMappingRequest() *UpdateMappingRequest {
	value := updateMappingPool.Get()
	if value == nil {
		return &UpdateMappingRequest{}
	}
	return value.(*UpdateMappingRequest)
}

// ReleaseUpdateMappingRequest returns the value to pool
func ReleaseUpdateMappingRequest(value *UpdateMappingRequest) {
	value.Reset()
	updateMappingPool.Put(value)
}

// AcquireScanMappingRequest returns value from pool
func AcquireScanMappingRequest() *ScanMappingRequest {
	value := scanMappingPool.Get()
	if value == nil {
		return &ScanMappingRequest{}
	}
	return value.(*ScanMappingRequest)
}

// ReleaseScanMappingRequest returns the value to pool
func ReleaseScanMappingRequest(value *ScanMappingRequest) {
	value.Reset()
	scanMappingPool.Put(value)
}

// AcquireStartingInstanceRequest returns value from pool
func AcquireStartingInstanceRequest() *StartingInstanceRequest {
	value := startingInstancePool.Get()
	if value == nil {
		return &StartingInstanceRequest{}
	}
	return value.(*StartingInstanceRequest)
}

// ReleaseStartingInstanceRequest returns the value to pool
func ReleaseStartingInstanceRequest(value *StartingInstanceRequest) {
	value.Reset()
	startingInstancePool.Put(value)
}

// AcquireStartedInstanceRequest returns value from pool
func AcquireStartedInstanceRequest() *StartedInstanceRequest {
	value := startedInstancePool.Get()
	if value == nil {
		return &StartedInstanceRequest{}
	}
	return value.(*StartedInstanceRequest)
}

// ReleaseStartedInstanceRequest returns the value to pool
func ReleaseStartedInstanceRequest(value *StartedInstanceRequest) {
	value.Reset()
	startedInstancePool.Put(value)
}

// AcquireStopInstanceRequest returns value from pool
func AcquireStopInstanceRequest() *StopInstanceRequest {
	value := stopInstancePool.Get()
	if value == nil {
		return &StopInstanceRequest{}
	}
	return value.(*StopInstanceRequest)
}

// ReleaseStopInstanceRequest returns the value to pool
func ReleaseStopInstanceRequest(value *StopInstanceRequest) {
	value.Reset()
	stopInstancePool.Put(value)
}

// AcquireStoppedInstanceRequest returns value from pool
func AcquireStoppedInstanceRequest() *StoppedInstanceRequest {
	value := stoppedInstancePool.Get()
	if value == nil {
		return &StoppedInstanceRequest{}
	}
	return value.(*StoppedInstanceRequest)
}

// ReleaseStoppedInstanceRequest returns the value to pool
func ReleaseStoppedInstanceRequest(value *StoppedInstanceRequest) {
	value.Reset()
	stoppedInstancePool.Put(value)
}

// AcquireCreateInstanceStateShardRequest returns value from pool
func AcquireCreateInstanceStateShardRequest() *CreateInstanceStateShardRequest {
	value := createInstanceStateShardPool.Get()
	if value == nil {
		return &CreateInstanceStateShardRequest{}
	}
	return value.(*CreateInstanceStateShardRequest)
}

// ReleaseCreateInstanceStateShardRequest returns the value to pool
func ReleaseCreateInstanceStateShardRequest(value *CreateInstanceStateShardRequest) {
	value.Reset()
	createInstanceStateShardPool.Put(value)
}

// AcquireUpdateInstanceStateShardRequest returns value from pool
func AcquireUpdateInstanceStateShardRequest() *UpdateInstanceStateShardRequest {
	value := updateInstanceStateShardPool.Get()
	if value == nil {
		return &UpdateInstanceStateShardRequest{}
	}
	return value.(*UpdateInstanceStateShardRequest)
}

// ReleaseUpdateInstanceStateShardRequest returns the value to pool
func ReleaseUpdateInstanceStateShardRequest(value *UpdateInstanceStateShardRequest) {
	value.Reset()
	updateInstanceStateShardPool.Put(value)
}

// AcquireRemoveInstanceStateShardRequest returns value from pool
func AcquireRemoveInstanceStateShardRequest() *RemoveInstanceStateShardRequest {
	value := removeInstanceStateShardPool.Get()
	if value == nil {
		return &RemoveInstanceStateShardRequest{}
	}
	return value.(*RemoveInstanceStateShardRequest)
}

// ReleaseRemoveInstanceStateShardRequest returns the value to pool
func ReleaseRemoveInstanceStateShardRequest(value *RemoveInstanceStateShardRequest) {
	value.Reset()
	removeInstanceStateShardPool.Put(value)
}
