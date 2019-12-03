package rpcpb

import (
	"sync"
)

var (
	setRequestPool         sync.Pool
	getRequestPool         sync.Pool
	deleteRequestPool      sync.Pool
	bmcreateRequestPool    sync.Pool
	bmaddRequestPool       sync.Pool
	bmremoveRequestPool    sync.Pool
	bmclearRequestPool     sync.Pool
	bmcontainsRequestPool  sync.Pool
	bmdelRequestPool       sync.Pool
	bmcountRequestPool     sync.Pool
	bmrangeRequestPool     sync.Pool
	startwfRequestPool     sync.Pool
	removewfRequestPool    sync.Pool
	createstateRequestPool sync.Pool
	removestateRequestPool sync.Pool

	setResponsePool         sync.Pool
	getResponsePool         sync.Pool
	deleteResponsePool      sync.Pool
	bmcreateResponsePool    sync.Pool
	bmaddResponsePool       sync.Pool
	bmremoveResponsePool    sync.Pool
	bmclearResponsePool     sync.Pool
	bmcontainsResponsePool  sync.Pool
	bmdelResponsePool       sync.Pool
	bmcountResponsePool     sync.Pool
	bmrangeResponsePool     sync.Pool
	startwfResponsePool     sync.Pool
	removewfResponsePool    sync.Pool
	createstateResponsePool sync.Pool
	removestateResponsePool sync.Pool
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
	value := bmcreateRequestPool.Get()
	if value == nil {
		return &BMCreateRequest{}
	}
	return value.(*BMCreateRequest)
}

// ReleaseBMCreateRequest returns the value to pool
func ReleaseBMCreateRequest(value *BMCreateRequest) {
	value.Reset()
	bmcreateRequestPool.Put(value)
}

// AcquireBMAddRequest returns value from pool
func AcquireBMAddRequest() *BMAddRequest {
	value := bmaddRequestPool.Get()
	if value == nil {
		return &BMAddRequest{}
	}
	return value.(*BMAddRequest)
}

// ReleaseBMAddRequest returns the value to pool
func ReleaseBMAddRequest(value *BMAddRequest) {
	value.Reset()
	bmaddRequestPool.Put(value)
}

// AcquireBMRemoveRequest returns value from pool
func AcquireBMRemoveRequest() *BMRemoveRequest {
	value := bmremoveRequestPool.Get()
	if value == nil {
		return &BMRemoveRequest{}
	}
	return value.(*BMRemoveRequest)
}

// ReleaseBMRemoveRequest returns the value to pool
func ReleaseBMRemoveRequest(value *BMRemoveRequest) {
	value.Reset()
	bmremoveRequestPool.Put(value)
}

// AcquireBMClearRequest returns value from pool
func AcquireBMClearRequest() *BMClearRequest {
	value := bmclearRequestPool.Get()
	if value == nil {
		return &BMClearRequest{}
	}
	return value.(*BMClearRequest)
}

// ReleaseBMClearRequest returns the value to pool
func ReleaseBMClearRequest(value *BMClearRequest) {
	value.Reset()
	bmclearRequestPool.Put(value)
}

// AcquireBMContainsRequest returns value from pool
func AcquireBMContainsRequest() *BMContainsRequest {
	value := bmcontainsRequestPool.Get()
	if value == nil {
		return &BMContainsRequest{}
	}
	return value.(*BMContainsRequest)
}

// ReleaseBMContainsRequest returns the value to pool
func ReleaseBMContainsRequest(value *BMContainsRequest) {
	value.Reset()
	bmcontainsRequestPool.Put(value)
}

// AcquireBMDelRequest returns value from pool
func AcquireBMDelRequest() *BMDelRequest {
	value := bmdelRequestPool.Get()
	if value == nil {
		return &BMDelRequest{}
	}
	return value.(*BMDelRequest)
}

// ReleaseBMDelRequest returns the value to pool
func ReleaseBMDelRequest(value *BMDelRequest) {
	value.Reset()
	bmdelRequestPool.Put(value)
}

// AcquireBMCountRequest returns value from pool
func AcquireBMCountRequest() *BMCountRequest {
	value := bmcountRequestPool.Get()
	if value == nil {
		return &BMCountRequest{}
	}
	return value.(*BMCountRequest)
}

// ReleaseBMCountRequest returns the value to pool
func ReleaseBMCountRequest(value *BMCountRequest) {
	value.Reset()
	bmcountRequestPool.Put(value)
}

// AcquireBMRangeRequest returns value from pool
func AcquireBMRangeRequest() *BMRangeRequest {
	value := bmcountRequestPool.Get()
	if value == nil {
		return &BMRangeRequest{}
	}
	return value.(*BMRangeRequest)
}

// ReleaseBMRangeRequest returns the value to pool
func ReleaseBMRangeRequest(value *BMRangeRequest) {
	value.Reset()
	bmcountRequestPool.Put(value)
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
	value := bmcreateResponsePool.Get()
	if value == nil {
		return &BMCreateResponse{}
	}
	return value.(*BMCreateResponse)
}

// ReleaseBMCreateResponse returns the value to pool
func ReleaseBMCreateResponse(value *BMCreateResponse) {
	value.Reset()
	bmcreateResponsePool.Put(value)
}

// AcquireBMAddResponse returns value from pool
func AcquireBMAddResponse() *BMAddResponse {
	value := bmaddResponsePool.Get()
	if value == nil {
		return &BMAddResponse{}
	}
	return value.(*BMAddResponse)
}

// ReleaseBMAddResponse returns the value to pool
func ReleaseBMAddResponse(value *BMAddResponse) {
	value.Reset()
	bmaddResponsePool.Put(value)
}

// AcquireBMRemoveResponse returns value from pool
func AcquireBMRemoveResponse() *BMRemoveResponse {
	value := bmremoveResponsePool.Get()
	if value == nil {
		return &BMRemoveResponse{}
	}
	return value.(*BMRemoveResponse)
}

// ReleaseBMRemoveResponse returns the value to pool
func ReleaseBMRemoveResponse(value *BMRemoveResponse) {
	value.Reset()
	bmremoveResponsePool.Put(value)
}

// AcquireBMClearResponse returns value from pool
func AcquireBMClearResponse() *BMClearResponse {
	value := bmclearResponsePool.Get()
	if value == nil {
		return &BMClearResponse{}
	}
	return value.(*BMClearResponse)
}

// ReleaseBMClearResponse returns the value to pool
func ReleaseBMClearResponse(value *BMClearResponse) {
	value.Reset()
	bmclearResponsePool.Put(value)
}

// AcquireBMContainsResponse returns value from pool
func AcquireBMContainsResponse() *BMContainsResponse {
	value := bmcontainsResponsePool.Get()
	if value == nil {
		return &BMContainsResponse{}
	}
	return value.(*BMContainsResponse)
}

// ReleaseBMContainsResponse returns the value to pool
func ReleaseBMContainsResponse(value *BMContainsResponse) {
	value.Reset()
	bmcontainsResponsePool.Put(value)
}

// AcquireBMDelResponse returns value from pool
func AcquireBMDelResponse() *BMDelResponse {
	value := bmdelResponsePool.Get()
	if value == nil {
		return &BMDelResponse{}
	}
	return value.(*BMDelResponse)
}

// ReleaseBMDelResponse returns the value to pool
func ReleaseBMDelResponse(value *BMDelResponse) {
	value.Reset()
	bmdelResponsePool.Put(value)
}

// AcquireBMCountResponse returns value from pool
func AcquireBMCountResponse() *BMCountResponse {
	value := bmcountResponsePool.Get()
	if value == nil {
		return &BMCountResponse{}
	}
	return value.(*BMCountResponse)
}

// ReleaseBMCountResponse returns the value to pool
func ReleaseBMCountResponse(value *BMCountResponse) {
	value.Reset()
	bmcountResponsePool.Put(value)
}

// AcquireBMRangeResponse returns value from pool
func AcquireBMRangeResponse() *BMRangeResponse {
	value := bmrangeResponsePool.Get()
	if value == nil {
		return &BMRangeResponse{}
	}
	return value.(*BMRangeResponse)
}

// ReleaseBMRangeResponse returns the value to pool
func ReleaseBMRangeResponse(value *BMRangeResponse) {
	value.Reset()
	bmrangeResponsePool.Put(value)
}

// AcquireStartWFRequest returns value from pool
func AcquireStartWFRequest() *StartWFRequest {
	value := startwfRequestPool.Get()
	if value == nil {
		return &StartWFRequest{}
	}
	return value.(*StartWFRequest)
}

// ReleaseStartWFRequest returns the value to pool
func ReleaseStartWFRequest(value *StartWFRequest) {
	value.Reset()
	startwfRequestPool.Put(value)
}

// AcquireRemoveWFRequest returns value from pool
func AcquireRemoveWFRequest() *RemoveWFRequest {
	value := removewfRequestPool.Get()
	if value == nil {
		return &RemoveWFRequest{}
	}
	return value.(*RemoveWFRequest)
}

// ReleaseRemoveWFRequest returns the value to pool
func ReleaseRemoveWFRequest(value *RemoveWFRequest) {
	value.Reset()
	removewfRequestPool.Put(value)
}

// AcquireStartWFResponse returns value from pool
func AcquireStartWFResponse() *StartWFResponse {
	value := startwfResponsePool.Get()
	if value == nil {
		return &StartWFResponse{}
	}
	return value.(*StartWFResponse)
}

// ReleaseStartWFResponse returns the value to pool
func ReleaseStartWFResponse(value *StartWFResponse) {
	value.Reset()
	startwfResponsePool.Put(value)
}

// AcquireRemoveWFResponse returns value from pool
func AcquireRemoveWFResponse() *RemoveWFResponse {
	value := removewfResponsePool.Get()
	if value == nil {
		return &RemoveWFResponse{}
	}
	return value.(*RemoveWFResponse)
}

// ReleaseRemoveWFResponse returns the value to pool
func ReleaseRemoveWFResponse(value *RemoveWFResponse) {
	value.Reset()
	removewfResponsePool.Put(value)
}

// AcquireCreateStateRequest returns value from pool
func AcquireCreateStateRequest() *CreateStateRequest {
	value := createstateRequestPool.Get()
	if value == nil {
		return &CreateStateRequest{}
	}
	return value.(*CreateStateRequest)
}

// ReleaseCreateStateRequest returns the value to pool
func ReleaseCreateStateRequest(value *CreateStateRequest) {
	value.Reset()
	createstateRequestPool.Put(value)
}

// AcquireRemoveStateRequest returns value from pool
func AcquireRemoveStateRequest() *RemoveStateRequest {
	value := removestateRequestPool.Get()
	if value == nil {
		return &RemoveStateRequest{}
	}
	return value.(*RemoveStateRequest)
}

// ReleaseRemoveStateRequest returns the value to pool
func ReleaseRemoveStateRequest(value *RemoveStateRequest) {
	value.Reset()
	removestateRequestPool.Put(value)
}

// AcquireCreateStateResponse returns value from pool
func AcquireCreateStateResponse() *CreateStateResponse {
	value := createstateResponsePool.Get()
	if value == nil {
		return &CreateStateResponse{}
	}
	return value.(*CreateStateResponse)
}

// ReleaseCreateStateResponse returns the value to pool
func ReleaseCreateStateResponse(value *CreateStateResponse) {
	value.Reset()
	createstateResponsePool.Put(value)
}

// AcquireRemoveStateResponse returns value from pool
func AcquireRemoveStateResponse() *RemoveStateResponse {
	value := removestateResponsePool.Get()
	if value == nil {
		return &RemoveStateResponse{}
	}
	return value.(*RemoveStateResponse)
}

// ReleaseRemoveStateResponse returns the value to pool
func ReleaseRemoveStateResponse(value *RemoveStateResponse) {
	value.Reset()
	removestateResponsePool.Put(value)
}
