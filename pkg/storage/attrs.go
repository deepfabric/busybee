package storage

import (
	"github.com/RoaringBitmap/roaring"
	"github.com/deepfabric/busybee/pkg/pb/metapb"
	"github.com/deepfabric/busybee/pkg/pb/rpcpb"
	"github.com/deepfabric/busybee/pkg/util"
)

var (
	attrRequestQueueAdd              = "req.queue.add"
	attrRequestQueueFetch            = "req.queue.fetch"
	attrRequestScanFetch             = "req.queue.scan"
	attrRequestCommitFetch           = "req.queue.commit"
	attrRequestQueueConcurrencyFetch = "req.queue.fetch.concurrency"
	attrRequestQueueJoinGroup        = "req.queue.join.group"
	attrRequestSet                   = "req.kv.set"
	attrRequestSetIf                 = "req.kv.setif"
	attrRequestGet                   = "req.kv.get"
	attrRequestScan                  = "req.kv.scan"
	attrRequestDelete                = "req.kv.delete"
	attrRequestDeleteIf              = "req.kv.deleteif"
	attrRequestUpdateProfile         = "req.profile.update"
	attrRequestUpdateMapping         = "req.mapping.update"
	attrRequestBMCreate              = "req.bm.create"
	attrRequestBMAdd                 = "req.bm.add"
	attrRequestBMRemove              = "req.bm.remove"
	attrRequestBMClear               = "req.bm.clear"
	attrRequestBMContains            = "req.bm.contains"
	attrRequestBMCount               = "req.bm.count"
	attrRequestBMRange               = "req.bm.range"
	attrRequestAllocID               = "req.id.alloc"
	attrRequestResetID               = "req.id.reset"
	attrRequestWorkflowStart         = "req.wf.start"
	attrRequestWorkflowStarted       = "req.wf.started"
	attrRequestWorkflowStop          = "req.wf.stop"
	attrRequestWorkflowStopped       = "req.wf.stopped"
	attrRequestWorkflowUpdate        = "req.wf.update"
	attrRequestWorkflowShardCreate   = "req.wf.shard.create"
	attrRequestWorkflowShardUpdate   = "req.wf.shard.update"
	attrRequestWorkflowShardRemove   = "req.wf.shard.remove"

	attrResponseBool           = "resp.bool"
	attrResponseBytes          = "resp.bytes"
	attrResponseUint64         = "resp.uint64"
	attrResponseUint32Range    = "resp.uint32.range"
	attrResponseUint32Slice    = "resp.uint32.slice"
	attrResponseBytesSlice     = "resp.bytes.slice"
	attrResponseFetch          = "resp.queue.fetch"
	attrResponseQueueJoinGroup = "resp.queue.join.group"

	attrTempBM               = "temp.BM"
	attrTempWorkflowInstance = "temp.wf.instance"

	attrQueueWriteBatchKey = "queue.wb"
	attrQueueStates        = "queue.states"
)

func getTempWorkflowInstance(attrs map[string]interface{}) *metapb.WorkflowInstance {
	var value *metapb.WorkflowInstance

	if v, ok := attrs[attrTempWorkflowInstance]; ok {
		value = v.(*metapb.WorkflowInstance)
	} else {
		value = &metapb.WorkflowInstance{}
		attrs[attrTempWorkflowInstance] = value
	}

	value.Reset()
	return value
}

func getQueueFetchResponse(attrs map[string]interface{}) *rpcpb.QueueFetchResponse {
	var value *rpcpb.QueueFetchResponse

	if v, ok := attrs[attrResponseFetch]; ok {
		value = v.(*rpcpb.QueueFetchResponse)
	} else {
		value = &rpcpb.QueueFetchResponse{}
		attrs[attrResponseFetch] = value
	}

	value.Reset()
	return value
}

func getCreateInstanceStateShardRequest(attrs map[string]interface{}) *rpcpb.CreateInstanceStateShardRequest {
	var value *rpcpb.CreateInstanceStateShardRequest

	if v, ok := attrs[attrRequestWorkflowShardCreate]; ok {
		value = v.(*rpcpb.CreateInstanceStateShardRequest)
	} else {
		value = &rpcpb.CreateInstanceStateShardRequest{}
		attrs[attrRequestWorkflowShardCreate] = value
	}

	value.Reset()
	return value
}

func getRemoveInstanceStateShardRequest(attrs map[string]interface{}) *rpcpb.RemoveInstanceStateShardRequest {
	var value *rpcpb.RemoveInstanceStateShardRequest

	if v, ok := attrs[attrRequestWorkflowShardRemove]; ok {
		value = v.(*rpcpb.RemoveInstanceStateShardRequest)
	} else {
		value = &rpcpb.RemoveInstanceStateShardRequest{}
		attrs[attrRequestWorkflowShardRemove] = value
	}

	value.Reset()
	return value
}

func getUpdateInstanceStateShardRequest(attrs map[string]interface{}) *rpcpb.UpdateInstanceStateShardRequest {
	var value *rpcpb.UpdateInstanceStateShardRequest

	if v, ok := attrs[attrRequestWorkflowShardUpdate]; ok {
		value = v.(*rpcpb.UpdateInstanceStateShardRequest)
	} else {
		value = &rpcpb.UpdateInstanceStateShardRequest{}
		attrs[attrRequestWorkflowShardUpdate] = value
	}

	value.Reset()
	return value
}

func getUpdateWorkflowRequest(attrs map[string]interface{}) *rpcpb.UpdateWorkflowRequest {
	var value *rpcpb.UpdateWorkflowRequest

	if v, ok := attrs[attrRequestWorkflowUpdate]; ok {
		value = v.(*rpcpb.UpdateWorkflowRequest)
	} else {
		value = &rpcpb.UpdateWorkflowRequest{}
		attrs[attrRequestWorkflowUpdate] = value
	}

	value.Reset()
	return value
}

func getStartedInstanceRequest(attrs map[string]interface{}) *rpcpb.StartedInstanceRequest {
	var value *rpcpb.StartedInstanceRequest

	if v, ok := attrs[attrRequestWorkflowStarted]; ok {
		value = v.(*rpcpb.StartedInstanceRequest)
	} else {
		value = &rpcpb.StartedInstanceRequest{}
		attrs[attrRequestWorkflowStarted] = value
	}

	value.Reset()
	return value
}

func getStopInstanceRequest(attrs map[string]interface{}) *rpcpb.StopInstanceRequest {
	var value *rpcpb.StopInstanceRequest

	if v, ok := attrs[attrRequestWorkflowStop]; ok {
		value = v.(*rpcpb.StopInstanceRequest)
	} else {
		value = &rpcpb.StopInstanceRequest{}
		attrs[attrRequestWorkflowStop] = value
	}

	value.Reset()
	return value
}

func getStoppedInstanceRequest(attrs map[string]interface{}) *rpcpb.StoppedInstanceRequest {
	var value *rpcpb.StoppedInstanceRequest

	if v, ok := attrs[attrRequestWorkflowStopped]; ok {
		value = v.(*rpcpb.StoppedInstanceRequest)
	} else {
		value = &rpcpb.StoppedInstanceRequest{}
		attrs[attrRequestWorkflowStopped] = value
	}

	value.Reset()
	return value
}

func getStartingInstanceRequest(attrs map[string]interface{}) *rpcpb.StartingInstanceRequest {
	var value *rpcpb.StartingInstanceRequest

	if v, ok := attrs[attrRequestWorkflowStart]; ok {
		value = v.(*rpcpb.StartingInstanceRequest)
	} else {
		value = &rpcpb.StartingInstanceRequest{}
		attrs[attrRequestWorkflowStart] = value
	}

	value.Reset()
	return value
}

func getUint32SliceResponse(attrs map[string]interface{}) *rpcpb.Uint32SliceResponse {
	var value *rpcpb.Uint32SliceResponse

	if v, ok := attrs[attrResponseUint32Slice]; ok {
		value = v.(*rpcpb.Uint32SliceResponse)
	} else {
		value = &rpcpb.Uint32SliceResponse{}
		attrs[attrResponseUint32Slice] = value
	}

	value.Reset()
	return value
}

func getUint32RangeResponse(attrs map[string]interface{}) *rpcpb.Uint32RangeResponse {
	var value *rpcpb.Uint32RangeResponse

	if v, ok := attrs[attrResponseUint32Range]; ok {
		value = v.(*rpcpb.Uint32RangeResponse)
	} else {
		value = &rpcpb.Uint32RangeResponse{}
		attrs[attrResponseUint32Range] = value
	}

	value.Reset()
	return value
}

func getBoolResponse(attrs map[string]interface{}) *rpcpb.BoolResponse {
	var value *rpcpb.BoolResponse

	if v, ok := attrs[attrResponseBool]; ok {
		value = v.(*rpcpb.BoolResponse)
	} else {
		value = &rpcpb.BoolResponse{}
		attrs[attrResponseBool] = value
	}

	value.Reset()
	return value
}

func getBytesResponse(attrs map[string]interface{}) *rpcpb.BytesResponse {
	var value *rpcpb.BytesResponse

	if v, ok := attrs[attrResponseBytes]; ok {
		value = v.(*rpcpb.BytesResponse)
	} else {
		value = &rpcpb.BytesResponse{}
		attrs[attrResponseBytes] = value
	}

	value.Reset()
	return value
}

func getBytesSliceResponse(attrs map[string]interface{}) *rpcpb.BytesSliceResponse {
	var value *rpcpb.BytesSliceResponse

	if v, ok := attrs[attrResponseBytesSlice]; ok {
		value = v.(*rpcpb.BytesSliceResponse)
	} else {
		value = &rpcpb.BytesSliceResponse{}
		attrs[attrResponseBytesSlice] = value
	}

	value.Reset()
	return value
}

func getUpdateMappingRequest(attrs map[string]interface{}) *rpcpb.UpdateMappingRequest {
	var value *rpcpb.UpdateMappingRequest

	if v, ok := attrs[attrRequestUpdateMapping]; ok {
		value = v.(*rpcpb.UpdateMappingRequest)
	} else {
		value = &rpcpb.UpdateMappingRequest{}
		attrs[attrRequestUpdateMapping] = value
	}

	value.Reset()
	return value
}

func getUpdateProfileRequest(attrs map[string]interface{}) *rpcpb.UpdateProfileRequest {
	var value *rpcpb.UpdateProfileRequest

	if v, ok := attrs[attrRequestUpdateProfile]; ok {
		value = v.(*rpcpb.UpdateProfileRequest)
	} else {
		value = &rpcpb.UpdateProfileRequest{}
		attrs[attrRequestUpdateProfile] = value
	}

	value.Reset()
	return value
}

func getScanRequest(attrs map[string]interface{}) *rpcpb.ScanRequest {
	var value *rpcpb.ScanRequest

	if v, ok := attrs[attrRequestScan]; ok {
		value = v.(*rpcpb.ScanRequest)
	} else {
		value = &rpcpb.ScanRequest{}
		attrs[attrRequestScan] = value
	}

	value.Reset()
	return value
}

func getGetRequest(attrs map[string]interface{}) *rpcpb.GetRequest {
	var value *rpcpb.GetRequest

	if v, ok := attrs[attrRequestGet]; ok {
		value = v.(*rpcpb.GetRequest)
	} else {
		value = &rpcpb.GetRequest{}
		attrs[attrRequestGet] = value
	}

	value.Reset()
	return value
}

func getResetIDRequest(attrs map[string]interface{}) *rpcpb.ResetIDRequest {
	var value *rpcpb.ResetIDRequest

	if v, ok := attrs[attrRequestResetID]; ok {
		value = v.(*rpcpb.ResetIDRequest)
	} else {
		value = &rpcpb.ResetIDRequest{}
		attrs[attrRequestResetID] = value
	}

	value.Reset()
	return value
}

func getAllocIDRequest(attrs map[string]interface{}) *rpcpb.AllocIDRequest {
	var value *rpcpb.AllocIDRequest

	if v, ok := attrs[attrRequestAllocID]; ok {
		value = v.(*rpcpb.AllocIDRequest)
	} else {
		value = &rpcpb.AllocIDRequest{}
		attrs[attrRequestAllocID] = value
	}

	value.Reset()
	return value
}

func getTempBM(attrs map[string]interface{}) *roaring.Bitmap {
	var value *roaring.Bitmap

	if v, ok := attrs[attrTempBM]; ok {
		value = v.(*roaring.Bitmap)
	} else {
		value = util.AcquireBitmap()
		attrs[attrTempBM] = value
	}

	value.Clear()
	return value
}

func getBMCountRequest(attrs map[string]interface{}) *rpcpb.BMCountRequest {
	var value *rpcpb.BMCountRequest

	if v, ok := attrs[attrRequestBMCount]; ok {
		value = v.(*rpcpb.BMCountRequest)
	} else {
		value = &rpcpb.BMCountRequest{}
		attrs[attrRequestBMCount] = value
	}

	value.Reset()
	return value
}

func getBMRangeRequest(attrs map[string]interface{}) *rpcpb.BMRangeRequest {
	var value *rpcpb.BMRangeRequest

	if v, ok := attrs[attrRequestBMRange]; ok {
		value = v.(*rpcpb.BMRangeRequest)
	} else {
		value = &rpcpb.BMRangeRequest{}
		attrs[attrRequestBMRange] = value
	}

	value.Reset()
	return value
}

func getBMContainsRequest(attrs map[string]interface{}) *rpcpb.BMContainsRequest {
	var value *rpcpb.BMContainsRequest

	if v, ok := attrs[attrRequestBMContains]; ok {
		value = v.(*rpcpb.BMContainsRequest)
	} else {
		value = &rpcpb.BMContainsRequest{}
		attrs[attrRequestBMContains] = value
	}

	value.Reset()
	return value
}

func getBMClearRequest(attrs map[string]interface{}) *rpcpb.BMClearRequest {
	var value *rpcpb.BMClearRequest

	if v, ok := attrs[attrRequestBMClear]; ok {
		value = v.(*rpcpb.BMClearRequest)
	} else {
		value = &rpcpb.BMClearRequest{}
		attrs[attrRequestBMClear] = value
	}

	value.Reset()
	return value
}

func getBMRemoveRequest(attrs map[string]interface{}) *rpcpb.BMRemoveRequest {
	var value *rpcpb.BMRemoveRequest

	if v, ok := attrs[attrRequestBMRemove]; ok {
		value = v.(*rpcpb.BMRemoveRequest)
	} else {
		value = &rpcpb.BMRemoveRequest{}
		attrs[attrRequestBMRemove] = value
	}

	value.Reset()
	return value
}

func getBMAddRequest(attrs map[string]interface{}) *rpcpb.BMAddRequest {
	var value *rpcpb.BMAddRequest

	if v, ok := attrs[attrRequestBMAdd]; ok {
		value = v.(*rpcpb.BMAddRequest)
	} else {
		value = &rpcpb.BMAddRequest{}
		attrs[attrRequestBMAdd] = value
	}

	value.Reset()
	return value
}

func getBMCreateRequest(attrs map[string]interface{}) *rpcpb.BMCreateRequest {
	var value *rpcpb.BMCreateRequest

	if v, ok := attrs[attrRequestBMCreate]; ok {
		value = v.(*rpcpb.BMCreateRequest)
	} else {
		value = &rpcpb.BMCreateRequest{}
		attrs[attrRequestBMCreate] = value
	}

	value.Reset()
	return value
}

func getSetRequest(attrs map[string]interface{}) *rpcpb.SetRequest {
	var value *rpcpb.SetRequest

	if v, ok := attrs[attrRequestSet]; ok {
		value = v.(*rpcpb.SetRequest)
	} else {
		value = &rpcpb.SetRequest{}
		attrs[attrRequestSet] = value
	}

	value.Reset()
	return value
}

func getSetIfRequest(attrs map[string]interface{}) *rpcpb.SetIfRequest {
	var value *rpcpb.SetIfRequest

	if v, ok := attrs[attrRequestSetIf]; ok {
		value = v.(*rpcpb.SetIfRequest)
	} else {
		value = &rpcpb.SetIfRequest{}
		attrs[attrRequestSetIf] = value
	}

	value.Reset()
	return value
}

func getDeleteIfRequest(attrs map[string]interface{}) *rpcpb.DeleteIfRequest {
	var value *rpcpb.DeleteIfRequest

	if v, ok := attrs[attrRequestDeleteIf]; ok {
		value = v.(*rpcpb.DeleteIfRequest)
	} else {
		value = &rpcpb.DeleteIfRequest{}
		attrs[attrRequestDeleteIf] = value
	}

	value.Reset()
	return value
}

func getDeleteRequest(attrs map[string]interface{}) *rpcpb.DeleteRequest {
	var value *rpcpb.DeleteRequest

	if v, ok := attrs[attrRequestDelete]; ok {
		value = v.(*rpcpb.DeleteRequest)
	} else {
		value = &rpcpb.DeleteRequest{}
		attrs[attrRequestDelete] = value
	}

	value.Reset()
	return value
}

func getQueueJoinGroupRequest(attrs map[string]interface{}) *rpcpb.QueueJoinGroupRequest {
	var value *rpcpb.QueueJoinGroupRequest

	if v, ok := attrs[attrRequestQueueJoinGroup]; ok {
		value = v.(*rpcpb.QueueJoinGroupRequest)
	} else {
		value = &rpcpb.QueueJoinGroupRequest{}
		attrs[attrRequestQueueJoinGroup] = value
	}

	value.Reset()
	return value
}

func getQueueJoinGroupResponse(attrs map[string]interface{}) *rpcpb.QueueJoinGroupResponse {
	var value *rpcpb.QueueJoinGroupResponse

	if v, ok := attrs[attrResponseQueueJoinGroup]; ok {
		value = v.(*rpcpb.QueueJoinGroupResponse)
	} else {
		value = &rpcpb.QueueJoinGroupResponse{}
		attrs[attrResponseQueueJoinGroup] = value
	}

	value.Reset()
	return value
}

func getQueueFetchRequest(attrs map[string]interface{}) *rpcpb.QueueFetchRequest {
	var value *rpcpb.QueueFetchRequest

	if v, ok := attrs[attrRequestQueueFetch]; ok {
		value = v.(*rpcpb.QueueFetchRequest)
	} else {
		value = &rpcpb.QueueFetchRequest{}
		attrs[attrRequestQueueFetch] = value
	}

	value.Reset()
	return value
}

func getQueueScanRequest(attrs map[string]interface{}) *rpcpb.QueueScanRequest {
	var value *rpcpb.QueueScanRequest

	if v, ok := attrs[attrRequestScanFetch]; ok {
		value = v.(*rpcpb.QueueScanRequest)
	} else {
		value = &rpcpb.QueueScanRequest{}
		attrs[attrRequestScanFetch] = value
	}

	value.Reset()
	return value
}

func getQueueCommitRequest(attrs map[string]interface{}) *rpcpb.QueueCommitRequest {
	var value *rpcpb.QueueCommitRequest

	if v, ok := attrs[attrRequestCommitFetch]; ok {
		value = v.(*rpcpb.QueueCommitRequest)
	} else {
		value = &rpcpb.QueueCommitRequest{}
		attrs[attrRequestCommitFetch] = value
	}

	value.Reset()
	return value
}

func getQueueAddRequest(attrs map[string]interface{}) *rpcpb.QueueAddRequest {
	var value *rpcpb.QueueAddRequest

	if v, ok := attrs[attrRequestQueueAdd]; ok {
		value = v.(*rpcpb.QueueAddRequest)
	} else {
		value = &rpcpb.QueueAddRequest{}
		attrs[attrRequestQueueAdd] = value
	}

	value.Reset()
	return value
}

func getUint64Response(attrs map[string]interface{}) *rpcpb.Uint64Response {
	var value *rpcpb.Uint64Response

	if v, ok := attrs[attrResponseUint64]; ok {
		value = v.(*rpcpb.Uint64Response)
	} else {
		value = &rpcpb.Uint64Response{}
		attrs[attrResponseUint64] = value
	}

	value.Reset()
	return value
}
