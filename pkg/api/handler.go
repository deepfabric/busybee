package api

import (
	"fmt"

	"github.com/deepfabric/beehive/util"
	"github.com/deepfabric/busybee/pkg/metric"
	"github.com/deepfabric/busybee/pkg/pb/metapb"
	"github.com/deepfabric/busybee/pkg/pb/rpcpb"
	"github.com/deepfabric/busybee/pkg/storage"
	"github.com/fagongzi/log"
	"github.com/fagongzi/util/protoc"
)

type ctx struct {
	sid interface{}
	req *rpcpb.Request
}

func (s *server) onReq(sid interface{}, req *rpcpb.Request) error {
	ctx := ctx{sid: sid, req: req}

	switch req.Type {
	case rpcpb.Set:
		return s.doSet(ctx)
	case rpcpb.Get:
		return s.doGet(ctx)
	case rpcpb.Delete:
		return s.doDelete(ctx)
	case rpcpb.Scan:
		return s.doScanKey(ctx)
	case rpcpb.BMCreate:
		return s.doBMCreate(ctx)
	case rpcpb.BMAdd:
		return s.doBMAdd(ctx)
	case rpcpb.BMRemove:
		return s.doBMRemove(ctx)
	case rpcpb.BMClear:
		return s.doBMClear(ctx)
	case rpcpb.BMRange:
		return s.doBMRange(ctx)
	case rpcpb.BMCount:
		return s.doBMCount(ctx)
	case rpcpb.BMContains:
		return s.doBMContains(ctx)
	case rpcpb.TenantInit:
		return s.doTenantInit(ctx)
	case rpcpb.StartingInstance:
		return s.doStartInstance(ctx)
	case rpcpb.LastInstance:
		return s.doLastInstance(ctx)
	case rpcpb.HistoryInstance:
		return s.doHistoryInstance(ctx)
	case rpcpb.UpdateCrowd:
		return s.doUpdateCrowd(ctx)
	case rpcpb.UpdateWorkflow:
		return s.doUpdateWorkflow(ctx)
	case rpcpb.StopInstance:
		return s.doStopInstance(ctx)
	case rpcpb.InstanceCountState:
		return s.doCountInstance(ctx)
	case rpcpb.InstanceCrowdState:
		return s.doCrowdInstance(ctx)
	case rpcpb.UpdateMapping:
		return s.doUpdateMapping(ctx)
	case rpcpb.GetMapping:
		return s.doGetMapping(ctx)
	case rpcpb.UpdateProfile:
		return s.doUpdateProfile(ctx)
	case rpcpb.ScanMapping:
		return s.doScanMapping(ctx)
	case rpcpb.GetIDSet:
		return s.doGetIDSet(ctx)
	case rpcpb.GetProfile:
		return s.doGetProfile(ctx)
	case rpcpb.AddEvent:
		return s.doAddEvent(ctx)
	case rpcpb.FetchNotify:
		return s.doFetchNotify(ctx)
	case rpcpb.AllocID:
		return s.doAllocID(ctx)
	case rpcpb.ResetID:
		return s.doResetID(ctx)
	}

	return fmt.Errorf("not support type %d", req.Type)
}

func (s *server) doSet(ctx ctx) error {
	s.engine.Storage().AsyncExecCommand(&ctx.req.Set, s.onResp, ctx)
	return nil
}

func (s *server) doGet(ctx ctx) error {
	s.engine.Storage().AsyncExecCommand(&ctx.req.Get, s.onResp, ctx)
	return nil
}

func (s *server) doDelete(ctx ctx) error {
	s.engine.Storage().AsyncExecCommand(&ctx.req.Delete, s.onResp, ctx)
	return nil
}

func (s *server) doBMCreate(ctx ctx) error {
	s.engine.Storage().AsyncExecCommand(&ctx.req.BmCreate, s.onResp, ctx)
	return nil
}

func (s *server) doBMAdd(ctx ctx) error {
	s.engine.Storage().AsyncExecCommand(&ctx.req.BmAdd, s.onResp, ctx)
	return nil
}

func (s *server) doBMRemove(ctx ctx) error {
	s.engine.Storage().AsyncExecCommand(&ctx.req.BmRemove, s.onResp, ctx)
	return nil
}

func (s *server) doBMClear(ctx ctx) error {
	s.engine.Storage().AsyncExecCommand(&ctx.req.BmClear, s.onResp, ctx)
	return nil
}

func (s *server) doBMCount(ctx ctx) error {
	s.engine.Storage().AsyncExecCommand(&ctx.req.BmCount, s.onResp, ctx)
	return nil
}

func (s *server) doBMContains(ctx ctx) error {
	s.engine.Storage().AsyncExecCommand(&ctx.req.BmContains, s.onResp, ctx)
	return nil
}

func (s *server) doTenantInit(ctx ctx) error {
	err := s.engine.TenantInit(ctx.req.TenantInit.ID,
		ctx.req.TenantInit.InputQueuePartitions)
	if err != nil {
		return err
	}

	s.onResp(ctx, nil, nil)
	return nil
}

func (s *server) doLastInstance(ctx ctx) error {
	value, err := s.engine.LastInstance(ctx.req.LastInstance.WorkflowID)
	if err != nil {
		return err
	}

	if value == nil {
		s.onResp(ctx, nil, nil)
	} else {
		s.onResp(ctx, protoc.MustMarshal(value), nil)
	}

	return nil
}

func (s *server) doHistoryInstance(ctx ctx) error {
	value, err := s.engine.HistoryInstance(ctx.req.HistoryInstance.WorkflowID,
		ctx.req.HistoryInstance.InstanceID)
	if err != nil {
		return err
	}

	if value == nil {
		s.onResp(ctx, nil, nil)
	} else {
		s.onResp(ctx, protoc.MustMarshal(value), nil)
	}

	return nil
}

func (s *server) doStartInstance(ctx ctx) error {
	_, err := s.engine.StartInstance(ctx.req.StartInstance.Instance.Snapshot,
		ctx.req.StartInstance.Instance.Loader,
		ctx.req.StartInstance.Instance.LoaderMeta,
		ctx.req.StartInstance.Instance.Workers)
	if err != nil {
		return err
	}

	s.onResp(ctx, nil, nil)
	return nil
}

func (s *server) doUpdateWorkflow(ctx ctx) error {
	err := s.engine.UpdateWorkflow(ctx.req.UpdateWorkflow.Workflow)
	if err != nil {
		return err
	}

	s.onResp(ctx, nil, nil)
	return nil
}

func (s *server) doUpdateCrowd(ctx ctx) error {
	err := s.engine.UpdateCrowd(ctx.req.UpdateCrowd.ID,
		ctx.req.UpdateCrowd.Loader,
		ctx.req.UpdateCrowd.LoaderMeta)
	if err != nil {
		return err
	}

	s.onResp(ctx, nil, nil)
	return nil
}

func (s *server) doStopInstance(ctx ctx) error {
	err := s.engine.StopInstance(ctx.req.StopInstance.WorkflowID)
	if err != nil {
		return err
	}

	s.onResp(ctx, nil, nil)
	return nil
}

func (s *server) doCountInstance(ctx ctx) error {
	state, err := s.engine.InstanceCountState(ctx.req.CountInstance.WorkflowID)
	if err != nil {
		return err
	}

	s.onResp(ctx, protoc.MustMarshal(&state), nil)
	return nil
}

func (s *server) doCrowdInstance(ctx ctx) error {
	state, err := s.engine.InstanceStepState(ctx.req.CrowdInstance.WorkflowID,
		ctx.req.CrowdInstance.Name)
	if err != nil {
		return err
	}

	s.onResp(ctx, protoc.MustMarshal(&state), nil)
	return nil
}

func (s *server) doUpdateMapping(ctx ctx) error {
	err := s.engine.Service().UpdateMapping(&ctx.req.UpdateMapping)
	if err != nil {
		return err
	}

	s.onResp(ctx, nil, nil)
	return nil
}

func (s *server) doGetMapping(ctx ctx) error {
	value, err := s.engine.Service().GetIDValue(ctx.req.GetMapping.ID,
		ctx.req.GetMapping.From, ctx.req.GetMapping.To)
	if err != nil {
		return err
	}

	s.onResp(ctx, value, nil)
	return nil
}

func (s *server) doScanKey(ctx ctx) error {
	s.engine.Storage().AsyncExecCommandWithGroup(&ctx.req.Scan, ctx.req.Scan.Group,
		s.onResp, ctx)
	return nil
}

func (s *server) doScanMapping(ctx ctx) error {
	scanReq := rpcpb.AcquireScanRequest()
	scanReq.Start = storage.MappingIDKey(ctx.req.ScanMapping.ID, ctx.req.ScanMapping.From)
	scanReq.End = storage.MappingIDKey(ctx.req.ScanMapping.ID, ctx.req.ScanMapping.To)
	scanReq.Limit = ctx.req.ScanMapping.Limit

	s.engine.Storage().AsyncExecCommand(scanReq, s.onResp, ctx)
	return nil
}

func (s *server) doGetIDSet(ctx ctx) error {
	ctx.req.Get.Key = storage.MappingIDKey(ctx.req.GetIDSet.ID, ctx.req.GetIDSet.UserID)
	s.engine.Storage().AsyncExecCommand(&ctx.req.Get, s.onResp, ctx)
	return nil
}

func (s *server) doUpdateProfile(ctx ctx) error {
	err := s.engine.Service().UpdateProfile(ctx.req.UpdateProfile.ID,
		ctx.req.UpdateProfile.UserID,
		ctx.req.UpdateProfile.Value)
	if err != nil {
		return err
	}

	s.onResp(ctx, nil, nil)
	return nil
}

func (s *server) doGetProfile(ctx ctx) error {
	value, err := s.engine.Service().GetProfileField(ctx.req.GetProfile.ID,
		ctx.req.GetProfile.UserID,
		ctx.req.GetProfile.Field)
	if err != nil {
		return err
	}

	s.onResp(ctx, value, nil)
	return nil
}

func (s *server) doAddEvent(ctx ctx) error {
	value, ok := s.tenantQueues.Load(ctx.req.AddEvent.Event.TenantID)
	if !ok {
		tq := newTenantQueue(ctx.req.AddEvent.Event.TenantID, s.engine, s.onResp)
		value, ok = s.tenantQueues.LoadOrStore(ctx.req.AddEvent.Event.TenantID, tq)
		if !ok {
			err := tq.start()
			if err != nil {
				s.onResp(ctx, nil, err)
				return nil
			}
			value = tq
		} else {
			tq.stop()
		}
	}

	value.(*tenantQueue).add(ctx)
	return nil
}

func (s *server) doFetchNotify(ctx ctx) error {
	req := rpcpb.AcquireQueueFetchRequest()
	req.Key = storage.PartitionKey(ctx.req.FetchNotify.ID, 0)
	req.CompletedOffset = ctx.req.FetchNotify.CompletedOffset
	req.Count = ctx.req.FetchNotify.Count
	req.Concurrency = ctx.req.FetchNotify.Concurrency
	req.Consumer = []byte(ctx.req.FetchNotify.Consumer)
	s.engine.Storage().AsyncExecCommandWithGroup(req, metapb.TenantOutputGroup, s.onResp, ctx)
	return nil
}

func (s *server) doAllocID(ctx ctx) error {
	s.engine.Storage().AsyncExecCommand(&ctx.req.AllocID, s.onResp, ctx)
	return nil
}

func (s *server) doResetID(ctx ctx) error {
	s.engine.Storage().AsyncExecCommand(&ctx.req.ResetID, s.onResp, ctx)
	return nil
}

func (s *server) doBMRange(ctx ctx) error {
	s.engine.Storage().AsyncExecCommand(&ctx.req.BmRange, s.onResp, ctx)
	return nil
}

func (s *server) onResp(arg interface{}, value []byte, err error) {
	ctx := arg.(ctx)
	if log.DebugEnabled() {
		log.Debugf("%d api received response", ctx.req.ID)
	}
	if rs, ok := s.sessions.Load(ctx.sid); ok {
		resp := rpcpb.AcquireResponse()
		resp.Type = ctx.req.Type
		resp.ID = ctx.req.ID
		if err != nil {
			metric.IncRequestFailed(resp.Type.String())
			resp.Error.Error = err.Error()
			rs.(*util.Session).OnResp(resp)
			return
		}

		switch ctx.req.Type {
		case rpcpb.Set, rpcpb.Delete, rpcpb.BMCreate, rpcpb.BMAdd,
			rpcpb.BMRemove, rpcpb.BMClear, rpcpb.StartingInstance,
			rpcpb.UpdateCrowd, rpcpb.UpdateWorkflow, rpcpb.StopInstance,
			rpcpb.UpdateMapping, rpcpb.UpdateProfile, rpcpb.AddEvent,
			rpcpb.ResetID:
			// empty response
		case rpcpb.Get, rpcpb.GetIDSet:
			protoc.MustUnmarshal(&resp.BytesResp, value)
		case rpcpb.BMRange:
			protoc.MustUnmarshal(&resp.Uint32SliceResp, value)
		case rpcpb.BMCount:
			protoc.MustUnmarshal(&resp.Uint64Resp, value)
		case rpcpb.BMContains:
			protoc.MustUnmarshal(&resp.BoolResp, value)
		case rpcpb.InstanceCountState, rpcpb.InstanceCrowdState,
			rpcpb.GetMapping, rpcpb.GetProfile, rpcpb.LastInstance,
			rpcpb.HistoryInstance:
			resp.BytesResp.Value = value
		case rpcpb.FetchNotify, rpcpb.Scan, rpcpb.ScanMapping:
			protoc.MustUnmarshal(&resp.BytesSliceResp, value)
		case rpcpb.AllocID:
			protoc.MustUnmarshal(&resp.Uint32RangeResp, value)
		}

		rs.(*util.Session).OnResp(resp)
		metric.IncRequestSucceed(resp.Type.String())
	} else {
		if log.DebugEnabled() {
			log.Debugf("%d api received response, missing session", ctx.req.ID)
		}
	}
}
