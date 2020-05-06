package core

import (
	"fmt"
	"math"
	"testing"
	"time"

	"github.com/RoaringBitmap/roaring"
	"github.com/deepfabric/busybee/pkg/notify"
	"github.com/deepfabric/busybee/pkg/pb/metapb"
	"github.com/deepfabric/busybee/pkg/pb/rpcpb"
	"github.com/deepfabric/busybee/pkg/storage"
	"github.com/deepfabric/busybee/pkg/util"
	"github.com/deepfabric/prophet"
	"github.com/fagongzi/goetty"
	"github.com/fagongzi/util/protoc"
	"github.com/stretchr/testify/assert"
)

func TestStart(t *testing.T) {
	store, deferFunc := storage.NewTestStorage(t, false)
	defer deferFunc()

	ng, err := NewEngine(store, notify.NewQueueBasedNotifier(store))
	assert.NoError(t, err, "TestStart failed")
	assert.NoError(t, ng.Start(), "TestStart failed")
	defer ng.Stop()
}

func TestStop(t *testing.T) {
	store, deferFunc := storage.NewTestStorage(t, false)
	defer deferFunc()

	ng, err := NewEngine(store, notify.NewQueueBasedNotifier(store))
	assert.NoError(t, err, "TestStop failed")
	assert.NoError(t, ng.Start(), "TestStop failed")
	assert.NoError(t, ng.Stop(), "TestStop failed")

	defer ng.Stop()
}

func TestTenantInit(t *testing.T) {
	store, deferFunc := storage.NewTestStorage(t, false)
	defer deferFunc()

	ng, err := NewEngine(store, notify.NewQueueBasedNotifier(store))
	assert.NoError(t, err, "TestTenantInit failed")
	assert.NoError(t, ng.Start(), "TestTenantInit failed")
	defer ng.Stop()

	tid := uint64(10001)
	err = ng.(*engine).tenantInitWithReplicas(metapb.Tenant{
		ID: tid,
		Input: metapb.TenantQueue{
			Partitions:      2,
			ConsumerTimeout: 60,
		},
		Output: metapb.TenantQueue{
			Partitions:      2,
			ConsumerTimeout: 60,
		},
	}, 1)
	assert.NoError(t, err, "TestTenantInit failed")
	time.Sleep(time.Second * 2)
	c := 0
	err = store.RaftStore().Prophet().GetStore().LoadResources(16, func(res prophet.Resource) {
		c++
	})
	assert.Equal(t, 5, c, "TestTenantInit failed")

	buf := goetty.NewByteBuf(32)
	data, err := store.GetWithGroup(storage.QueueMetaKey(tid, 0, buf), metapb.TenantOutputGroup)
	assert.NoError(t, err, "TestTenantInit failed")
	assert.NotEmpty(t, data, "TestTenantInit failed")
}

func TestStopInstanceAndRestart(t *testing.T) {
	store, deferFunc := storage.NewTestStorage(t, false)
	defer deferFunc()

	ng, err := NewEngine(store, notify.NewQueueBasedNotifier(store))
	assert.NoError(t, err, "TestStopInstance failed")
	assert.NoError(t, ng.Start(), "TestStopInstance failed")
	defer ng.Stop()

	err = ng.(*engine).tenantInitWithReplicas(metapb.Tenant{
		ID: 10001,
		Input: metapb.TenantQueue{
			Partitions:      2,
			ConsumerTimeout: 60,
		},
		Output: metapb.TenantQueue{
			Partitions:      1,
			ConsumerTimeout: 60,
		},
	}, 1)
	assert.NoError(t, err, "TestStopInstance failed")
	time.Sleep(time.Second * 2)

	for i := 0; i < 2; i++ {
		bm := roaring.BitmapOf(1, 2, 3, 4)
		if i == 1 {
			bm = roaring.BitmapOf(1, 2, 3, 4, 5, 6, 7, 8, 9)
		}

		_, err = ng.StartInstance(metapb.Workflow{
			ID:       10000,
			TenantID: 10001,
			Name:     "test_wf",
			Steps: []metapb.Step{
				{
					Name: "step_end_1",
					Execution: metapb.Execution{
						Type:   metapb.Direct,
						Direct: &metapb.DirectExecution{},
					},
				},
			},
		}, metapb.RawLoader, util.MustMarshalBM(bm))

		assert.NoError(t, waitTestWorkflow(ng, 10000, metapb.Running), "TestStopInstance failed")

		err = ng.StopInstance(10000)
		assert.NoError(t, err, "TestStopInstance failed")

		assert.NoError(t, waitTestWorkflow(ng, 10000, metapb.Stopped), "TestStopInstance failed")

		assert.Equal(t, 0, ng.(*engine).workerCount(), "TestStopInstance failed")

		value, err := ng.Storage().Get(storage.WorkflowCurrentInstanceKey(10000))
		assert.NoError(t, err, "TestStopInstance failed")

		instance := &metapb.WorkflowInstance{}
		protoc.MustUnmarshal(instance, value)
		assert.Equal(t, metapb.Stopped, instance.State, "TestStopInstance failed")
		assert.True(t, instance.StoppedAt > 0, "TestStopInstance failed")
		shards, err := ng.(*engine).getInstanceWorkers(instance)
		assert.NoError(t, err, "TestStopInstance failed")
		assert.Empty(t, shards, "TestStopInstance failed")
		if i == 1 {
			bm, err := ng.(*engine).loadBM(instance.Loader, instance.LoaderMeta)
			assert.NoError(t, err, "TestStopInstance failed")

			assert.Equal(t, uint32(5), bm.Minimum(), "TestStopInstance failed")
			assert.Equal(t, uint32(9), bm.Maximum(), "TestStopInstance failed")
		}

		buf := goetty.NewByteBuf(32)
		value, err = ng.Storage().Get(storage.WorkflowHistoryInstanceKey(instance.Snapshot.ID, instance.InstanceID, buf))
		assert.NoError(t, err, "TestStopInstance failed")
		assert.NotEmpty(t, value, "TestStopInstance failed")
	}
}

func TestStartInstance(t *testing.T) {
	store, deferFunc := storage.NewTestStorage(t, false)
	defer deferFunc()

	ng, err := NewEngine(store, notify.NewQueueBasedNotifier(store))
	assert.NoError(t, err, "TestStartInstance failed")
	assert.NoError(t, ng.Start(), "TestStartInstance failed")
	defer ng.Stop()

	tid := uint64(10001)
	wid := uint64(10000)

	err = ng.(*engine).tenantInitWithReplicas(metapb.Tenant{
		ID: tid,
		Input: metapb.TenantQueue{
			Partitions:      2,
			ConsumerTimeout: 60,
		},
		Output: metapb.TenantQueue{
			Partitions:      1,
			ConsumerTimeout: 60,
		},
	}, 1)
	assert.NoError(t, err, "TestStartInstance failed")
	time.Sleep(time.Second * 2)

	bm := roaring.BitmapOf(1, 2, 3, 4)
	_, err = ng.StartInstance(metapb.Workflow{
		ID:       wid,
		TenantID: tid,
		StopAt:   time.Now().Add(time.Second * 10).Unix(),
		Name:     "test_wf",
		Steps: []metapb.Step{
			{
				Name: "step_start",
				Execution: metapb.Execution{
					Type: metapb.Branch,
					Branches: []metapb.ConditionExecution{
						{
							Condition: metapb.Expr{
								Value: []byte("{num: event.uid} == 1"),
							},
							NextStep: "step_end_1",
						},
						{
							Condition: metapb.Expr{
								Value: []byte("1 == 1"),
							},
							NextStep: "step_end_else",
						},
					},
				},
			},
			{
				Name: "step_end_1",
				Execution: metapb.Execution{
					Type:   metapb.Direct,
					Direct: &metapb.DirectExecution{},
				},
			},
			{
				Name: "step_end_else",
				Execution: metapb.Execution{
					Type:   metapb.Direct,
					Direct: &metapb.DirectExecution{},
				},
			},
		},
	}, metapb.RawLoader, util.MustMarshalBM(bm))
	assert.NoError(t, err, "TestStartInstance failed")

	assert.NoError(t, waitTestWorkflow(ng, wid, metapb.Running), "TestStartInstance failed")

	for i := uint32(0); i < 1; i++ {
		data, err := store.GetWithGroup(storage.TenantRunnerWorkerKey(tid, 0, wid, i), metapb.TenantRunnerGroup)
		assert.NoError(t, err, "TestStartInstance failed")
		assert.NotEmpty(t, data, "TestStartInstance failed")
	}

	data, err := store.GetWithGroup(storage.TenantRunnerMetadataKey(tid, 0), metapb.TenantRunnerGroup)
	assert.NoError(t, err, "TestStartInstance failed")
	assert.NotEmpty(t, data, "TestStartInstance failed")
	metadata := &metapb.WorkerRunner{}
	protoc.MustUnmarshal(metadata, data)
	assert.Equal(t, 1, len(metadata.Workers), "TestStartInstance failed")

	err = ng.Storage().PutToQueue(10001, 0, metapb.TenantInputGroup, protoc.MustMarshal(&metapb.Event{
		Type: metapb.UserType,
		User: &metapb.UserEvent{
			TenantID: 10001,
			UserID:   1,
			Data: []metapb.KV{
				{
					Key:   []byte("uid"),
					Value: []byte("1"),
				},
			},
		},
	}))
	assert.NoError(t, err, "TestStartInstance failed")

	time.Sleep(time.Second)

	buf := goetty.NewByteBuf(32)
	req := rpcpb.AcquireScanRequest()
	req.Start = storage.QueueItemKey(storage.PartitionKey(10001, 0), 1, buf)
	req.End = storage.QueueItemKey(storage.PartitionKey(10001, 0), 100, buf)
	req.Limit = 100
	value, err := store.ExecCommandWithGroup(req, metapb.TenantOutputGroup)
	assert.NoError(t, err, "TestTriggerDirect failed")
	scanResp := rpcpb.AcquireBytesSliceResponse()
	protoc.MustUnmarshal(scanResp, value)
	assert.Equal(t, 1, len(scanResp.Keys), "TestStartInstance failed")

	states, err := ng.InstanceCountState(10000)
	assert.NoError(t, err, "TestStartInstance failed")
	m := make(map[string]uint64)
	for _, state := range states.States {
		m[state.Step] = state.Count
	}
	assert.Equal(t, uint64(3), m["step_start"], "TestStartInstance failed")
	assert.Equal(t, uint64(1), m["step_end_1"], "TestStartInstance failed")
	assert.Equal(t, uint64(0), m["step_end_else"], "TestStartInstance failed")

	state, err := ng.InstanceStepState(10000, "step_start")
	assert.NoError(t, err, "TestStartInstance failed")

	bm, err = ng.(*engine).loadBM(state.Loader, state.LoaderMeta)
	assert.NoError(t, err, "TestStartInstance failed")
	assert.Equal(t, uint64(3), bm.GetCardinality(), "TestStartInstance failed")

	time.Sleep(time.Second * 9)

	for i := uint32(0); i < 3; i++ {
		data, err := store.GetWithGroup(storage.TenantRunnerWorkerKey(tid, 0, wid, i), metapb.TenantRunnerGroup)
		assert.NoError(t, err, "TestStartInstance failed")
		assert.Empty(t, data, "TestStartInstance failed")
	}

	data, err = store.GetWithGroup(storage.TenantRunnerMetadataKey(tid, 0), metapb.TenantRunnerGroup)
	assert.NoError(t, err, "TestStartInstance failed")
	assert.NotEmpty(t, data, "TestStartInstance failed")
	metadata = &metapb.WorkerRunner{}
	protoc.MustUnmarshal(metadata, data)
	assert.Equal(t, 0, len(metadata.Workers), "TestStartInstance failed")
}

func TestTriggerDirect(t *testing.T) {
	store, deferFunc := storage.NewTestStorage(t, false)
	defer deferFunc()

	ng, err := NewEngine(store, notify.NewQueueBasedNotifier(store))
	assert.NoError(t, err, "TestTriggerDirect failed")
	assert.NoError(t, ng.Start(), "TestTriggerDirect failed")
	defer ng.Stop()

	err = ng.(*engine).tenantInitWithReplicas(metapb.Tenant{
		ID: 10001,
		Input: metapb.TenantQueue{
			Partitions:      2,
			ConsumerTimeout: 60,
		},
		Output: metapb.TenantQueue{
			Partitions:      1,
			ConsumerTimeout: 60,
		},
	}, 1)
	assert.NoError(t, err, "TestTriggerDirect failed")
	time.Sleep(time.Second * 2)

	bm := roaring.BitmapOf(1, 2, 3, 4)
	_, err = ng.StartInstance(metapb.Workflow{
		ID:       10000,
		TenantID: 10001,
		StopAt:   time.Now().Add(time.Second * 10).Unix(),
		Name:     "test_wf",
		Steps: []metapb.Step{
			{
				Name: "step_start",
				Execution: metapb.Execution{
					Type: metapb.Branch,
					Branches: []metapb.ConditionExecution{
						{
							Condition: metapb.Expr{
								Value: []byte("{num: event.move} == 1"),
							},
							NextStep: "next_1",
						},
						{
							Condition: metapb.Expr{
								Value: []byte("1 == 1"),
							},
							NextStep: "end",
						},
					},
				},
			},
			{
				Name: "next_1",
				Execution: metapb.Execution{
					Type: metapb.Direct,
					Direct: &metapb.DirectExecution{
						NextStep: "next_2",
					},
				},
			},
			{
				Name: "next_2",
				Execution: metapb.Execution{
					Type: metapb.Direct,
					Direct: &metapb.DirectExecution{
						NextStep: "end",
					},
				},
			},
			{
				Name: "end",
				Execution: metapb.Execution{
					Type:   metapb.Direct,
					Direct: &metapb.DirectExecution{},
				},
			},
		},
	}, metapb.RawLoader, util.MustMarshalBM(bm))
	assert.NoError(t, err, "TestTriggerDirect failed")
	assert.NoError(t, waitTestWorkflow(ng, 10000, metapb.Running), "TestTriggerDirect failed")

	err = ng.Storage().PutToQueue(10001, 0, metapb.TenantInputGroup, protoc.MustMarshal(&metapb.Event{
		Type: metapb.UserType,
		User: &metapb.UserEvent{
			TenantID: 10001,
			UserID:   1,
			Data: []metapb.KV{
				{
					Key:   []byte("move"),
					Value: []byte("1"),
				},
			},
		},
	}))
	assert.NoError(t, err, "TestTriggerDirect failed")
	time.Sleep(time.Second)

	buf := goetty.NewByteBuf(32)
	req := rpcpb.AcquireScanRequest()
	req.Start = storage.QueueItemKey(storage.PartitionKey(10001, 0), 1, buf)
	req.End = storage.QueueItemKey(storage.PartitionKey(10001, 0), 100, buf)
	req.Limit = 100
	value, err := store.ExecCommandWithGroup(req, metapb.TenantOutputGroup)
	assert.NoError(t, err, "TestTriggerDirect failed")
	scanResp := rpcpb.AcquireBytesSliceResponse()
	protoc.MustUnmarshal(scanResp, value)
	assert.Equal(t, 3, len(scanResp.Keys), "TestTriggerDirect failed")

	states, err := ng.InstanceCountState(10000)
	assert.NoError(t, err, "TestTriggerDirect failed")
	m := make(map[string]uint64)
	for _, state := range states.States {
		m[state.Step] = state.Count
	}
	assert.Equal(t, uint64(3), m["step_start"], "TestTriggerDirect failed")
	assert.Equal(t, uint64(0), m["next1"], "TestTriggerDirect failed")
	assert.Equal(t, uint64(0), m["next2"], "TestTriggerDirect failed")
	assert.Equal(t, uint64(1), m["end"], "TestTriggerDirect failed")

	state, err := ng.InstanceStepState(10000, "step_start")
	assert.NoError(t, err, "TestTriggerDirect failed")
	bm, err = ng.(*engine).loadBM(state.Loader, state.LoaderMeta)
	assert.NoError(t, err, "TestTriggerDirect failed")
	assert.Equal(t, uint64(3), bm.GetCardinality(), "TestStartInstance failed")

	state, err = ng.InstanceStepState(10000, "next1")
	assert.NoError(t, err, "TestTriggerDirect failed")
	bm, err = ng.(*engine).loadBM(state.Loader, state.LoaderMeta)
	assert.NoError(t, err, "TestTriggerDirect failed")
	assert.Equal(t, uint64(0), bm.GetCardinality(), "TestStartInstance failed")

	state, err = ng.InstanceStepState(10000, "next2")
	assert.NoError(t, err, "TestTriggerDirect failed")
	bm, err = ng.(*engine).loadBM(state.Loader, state.LoaderMeta)
	assert.NoError(t, err, "TestTriggerDirect failed")
	assert.Equal(t, uint64(0), bm.GetCardinality(), "TestStartInstance failed")

	state, err = ng.InstanceStepState(10000, "end")
	assert.NoError(t, err, "TestTriggerDirect failed")
	bm, err = ng.(*engine).loadBM(state.Loader, state.LoaderMeta)
	assert.NoError(t, err, "TestTriggerDirect failed")
	assert.Equal(t, uint64(1), bm.GetCardinality(), "TestStartInstance failed")
}

func TestUpdateCrowd(t *testing.T) {
	store, deferFunc := storage.NewTestStorage(t, false)
	defer deferFunc()

	tid := uint64(10001)
	wid := uint64(10000)

	ng, err := NewEngine(store, notify.NewQueueBasedNotifier(store))
	assert.NoError(t, err, "TestUpdateCrowd failed")
	assert.NoError(t, ng.Start(), "TestUpdateCrowd failed")
	defer ng.Stop()

	err = ng.(*engine).tenantInitWithReplicas(metapb.Tenant{
		ID: tid,
		Input: metapb.TenantQueue{
			Partitions:      2,
			ConsumerTimeout: 60,
		},
		Output: metapb.TenantQueue{
			Partitions:      1,
			ConsumerTimeout: 60,
		},
	}, 1)
	assert.NoError(t, err, "TestUpdateCrowd failed")
	time.Sleep(time.Second * 2)

	bm := roaring.BitmapOf(2, 3, 4)
	_, err = ng.StartInstance(metapb.Workflow{
		ID:       wid,
		TenantID: tid,
		Name:     "test_wf",
		Steps: []metapb.Step{
			{
				Name: "step_start",
				Execution: metapb.Execution{
					Type: metapb.Branch,
					Branches: []metapb.ConditionExecution{
						{
							Condition: metapb.Expr{
								Value: []byte("{num: event.uid} == 1"),
							},
							NextStep: "step_end_1",
						},
						{
							Condition: metapb.Expr{
								Value: []byte("1 == 1"),
							},
							NextStep: "step_end_else",
						},
					},
				},
			},
			{
				Name: "step_end_1",
				Execution: metapb.Execution{
					Type:   metapb.Direct,
					Direct: &metapb.DirectExecution{},
				},
			},
			{
				Name: "step_end_else",
				Execution: metapb.Execution{
					Type:   metapb.Direct,
					Direct: &metapb.DirectExecution{},
				},
			},
		},
	}, metapb.RawLoader, util.MustMarshalBM(bm))
	assert.NoError(t, err, "TestUpdateCrowd failed")

	assert.NoError(t, waitTestWorkflow(ng, 10000, metapb.Running), "TestUpdateCrowd failed")

	err = ng.Storage().PutToQueue(tid, 0, metapb.TenantInputGroup, protoc.MustMarshal(&metapb.Event{
		Type: metapb.UserType,
		User: &metapb.UserEvent{
			TenantID: tid,
			UserID:   2,
			Data: []metapb.KV{
				{
					Key:   []byte("uid"),
					Value: []byte("2"),
				},
			},
		},
	}), protoc.MustMarshal(&metapb.Event{
		Type: metapb.UserType,
		User: &metapb.UserEvent{
			TenantID: tid,
			UserID:   3,
			Data: []metapb.KV{
				{
					Key:   []byte("uid"),
					Value: []byte("3"),
				},
			},
		},
	}), protoc.MustMarshal(&metapb.Event{
		Type: metapb.UserType,
		User: &metapb.UserEvent{
			TenantID: tid,
			UserID:   4,
			Data: []metapb.KV{
				{
					Key:   []byte("uid"),
					Value: []byte("4"),
				},
			},
		},
	}))
	assert.NoError(t, err, "TestStartInstance failed")

	time.Sleep(time.Second * 2)

	states, err := ng.InstanceCountState(wid)
	assert.NoError(t, err, "TestUpdateCrowd failed")
	m := make(map[string]uint64)
	for _, state := range states.States {
		m[state.Step] = state.Count
	}
	assert.Equal(t, uint64(0), m["step_start"], "TestUpdateCrowd failed")
	assert.Equal(t, uint64(0), m["step_end_1"], "TestUpdateCrowd failed")
	assert.Equal(t, uint64(3), m["step_end_else"], "TestUpdateCrowd failed")

	// 2,3,4 -> 1,2,3,5
	err = ng.UpdateCrowd(wid, metapb.RawLoader, util.MustMarshalBM(roaring.BitmapOf(1, 2, 3, 5)))
	assert.NoError(t, err, "TestUpdateCrowd failed")

	time.Sleep(time.Second * 2)

	err = ng.Storage().PutToQueue(tid, 0, metapb.TenantInputGroup, protoc.MustMarshal(&metapb.Event{
		Type: metapb.UserType,
		User: &metapb.UserEvent{
			TenantID: tid,
			UserID:   1,
			Data: []metapb.KV{
				{
					Key:   []byte("uid"),
					Value: []byte("1"),
				},
			},
		},
	}))
	assert.NoError(t, err, "TestUpdateCrowd failed")

	time.Sleep(time.Second * 2)

	states, err = ng.InstanceCountState(10000)
	assert.NoError(t, err, "TestUpdateCrowd failed")
	m = make(map[string]uint64)
	for _, state := range states.States {
		m[state.Step] = state.Count
	}
	assert.Equal(t, uint64(1), m["step_start"], "TestUpdateCrowd failed")
	assert.Equal(t, uint64(1), m["step_end_1"], "TestUpdateCrowd failed")
	assert.Equal(t, uint64(2), m["step_end_else"], "TestUpdateCrowd failed")
}

func TestUpdateWorkflow(t *testing.T) {
	store, deferFunc := storage.NewTestStorage(t, false)
	defer deferFunc()

	tid := uint64(10001)
	wid := uint64(10000)

	ng, err := NewEngine(store, notify.NewQueueBasedNotifier(store))
	assert.NoError(t, err, "TestUpdateWorkflow failed")
	assert.NoError(t, ng.Start(), "TestUpdateWorkflow failed")
	defer ng.Stop()

	err = ng.(*engine).tenantInitWithReplicas(metapb.Tenant{
		ID: tid,
		Input: metapb.TenantQueue{
			Partitions:      2,
			ConsumerTimeout: 60,
		},
		Output: metapb.TenantQueue{
			Partitions:      1,
			ConsumerTimeout: 60,
		},
	}, 1)
	assert.NoError(t, err, "TestUpdateWorkflow failed")
	time.Sleep(time.Second * 2)

	bm := roaring.BitmapOf(1, 2, 3, 4)
	_, err = ng.StartInstance(metapb.Workflow{
		ID:       wid,
		TenantID: tid,
		Name:     "test_wf",
		Steps: []metapb.Step{
			{
				Name: "step_start",
				Execution: metapb.Execution{
					Type: metapb.Branch,
					Branches: []metapb.ConditionExecution{
						{
							Condition: metapb.Expr{
								Value: []byte("{num: event.uid} == 1"),
							},
							NextStep: "step_end_1",
						},
						{
							Condition: metapb.Expr{
								Value: []byte("{num: event.uid} == 2"),
							},
							NextStep: "step_end_2",
						},
						{
							Condition: metapb.Expr{
								Value: []byte("{num: event.uid} == 3"),
							},
							NextStep: "step_end_3",
						},
						{
							Condition: metapb.Expr{
								Value: []byte("{num: event.uid} == 4"),
							},
							NextStep: "step_end_4",
						},
						{
							Condition: metapb.Expr{
								Value: []byte("1 == 1"),
							},
							NextStep: "step_end_else",
						},
					},
				},
			},
			{
				Name: "step_end_1",
				Execution: metapb.Execution{
					Type:   metapb.Direct,
					Direct: &metapb.DirectExecution{},
				},
			},
			{
				Name: "step_end_2",
				Execution: metapb.Execution{
					Type:   metapb.Direct,
					Direct: &metapb.DirectExecution{},
				},
			},
			{
				Name: "step_end_3",
				Execution: metapb.Execution{
					Type:   metapb.Direct,
					Direct: &metapb.DirectExecution{},
				},
			},
			{
				Name: "step_end_4",
				Execution: metapb.Execution{
					Type:   metapb.Direct,
					Direct: &metapb.DirectExecution{},
				},
			},
			{
				Name: "step_end_else",
				Execution: metapb.Execution{
					Type:   metapb.Direct,
					Direct: &metapb.DirectExecution{},
				},
			},
		},
	}, metapb.RawLoader, util.MustMarshalBM(bm))
	assert.NoError(t, err, "TestUpdateWorkflow failed")

	assert.NoError(t, waitTestWorkflow(ng, 10000, metapb.Running), "TestUpdateWorkflow failed")

	err = ng.Storage().PutToQueue(tid, 0, metapb.TenantInputGroup, protoc.MustMarshal(&metapb.Event{
		Type: metapb.UserType,
		User: &metapb.UserEvent{
			TenantID: tid,
			UserID:   1,
			Data: []metapb.KV{
				{
					Key:   []byte("uid"),
					Value: []byte("1"),
				},
			},
		},
	}), protoc.MustMarshal(&metapb.Event{
		Type: metapb.UserType,
		User: &metapb.UserEvent{
			TenantID: tid,
			UserID:   2,
			Data: []metapb.KV{
				{
					Key:   []byte("uid"),
					Value: []byte("2"),
				},
			},
		},
	}), protoc.MustMarshal(&metapb.Event{
		Type: metapb.UserType,
		User: &metapb.UserEvent{
			TenantID: tid,
			UserID:   3,
			Data: []metapb.KV{
				{
					Key:   []byte("uid"),
					Value: []byte("3"),
				},
			},
		},
	}))
	assert.NoError(t, err, "TestUpdateWorkflow failed")

	time.Sleep(time.Second * 2)

	states, err := ng.InstanceCountState(wid)
	assert.NoError(t, err, "TestUpdateWorkflow failed")
	m := make(map[string]uint64)
	for _, state := range states.States {
		m[state.Step] = state.Count
	}
	assert.Equal(t, uint64(1), m["step_start"], "TestUpdateWorkflow failed")
	assert.Equal(t, uint64(1), m["step_end_1"], "TestUpdateWorkflow failed")
	assert.Equal(t, uint64(1), m["step_end_2"], "TestUpdateWorkflow failed")
	assert.Equal(t, uint64(1), m["step_end_3"], "TestUpdateWorkflow failed")
	assert.Equal(t, uint64(0), m["step_end_4"], "TestUpdateWorkflow failed")
	assert.Equal(t, uint64(0), m["step_end_else"], "TestUpdateWorkflow failed")

	err = ng.UpdateCrowd(wid, metapb.RawLoader, util.MustMarshalBM(roaring.BitmapOf(1, 2, 3, 5)))
	assert.NoError(t, err, "TestUpdateCrowd failed")

	err = ng.UpdateWorkflow(metapb.Workflow{
		ID:       wid,
		TenantID: tid,
		Name:     "test_wf",
		Steps: []metapb.Step{
			{
				Name: "step_start",
				Execution: metapb.Execution{
					Type: metapb.Branch,
					Branches: []metapb.ConditionExecution{
						{
							Condition: metapb.Expr{
								Value: []byte("{num: event.uid} == 1"),
							},
							NextStep: "step_end_1",
						},
						{
							Condition: metapb.Expr{
								Value: []byte("{num: event.uid} == 5"),
							},
							NextStep: "step_end_5",
						},
						{
							Condition: metapb.Expr{
								Value: []byte("{num: event.uid} == 3"),
							},
							NextStep: "step_end_3",
						},
						{
							Condition: metapb.Expr{
								Value: []byte("{num: event.uid} == 4"),
							},
							NextStep: "step_end_4",
						},
						{
							Condition: metapb.Expr{
								Value: []byte("1 == 1"),
							},
							NextStep: "step_end_else",
						},
					},
				},
			},
			{
				Name: "step_end_1",
				Execution: metapb.Execution{
					Type:   metapb.Direct,
					Direct: &metapb.DirectExecution{},
				},
			},
			{
				Name: "step_end_5",
				Execution: metapb.Execution{
					Type:   metapb.Direct,
					Direct: &metapb.DirectExecution{},
				},
			},
			{
				Name: "step_end_3",
				Execution: metapb.Execution{
					Type:   metapb.Direct,
					Direct: &metapb.DirectExecution{},
				},
			},
			{
				Name: "step_end_4",
				Execution: metapb.Execution{
					Type:   metapb.Direct,
					Direct: &metapb.DirectExecution{},
				},
			},
			{
				Name: "step_end_else",
				Execution: metapb.Execution{
					Type:   metapb.Direct,
					Direct: &metapb.DirectExecution{},
				},
			},
		},
	})
	assert.NoError(t, err, "TestUpdateCrowd failed")

	time.Sleep(time.Second * 2)

	states, err = ng.InstanceCountState(wid)
	assert.NoError(t, err, "TestUpdateCrowd failed")
	m = make(map[string]uint64)
	for _, state := range states.States {
		m[state.Step] = state.Count
	}

	assert.Equal(t, uint64(1), m["step_start"], "TestUpdateWorkflow failed")
	assert.Equal(t, uint64(1), m["step_end_1"], "TestUpdateWorkflow failed")
	assert.Equal(t, uint64(0), m["step_end_5"], "TestUpdateWorkflow failed")
	assert.Equal(t, uint64(1), m["step_end_3"], "TestUpdateWorkflow failed")
	assert.Equal(t, uint64(0), m["step_end_4"], "TestUpdateWorkflow failed")
	assert.Equal(t, uint64(0), m["step_end_else"], "TestUpdateWorkflow failed")
}

func TestTransaction(t *testing.T) {
	store, deferFunc := storage.NewTestStorage(t, false)
	defer deferFunc()

	tid := uint64(10001)
	wid := uint64(10000)

	ng, err := NewEngine(store, notify.NewQueueBasedNotifier(store))
	assert.NoError(t, err, "TestTransaction failed")
	assert.NoError(t, ng.Start(), "TestTransaction failed")
	defer ng.Stop()

	err = ng.(*engine).tenantInitWithReplicas(metapb.Tenant{
		ID: tid,
		Input: metapb.TenantQueue{
			Partitions:      2,
			ConsumerTimeout: 60,
		},
		Output: metapb.TenantQueue{
			Partitions:      1,
			ConsumerTimeout: 60,
		},
	}, 1)
	assert.NoError(t, err, "TestTransaction failed")
	time.Sleep(time.Second * 2)

	bm := roaring.BitmapOf(1, 2)
	_, err = ng.StartInstance(metapb.Workflow{
		ID:       wid,
		TenantID: tid,
		Name:     "test_wf",
		Steps: []metapb.Step{
			{
				Name: "step_start",
				Execution: metapb.Execution{
					Type: metapb.Branch,
					Branches: []metapb.ConditionExecution{
						{
							Condition: metapb.Expr{
								Value: []byte("{num: event.uid} == 1"),
							},
							NextStep: "step_end_1",
						},
						{
							Condition: metapb.Expr{
								Value: []byte("{num: kv.uid} == 2"),
							},
							NextStep: "step_end_2",
						},
						{
							Condition: metapb.Expr{
								Value: []byte("1 == 1"),
							},
							NextStep: "step_end_else",
						},
					},
				},
			},
			{
				Name: "step_end_1",
				Execution: metapb.Execution{
					Type:   metapb.Direct,
					Direct: &metapb.DirectExecution{},
				},
			},
			{
				Name: "step_end_2",
				Execution: metapb.Execution{
					Type:   metapb.Direct,
					Direct: &metapb.DirectExecution{},
				},
			},
			{
				Name: "step_end_else",
				Execution: metapb.Execution{
					Type:   metapb.Direct,
					Direct: &metapb.DirectExecution{},
				},
			},
		},
	}, metapb.RawLoader, util.MustMarshalBM(bm))
	assert.NoError(t, err, "TestTransaction failed")
	assert.NoError(t, waitTestWorkflow(ng, 10000, metapb.Running), "TestTransaction failed")

	changed := make(chan interface{})
	ng.Storage().Set([]byte("uid"), []byte("abc"))
	go func() {
		time.Sleep(time.Second * 3)
		ng.Storage().Set([]byte("uid"), []byte("2"))
		changed <- 0
	}()

	err = ng.Storage().PutToQueue(tid, 0, metapb.TenantInputGroup, protoc.MustMarshal(&metapb.Event{
		Type: metapb.UserType,
		User: &metapb.UserEvent{
			TenantID: tid,
			UserID:   1,
			Data: []metapb.KV{
				{
					Key:   []byte("uid"),
					Value: []byte("1"),
				},
			},
		},
	}), protoc.MustMarshal(&metapb.Event{
		Type: metapb.UpdateCrowdType,
		UpdateCrowd: &metapb.UpdateCrowdEvent{
			WorkflowID: wid,
			Index:      0,
			Crowd:      util.MustMarshalBM(roaring.BitmapOf(1, 2)),
		},
	}), protoc.MustMarshal(&metapb.Event{
		Type: metapb.UpdateCrowdType,
		UpdateCrowd: &metapb.UpdateCrowdEvent{
			WorkflowID: wid,
			Index:      1,
			Crowd:      util.MustMarshalBM(roaring.BitmapOf(1, 2)),
		},
	}), protoc.MustMarshal(&metapb.Event{
		Type: metapb.UpdateCrowdType,
		UpdateCrowd: &metapb.UpdateCrowdEvent{
			WorkflowID: wid,
			Index:      2,
			Crowd:      util.MustMarshalBM(roaring.BitmapOf(1, 2)),
		},
	}), protoc.MustMarshal(&metapb.Event{
		Type: metapb.UserType,
		User: &metapb.UserEvent{
			TenantID: tid,
			UserID:   2,
		},
	}))
	assert.NoError(t, err, "TestTransaction failed")
	time.Sleep(time.Second * 1)

	states, err := ng.InstanceCountState(wid)
	assert.NoError(t, err, "TestTransaction failed")
	m := make(map[string]uint64)
	for _, state := range states.States {
		m[state.Step] = state.Count
	}
	assert.Equal(t, uint64(1), m["step_start"], "TestTransaction failed")
	assert.Equal(t, uint64(1), m["step_end_1"], "TestTransaction failed")
	assert.Equal(t, uint64(0), m["step_end_2"], "TestTransaction failed")
	assert.Equal(t, uint64(0), m["step_end_else"], "TestTransaction failed")

	<-changed
	time.Sleep(time.Second)
	states, err = ng.InstanceCountState(wid)
	assert.NoError(t, err, "TestTransaction failed")
	m = make(map[string]uint64)
	for _, state := range states.States {
		m[state.Step] = state.Count
	}
	assert.Equal(t, uint64(0), m["step_start"], "TestTransaction failed")
	assert.Equal(t, uint64(1), m["step_end_1"], "TestTransaction failed")
	assert.Equal(t, uint64(1), m["step_end_2"], "TestTransaction failed")
	assert.Equal(t, uint64(0), m["step_end_else"], "TestTransaction failed")
}

func TestTimerWithUseStepCrowdToDrive(t *testing.T) {
	store, deferFunc := storage.NewTestStorage(t, false)
	defer deferFunc()

	tid := uint64(10001)
	wid := uint64(10000)

	ng, err := NewEngine(store, notify.NewQueueBasedNotifier(store))
	assert.NoError(t, err, "TestTimerWithUseStepCrowdToDrive failed")
	assert.NoError(t, ng.Start(), "TestTimerWithUseStepCrowdToDrive failed")
	defer ng.Stop()

	err = ng.(*engine).tenantInitWithReplicas(metapb.Tenant{
		ID: tid,
		Input: metapb.TenantQueue{
			Partitions:      2,
			ConsumerTimeout: 60,
		},
		Output: metapb.TenantQueue{
			Partitions:      1,
			ConsumerTimeout: 60,
		},
	}, 1)
	assert.NoError(t, err, "TestTimerWithUseStepCrowdToDrive failed")
	time.Sleep(time.Second * 2)

	bm := roaring.BitmapOf(1, 2)
	_, err = ng.StartInstance(metapb.Workflow{
		ID:       wid,
		TenantID: tid,
		Name:     "test_wf",
		Steps: []metapb.Step{
			{
				Name: "step_start",
				Execution: metapb.Execution{
					Type: metapb.Timer,
					Timer: &metapb.TimerExecution{
						Cron:                "*/1 * * * * ?",
						UseStepCrowdToDrive: true,
						NextStep:            "step_end",
					},
				},
			},
			{
				Name: "step_end",
				Execution: metapb.Execution{
					Type:   metapb.Direct,
					Direct: &metapb.DirectExecution{},
				},
			},
		},
	}, metapb.RawLoader, util.MustMarshalBM(bm))
	assert.NoError(t, err, "TestTimerWithUseStepCrowdToDrive failed")
	assert.NoError(t, waitTestWorkflow(ng, 10000, metapb.Running), "TestTimerWithUseStepCrowdToDrive failed")

	time.Sleep(time.Second * 2)
	states, err := ng.InstanceCountState(wid)
	assert.NoError(t, err, "TestTimerWithUseStepCrowdToDrive failed")
	m := make(map[string]uint64)
	for _, state := range states.States {
		m[state.Step] = state.Count
	}
	assert.Equal(t, uint64(0), m["step_start"], "TestTimerWithUseStepCrowdToDrive failed")
	assert.Equal(t, uint64(2), m["step_end"], "TestTimerWithUseStepCrowdToDrive failed")
}

func TestLastTransactionNotCompleted(t *testing.T) {
	store, deferFunc := storage.NewTestStorage(t, false)
	defer deferFunc()

	tid := uint64(10001)
	wid := uint64(10000)

	ng, err := NewEngine(store, notify.NewQueueBasedNotifier(store))
	assert.NoError(t, err, "TestLastTransaction failed")
	assert.NoError(t, ng.Start(), "TestLastTransaction failed")
	defer ng.Stop()

	err = ng.(*engine).tenantInitWithReplicas(metapb.Tenant{
		ID: tid,
		Input: metapb.TenantQueue{
			Partitions:      2,
			ConsumerTimeout: 60,
		},
		Output: metapb.TenantQueue{
			Partitions:      1,
			ConsumerTimeout: 60,
		},
	}, 1)
	assert.NoError(t, err, "TestLastTransaction failed")
	time.Sleep(time.Second * 2)

	bm := roaring.BitmapOf(1)
	instanceID, err := ng.StartInstance(metapb.Workflow{
		ID:       wid,
		TenantID: tid,
		Name:     "test_wf",
		Steps: []metapb.Step{
			{
				Name: "step_start",
				Execution: metapb.Execution{
					Type: metapb.Branch,
					Branches: []metapb.ConditionExecution{
						{
							Condition: metapb.Expr{
								Value: []byte("{num: event.uid} == 1"),
							},
							NextStep: "step_end",
						},
						{
							Condition: metapb.Expr{
								Value: []byte("1 == 1"),
							},
							NextStep: "step_end",
						},
					},
				},
			},
			{
				Name: "step_end",
				Execution: metapb.Execution{
					Type:   metapb.Direct,
					Direct: &metapb.DirectExecution{},
				},
			},
		},
	}, metapb.RawLoader, util.MustMarshalBM(bm))
	assert.NoError(t, err, "TestLastTransaction failed")

	key := make([]byte, 12, 12)
	goetty.Uint64ToBytesTo(instanceID, key)
	goetty.Uint32ToBytesTo(0, key[8:])
	_, err = store.ExecCommandWithGroup(&rpcpb.SetRequest{
		Key: storage.QueueKVKey(tid, key),
		Value: protoc.MustMarshal(&metapb.WorkflowInstanceWorkerState{
			Version:    10,
			TenantID:   tid,
			WorkflowID: wid,
			InstanceID: instanceID,
			Index:      0,
			States: []metapb.StepState{
				{
					Step: metapb.Step{
						Name: "step_start",
						Execution: metapb.Execution{
							Type: metapb.Branch,
							Branches: []metapb.ConditionExecution{
								{
									Condition: metapb.Expr{
										Value: []byte("{num: event.uid} == 1"),
									},
									NextStep: "step_end",
								},
								{
									Condition: metapb.Expr{
										Value: []byte("1 == 1"),
									},
									NextStep: "step_end",
								},
							},
						},
					},
					TotalCrowd: 0,
					Loader:     metapb.RawLoader,
					LoaderMeta: emptyBMData.Bytes(),
				},
				{
					Step: metapb.Step{
						Name: "step_end",
						Execution: metapb.Execution{
							Type:   metapb.Direct,
							Direct: &metapb.DirectExecution{},
						},
					},
					TotalCrowd: 1,
					Loader:     metapb.RawLoader,
					LoaderMeta: util.MustMarshalBM(roaring.BitmapOf(1)),
				},
			},
		}),
	}, metapb.TenantOutputGroup)
	assert.NoError(t, err, "TestLastTransaction failed")

	assert.NoError(t, waitTestWorkflow(ng, wid, metapb.Running), "TestLastTransaction failed")

	states, err := ng.InstanceCountState(wid)
	assert.NoError(t, err, "TestLastTransaction failed")
	m := make(map[string]uint64)
	for _, state := range states.States {
		m[state.Step] = state.Count
	}
	assert.Equal(t, uint64(0), m["step_start"], "TestLastTransaction failed")
	assert.Equal(t, uint64(1), m["step_end"], "TestLastTransaction failed")
}

func TestLastTransactionCompleted(t *testing.T) {
	store, deferFunc := storage.NewTestStorage(t, false)
	defer deferFunc()

	tid := uint64(10001)
	wid := uint64(10000)

	ng, err := NewEngine(store, notify.NewQueueBasedNotifier(store))
	assert.NoError(t, err, "TestLastTransactionCompleted failed")
	assert.NoError(t, ng.Start(), "TestLastTransactionCompleted failed")
	defer ng.Stop()

	err = ng.(*engine).tenantInitWithReplicas(metapb.Tenant{
		ID: tid,
		Input: metapb.TenantQueue{
			Partitions:      2,
			ConsumerTimeout: 60,
		},
		Output: metapb.TenantQueue{
			Partitions:      1,
			ConsumerTimeout: 60,
		},
	}, 1)
	assert.NoError(t, err, "TestLastTransactionCompleted failed")
	time.Sleep(time.Second * 2)

	bm := roaring.BitmapOf(1)
	instanceID, err := ng.StartInstance(metapb.Workflow{
		ID:       wid,
		TenantID: tid,
		Name:     "test_wf",
		Steps: []metapb.Step{
			{
				Name: "step_start",
				Execution: metapb.Execution{
					Type: metapb.Branch,
					Branches: []metapb.ConditionExecution{
						{
							Condition: metapb.Expr{
								Value: []byte("{num: event.uid} == 1"),
							},
							NextStep: "step_end",
						},
						{
							Condition: metapb.Expr{
								Value: []byte("1 == 1"),
							},
							NextStep: "step_end",
						},
					},
				},
			},
			{
				Name: "step_end",
				Execution: metapb.Execution{
					Type:   metapb.Direct,
					Direct: &metapb.DirectExecution{},
				},
			},
		},
	}, metapb.RawLoader, util.MustMarshalBM(bm))
	assert.NoError(t, err, "TestLastTransactionCompleted failed")

	key := make([]byte, 12, 12)
	goetty.Uint64ToBytesTo(instanceID, key)
	goetty.Uint32ToBytesTo(0, key[8:])
	_, err = store.ExecCommandWithGroup(&rpcpb.SetRequest{
		Key: storage.QueueKVKey(tid, key),
		Value: protoc.MustMarshal(&metapb.WorkflowInstanceWorkerState{
			Version:    0,
			TenantID:   tid,
			WorkflowID: wid,
			InstanceID: instanceID,
			Index:      0,
			States: []metapb.StepState{
				{
					Step: metapb.Step{
						Name: "step_start",
						Execution: metapb.Execution{
							Type: metapb.Branch,
							Branches: []metapb.ConditionExecution{
								{
									Condition: metapb.Expr{
										Value: []byte("{num: event.uid} == 1"),
									},
									NextStep: "step_end",
								},
								{
									Condition: metapb.Expr{
										Value: []byte("1 == 1"),
									},
									NextStep: "step_end",
								},
							},
						},
					},
					TotalCrowd: 0,
					Loader:     metapb.RawLoader,
					LoaderMeta: emptyBMData.Bytes(),
				},
				{
					Step: metapb.Step{
						Name: "step_end",
						Execution: metapb.Execution{
							Type:   metapb.Direct,
							Direct: &metapb.DirectExecution{},
						},
					},
					TotalCrowd: 1,
					Loader:     metapb.RawLoader,
					LoaderMeta: util.MustMarshalBM(roaring.BitmapOf(1)),
				},
			},
		}),
	}, metapb.TenantOutputGroup)
	assert.NoError(t, err, "TestLastTransactionCompleted failed")

	assert.NoError(t, waitTestWorkflow(ng, wid, metapb.Running), "TestLastTransactionCompleted failed")

	states, err := ng.InstanceCountState(wid)
	assert.NoError(t, err, "TestLastTransactionCompleted failed")
	m := make(map[string]uint64)
	for _, state := range states.States {
		m[state.Step] = state.Count
	}
	assert.Equal(t, uint64(1), m["step_start"], "TestLastTransactionCompleted failed")
	assert.Equal(t, uint64(0), m["step_end"], "TestLastTransactionCompleted failed")
}

func TestTriggerTTLTimeout(t *testing.T) {
	store, deferFunc := storage.NewTestStorage(t, false)
	defer deferFunc()

	ng, err := NewEngine(store, notify.NewQueueBasedNotifier(store))
	assert.NoError(t, err, "TestTriggerTTLTimeout failed")
	assert.NoError(t, ng.Start(), "TestTriggerTTLTimeout failed")
	defer ng.Stop()

	old := ttlTriggerInterval
	ttlTriggerInterval = time.Millisecond * 100
	defer func() {
		ttlTriggerInterval = old
	}()

	err = ng.(*engine).tenantInitWithReplicas(metapb.Tenant{
		ID: 10001,
		Input: metapb.TenantQueue{
			Partitions:      2,
			ConsumerTimeout: 60,
		},
		Output: metapb.TenantQueue{
			Partitions:      1,
			ConsumerTimeout: 60,
		},
	}, 1)
	assert.NoError(t, err, "TestTriggerTTLTimeout failed")
	time.Sleep(time.Second * 2)

	bm := roaring.BitmapOf(1, 2, 3, 4)
	_, err = ng.StartInstance(metapb.Workflow{
		ID:       10000,
		TenantID: 10001,
		Name:     "test_wf",
		Steps: []metapb.Step{
			{
				Name: "step_start",
				TTL:  1,
				Execution: metapb.Execution{
					Type: metapb.Branch,
					Branches: []metapb.ConditionExecution{
						{
							Condition: metapb.Expr{
								Value: []byte("{num: event.uid} == 1"),
							},
							NextStep: "step_end_1",
						},
						{
							Condition: metapb.Expr{
								Value: []byte(`{str: kv.ttl}  != ""`),
							},
							NextStep: "step_end_else",
						},
					},
				},
			},
			{
				Name: "step_end_1",
				Execution: metapb.Execution{
					Type:   metapb.Direct,
					Direct: &metapb.DirectExecution{},
				},
			},
			{
				Name: "step_end_else",
				Execution: metapb.Execution{
					Type:   metapb.Direct,
					Direct: &metapb.DirectExecution{},
				},
			},
		},
	}, metapb.RawLoader, util.MustMarshalBM(bm))
	assert.NoError(t, err, "TestTriggerTTLTimeout failed")

	assert.NoError(t, waitTestWorkflow(ng, 10000, metapb.Running), "TestTriggerTTLTimeout failed")

	err = ng.Storage().PutToQueue(10001, 0, metapb.TenantInputGroup, protoc.MustMarshal(&metapb.Event{
		Type: metapb.UserType,
		User: &metapb.UserEvent{
			TenantID: 10001,
			UserID:   1,
			Data: []metapb.KV{
				{
					Key:   []byte("uid"),
					Value: []byte("1"),
				},
			},
		},
	}))
	assert.NoError(t, err, "TestTriggerTTLTimeout failed")

	time.Sleep(time.Second * 2)
	err = ng.Storage().Set([]byte("ttl"), []byte("1"))
	assert.NoError(t, err, "TestTriggerTTLTimeout failed")
	time.Sleep(time.Second * 2)

	states, err := ng.InstanceCountState(10000)
	assert.NoError(t, err, "TestTriggerTTLTimeout failed")
	m := make(map[string]uint64)
	for _, state := range states.States {
		m[state.Step] = state.Count
	}
	assert.Equal(t, uint64(0), m["step_start"], "TestTriggerTTLTimeout failed")
	assert.Equal(t, uint64(1), m["step_end_1"], "TestTriggerTTLTimeout failed")
	assert.Equal(t, uint64(3), m["step_end_else"], "TestTriggerTTLTimeout failed")
}

type testCheckerNotifier struct {
	notifies []metapb.Notify
}

func (nt *testCheckerNotifier) Notify(id uint64, buf *goetty.ByteBuf, values []metapb.Notify, conds *rpcpb.Condition, kvs ...[]byte) error {
	nt.notifies = append(nt.notifies, values...)
	return nil
}

func TestNotifyWithTTL(t *testing.T) {
	store, deferFunc := storage.NewTestStorage(t, false)
	defer deferFunc()

	nf := &testCheckerNotifier{}
	ng, err := NewEngine(store, nf)
	assert.NoError(t, err, "TestNotifyWithTTL failed")
	assert.NoError(t, ng.Start(), "TestNotifyWithTTL failed")
	defer ng.Stop()

	err = ng.(*engine).tenantInitWithReplicas(metapb.Tenant{
		ID: 10001,
		Input: metapb.TenantQueue{
			Partitions:      2,
			ConsumerTimeout: 60,
		},
		Output: metapb.TenantQueue{
			Partitions:      1,
			ConsumerTimeout: 60,
		},
	}, 1)
	assert.NoError(t, err, "TestNotifyWithTTL failed")
	time.Sleep(time.Second * 2)

	bm := roaring.BitmapOf(1)
	_, err = ng.StartInstance(metapb.Workflow{
		ID:       10000,
		TenantID: 10001,
		Name:     "test_wf",
		Steps: []metapb.Step{
			{
				Name: "step_start",
				Execution: metapb.Execution{
					Type: metapb.Direct,
					Direct: &metapb.DirectExecution{
						NextStep: "step_1",
					},
				},
			},
			{
				Name: "step_1",
				Execution: metapb.Execution{
					Type: metapb.Direct,
					Direct: &metapb.DirectExecution{
						NextStep: "step_2",
					},
				},
			},
			{
				TTL:  10,
				Name: "step_2",
				Execution: metapb.Execution{
					Type:   metapb.Direct,
					Direct: &metapb.DirectExecution{},
				},
			},
		},
	}, metapb.RawLoader, util.MustMarshalBM(bm))
	assert.NoError(t, err, "TestNotifyWithTTL failed")

	assert.NoError(t, waitTestWorkflow(ng, 10000, metapb.Running), "TestNotifyWithTTL failed")

	err = ng.Storage().PutToQueue(10001, 0, metapb.TenantInputGroup, protoc.MustMarshal(&metapb.Event{
		Type: metapb.UserType,
		User: &metapb.UserEvent{
			TenantID: 10001,
			UserID:   1,
			Data: []metapb.KV{
				{
					Key:   []byte("uid"),
					Value: []byte("1"),
				},
			},
		},
	}))
	assert.NoError(t, err, "TestNotifyWithTTL failed")

	time.Sleep(time.Second)

	assert.Equal(t, 2, len(nf.notifies), "TestNotifyWithTTL failed")
	assert.Equal(t, int32(10), nf.notifies[1].ToStepCycleTTL, "TestNotifyWithTTL failed")
}

func TestStepCountAndNotiesMatched(t *testing.T) {
	store, deferFunc := storage.NewTestStorage(t, false)
	defer deferFunc()

	ng, err := NewEngine(store, notify.NewQueueBasedNotifier(store))
	assert.NoError(t, err, "TestStepCountAndNotiesMatched failed")
	assert.NoError(t, ng.Start(), "TestStepCountAndNotiesMatched failed")
	defer ng.Stop()

	tid := uint64(10001)
	wid := uint64(10000)
	err = ng.(*engine).tenantInitWithReplicas(metapb.Tenant{
		ID: tid,
		Input: metapb.TenantQueue{
			Partitions:      2,
			ConsumerTimeout: 60,
		},
		Output: metapb.TenantQueue{
			Partitions:      1,
			ConsumerTimeout: 60,
		},
	}, 1)
	assert.NoError(t, err, "TestStepCountAndNotiesMatched failed")
	time.Sleep(time.Second * 2)

	bm := roaring.BitmapOf()
	for i := uint32(1); i <= 10000; i++ {
		bm.Add(i)
	}
	_, err = ng.StartInstance(metapb.Workflow{
		ID:       wid,
		TenantID: tid,
		Name:     "test_wf",
		Steps: []metapb.Step{
			{
				Name: "step_start_matched",
				Execution: metapb.Execution{
					Type: metapb.Branch,
					Branches: []metapb.ConditionExecution{
						{
							Condition: metapb.Expr{
								Value: []byte("{num: event.data} == 1"),
							},
							NextStep: "step_direct_matched",
						},
						{
							Condition: metapb.Expr{
								Value: []byte(`1 == 1`),
							},
							NextStep: "step_end_matched",
						},
					},
				},
			},
			{
				Name: "step_direct_matched",
				Execution: metapb.Execution{
					Type: metapb.Direct,
					Direct: &metapb.DirectExecution{
						NextStep: "step_end_matched",
					},
				},
			},
			{
				Name: "step_end_matched",
				Execution: metapb.Execution{
					Type:   metapb.Direct,
					Direct: &metapb.DirectExecution{},
				},
			},
		},
	}, metapb.RawLoader, util.MustMarshalBM(bm))
	assert.NoError(t, err, "TestStepCountAndNotiesMatched failed")

	assert.NoError(t, waitTestWorkflow(ng, 10000, metapb.Running), "TestStepCountAndNotiesMatched failed")

	var events [][]byte
	for i := uint32(1); i <= 10000; i++ {
		events = append(events, protoc.MustMarshal(&metapb.Event{
			Type: metapb.UserType,
			User: &metapb.UserEvent{
				TenantID: tid,
				UserID:   i,
				Data: []metapb.KV{
					{
						Key:   []byte("data"),
						Value: []byte("1"),
					},
				},
			},
		}))

		if len(events) == 256 {
			err = ng.Storage().PutToQueueWithAlloc(tid, metapb.TenantInputGroup, events...)
			assert.NoError(t, err, "TestStepCountAndNotiesMatched failed")
			events = events[:0]
		}
	}

	if len(events) > 0 {
		err = ng.Storage().PutToQueueWithAlloc(tid, metapb.TenantInputGroup, events...)
		assert.NoError(t, err, "TestStepCountAndNotiesMatched failed")
	}

	time.Sleep(time.Second * 5)

	states, err := ng.InstanceCountState(wid)
	assert.NoError(t, err, "TestStepCountAndNotiesMatched failed")
	m := make(map[string]int)
	for _, state := range states.States {
		m[state.Step] = int(state.Count)
	}
	assert.Equal(t, 0, m["step_start_matched"], "TestStepCountAndNotiesMatched failed")
	assert.Equal(t, 0, m["step_direct_matched"], "TestStepCountAndNotiesMatched failed")
	assert.Equal(t, 10000, m["step_end_matched"], "TestStepCountAndNotiesMatched failed")

	bmDirect := acquireBM()
	bmEnd := acquireBM()
	nt := &metapb.Notify{}
	buf := goetty.NewByteBuf(32)
	partitionKey := storage.PartitionKey(tid, 0)
	from := uint64(0)
	buf2 := goetty.NewByteBuf(32)
	endKey := storage.QueueItemKey(partitionKey, math.MaxUint32, buf2)
	for {
		buf.Clear()
		keys, values, err := ng.Storage().ScanWithGroup(storage.QueueItemKey(partitionKey, from, buf), endKey, 256, metapb.TenantOutputGroup)
		assert.NoError(t, err, "TestStepCountAndNotiesMatched failed")
		if err != nil {
			break
		}

		if len(values) == 0 {
			break
		}

		for _, value := range values {
			nt.Reset()
			protoc.MustUnmarshal(nt, value)

			if nt.ToStep == "step_direct_matched" {
				if nt.UserID > 0 {
					bmDirect.Add(nt.UserID)
				} else {
					bmDirect.Or(util.MustParseBM(nt.Crowd))
				}
			} else if nt.ToStep == "step_end_matched" {
				if nt.UserID > 0 {
					bmEnd.Add(nt.UserID)
				} else {
					bmEnd.Or(util.MustParseBM(nt.Crowd))
				}
			}
		}

		last := keys[len(keys)-1]
		from = goetty.Byte2UInt64(last[len(last)-8:]) + 1
	}

	assert.Equal(t, 10000, int(bmDirect.GetCardinality()), "TestStepCountAndNotiesMatched failed")
	assert.Equal(t, 10000, int(bmEnd.GetCardinality()), "TestStepCountAndNotiesMatched failed")
}

type errorNotify struct {
	times    int
	max      int
	delegate notify.Notifier
}

func newErrorNotify(max int, delegate notify.Notifier) notify.Notifier {
	return &errorNotify{
		max:      max,
		delegate: delegate,
	}
}

func (nt *errorNotify) Notify(id uint64, buf *goetty.ByteBuf, notifies []metapb.Notify, cond *rpcpb.Condition, kvs ...[]byte) error {
	err := nt.delegate.Notify(id, buf, notifies, cond, kvs...)

	if nt.times >= nt.max {
		return err
	}

	nt.times++
	return fmt.Errorf("error")
}

func TestNotifyWithErrorRetry(t *testing.T) {
	store, deferFunc := storage.NewTestStorage(t, false)
	defer deferFunc()

	ng, err := NewEngine(store, newErrorNotify(1, notify.NewQueueBasedNotifier(store)))
	assert.NoError(t, err, "TestNotifyWithErrorRetry failed")
	assert.NoError(t, ng.Start(), "TestNotifyWithErrorRetry failed")
	defer ng.Stop()

	err = ng.(*engine).tenantInitWithReplicas(metapb.Tenant{
		ID: 10001,
		Input: metapb.TenantQueue{
			Partitions:      2,
			ConsumerTimeout: 60,
		},
		Output: metapb.TenantQueue{
			Partitions:      1,
			ConsumerTimeout: 60,
		},
	}, 1)
	assert.NoError(t, err, "TestNotifyWithErrorRetry failed")
	time.Sleep(time.Second * 2)

	bm := roaring.BitmapOf(1, 2, 3, 4)
	_, err = ng.StartInstance(metapb.Workflow{
		ID:       10000,
		TenantID: 10001,
		Name:     "test_wf",
		Steps: []metapb.Step{
			{
				Name: "step_start",
				Execution: metapb.Execution{
					Type: metapb.Branch,
					Branches: []metapb.ConditionExecution{
						{
							Condition: metapb.Expr{
								Value: []byte("{num: event.uid} == 1"),
							},
							NextStep: "step_end",
						},
						{
							Condition: metapb.Expr{
								Value: []byte("1 == 1"),
							},
							NextStep: "step_end",
						},
					},
				},
			},
			{
				Name: "step_end",
				Execution: metapb.Execution{
					Type:   metapb.Direct,
					Direct: &metapb.DirectExecution{},
				},
			},
		},
	}, metapb.RawLoader, util.MustMarshalBM(bm))
	assert.NoError(t, err, "TestNotifyWithErrorRetry failed")

	time.Sleep(time.Second)

	err = ng.Storage().PutToQueue(10001, 0, metapb.TenantInputGroup, protoc.MustMarshal(&metapb.Event{
		Type: metapb.UserType,
		User: &metapb.UserEvent{
			TenantID: 10001,
			UserID:   1,
			Data: []metapb.KV{
				{
					Key:   []byte("uid"),
					Value: []byte("1"),
				},
			},
		},
	}))
	assert.NoError(t, err, "TestNotifyWithErrorRetry failed")

	time.Sleep(time.Second * 8)

	buf := goetty.NewByteBuf(32)
	req := rpcpb.AcquireScanRequest()
	req.Start = storage.QueueItemKey(storage.PartitionKey(10001, 0), 1, buf)
	req.End = storage.QueueItemKey(storage.PartitionKey(10001, 0), 100, buf)
	req.Limit = 100
	value, err := store.ExecCommandWithGroup(req, metapb.TenantOutputGroup)
	assert.NoError(t, err, "TestNotifyWithErrorRetry failed")
	scanResp := rpcpb.AcquireBytesSliceResponse()
	protoc.MustUnmarshal(scanResp, value)
	assert.Equal(t, 1, len(scanResp.Keys), "TestNotifyWithErrorRetry failed")
}

func TestStepWithPreLoad(t *testing.T) {
	store, deferFunc := storage.NewTestStorage(t, false)
	defer deferFunc()

	ng, err := NewEngine(store, notify.NewQueueBasedNotifier(store))
	assert.NoError(t, err, "TestStepWithPreLoad failed")
	assert.NoError(t, ng.Start(), "TestStepWithPreLoad failed")
	defer ng.Stop()

	tid := uint64(10001)
	wid := uint64(10000)
	err = ng.(*engine).tenantInitWithReplicas(metapb.Tenant{
		ID:      tid,
		Runners: 4,
		Input: metapb.TenantQueue{
			Partitions:      2,
			ConsumerTimeout: 60,
		},
		Output: metapb.TenantQueue{
			Partitions:      1,
			ConsumerTimeout: 60,
		},
	}, 1)
	assert.NoError(t, err, "TestStepWithPreLoad failed")
	time.Sleep(time.Second * 2)

	err = store.Set([]byte("prev_1"), []byte("1"))
	assert.NoError(t, err, "TestStepWithPreLoad failed")

	bm := roaring.BitmapOf()
	for i := uint32(1); i <= 10000; i++ {
		bm.Add(i)
	}
	_, err = ng.StartInstance(metapb.Workflow{
		ID:       wid,
		TenantID: tid,
		Name:     "test_wf",
		Steps: []metapb.Step{
			{
				Name: "step_start",
				Execution: metapb.Execution{
					Type: metapb.Branch,
					Branches: []metapb.ConditionExecution{
						{
							Condition: metapb.Expr{
								Value: []byte("{num: dyna.prev_%s.event.uid} == 1"),
							},
							NextStep: "step_end_1",
						},
						{
							Condition: metapb.Expr{
								Value: []byte(`1 == 1`),
							},
							NextStep: "step_end_2",
						},
					},
				},
			},
			{
				Name: "step_end_1",
				Execution: metapb.Execution{
					Type:   metapb.Direct,
					Direct: &metapb.DirectExecution{},
				},
			},
			{
				Name: "step_end_2",
				Execution: metapb.Execution{
					Type:   metapb.Direct,
					Direct: &metapb.DirectExecution{},
				},
			},
		},
	}, metapb.RawLoader, util.MustMarshalBM(bm))
	assert.NoError(t, err, "TestStepWithPreLoad failed")

	assert.NoError(t, waitTestWorkflow(ng, 10000, metapb.Running), "TestStepWithPreLoad failed")

	var events [][]byte
	var p uint32
	for i := uint32(1); i <= 10000; i++ {
		events = append(events, protoc.MustMarshal(&metapb.Event{
			Type: metapb.UserType,
			User: &metapb.UserEvent{
				TenantID: tid,
				UserID:   i,
			},
		}))

		if len(events) == 256 {
			err = ng.Storage().PutToQueue(tid, p, metapb.TenantInputGroup, events...)
			assert.NoError(t, err, "TestStepWithPreLoad failed")
			events = events[:0]
			p++
			if p >= 2 {
				p = 0
			}
		}
	}

	if len(events) > 0 {
		err = ng.Storage().PutToQueue(tid, p, metapb.TenantInputGroup, events...)
		assert.NoError(t, err, "TestStepWithPreLoad failed")
	}

	time.Sleep(time.Second * 2)

	states, err := ng.InstanceCountState(wid)
	assert.NoError(t, err, "TestStepWithPreLoad failed")
	m := make(map[string]int)
	for _, state := range states.States {
		m[state.Step] = int(state.Count)
	}
	assert.Equal(t, 0, m["step_start"], "TestStepWithPreLoad failed")
	assert.Equal(t, 1, m["step_end_1"], "TestStepWithPreLoad failed")
	assert.Equal(t, 9999, m["step_end_2"], "TestStepWithPreLoad failed")
}

func waitTestWorkflow(ng Engine, wid uint64, state metapb.WorkflowInstanceState) error {
	for {
		instance, err := ng.LastInstance(wid)
		if err != nil {
			return err
		}

		if instance.State == state {
			time.Sleep(time.Millisecond * 100)
			return nil
		}

		time.Sleep(time.Microsecond * 10)
	}
}
