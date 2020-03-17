package core

import (
	"fmt"
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
}

func TestStop(t *testing.T) {
	store, deferFunc := storage.NewTestStorage(t, false)
	defer deferFunc()

	ng, err := NewEngine(store, notify.NewQueueBasedNotifier(store))
	assert.NoError(t, err, "TestStop failed")
	assert.NoError(t, ng.Start(), "TestStop failed")
	assert.NoError(t, ng.Stop(), "TestStop failed")
}

func TestTenantInit(t *testing.T) {
	store, deferFunc := storage.NewTestStorage(t, false)
	defer deferFunc()

	ng, err := NewEngine(store, notify.NewQueueBasedNotifier(store))
	assert.NoError(t, err, "TestTenantInit failed")
	assert.NoError(t, ng.Start(), "TestTenantInit failed")

	err = ng.TenantInit(10001, 1)
	assert.NoError(t, err, "TestTenantInit failed")

	time.Sleep(time.Millisecond * 500)

	c := 0
	err = store.RaftStore().Prophet().GetStore().LoadResources(16, func(res prophet.Resource) {
		c++
	})
	assert.Equal(t, 3, c, "TestTenantInit failed")
}

func TestStopInstanceAndRestart(t *testing.T) {
	store, deferFunc := storage.NewTestStorage(t, false)
	defer deferFunc()

	ng, err := NewEngine(store, notify.NewQueueBasedNotifier(store))
	assert.NoError(t, err, "TestStopInstance failed")
	assert.NoError(t, ng.Start(), "TestStopInstance failed")

	err = ng.TenantInit(10001, 1)
	assert.NoError(t, err, "TestStopInstance failed")
	time.Sleep(time.Second)

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
				metapb.Step{
					Name: "step_end_1",
					Execution: metapb.Execution{
						Type:   metapb.Direct,
						Direct: &metapb.DirectExecution{},
					},
				},
			},
		}, metapb.RawLoader, util.MustMarshalBM(bm), 3)

		assert.NoError(t, waitTestWorkflow(ng, 10000, metapb.Running), "TestStopInstance failed")

		c := 0
		ng.(*engine).workers.Range(func(key, value interface{}) bool {
			c++
			return true
		})
		assert.Equal(t, 3, c, "TestStopInstance failed")

		err = ng.StopInstance(10000)
		assert.NoError(t, err, "TestStopInstance failed")

		assert.NoError(t, waitTestWorkflow(ng, 10000, metapb.Stopped), "TestStopInstance failed")
		c = 0
		ng.(*engine).workers.Range(func(key, value interface{}) bool {
			c++
			return true
		})
		assert.Equal(t, 0, c, "TestStopInstance failed")

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

	err = ng.TenantInit(10001, 1)
	assert.NoError(t, err, "TestStartInstance failed")
	time.Sleep(time.Second)

	bm := roaring.BitmapOf(1, 2, 3, 4)
	_, err = ng.StartInstance(metapb.Workflow{
		ID:       10000,
		TenantID: 10001,
		StopAt:   time.Now().Add(time.Second * 10).Unix(),
		Name:     "test_wf",
		Steps: []metapb.Step{
			metapb.Step{
				Name: "step_start",
				Execution: metapb.Execution{
					Type: metapb.Branch,
					Branches: []metapb.ConditionExecution{
						metapb.ConditionExecution{
							Condition: metapb.Expr{
								Value: []byte("{num: event.uid} == 1"),
							},
							NextStep: "step_end_1",
						},
						metapb.ConditionExecution{
							Condition: metapb.Expr{
								Value: []byte("1 == 1"),
							},
							NextStep: "step_end_else",
						},
					},
				},
			},
			metapb.Step{
				Name: "step_end_1",
				Execution: metapb.Execution{
					Type:   metapb.Direct,
					Direct: &metapb.DirectExecution{},
				},
			},
			metapb.Step{
				Name: "step_end_else",
				Execution: metapb.Execution{
					Type:   metapb.Direct,
					Direct: &metapb.DirectExecution{},
				},
			},
		},
	}, metapb.RawLoader, util.MustMarshalBM(bm), 3)
	assert.NoError(t, err, "TestStartInstance failed")

	assert.NoError(t, waitTestWorkflow(ng, 10000, metapb.Running), "TestStartInstance failed")

	c := 0
	ng.(*engine).workers.Range(func(key, value interface{}) bool {
		c++
		return true
	})
	assert.Equal(t, 3, c, "TestStartInstance failed")

	err = ng.Storage().PutToQueue(10001, 0, metapb.TenantInputGroup, protoc.MustMarshal(&metapb.Event{
		Type: metapb.UserType,
		User: &metapb.UserEvent{
			TenantID: 10001,
			UserID:   1,
			Data: []metapb.KV{
				metapb.KV{
					Key:   []byte("uid"),
					Value: []byte("1"),
				},
			},
		},
	}))
	assert.NoError(t, err, "TestStartInstance failed")

	time.Sleep(time.Second)

	fetch := rpcpb.AcquireQueueFetchRequest()
	fetch.Key = storage.PartitionKey(10001, 0)
	fetch.CompletedOffset = 0
	fetch.Count = 1
	fetch.Consumer = []byte("c")
	data, err := ng.Storage().ExecCommandWithGroup(fetch, metapb.TenantOutputGroup)
	assert.NoError(t, err, "TestStartInstance failed")

	resp := rpcpb.AcquireBytesSliceResponse()
	protoc.MustUnmarshal(resp, data)
	assert.Equal(t, 1, len(resp.Values), "TestStartInstance failed")

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
	c = 0
	ng.(*engine).workers.Range(func(key, value interface{}) bool {
		c++
		return true
	})
	assert.Equal(t, 0, c, "TestStartInstance failed")
}

func TestTriggerDirect(t *testing.T) {
	store, deferFunc := storage.NewTestStorage(t, false)
	defer deferFunc()

	ng, err := NewEngine(store, notify.NewQueueBasedNotifier(store))
	assert.NoError(t, err, "TestTriggerDirect failed")
	assert.NoError(t, ng.Start(), "TestTriggerDirect failed")

	err = ng.TenantInit(10001, 1)
	assert.NoError(t, err, "TestTriggerDirect failed")
	time.Sleep(time.Second)

	bm := roaring.BitmapOf(1, 2, 3, 4)
	_, err = ng.StartInstance(metapb.Workflow{
		ID:       10000,
		TenantID: 10001,
		StopAt:   time.Now().Add(time.Second * 10).Unix(),
		Name:     "test_wf",
		Steps: []metapb.Step{
			metapb.Step{
				Name: "step_start",
				Execution: metapb.Execution{
					Type: metapb.Branch,
					Branches: []metapb.ConditionExecution{
						metapb.ConditionExecution{
							Condition: metapb.Expr{
								Value: []byte("{num: event.move} == 1"),
							},
							NextStep: "next_1",
						},
						metapb.ConditionExecution{
							Condition: metapb.Expr{
								Value: []byte("1 == 1"),
							},
							NextStep: "end",
						},
					},
				},
			},
			metapb.Step{
				Name: "next_1",
				Execution: metapb.Execution{
					Type: metapb.Direct,
					Direct: &metapb.DirectExecution{
						NextStep: "next_2",
					},
				},
			},
			metapb.Step{
				Name: "next_2",
				Execution: metapb.Execution{
					Type: metapb.Direct,
					Direct: &metapb.DirectExecution{
						NextStep: "end",
					},
				},
			},
			metapb.Step{
				Name: "end",
				Execution: metapb.Execution{
					Type:   metapb.Direct,
					Direct: &metapb.DirectExecution{},
				},
			},
		},
	}, metapb.RawLoader, util.MustMarshalBM(bm), 3)
	assert.NoError(t, err, "TestTriggerDirect failed")

	assert.NoError(t, waitTestWorkflow(ng, 10000, metapb.Running), "TestTriggerDirect failed")

	c := 0
	ng.(*engine).workers.Range(func(key, value interface{}) bool {
		c++
		return true
	})
	assert.Equal(t, 3, c, "TestTriggerDirect failed")

	err = ng.Storage().PutToQueue(10001, 0, metapb.TenantInputGroup, protoc.MustMarshal(&metapb.Event{
		Type: metapb.UserType,
		User: &metapb.UserEvent{
			TenantID: 10001,
			UserID:   1,
			Data: []metapb.KV{
				metapb.KV{
					Key:   []byte("move"),
					Value: []byte("1"),
				},
			},
		},
	}))
	assert.NoError(t, err, "TestTriggerDirect failed")
	time.Sleep(time.Second)

	fetch := rpcpb.AcquireQueueFetchRequest()
	fetch.Key = storage.PartitionKey(10001, 0)
	fetch.CompletedOffset = 0
	fetch.Count = 10
	fetch.Consumer = []byte("c")
	data, err := ng.Storage().ExecCommandWithGroup(fetch, metapb.TenantOutputGroup)
	assert.NoError(t, err, "TestTriggerDirect failed")

	resp := rpcpb.AcquireBytesSliceResponse()
	protoc.MustUnmarshal(resp, data)
	assert.Equal(t, 3, len(resp.Values), "TestTriggerDirect failed")

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

	err = ng.TenantInit(tid, 1)
	assert.NoError(t, err, "TestUpdateCrowd failed")
	time.Sleep(time.Second)

	bm := roaring.BitmapOf(2, 3, 4)
	_, err = ng.StartInstance(metapb.Workflow{
		ID:       wid,
		TenantID: tid,
		Name:     "test_wf",
		Steps: []metapb.Step{
			metapb.Step{
				Name: "step_start",
				Execution: metapb.Execution{
					Type: metapb.Branch,
					Branches: []metapb.ConditionExecution{
						metapb.ConditionExecution{
							Condition: metapb.Expr{
								Value: []byte("{num: event.uid} == 1"),
							},
							NextStep: "step_end_1",
						},
						metapb.ConditionExecution{
							Condition: metapb.Expr{
								Value: []byte("1 == 1"),
							},
							NextStep: "step_end_else",
						},
					},
				},
			},
			metapb.Step{
				Name: "step_end_1",
				Execution: metapb.Execution{
					Type:   metapb.Direct,
					Direct: &metapb.DirectExecution{},
				},
			},
			metapb.Step{
				Name: "step_end_else",
				Execution: metapb.Execution{
					Type:   metapb.Direct,
					Direct: &metapb.DirectExecution{},
				},
			},
		},
	}, metapb.RawLoader, util.MustMarshalBM(bm), 3)
	assert.NoError(t, err, "TestUpdateCrowd failed")

	assert.NoError(t, waitTestWorkflow(ng, 10000, metapb.Running), "TestUpdateCrowd failed")

	c := 0
	ng.(*engine).workers.Range(func(key, value interface{}) bool {
		c++
		return true
	})
	assert.Equal(t, 3, c, "TestUpdateCrowd failed")

	err = ng.Storage().PutToQueue(tid, 0, metapb.TenantInputGroup, protoc.MustMarshal(&metapb.Event{
		Type: metapb.UserType,
		User: &metapb.UserEvent{
			TenantID: tid,
			UserID:   2,
			Data: []metapb.KV{
				metapb.KV{
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
				metapb.KV{
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
				metapb.KV{
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

	err = ng.Storage().PutToQueue(tid, 0, metapb.TenantInputGroup, protoc.MustMarshal(&metapb.Event{
		Type: metapb.UserType,
		User: &metapb.UserEvent{
			TenantID: tid,
			UserID:   1,
			Data: []metapb.KV{
				metapb.KV{
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

	err = ng.TenantInit(tid, 1)
	assert.NoError(t, err, "TestUpdateWorkflow failed")
	time.Sleep(time.Second)

	bm := roaring.BitmapOf(1, 2, 3, 4)
	_, err = ng.StartInstance(metapb.Workflow{
		ID:       wid,
		TenantID: tid,
		Name:     "test_wf",
		Steps: []metapb.Step{
			metapb.Step{
				Name: "step_start",
				Execution: metapb.Execution{
					Type: metapb.Branch,
					Branches: []metapb.ConditionExecution{
						metapb.ConditionExecution{
							Condition: metapb.Expr{
								Value: []byte("{num: event.uid} == 1"),
							},
							NextStep: "step_end_1",
						},
						metapb.ConditionExecution{
							Condition: metapb.Expr{
								Value: []byte("{num: event.uid} == 2"),
							},
							NextStep: "step_end_2",
						},
						metapb.ConditionExecution{
							Condition: metapb.Expr{
								Value: []byte("{num: event.uid} == 3"),
							},
							NextStep: "step_end_3",
						},
						metapb.ConditionExecution{
							Condition: metapb.Expr{
								Value: []byte("{num: event.uid} == 4"),
							},
							NextStep: "step_end_4",
						},
						metapb.ConditionExecution{
							Condition: metapb.Expr{
								Value: []byte("1 == 1"),
							},
							NextStep: "step_end_else",
						},
					},
				},
			},
			metapb.Step{
				Name: "step_end_1",
				Execution: metapb.Execution{
					Type:   metapb.Direct,
					Direct: &metapb.DirectExecution{},
				},
			},
			metapb.Step{
				Name: "step_end_2",
				Execution: metapb.Execution{
					Type:   metapb.Direct,
					Direct: &metapb.DirectExecution{},
				},
			},
			metapb.Step{
				Name: "step_end_3",
				Execution: metapb.Execution{
					Type:   metapb.Direct,
					Direct: &metapb.DirectExecution{},
				},
			},
			metapb.Step{
				Name: "step_end_4",
				Execution: metapb.Execution{
					Type:   metapb.Direct,
					Direct: &metapb.DirectExecution{},
				},
			},
			metapb.Step{
				Name: "step_end_else",
				Execution: metapb.Execution{
					Type:   metapb.Direct,
					Direct: &metapb.DirectExecution{},
				},
			},
		},
	}, metapb.RawLoader, util.MustMarshalBM(bm), 3)
	assert.NoError(t, err, "TestUpdateWorkflow failed")

	assert.NoError(t, waitTestWorkflow(ng, 10000, metapb.Running), "TestUpdateWorkflow failed")
	c := 0
	ng.(*engine).workers.Range(func(key, value interface{}) bool {
		c++
		return true
	})
	assert.Equal(t, 3, c, "TestUpdateWorkflow failed")

	err = ng.Storage().PutToQueue(tid, 0, metapb.TenantInputGroup, protoc.MustMarshal(&metapb.Event{
		Type: metapb.UserType,
		User: &metapb.UserEvent{
			TenantID: tid,
			UserID:   1,
			Data: []metapb.KV{
				metapb.KV{
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
				metapb.KV{
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
				metapb.KV{
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
			metapb.Step{
				Name: "step_start",
				Execution: metapb.Execution{
					Type: metapb.Branch,
					Branches: []metapb.ConditionExecution{
						metapb.ConditionExecution{
							Condition: metapb.Expr{
								Value: []byte("{num: event.uid} == 1"),
							},
							NextStep: "step_end_1",
						},
						metapb.ConditionExecution{
							Condition: metapb.Expr{
								Value: []byte("{num: event.uid} == 5"),
							},
							NextStep: "step_end_5",
						},
						metapb.ConditionExecution{
							Condition: metapb.Expr{
								Value: []byte("{num: event.uid} == 3"),
							},
							NextStep: "step_end_3",
						},
						metapb.ConditionExecution{
							Condition: metapb.Expr{
								Value: []byte("{num: event.uid} == 4"),
							},
							NextStep: "step_end_4",
						},
						metapb.ConditionExecution{
							Condition: metapb.Expr{
								Value: []byte("1 == 1"),
							},
							NextStep: "step_end_else",
						},
					},
				},
			},
			metapb.Step{
				Name: "step_end_1",
				Execution: metapb.Execution{
					Type:   metapb.Direct,
					Direct: &metapb.DirectExecution{},
				},
			},
			metapb.Step{
				Name: "step_end_5",
				Execution: metapb.Execution{
					Type:   metapb.Direct,
					Direct: &metapb.DirectExecution{},
				},
			},
			metapb.Step{
				Name: "step_end_3",
				Execution: metapb.Execution{
					Type:   metapb.Direct,
					Direct: &metapb.DirectExecution{},
				},
			},
			metapb.Step{
				Name: "step_end_4",
				Execution: metapb.Execution{
					Type:   metapb.Direct,
					Direct: &metapb.DirectExecution{},
				},
			},
			metapb.Step{
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

func TestStartInstanceWithStepTTL(t *testing.T) {
	store, deferFunc := storage.NewTestStorage(t, false)
	defer deferFunc()

	ng, err := NewEngine(store, notify.NewQueueBasedNotifier(store))
	assert.NoError(t, err, "TestStartInstanceWithStepTTL failed")
	assert.NoError(t, ng.Start(), "TestStartInstanceWithStepTTL failed")

	err = ng.TenantInit(10001, 1)
	assert.NoError(t, err, "TestStartInstanceWithStepTTL failed")
	time.Sleep(time.Second)

	bm := roaring.BitmapOf(1, 2)
	_, err = ng.StartInstance(metapb.Workflow{
		ID:       10000,
		TenantID: 10001,
		Name:     "test_wf",
		Steps: []metapb.Step{
			metapb.Step{
				Name: "step_start",
				Execution: metapb.Execution{
					Type: metapb.Branch,
					Branches: []metapb.ConditionExecution{
						metapb.ConditionExecution{
							Condition: metapb.Expr{
								Value: []byte("{num: event.uid} == 1"),
							},
							NextStep: "step_ttl_start",
						},
						metapb.ConditionExecution{
							Condition: metapb.Expr{
								Value: []byte("{num: event.uid} == 2"),
							},
							NextStep: "step_ttl_start",
						},
						metapb.ConditionExecution{
							Condition: metapb.Expr{
								Value: []byte("1 == 1"),
							},
							NextStep: "step_end",
						},
					},
				},
			},
			metapb.Step{
				Name: "step_ttl_start",
				TTL:  2,
				Execution: metapb.Execution{
					Type: metapb.Branch,
					Branches: []metapb.ConditionExecution{
						metapb.ConditionExecution{
							Condition: metapb.Expr{
								Value: []byte("{num: func.wf_step_ttl} > 0"),
							},
							NextStep: "step_ttl_end",
						},
						metapb.ConditionExecution{
							Condition: metapb.Expr{
								Value: []byte("1 == 1"),
							},
							NextStep: "step_end",
						},
					},
				},
			},
			metapb.Step{
				Name: "step_ttl_end",
				Execution: metapb.Execution{
					Type:   metapb.Direct,
					Direct: &metapb.DirectExecution{},
				},
			},
			metapb.Step{
				Name: "step_end",
				Execution: metapb.Execution{
					Type:   metapb.Direct,
					Direct: &metapb.DirectExecution{},
				},
			},
		},
	}, metapb.RawLoader, util.MustMarshalBM(bm), 3)
	assert.NoError(t, err, "TestStartInstanceWithStepTTL failed")

	assert.NoError(t, waitTestWorkflow(ng, 10000, metapb.Running), "TestStartInstanceWithStepTTL failed")
	c := 0
	ng.(*engine).workers.Range(func(key, value interface{}) bool {
		c++
		return true
	})
	assert.Equal(t, 1, c, "TestStartInstanceWithStepTTL failed")

	err = ng.Storage().PutToQueue(10001, 0, metapb.TenantInputGroup, protoc.MustMarshal(&metapb.Event{
		Type: metapb.UserType,
		User: &metapb.UserEvent{
			TenantID: 10001,
			UserID:   1,
			Data: []metapb.KV{
				metapb.KV{
					Key:   []byte("uid"),
					Value: []byte("1"),
				},
			},
		},
	}), protoc.MustMarshal(&metapb.Event{
		Type: metapb.UserType,
		User: &metapb.UserEvent{
			TenantID: 10001,
			UserID:   2,
			Data: []metapb.KV{
				metapb.KV{
					Key:   []byte("uid"),
					Value: []byte("2"),
				},
			},
		},
	}))
	assert.NoError(t, err, "TestStartInstanceWithStepTTL failed")
	time.Sleep(time.Second)

	states, err := ng.InstanceCountState(10000)
	assert.NoError(t, err, "TestStartInstance failed")
	m := make(map[string]uint64)
	for _, state := range states.States {
		m[state.Step] = state.Count
	}
	assert.Equal(t, uint64(0), m["step_start"], "TestStartInstanceWithStepTTL failed")
	assert.Equal(t, uint64(2), m["step_ttl_start"], "TestStartInstanceWithStepTTL failed")
	assert.Equal(t, uint64(0), m["step_ttl_end"], "TestStartInstanceWithStepTTL failed")
	assert.Equal(t, uint64(0), m["ste_end"], "TestStartInstanceWithStepTTL failed")

	buf := goetty.NewByteBuf(24)
	v, err := store.Get(storage.WorkflowStepTTLKey(10000, 1, "step_ttl_start", buf))
	assert.NoError(t, err, "TestStartInstanceWithStepTTL failed")
	assert.NotEmpty(t, v, "TestStartInstanceWithStepTTL failed")

	v, err = store.Get(storage.WorkflowStepTTLKey(10000, 2, "step_ttl_start", buf))
	assert.NoError(t, err, "TestStartInstanceWithStepTTL failed")
	assert.NotEmpty(t, v, "TestStartInstanceWithStepTTL failed")

	err = ng.Storage().PutToQueue(10001, 0, metapb.TenantInputGroup, protoc.MustMarshal(&metapb.Event{
		Type: metapb.UserType,
		User: &metapb.UserEvent{
			TenantID: 10001,
			UserID:   1,
			Data: []metapb.KV{
				metapb.KV{
					Key:   []byte("uid"),
					Value: []byte("1"),
				},
			},
		},
	}))
	assert.NoError(t, err, "TestStartInstanceWithStepTTL failed")
	time.Sleep(time.Second * 2)
	err = ng.Storage().PutToQueue(10001, 0, metapb.TenantInputGroup, protoc.MustMarshal(&metapb.Event{
		Type: metapb.UserType,
		User: &metapb.UserEvent{
			TenantID: 10001,
			UserID:   2,
			Data: []metapb.KV{
				metapb.KV{
					Key:   []byte("uid"),
					Value: []byte("2"),
				},
			},
		},
	}))
	assert.NoError(t, err, "TestStartInstanceWithStepTTL failed")

	time.Sleep(time.Second)

	states, err = ng.InstanceCountState(10000)
	assert.NoError(t, err, "TestStartInstance failed")
	m = make(map[string]uint64)
	for _, state := range states.States {
		m[state.Step] = state.Count
	}
	assert.Equal(t, uint64(0), m["step_start"], "TestStartInstanceWithStepTTL failed")
	assert.Equal(t, uint64(0), m["step_ttl_start"], "TestStartInstanceWithStepTTL failed")
	assert.Equal(t, uint64(1), m["step_ttl_end"], "TestStartInstanceWithStepTTL failed")
	assert.Equal(t, uint64(1), m["step_end"], "TestStartInstanceWithStepTTL failed")
}

func TestTransaction(t *testing.T) {
	store, deferFunc := storage.NewTestStorage(t, false)
	defer deferFunc()

	tid := uint64(10001)
	wid := uint64(10000)

	ng, err := NewEngine(store, notify.NewQueueBasedNotifier(store))
	assert.NoError(t, err, "TestTransaction failed")
	assert.NoError(t, ng.Start(), "TestTransaction failed")

	err = ng.TenantInit(tid, 1)
	assert.NoError(t, err, "TestTransaction failed")
	time.Sleep(time.Second)

	bm := roaring.BitmapOf(1, 2)
	_, err = ng.StartInstance(metapb.Workflow{
		ID:       wid,
		TenantID: tid,
		Name:     "test_wf",
		Steps: []metapb.Step{
			metapb.Step{
				Name: "step_start",
				Execution: metapb.Execution{
					Type: metapb.Branch,
					Branches: []metapb.ConditionExecution{
						metapb.ConditionExecution{
							Condition: metapb.Expr{
								Value: []byte("{num: event.uid} == 1"),
							},
							NextStep: "step_end_1",
						},
						metapb.ConditionExecution{
							Condition: metapb.Expr{
								Value: []byte("{num: kv.uid} == 2"),
							},
							NextStep: "step_end_2",
						},
						metapb.ConditionExecution{
							Condition: metapb.Expr{
								Value: []byte("1 == 1"),
							},
							NextStep: "step_end_else",
						},
					},
				},
			},
			metapb.Step{
				Name: "step_end_1",
				Execution: metapb.Execution{
					Type:   metapb.Direct,
					Direct: &metapb.DirectExecution{},
				},
			},
			metapb.Step{
				Name: "step_end_2",
				Execution: metapb.Execution{
					Type:   metapb.Direct,
					Direct: &metapb.DirectExecution{},
				},
			},
			metapb.Step{
				Name: "step_end_else",
				Execution: metapb.Execution{
					Type:   metapb.Direct,
					Direct: &metapb.DirectExecution{},
				},
			},
		},
	}, metapb.RawLoader, util.MustMarshalBM(bm), 3)
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
				metapb.KV{
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

	err = ng.TenantInit(tid, 1)
	assert.NoError(t, err, "TestTimerWithUseStepCrowdToDrive failed")
	time.Sleep(time.Second)

	bm := roaring.BitmapOf(1, 2)
	_, err = ng.StartInstance(metapb.Workflow{
		ID:       wid,
		TenantID: tid,
		Name:     "test_wf",
		Steps: []metapb.Step{
			metapb.Step{
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
			metapb.Step{
				Name: "step_end",
				Execution: metapb.Execution{
					Type:   metapb.Direct,
					Direct: &metapb.DirectExecution{},
				},
			},
		},
	}, metapb.RawLoader, util.MustMarshalBM(bm), 3)
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

	err = ng.TenantInit(tid, 1)
	assert.NoError(t, err, "TestLastTransaction failed")
	time.Sleep(time.Second)

	bm := roaring.BitmapOf(1)
	instanceID, err := ng.StartInstance(metapb.Workflow{
		ID:       wid,
		TenantID: tid,
		Name:     "test_wf",
		Steps: []metapb.Step{
			metapb.Step{
				Name: "step_start",
				Execution: metapb.Execution{
					Type: metapb.Branch,
					Branches: []metapb.ConditionExecution{
						metapb.ConditionExecution{
							Condition: metapb.Expr{
								Value: []byte("{num: event.uid} == 1"),
							},
							NextStep: "step_end",
						},
						metapb.ConditionExecution{
							Condition: metapb.Expr{
								Value: []byte("1 == 1"),
							},
							NextStep: "step_end",
						},
					},
				},
			},
			metapb.Step{
				Name: "step_end",
				Execution: metapb.Execution{
					Type:   metapb.Direct,
					Direct: &metapb.DirectExecution{},
				},
			},
		},
	}, metapb.RawLoader, util.MustMarshalBM(bm), 1)
	assert.NoError(t, err, "TestLastTransaction failed")

	key := make([]byte, 12, 12)
	goetty.Uint64ToBytesTo(instanceID, key)
	goetty.Uint32ToBytesTo(0, key[8:])
	_, err = store.ExecCommandWithGroup(&rpcpb.SetRequest{
		Key: storage.PartitionKVKey(tid, 0, key),
		Value: protoc.MustMarshal(&metapb.WorkflowInstanceWorkerState{
			Version:    10,
			TenantID:   tid,
			WorkflowID: wid,
			InstanceID: instanceID,
			Index:      0,
			States: []metapb.StepState{
				metapb.StepState{
					Step: metapb.Step{
						Name: "step_start",
						Execution: metapb.Execution{
							Type: metapb.Branch,
							Branches: []metapb.ConditionExecution{
								metapb.ConditionExecution{
									Condition: metapb.Expr{
										Value: []byte("{num: event.uid} == 1"),
									},
									NextStep: "step_end",
								},
								metapb.ConditionExecution{
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
				metapb.StepState{
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

	err = ng.TenantInit(tid, 1)
	assert.NoError(t, err, "TestLastTransactionCompleted failed")
	time.Sleep(time.Second)

	bm := roaring.BitmapOf(1)
	instanceID, err := ng.StartInstance(metapb.Workflow{
		ID:       wid,
		TenantID: tid,
		Name:     "test_wf",
		Steps: []metapb.Step{
			metapb.Step{
				Name: "step_start",
				Execution: metapb.Execution{
					Type: metapb.Branch,
					Branches: []metapb.ConditionExecution{
						metapb.ConditionExecution{
							Condition: metapb.Expr{
								Value: []byte("{num: event.uid} == 1"),
							},
							NextStep: "step_end",
						},
						metapb.ConditionExecution{
							Condition: metapb.Expr{
								Value: []byte("1 == 1"),
							},
							NextStep: "step_end",
						},
					},
				},
			},
			metapb.Step{
				Name: "step_end",
				Execution: metapb.Execution{
					Type:   metapb.Direct,
					Direct: &metapb.DirectExecution{},
				},
			},
		},
	}, metapb.RawLoader, util.MustMarshalBM(bm), 1)
	assert.NoError(t, err, "TestLastTransactionCompleted failed")

	key := make([]byte, 12, 12)
	goetty.Uint64ToBytesTo(instanceID, key)
	goetty.Uint32ToBytesTo(0, key[8:])
	_, err = store.ExecCommandWithGroup(&rpcpb.SetRequest{
		Key: storage.PartitionKVKey(tid, 0, key),
		Value: protoc.MustMarshal(&metapb.WorkflowInstanceWorkerState{
			Version:    0,
			TenantID:   tid,
			WorkflowID: wid,
			InstanceID: instanceID,
			Index:      0,
			States: []metapb.StepState{
				metapb.StepState{
					Step: metapb.Step{
						Name: "step_start",
						Execution: metapb.Execution{
							Type: metapb.Branch,
							Branches: []metapb.ConditionExecution{
								metapb.ConditionExecution{
									Condition: metapb.Expr{
										Value: []byte("{num: event.uid} == 1"),
									},
									NextStep: "step_end",
								},
								metapb.ConditionExecution{
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
				metapb.StepState{
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

func (nt *errorNotify) Notify(id uint64, buf *goetty.ByteBuf, notifies []metapb.Notify, kvs ...[]byte) error {
	if nt.times >= nt.max {
		return nt.delegate.Notify(id, buf, notifies, kvs...)
	}

	nt.times++
	return fmt.Errorf("error")
}

func TestStartInstanceWithNotifyError(t *testing.T) {
	store, deferFunc := storage.NewTestStorage(t, false)
	defer deferFunc()

	ng, err := NewEngine(store, newErrorNotify(1, notify.NewQueueBasedNotifier(store)))
	assert.NoError(t, err, "TestStartInstance failed")
	assert.NoError(t, ng.Start(), "TestStartInstance failed")

	err = ng.TenantInit(10001, 1)
	assert.NoError(t, err, "TestStartInstance failed")
	time.Sleep(time.Second)

	bm := roaring.BitmapOf(1, 2, 3, 4)
	_, err = ng.StartInstance(metapb.Workflow{
		ID:       10000,
		TenantID: 10001,
		Name:     "test_wf",
		Steps: []metapb.Step{
			metapb.Step{
				Name: "step_start",
				Execution: metapb.Execution{
					Type: metapb.Branch,
					Branches: []metapb.ConditionExecution{
						metapb.ConditionExecution{
							Condition: metapb.Expr{
								Value: []byte("{num: event.uid} == 1"),
							},
							NextStep: "step_end_1",
						},
						metapb.ConditionExecution{
							Condition: metapb.Expr{
								Value: []byte("1 == 1"),
							},
							NextStep: "step_end_else",
						},
					},
				},
			},
			metapb.Step{
				Name: "step_end_1",
				Execution: metapb.Execution{
					Type:   metapb.Direct,
					Direct: &metapb.DirectExecution{},
				},
			},
			metapb.Step{
				Name: "step_end_else",
				Execution: metapb.Execution{
					Type:   metapb.Direct,
					Direct: &metapb.DirectExecution{},
				},
			},
		},
	}, metapb.RawLoader, util.MustMarshalBM(bm), 3)
	assert.NoError(t, err, "TestStartInstance failed")

	time.Sleep(time.Second)
	c := 0
	ng.(*engine).workers.Range(func(key, value interface{}) bool {
		c++
		return true
	})
	assert.Equal(t, 3, c, "TestStartInstance failed")

	err = ng.Storage().PutToQueue(10001, 0, metapb.TenantInputGroup, protoc.MustMarshal(&metapb.Event{
		Type: metapb.UserType,
		User: &metapb.UserEvent{
			TenantID: 10001,
			UserID:   1,
			Data: []metapb.KV{
				metapb.KV{
					Key:   []byte("uid"),
					Value: []byte("1"),
				},
			},
		},
	}))
	assert.NoError(t, err, "TestStartInstance failed")

	time.Sleep(time.Second * 8)

	fetch := rpcpb.AcquireQueueFetchRequest()
	fetch.Key = storage.PartitionKey(10001, 0)
	fetch.CompletedOffset = 0
	fetch.Count = 1
	fetch.Consumer = []byte("c")
	data, err := ng.Storage().ExecCommandWithGroup(fetch, metapb.TenantOutputGroup)
	assert.NoError(t, err, "TestStartInstance failed")

	resp := rpcpb.AcquireBytesSliceResponse()
	protoc.MustUnmarshal(resp, data)
	assert.Equal(t, 1, len(resp.Values), "TestStartInstance failed")

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
