package core

import (
	"fmt"
	"testing"
	"time"

	"github.com/RoaringBitmap/roaring"
	"github.com/deepfabric/busybee/pkg/notify"
	"github.com/deepfabric/busybee/pkg/pb/metapb"
	"github.com/deepfabric/busybee/pkg/pb/rpcpb"
	"github.com/deepfabric/busybee/pkg/queue"
	"github.com/deepfabric/busybee/pkg/storage"
	"github.com/deepfabric/busybee/pkg/util"
	"github.com/deepfabric/prophet"
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

func TestCreateTenantQueue(t *testing.T) {
	store, deferFunc := storage.NewTestStorage(t, false)
	defer deferFunc()

	ng, err := NewEngine(store, notify.NewQueueBasedNotifier(store))
	assert.NoError(t, err, "TestCreateTenantQueue failed")
	assert.NoError(t, ng.Start(), "TestCreateTenantQueue failed")

	err = ng.CreateTenantQueue(10001, 1)
	assert.NoError(t, err, "TestCreateTenantQueue failed")

	time.Sleep(time.Millisecond * 500)

	c := 0
	err = store.RaftStore().Prophet().GetStore().LoadResources(16, func(res prophet.Resource) {
		c++
	})
	assert.Equal(t, 3, c, "TestCreateTenantQueue failed")
}

func TestStartInstance(t *testing.T) {
	store, deferFunc := storage.NewTestStorage(t, false)
	defer deferFunc()

	ng, err := NewEngine(store, notify.NewQueueBasedNotifier(store))
	assert.NoError(t, err, "TestStartInstance failed")
	assert.NoError(t, ng.Start(), "TestStartInstance failed")

	err = ng.CreateTenantQueue(10001, 1)
	assert.NoError(t, err, "TestStartInstance failed")
	time.Sleep(time.Second)

	bm := roaring.BitmapOf(1, 2, 3, 4)
	err = ng.StartInstance(metapb.Workflow{
		ID:       10000,
		TenantID: 10001,
		Duration: 10,
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
	}, util.MustMarshalBM(bm), 3)
	assert.NoError(t, err, "TestStartInstance failed")

	time.Sleep(time.Second * 2)
	c := 0
	ng.(*engine).workers.Range(func(key, value interface{}) bool {
		c++
		return true
	})
	assert.Equal(t, 2, c, "TestStartInstance failed")

	req := rpcpb.AcquireQueueAddRequest()
	req.Items = [][]byte{
		protoc.MustMarshal(&metapb.Event{
			TenantID: 10001,
			UserID:   1,
			Data: []metapb.KV{
				metapb.KV{
					Key:   []byte("uid"),
					Value: []byte("1"),
				},
			},
		}),
	}
	req.Key = queue.PartitionKey(10001, 0)
	_, err = ng.Storage().ExecCommandWithGroup(req, metapb.TenantInputGroup)
	assert.NoError(t, err, "TestStartInstance failed")

	time.Sleep(time.Second)

	fetch := rpcpb.AcquireQueueFetchRequest()
	fetch.Key = queue.PartitionKey(10001, 0)
	fetch.AfterOffset = 0
	fetch.Count = 1
	fetch.Consumer = []byte("ccccccccccccccccccccccccccc")
	data, err := ng.Storage().ExecCommandWithGroup(fetch, metapb.TenantOutputGroup)
	assert.NoError(t, err, "TestStartInstance failed")

	resp := rpcpb.AcquireBytesSliceResponse()
	protoc.MustUnmarshal(resp, data)
	assert.Equal(t, 1, len(resp.Items), "TestStartInstance failed")

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
	bm = util.MustParseBM(state.Crowd)
	assert.Equal(t, uint64(3), bm.GetCardinality(), "TestStartInstance failed")

	time.Sleep(time.Second * 9)
	c = 0
	ng.(*engine).workers.Range(func(key, value interface{}) bool {
		c++
		return true
	})
	assert.Equal(t, 0, c, "TestStartInstance failed")
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

func (nt *errorNotify) Notify(id uint64, notifies ...metapb.Notify) error {
	if nt.times >= nt.max {
		return nt.delegate.Notify(id, notifies...)
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

	err = ng.CreateTenantQueue(10001, 1)
	assert.NoError(t, err, "TestStartInstance failed")
	time.Sleep(time.Second)

	bm := roaring.BitmapOf(1, 2, 3, 4)
	err = ng.StartInstance(metapb.Workflow{
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
	}, util.MustMarshalBM(bm), 3)
	assert.NoError(t, err, "TestStartInstance failed")

	time.Sleep(time.Second)
	c := 0
	ng.(*engine).workers.Range(func(key, value interface{}) bool {
		c++
		return true
	})
	assert.Equal(t, 2, c, "TestStartInstance failed")

	req := rpcpb.AcquireQueueAddRequest()
	req.Items = [][]byte{
		protoc.MustMarshal(&metapb.Event{
			TenantID: 10001,
			UserID:   1,
			Data: []metapb.KV{
				metapb.KV{
					Key:   []byte("uid"),
					Value: []byte("1"),
				},
			},
		}),
	}
	req.Key = queue.PartitionKey(10001, 0)
	_, err = ng.Storage().ExecCommandWithGroup(req, metapb.TenantInputGroup)
	assert.NoError(t, err, "TestStartInstance failed")

	time.Sleep(time.Second * 8)

	fetch := rpcpb.AcquireQueueFetchRequest()
	fetch.Key = queue.PartitionKey(10001, 0)
	fetch.AfterOffset = 0
	fetch.Count = 1
	fetch.Consumer = []byte("ccccccccccccccccccccccccccc")
	data, err := ng.Storage().ExecCommandWithGroup(fetch, metapb.TenantOutputGroup)
	assert.NoError(t, err, "TestStartInstance failed")

	resp := rpcpb.AcquireBytesSliceResponse()
	protoc.MustUnmarshal(resp, data)
	assert.Equal(t, 1, len(resp.Items), "TestStartInstance failed")

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
	bm = util.MustParseBM(state.Crowd)
	assert.Equal(t, uint64(3), bm.GetCardinality(), "TestStartInstance failed")
}
