package core

import (
	"testing"
	"time"

	"github.com/RoaringBitmap/roaring"
	"github.com/deepfabric/busybee/pkg/notify"
	"github.com/deepfabric/busybee/pkg/pb/metapb"
	"github.com/deepfabric/busybee/pkg/storage"
	"github.com/deepfabric/busybee/pkg/util"
	"github.com/deepfabric/prophet"
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
	time.Sleep(time.Millisecond * 500)

	bm := roaring.BitmapOf(1, 2, 3, 4)
	err = ng.StartInstance(metapb.Workflow{
		ID:       10000,
		TenantID: 10001,
		Name:     "test_wf",
		Duration: 10,
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
							Execution: &metapb.Execution{
								Direct: &metapb.DirectExecution{
									NextStep: "step_end_1",
								},
							},
						},
						metapb.ConditionExecution{
							Condition: metapb.Expr{
								Value: []byte("1 == 1"),
							},
							Execution: & metapb.Execution{
								Direct: &metapb.DirectExecution{
									NextStep: "step_end_else",
								},
							},
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

	time.Sleep(time.Second * 5)
	c := 0
	ng.(*engine).workers.Range(func(key, value interface{}) bool {
		c++
		return true
	})
	assert.Equal(t, 2, c, "TestStartInstance failed")

	time.Sleep(time.Second * 6)
	c = 0
	ng.(*engine).workers.Range(func(key, value interface{}) bool {
		c++
		return true
	})
	assert.Equal(t, 0, c, "TestStartInstance failed")
}
