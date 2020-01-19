package main

import (
	"flag"
	"time"

	"github.com/deepfabric/busybee/pkg/client"
	"github.com/deepfabric/busybee/pkg/pb/metapb"
	"github.com/deepfabric/busybee/pkg/util"
	"github.com/fagongzi/log"
	"github.com/fagongzi/util/protoc"
)

var (
	api = flag.String("addr", "127.0.0.1:9091", "api address")
)

func main() {
	cli := client.NewClient(*api)

	err := cli.BMCreate([]byte("bm1"), 1, 2, 3)
	if err != nil {
		log.Fatalf("create bm1 failed with %+v", err)
	}

	err = cli.BMCreate([]byte("bm2"), 1, 2, 3)
	if err != nil {
		log.Fatalf("create bm2 failed with %+v", err)
	}

	wid := createTimerWorkflow(cli)
	log.Infof("workflow %d created", wid)
	instanceID := createInstance(wid, cli, 1, 2, 3, 4)
	log.Infof("workflow instance %d created", instanceID)

	err = cli.CreateQueue(instanceID, metapb.NotifyGroup)
	if err != nil {
		log.Fatalf("create instance notify queue failed with %+v", err)
	}

	time.Sleep(time.Second * 10)

	go func() {
		log.Fatalf("consumer queue failed with %+v", cli.ConsumeQueue(instanceID, metapb.NotifyGroup, func(value []byte) error {
			nt := metapb.Notify{}
			protoc.MustUnmarshal(&nt, value)
			log.Infof("notify: %+v", nt)
			return nil
		}))
	}()

	startInstance(instanceID, cli)
	log.Infof("workflow instance %d started", instanceID)
	time.Sleep(time.Second * 8)
	err = cli.StepInstance(metapb.Event{
		UserID:     1,
		TenantID:   10000,
		InstanceID: instanceID,
		Data: []metapb.KV{
			metapb.KV{Key: []byte("uid"), Value: []byte("1")},
		},
	})
	if err != nil {
		log.Fatalf("step instance failed with %+v", err)
	}

	err = cli.StepInstance(metapb.Event{
		UserID:     2,
		TenantID:   10000,
		InstanceID: instanceID,
		Data: []metapb.KV{
			metapb.KV{Key: []byte("uid"), Value: []byte("2")},
		},
	})
	if err != nil {
		log.Fatalf("step instance failed with %+v", err)
	}

	state, err := cli.InstanceCountState(instanceID)
	if err != nil {
		log.Fatalf("InstanceCountState failed with %+v", err)
	}

	log.Infof("Total: %d", state.Total)
	for _, st := range state.States {
		log.Infof("%s: %d", st.Step, st.Count)
	}
}

func main1() {
	cli := client.NewClient(*api)
	wid := createWorkflow(cli)
	log.Infof("workflow %d created", wid)
	instanceID := createInstance(wid, cli, 1, 2, 3)
	log.Infof("workflow instance %d created", instanceID)

	err := cli.CreateQueue(instanceID, metapb.NotifyGroup)
	if err != nil {
		log.Fatalf("create instance notify queue failed with %+v", err)
	}

	time.Sleep(time.Second * 10)

	go func() {
		log.Fatalf("consumer queue failed with %+v", cli.ConsumeQueue(instanceID, metapb.NotifyGroup, func(value []byte) error {
			nt := metapb.Notify{}
			protoc.MustUnmarshal(&nt, value)
			log.Infof("notify: %+v", nt)
			return nil
		}))
	}()

	startInstance(instanceID, cli)
	log.Infof("workflow instance %d started", instanceID)
	time.Sleep(time.Second * 8)
	err = cli.StepInstance(metapb.Event{
		UserID:     1,
		TenantID:   10000,
		InstanceID: instanceID,
		Data: []metapb.KV{
			metapb.KV{Key: []byte("uid"), Value: []byte("1")},
		},
	})
	if err != nil {
		log.Fatalf("step instance failed with %+v", err)
	}

	err = cli.StepInstance(metapb.Event{
		UserID:     2,
		TenantID:   10000,
		InstanceID: instanceID,
		Data: []metapb.KV{
			metapb.KV{Key: []byte("uid"), Value: []byte("2")},
		},
	})
	if err != nil {
		log.Fatalf("step instance failed with %+v", err)
	}

	state, err := cli.InstanceCountState(instanceID)
	if err != nil {
		log.Fatalf("InstanceCountState failed with %+v", err)
	}

	log.Infof("Total: %d", state.Total)
	for _, st := range state.States {
		log.Infof("%s: %d", st.Step, st.Count)
	}
}

func createWorkflow(cli client.Client) uint64 {
	wf := metapb.Workflow{
		TenantID: 10000,
		Name:     "test_wf",
		Duration: 5,
		Steps: []metapb.Step{
			metapb.Step{
				Name: "step_0",
				Execution: metapb.Execution{
					Type: metapb.Branch,
					Branches: []metapb.ConditionExecution{
						metapb.ConditionExecution{
							Condition: metapb.Expr{
								Value: []byte("{num: event.uid} == 1"),
							},
							Execution: metapb.Execution{
								Direct: &metapb.DirectExecution{
									NextStep: "step_1_end",
								},
							},
						},
						metapb.ConditionExecution{
							Condition: metapb.Expr{
								Value: []byte("{num: event.uid} == 2"),
							},
							Execution: metapb.Execution{
								Direct: &metapb.DirectExecution{
									NextStep: "step_2_end",
								},
							},
						},
					},
				},
				EnterAction: "into step_0",
				LeaveAction: "leave step_0",
			},
			metapb.Step{
				Name: "step_1_end",
				Execution: metapb.Execution{
					Type:   metapb.Direct,
					Direct: &metapb.DirectExecution{},
				},
				EnterAction: "into step_1_end",
				LeaveAction: "leave step_1_end",
			},
			metapb.Step{
				Name: "step_2_end",
				Execution: metapb.Execution{
					Type:   metapb.Direct,
					Direct: &metapb.DirectExecution{},
				},
				EnterAction: "into step_2_end",
				LeaveAction: "leave step_2_end",
			},
		},
	}

	id, err := cli.CreateWorkflow(wf)
	if err != nil {
		log.Fatalf("create workflow failed with %+v", err)
	}

	return id
}

func createInstance(id uint64, cli client.Client, uids ...uint32) uint64 {
	bm := util.AcquireBitmap()
	bm.AddMany(uids)
	instance, err := cli.CreateInstance(id, util.MustMarshalBM(bm), 2)
	if err != nil {
		log.Fatalf("create workflow instance failed with %+v", err)
	}

	return instance
}

func startInstance(id uint64, cli client.Client) {
	err := cli.StartInstance(id)
	if err != nil {
		log.Fatalf("start workflow instance failed with %+v", err)
	}
}

func createTimerWorkflow(cli client.Client) uint64 {
	wf := metapb.Workflow{
		TenantID: 10000,
		Name:     "test_timer_wf",
		Duration: 1000,
		Steps: []metapb.Step{
			metapb.Step{
				Name: "step_0",
				Execution: metapb.Execution{
					Type: metapb.Timer,
					Timer: &metapb.TimerExecution{
						Condition: &metapb.Expr{
							Value: []byte("{bm: kv.bm1} && {bm: kv.bm2}"),
							Type:  metapb.BMResult,
						},
						Cron:     "*/5 * * * * ?",
						NextStep: "step_1",
					},
				},
				EnterAction: "into step_0",
				LeaveAction: "leave step_0",
			},
			metapb.Step{
				Name: "step_1",
				Execution: metapb.Execution{
					Type: metapb.Branch,
					Branches: []metapb.ConditionExecution{
						metapb.ConditionExecution{
							Condition: metapb.Expr{
								Value: []byte("{num: event.uid} == 1"),
							},
							Execution: metapb.Execution{
								Direct: &metapb.DirectExecution{
									NextStep: "step_1_end",
								},
							},
						},
						metapb.ConditionExecution{
							Condition: metapb.Expr{
								Value: []byte("{num: event.uid} == 2"),
							},
							Execution: metapb.Execution{
								Direct: &metapb.DirectExecution{
									NextStep: "step_2_end",
								},
							},
						},
					},
				},
				EnterAction: "into step_1",
				LeaveAction: "leave step_1",
			},
			metapb.Step{
				Name: "step_1_end",
				Execution: metapb.Execution{
					Type:   metapb.Direct,
					Direct: &metapb.DirectExecution{},
				},
				EnterAction: "into step_1_end",
				LeaveAction: "leave step_1_end",
			},
			metapb.Step{
				Name: "step_2_end",
				Execution: metapb.Execution{
					Type:   metapb.Direct,
					Direct: &metapb.DirectExecution{},
				},
				EnterAction: "into step_2_end",
				LeaveAction: "leave step_2_end",
			},
		},
	}

	id, err := cli.CreateWorkflow(wf)
	if err != nil {
		log.Fatalf("create workflow failed with %+v", err)
	}

	return id
}
