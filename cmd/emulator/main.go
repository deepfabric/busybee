package main

import (
	"flag"

	"github.com/deepfabric/busybee/pkg/client"
	"github.com/deepfabric/busybee/pkg/pb/metapb"
	"github.com/deepfabric/busybee/pkg/util"
	"github.com/fagongzi/log"
)

var (
	api = flag.String("addr", "127.0.0.1:9091", "api address")
)

func main() {
	cli := client.NewClient(*api)
	id := createWorkflow(cli)
	log.Infof("workflow %d created", id)
	id = createInstance(id, cli)
	log.Infof("workflow instance %d created", id)
	startInstance(id, cli)
	log.Infof("workflow instance %d started", id)
}

func createWorkflow(cli client.Client) uint64 {
	wf := metapb.Workflow{
		TenantID: 10000,
		Name:     "test_wf",
		Steps: []metapb.Step{
			metapb.Step{
				Name: "step_0",
				Execution: metapb.Execution{
					Type: metapb.Branch,
					Branches: []metapb.ConditionExecution{
						metapb.ConditionExecution{
							Condition: metapb.Expr{
								Type:    metapb.Number,
								Sources: []string{"uid"},
								Cmp:     metapb.Equal,
								Expect:  "1",
							},
							Execution: metapb.Execution{
								Direct: &metapb.DirectExecution{
									NextStep: "step_1_end",
								},
							},
						},
						metapb.ConditionExecution{
							Condition: metapb.Expr{
								Type:    metapb.Number,
								Sources: []string{"uid"},
								Cmp:     metapb.Equal,
								Expect:  "2",
							},
							Execution: metapb.Execution{
								Direct: &metapb.DirectExecution{
									NextStep: "step_2_end",
								},
							},
						},
					},
				},
			},
			metapb.Step{
				Name: "step_1_end",
				Execution: metapb.Execution{
					Type:   metapb.Direct,
					Direct: &metapb.DirectExecution{},
				},
			},
			metapb.Step{
				Name: "step_2_end",
				Execution: metapb.Execution{
					Type:   metapb.Direct,
					Direct: &metapb.DirectExecution{},
				},
			},
		},
	}

	id, err := cli.CreateWorkflow(wf)
	if err != nil {
		log.Fatalf("create workflow failed with %+v", err)
	}

	return id
}

func createInstance(id uint64, cli client.Client) uint64 {
	bm := util.AcquireBitmap()
	bm.Add(1)
	bm.Add(2)
	bm.Add(3)
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
