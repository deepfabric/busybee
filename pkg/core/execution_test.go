package core

import (
	"testing"

	"github.com/deepfabric/busybee/pkg/pb/metapb"
	"github.com/stretchr/testify/assert"
)

func TestDirect(t *testing.T) {
	step := metapb.Step{
		Name: "test",
		Execution: metapb.Execution{
			Type:   metapb.Direct,
			Direct: &metapb.DirectExecution{},
		},
	}
	_, err := newExcution(step.Name, step.Execution)
	assert.NoError(t, err, "TestDirect failed")
}

func TestTimer(t *testing.T) {
	step := metapb.Step{
		Name: "test",
		Execution: metapb.Execution{
			Type: metapb.Timer,
			Timer: &metapb.TimerExecution{
				Cron: "* * * * * * * *",
			},
		},
	}
	_, err := newExcution(step.Name, step.Execution)
	assert.NoError(t, err, "TestTimer failed")
}

func TestCondition(t *testing.T) {
	step := metapb.Step{
		Name: "test",
		Execution: metapb.Execution{
			Type: metapb.Branch,
			Branches: []metapb.ConditionExecution{
				metapb.ConditionExecution{
					Condition: metapb.Expr{
						Value: []byte("1 == 1"),
					},
					NextStep: "end",
				},
				metapb.ConditionExecution{
					Condition: metapb.Expr{
						Value: []byte("1 == 1"),
					},
					Execution: &metapb.Execution{
						Type: metapb.Direct,
						Direct: &metapb.DirectExecution{
							NextStep: "end",
						},
					},
				},
			},
		},
	}
	_, err := newExcution(step.Name, step.Execution)
	assert.NoError(t, err, "TestCondition failed")
}
