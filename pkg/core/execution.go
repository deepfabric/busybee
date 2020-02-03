package core

import (
	"fmt"

	"github.com/RoaringBitmap/roaring"
	"github.com/deepfabric/busybee/pkg/expr"
	"github.com/deepfabric/busybee/pkg/pb/metapb"
)

type stepChangedFunc func(batch *executionbatch) error

type excution interface {
	Execute(expr.Ctx, stepChangedFunc, *executionbatch) error
}

func checkExcution(workflow metapb.Workflow) error {
	for _, step := range workflow.Steps {
		_, err := newExcution(step.Name, step.Execution)
		if err != nil {
			return err
		}
	}

	return nil
}

func newExcution(currentStep string, exec metapb.Execution) (excution, error) {
	switch exec.Type {
	case metapb.Direct:
		if exec.Direct == nil {
			return nil, fmt.Errorf("Missing Direct Execution")
		}

		return &directExecution{
			step:     currentStep,
			nextStep: exec.Direct.NextStep,
		}, nil
	case metapb.Timer:
		if exec.Timer == nil {
			return nil, fmt.Errorf("Missing Timer Execution")
		}

		var exprRuntime expr.Runtime
		if exec.Timer.Condition != nil {
			r, err := expr.NewRuntime(*exec.Timer.Condition)
			if err != nil {
				return nil, err
			}
			exprRuntime = r
		}

		return &conditionExecution{
			conditionExpr: exprRuntime,
			exec: &directExecution{
				step:     currentStep,
				nextStep: exec.Timer.NextStep,
			},
		}, nil
	case metapb.Branch:
		if len(exec.Branches) < 2 {
			return nil, fmt.Errorf("Branch count must > 1, but %d", len(exec.Branches))
		}

		value := &branchExecution{}
		for _, branch := range exec.Branches {
			r, err := expr.NewRuntime(branch.Condition)
			if err != nil {
				return nil, err
			}

			if branch.Execution != nil {
				exec, err := newExcution(currentStep, *branch.Execution)
				if err != nil {
					return nil, err
				}

				value.branches = append(value.branches, &conditionExecution{
					conditionExpr: r,
					exec:          exec,
				})
			} else {
				value.branches = append(value.branches, &conditionExecution{
					conditionExpr: r,
					exec: &directExecution{
						nextStep: branch.NextStep,
					},
				})
			}
		}

		return value, nil
	case metapb.Parallel:
		if len(exec.Parallel.Parallels) < 2 {
			return nil, fmt.Errorf("Parallels count must > 1, but %d",
				len(exec.Parallel.Parallels))
		}

		value := &parallelExecution{
			step:     currentStep,
			nextStep: exec.Parallel.NextStep,
		}
		for _, parallel := range exec.Parallel.Parallels {
			exec, err := newExcution(currentStep, parallel)
			if err != nil {
				return nil, err
			}

			value.exectuors = append(value.exectuors, exec)
		}

		return value, nil
	}

	return nil, nil
}

type directExecution struct {
	step     string
	nextStep string
}

func (e *directExecution) Execute(ctx expr.Ctx, cb stepChangedFunc, batch *executionbatch) error {
	batch.event = ctx.Event()
	batch.from = e.step
	batch.to = e.nextStep
	return cb(batch)
}

type conditionExecution struct {
	conditionExpr expr.Runtime
	exec          excution
}

func (e *conditionExecution) Execute(ctx expr.Ctx, cb stepChangedFunc, batch *executionbatch) error {
	if e.conditionExpr != nil {
		matches, value, err := e.conditionExpr.Exec(ctx)
		if err != nil {
			return err
		}

		if !matches {
			return nil
		}

		if bm, ok := value.(*roaring.Bitmap); ok {
			batch.crowd = bm
			return e.exec.Execute(ctx, cb, batch)
		}
	}

	return e.exec.Execute(ctx, cb, batch)
}

type branchExecution struct {
	branches []excution
}

func (e *branchExecution) Execute(ctx expr.Ctx, cb stepChangedFunc, batch *executionbatch) error {
	for _, exec := range e.branches {
		err := exec.Execute(ctx, cb, batch)
		if err != nil {
			return err
		}
	}

	return nil
}

type parallelExecution struct {
	step      string
	nextStep  string
	exectuors []excution
}

func (e *parallelExecution) Execute(ctx expr.Ctx, cb stepChangedFunc, batch *executionbatch) error {
	for _, exec := range e.exectuors {
		err := exec.Execute(ctx, cb, batch)
		if err != nil {
			return err
		}

	}

	batch.event = ctx.Event()
	batch.from = e.step
	batch.to = e.nextStep
	return cb(batch)
}
