package core

import (
	"github.com/RoaringBitmap/roaring"
	"github.com/deepfabric/busybee/pkg/expr"
	"github.com/deepfabric/busybee/pkg/pb/metapb"
)

type stepChangedFunc func(batch *executionbatch) error

type excution interface {
	Execute(metapb.Event, stepChangedFunc, *executionbatch) error
}

func newExcution(currentStep string,
	exec metapb.Execution,
	fetcher expr.ValueFetcher) (excution, error) {
	switch exec.Type {
	case metapb.Direct:
		return &directExecution{
			step:     currentStep,
			nextStep: exec.Direct.NextStep,
		}, nil
	case metapb.Timer:
		var exprRuntime expr.Runtime
		if exec.Timer.Condition != nil {
			r, err := expr.NewRuntime(*exec.Timer.Condition, fetcher)
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
		value := &branchExecution{}
		for _, branch := range exec.Branches {
			r, err := expr.NewRuntime(branch.Condition, fetcher)
			if err != nil {
				return nil, err
			}

			exec, err := newExcution(currentStep, branch.Execution, fetcher)
			if err != nil {
				return nil, err
			}

			value.branches = append(value.branches, &conditionExecution{
				conditionExpr: r,
				exec:          exec,
			})
		}

		return value, nil
	case metapb.Parallel:
		value := &parallelExecution{
			step:     currentStep,
			nextStep: exec.Parallel.NextStep,
		}
		for _, parallel := range exec.Parallel.Parallels {
			exec, err := newExcution(currentStep, parallel, fetcher)
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

func (e *directExecution) Execute(event metapb.Event, cb stepChangedFunc, batch *executionbatch) error {
	batch.event = event
	batch.from = e.step
	batch.to = e.nextStep
	return cb(batch)
}

type conditionExecution struct {
	conditionExpr expr.Runtime
	exec          excution
}

func (e *conditionExecution) Execute(event metapb.Event, cb stepChangedFunc, batch *executionbatch) error {
	if e.conditionExpr != nil {
		matches, value, err := e.conditionExpr.Exec(&event)
		if err != nil {
			return err
		}

		if !matches {
			return nil
		}

		if bm, ok := value.(*roaring.Bitmap); ok {
			batch.crowd = bm
			return e.exec.Execute(event, cb, batch)
		}
	}

	return e.exec.Execute(event, cb, batch)
}

type branchExecution struct {
	branches []excution
}

func (e *branchExecution) Execute(event metapb.Event, cb stepChangedFunc, batch *executionbatch) error {
	for _, exec := range e.branches {
		err := exec.Execute(event, cb, batch)
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

func (e *parallelExecution) Execute(event metapb.Event, cb stepChangedFunc, batch *executionbatch) error {
	for _, exec := range e.exectuors {
		err := exec.Execute(event, cb, batch)
		if err != nil {
			return err
		}

	}

	batch.event = event
	batch.from = e.step
	batch.to = e.nextStep
	return cb(batch)
}
