package core

import (
	"fmt"

	"github.com/RoaringBitmap/roaring"
	"github.com/deepfabric/busybee/pkg/expr"
	"github.com/deepfabric/busybee/pkg/pb/metapb"
	"github.com/deepfabric/busybee/pkg/util"
)

type who struct {
	user  uint32
	users *roaring.Bitmap
}

type changedCtx struct {
	from string
	to   string
	who  who
	ttl  int32
}

func (ctx changedCtx) user() uint32 {
	if ctx.who.users.GetCardinality() == 1 {
		return ctx.who.users.Minimum()
	}

	return 0
}

func (ctx changedCtx) crowd() []byte {
	if nil == ctx.who.users {
		return nil
	}

	return util.MustMarshalBM(ctx.who.users)
}

func (ctx *changedCtx) add(changed changedCtx) {
	if changed.who.user > 0 {
		ctx.who.users.Add(changed.who.user)
	} else {
		ctx.who.users.Or(changed.who.users)
	}
}

type stepChangedFunc func(batch *executionbatch, ctx changedCtx) error

type excution interface {
	Execute(expr.Ctx, stepChangedFunc, *executionbatch, who) error
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
			step:          currentStep,
			nextStep:      exec.Timer.NextStep,
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
					step:          currentStep,
					nextStep:      branch.NextStep,
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

func (e *directExecution) Execute(ctx expr.Ctx, cb stepChangedFunc,
	batch *executionbatch, target who) error {
	return cb(batch, changedCtx{e.step, e.nextStep, target, 0})
}

type conditionExecution struct {
	conditionExpr expr.Runtime
	exec          excution
	step          string
	nextStep      string
}

func (e *conditionExecution) executeWithMatches(ctx expr.Ctx, cb stepChangedFunc,
	batch *executionbatch, target who) (bool, error) {
	if e.conditionExpr != nil {
		matches, value, err := e.conditionExpr.Exec(ctx)
		if err != nil {
			return false, err
		}

		if !matches {
			return false, nil
		}

		if bm, ok := value.(*roaring.Bitmap); ok {
			target = who{0, bm}
		}
	}

	if e.exec != nil {
		err := e.exec.Execute(ctx, cb, batch, target)
		if err != nil {
			return false, err
		}

		return true, nil
	}

	return true, cb(batch, changedCtx{e.step, e.nextStep, target, 0})
}

func (e *conditionExecution) Execute(ctx expr.Ctx, cb stepChangedFunc,
	batch *executionbatch, target who) error {
	if e.conditionExpr != nil {
		matches, value, err := e.conditionExpr.Exec(ctx)
		if err != nil {
			return err
		}

		if !matches {
			return nil
		}

		if bm, ok := value.(*roaring.Bitmap); ok {
			target = who{0, bm}
		}
	}

	if e.exec != nil {
		err := e.exec.Execute(ctx, cb, batch, target)
		if err != nil {
			return err
		}

		return nil
	}

	return cb(batch, changedCtx{e.step, e.nextStep, target, 0})
}

type branchExecution struct {
	branches []*conditionExecution
}

func (e *branchExecution) Execute(ctx expr.Ctx, cb stepChangedFunc,
	batch *executionbatch, target who) error {
	for _, exec := range e.branches {
		ok, err := exec.executeWithMatches(ctx, cb, batch, target)
		if err != nil {
			return err
		}

		if ok {
			break
		}
	}

	return nil
}

type parallelExecution struct {
	step      string
	nextStep  string
	exectuors []excution
}

func (e *parallelExecution) Execute(ctx expr.Ctx, cb stepChangedFunc,
	batch *executionbatch, target who) error {
	for _, exec := range e.exectuors {
		err := exec.Execute(ctx, cb, batch, target)
		if err != nil {
			return err
		}
	}

	return cb(batch, changedCtx{e.step, e.nextStep, target, 0})
}
