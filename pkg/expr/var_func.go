package expr

import (
	"fmt"
	"time"

	engine "github.com/fagongzi/expr"
	"github.com/fagongzi/log"
)

// func.name
type funcVar struct {
	valueType engine.VarType
	dynaFunc  func(Ctx) (interface{}, error)
}

func newFuncVar(name string, valueType engine.VarType) (engine.Expr, error) {
	expr := &funcVar{
		valueType: valueType,
	}

	switch name {
	case "year":
		expr.dynaFunc = yearFunc
	case "month":
		expr.dynaFunc = monthFunc
	case "day":
		expr.dynaFunc = dayFunc
	case "week":
		expr.dynaFunc = weekFunc
	case "time":
		expr.dynaFunc = timeFunc
	case "date":
		expr.dynaFunc = dateFunc
	case "datetime":
		expr.dynaFunc = datetimeFunc
	case "wf_step_crowd":
		expr.dynaFunc = stepCrowdFunc
	case "wf_step_ttl":
		expr.dynaFunc = stepTTLFunc
	default:
		return nil, fmt.Errorf("func %s not support", name)
	}

	return expr, nil
}

func (v *funcVar) Exec(data interface{}) (interface{}, error) {
	ctx, ok := data.(Ctx)
	if !ok {
		log.Fatalf("BUG: invalid expr ctx type %T", ctx)
	}

	value, err := v.dynaFunc(ctx)
	if err != nil {
		return nil, err
	}

	return convertByType(value, v.valueType)
}

func weekFunc(ctx Ctx) (interface{}, error) {
	return int64(time.Now().Weekday()), nil
}

func timeFunc(ctx Ctx) (interface{}, error) {
	return time.Now().Format("150405"), nil
}

func datetimeFunc(ctx Ctx) (interface{}, error) {
	return time.Now().Format("20060102150405"), nil
}

func dateFunc(ctx Ctx) (interface{}, error) {
	return time.Now().Format("20060102"), nil
}

func yearFunc(ctx Ctx) (interface{}, error) {
	return int64(time.Now().Year()), nil
}

func monthFunc(ctx Ctx) (interface{}, error) {
	return int64(time.Now().Month()), nil
}

func dayFunc(ctx Ctx) (interface{}, error) {
	return int64(time.Now().Day()), nil
}

func stepCrowdFunc(ctx Ctx) (interface{}, error) {
	return ctx.StepCrowd(), nil
}

func stepTTLFunc(ctx Ctx) (interface{}, error) {
	return ctx.StepTTL()
}
