package expr

import (
	"bytes"
	"fmt"
	"time"

	engine "github.com/fagongzi/expr"
	"github.com/fagongzi/log"
	"github.com/fagongzi/util/format"
	"github.com/fagongzi/util/hack"
)

var (
	uid = []byte("uid")
)

// dyna.xxx%+vxxx.year
type dynamicKVVar struct {
	pattern   string
	attr      []byte
	valueType engine.VarType

	currentKeyFunc func(Ctx) ([]byte, error)
}

func newDynamicKVVar(pattern string, dynamics []string, valueType engine.VarType) (engine.Expr, error) {
	expr := &dynamicKVVar{
		pattern:   pattern,
		valueType: valueType,
	}

	switch dynamics[0] {
	case "year":
		expr.currentKeyFunc = expr.getCurrentYearKey
	case "month":
		expr.currentKeyFunc = expr.getCurrentMonthKey
	case "day":
		expr.currentKeyFunc = expr.getCurrentDayKey
	case "event":
		if len(dynamics) != 2 {
			return nil, fmt.Errorf("event dynamic need a attr")
		}
		expr.attr = []byte(dynamics[1])
		expr.currentKeyFunc = expr.getFromEvent
	case "kv":
		if len(dynamics) != 2 {
			return nil, fmt.Errorf("kv dynamic need a attr")
		}
		expr.attr = []byte(dynamics[1])
		expr.currentKeyFunc = expr.getFromKV
	case "profile":
		if len(dynamics) != 2 {
			return nil, fmt.Errorf("profile dynamic need a attr")
		}
		expr.attr = []byte(dynamics[1])
		expr.currentKeyFunc = expr.getFromProfile
	default:
		return nil, fmt.Errorf("%s dynamic not support", dynamics[0])
	}

	return expr, nil
}

func (v *dynamicKVVar) Exec(data interface{}) (interface{}, error) {
	ctx, ok := data.(Ctx)
	if !ok {
		log.Fatalf("BUG: invalid expr ctx type %T", ctx)
	}

	attr, err := v.currentKeyFunc(ctx)
	if err != nil {
		return nil, err
	}

	value, err := ctx.KV(attr)
	if err != nil {
		return nil, err
	}

	return byValueType(value, v.valueType)
}

func (v *dynamicKVVar) getCurrentYearKey(ctx Ctx) ([]byte, error) {
	return hack.StringToSlice(fmt.Sprintf(v.pattern, time.Now().Year())), nil
}

func (v *dynamicKVVar) getCurrentMonthKey(ctx Ctx) ([]byte, error) {
	return hack.StringToSlice(fmt.Sprintf(v.pattern, time.Now().Month())), nil
}

func (v *dynamicKVVar) getCurrentDayKey(ctx Ctx) ([]byte, error) {
	return hack.StringToSlice(fmt.Sprintf(v.pattern, time.Now().Day())), nil
}

func (v *dynamicKVVar) getFromEvent(ctx Ctx) ([]byte, error) {
	if bytes.Compare(uid, v.attr) == 0 {
		return format.Uint32ToBytes(ctx.Event().UserID), nil
	}

	for _, kv := range ctx.Event().Data {
		if bytes.Compare(kv.Key, v.attr) == 0 {
			return hack.StringToSlice(fmt.Sprintf(v.pattern, hack.SliceToString(kv.Value))), nil
		}
	}

	return nil, nil
}

func (v *dynamicKVVar) getFromKV(ctx Ctx) ([]byte, error) {
	value, err := ctx.KV(v.attr)
	if err != nil {
		return nil, err
	}

	return hack.StringToSlice(fmt.Sprintf(v.pattern, hack.SliceToString(value))), nil
}

func (v *dynamicKVVar) getFromProfile(ctx Ctx) ([]byte, error) {
	value, err := ctx.Profile(v.attr)
	if err != nil {
		return nil, err
	}

	return hack.StringToSlice(fmt.Sprintf(v.pattern, hack.SliceToString(value))), nil
}
