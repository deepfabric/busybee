package expr

import (
	"bytes"
	"fmt"
	"time"

	"github.com/deepfabric/busybee/pkg/util"
	engine "github.com/fagongzi/expr"
	"github.com/fagongzi/log"
	"github.com/fagongzi/util/format"
	"github.com/fagongzi/util/hack"
)

var (
	uid = []byte("uid")
	tid = []byte("tid")
	wid = []byte("wid")
)

// dyna.xxx%+vxxx.year
type dynamicKVVar struct {
	pattern   string
	attr      []byte
	valueType engine.VarType

	currentKeyFunc func(Ctx, bool) ([]byte, error)
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

	attr, err := v.currentKeyFunc(ctx, false)
	if err != nil {
		return nil, err
	}

	value, err := ctx.KV(attr)
	if err != nil {
		return nil, err
	}

	return byValueType(value, v.valueType)
}

func (v *dynamicKVVar) getCurrentYearKey(ctx Ctx, onlyKey bool) ([]byte, error) {
	return hack.StringToSlice(fmt.Sprintf(v.pattern, time.Now().Year())), nil
}

func (v *dynamicKVVar) getCurrentMonthKey(ctx Ctx, onlyKey bool) ([]byte, error) {
	return hack.StringToSlice(fmt.Sprintf(v.pattern, time.Now().Month())), nil
}

func (v *dynamicKVVar) getCurrentDayKey(ctx Ctx, onlyKey bool) ([]byte, error) {
	return hack.StringToSlice(fmt.Sprintf(v.pattern, time.Now().Day())), nil
}

func (v *dynamicKVVar) getFromEvent(ctx Ctx, onlyKey bool) ([]byte, error) {
	var value []byte
	if bytes.Compare(uid, v.attr) == 0 {
		value = format.UInt64ToString(uint64(ctx.Event().UserID))
	} else if bytes.Compare(tid, v.attr) == 0 {
		value = format.UInt64ToString(uint64(ctx.Event().TenantID))
	} else if bytes.Compare(wid, v.attr) == 0 {
		value = format.UInt64ToString(uint64(ctx.Event().WorkflowID))
	} else {
		for _, kv := range ctx.Event().Data {
			if bytes.Compare(kv.Key, v.attr) == 0 {
				value = kv.Value
				break
			}
		}
	}

	return hack.StringToSlice(fmt.Sprintf(v.pattern, hack.SliceToString(value))), nil
}

func (v *dynamicKVVar) getFromKV(ctx Ctx, onlyKey bool) ([]byte, error) {
	if onlyKey {
		return v.attr, nil
	}

	value, err := ctx.KV(v.attr)
	if err != nil {
		return nil, err
	}

	return hack.StringToSlice(fmt.Sprintf(v.pattern, hack.SliceToString(value))), nil
}

func (v *dynamicKVVar) getFromProfile(ctx Ctx, onlyKey bool) ([]byte, error) {
	if onlyKey {
		return ctx.Profile(v.attr), nil
	}

	value, err := ctx.KV(ctx.Profile(v.attr))
	if err != nil {
		return nil, err
	}

	if len(value) == 0 {
		return nil, nil
	}

	return hack.StringToSlice(fmt.Sprintf(v.pattern, hack.SliceToString(util.ExtractJSONField(value, hack.SliceToString(v.attr))))), nil
}

func (v *dynamicKVVar) DynaKey(ctx Ctx) ([]byte, error) {
	key, err := v.currentKeyFunc(ctx, true)
	if err != nil {
		return nil, err
	}

	return key, nil
}
