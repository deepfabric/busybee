package expr

import (
	"fmt"
	"time"

	"github.com/deepfabric/busybee/pkg/util"
	engine "github.com/fagongzi/expr"
	"github.com/fagongzi/log"
	"github.com/fagongzi/util/format"
	"github.com/fagongzi/util/hack"
)

// dyna.xxx%+vxxx.year
type dynamicKVVar struct {
	pattern   string
	dynamic   string
	valueType string

	currentKeyFunc func() []byte
}

func newDynamicKVVar(pattern, dynamic string, valueType string) (engine.Expr, error) {
	expr := &dynamicKVVar{
		pattern:   pattern,
		dynamic:   dynamic,
		valueType: valueType,
	}

	switch dynamic {
	case "year":
		expr.currentKeyFunc = expr.getCurrentYearKey
	case "month":
		expr.currentKeyFunc = expr.getCurrentMonthKey
	case "day":
		expr.currentKeyFunc = expr.getCurrentDayKey
	default:
		return nil, fmt.Errorf("%s dynamic not support", dynamic)
	}

	return expr, nil
}

func (v *dynamicKVVar) Exec(data interface{}) (interface{}, error) {
	ctx, ok := data.(Ctx)
	if !ok {
		log.Fatalf("BUG: invalid expr ctx type %T", ctx)
	}

	value, err := ctx.KV(v.currentKeyFunc())
	if err != nil {
		return nil, err
	}

	switch v.valueType {
	case stringVar:
		if len(value) == 0 {
			return "", nil
		}

		return hack.SliceToString(value), nil
	case int64Var:
		if len(value) == 0 {
			return "", nil
		}

		return format.ParseStrInt64(hack.SliceToString(value))
	case bitmapVar:
		if len(value) == 0 {
			return emptyBM, nil
		}

		bm := util.AcquireBitmap()
		util.MustParseBMTo(value, bm)
		return bm, nil
	default:
		return nil, fmt.Errorf("not support var type %s", v.valueType)
	}
}

func (v *dynamicKVVar) getCurrentYearKey() []byte {
	return hack.StringToSlice(fmt.Sprintf(v.pattern, time.Now().Year()))
}

func (v *dynamicKVVar) getCurrentMonthKey() []byte {
	return hack.StringToSlice(fmt.Sprintf(v.pattern, time.Now().Month()))
}

func (v *dynamicKVVar) getCurrentDayKey() []byte {
	return hack.StringToSlice(fmt.Sprintf(v.pattern, time.Now().Day()))
}
