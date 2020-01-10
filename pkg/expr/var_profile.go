package expr

import (
	"fmt"
	engine "github.com/fagongzi/expr"
	"github.com/fagongzi/log"
	"github.com/fagongzi/util/format"
	"github.com/fagongzi/util/hack"
)

type profileVar struct {
	attr      []byte
	valueType string
}

func newProfileVar(attr []byte, valueType string) engine.Expr {
	return &profileVar{
		attr:      attr,
		valueType: valueType,
	}
}

func (v *profileVar) Exec(data interface{}) (interface{}, error) {
	ctx, ok := data.(Ctx)
	if !ok {
		log.Fatalf("BUG: invalid expr ctx type %T", ctx)
	}

	value, err := ctx.Profile(v.attr)
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
		return nil, fmt.Errorf("bitmap can not with profile var")
	default:
		return nil, fmt.Errorf("not support var type %s", v.valueType)
	}
}
