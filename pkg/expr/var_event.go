package expr

import (
	"bytes"
	engine "github.com/fagongzi/expr"
	"github.com/fagongzi/log"
)

type eventVar struct {
	attr      []byte
	valueType engine.VarType
}

func newEventVar(attr []byte, valueType engine.VarType) engine.Expr {
	return &eventVar{
		attr:      attr,
		valueType: valueType,
	}
}

func (v *eventVar) Exec(data interface{}) (interface{}, error) {
	ctx, ok := data.(Ctx)
	if !ok {
		log.Fatalf("BUG: invalid expr ctx type %T", ctx)
	}

	var value []byte
	src := ctx.Event()
	for idx := range src.Data {
		if bytes.Compare(src.Data[idx].Key, v.attr) == 0 {
			value = src.Data[idx].Value
			break
		}
	}

	return byValueType(value, v.valueType)
}
