package expr

import (
	engine "github.com/fagongzi/expr"
	"github.com/fagongzi/log"
)

type kvVar struct {
	attr      []byte
	valueType engine.VarType
}

func newKVVar(attr []byte, valueType engine.VarType) engine.Expr {
	return &kvVar{
		attr:      attr,
		valueType: valueType,
	}
}

func (v *kvVar) Exec(data interface{}) (interface{}, error) {
	ctx, ok := data.(Ctx)
	if !ok {
		log.Fatalf("BUG: invalid expr ctx type %T", ctx)
	}

	value, err := ctx.KV(v.attr)
	if err != nil {
		return nil, err
	}

	return byValueType(value, v.valueType)
}
