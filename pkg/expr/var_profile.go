package expr

import (
	engine "github.com/fagongzi/expr"
	"github.com/fagongzi/log"
)

type profileVar struct {
	attr      []byte
	valueType engine.VarType
}

func newProfileVar(attr []byte, valueType engine.VarType) engine.Expr {
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

	return byValueType(value, v.valueType)
}
