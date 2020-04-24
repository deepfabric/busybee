package expr

import (
	"github.com/deepfabric/busybee/pkg/util"
	engine "github.com/fagongzi/expr"
	"github.com/fagongzi/log"
	"github.com/fagongzi/util/hack"
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

	key := ctx.Profile(v.attr)
	value, err := ctx.KV(key)
	if err != nil {
		return nil, err
	}

	if len(value) == 0 {
		return byValueType(value, v.valueType)
	}

	return byValueType(util.ExtractJSONField(value, hack.SliceToString(v.attr)), v.valueType)
}

func (v *profileVar) Key(ctx Ctx) ([]byte, error) {
	return ctx.Profile(v.attr), nil
}
