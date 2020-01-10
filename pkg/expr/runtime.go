package expr

import (
	"github.com/RoaringBitmap/roaring"
	"github.com/deepfabric/busybee/pkg/pb/metapb"
	engine "github.com/fagongzi/expr"
	"github.com/fagongzi/log"
)

// Runtime expr runtime
type Runtime interface {
	// Exec calc result using src event
	Exec(Ctx) (bool, interface{}, error)
}

type runtime struct {
	meta metapb.Expr
	expr engine.Expr
}

// NewRuntime create a expr runtime
func NewRuntime(meta metapb.Expr) (Runtime, error) {
	v, err := parser.Parse(meta.Value)
	if err != nil {
		return nil, err
	}

	return &runtime{
		meta: meta,
		expr: v,
	}, nil
}

func (rt *runtime) Exec(ctx Ctx) (bool, interface{}, error) {
	value, err := rt.expr.Exec(ctx)
	if err != nil {
		return false, nil, err
	}

	switch rt.meta.Type {
	case metapb.BoolResult:
		return value.(bool), nil, nil
	case metapb.BMResult:
		bm := value.(*roaring.Bitmap)
		if bm.GetCardinality() == 0 {
			return false, nil, nil
		}

		return true, bm, nil
	}

	log.Fatalf("BUG: never come here!")
	return false, nil, nil
}
