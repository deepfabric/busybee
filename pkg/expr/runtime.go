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
	Keys(Ctx, bool, func([]byte)) error
}

type fixedVarExpr interface {
	Key(Ctx) ([]byte, error)
}

type dynaVarExpr interface {
	DynaKey(Ctx) ([]byte, error)
}

type runtime struct {
	meta            metapb.Expr
	expr            engine.Expr
	fixed           []fixedVarExpr
	dynas           []dynaVarExpr
	fixedKeys       [][]byte
	fixedKeysParsed bool
	dynaKeys        [][]byte
	dynaKeysParsed  bool
}

// NewRuntime create a expr runtime
func NewRuntime(meta metapb.Expr) (Runtime, error) {
	var fixed []fixedVarExpr
	var dynas []dynaVarExpr
	v, err := parser.Parse(meta.Value, func(expr engine.Expr) {
		if v, ok := expr.(fixedVarExpr); ok {
			fixed = append(fixed, v)
		} else if v, ok := expr.(dynaVarExpr); ok {
			dynas = append(dynas, v)
		}
	})
	if err != nil {
		return nil, err
	}

	return &runtime{
		meta:  meta,
		expr:  v,
		fixed: fixed,
		dynas: dynas,
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

func (rt *runtime) Keys(ctx Ctx, resetDyna bool, cb func([]byte)) error {
	err := rt.handleDynaKeys(ctx, resetDyna, cb)
	if err != nil {
		return err
	}

	return rt.handleFixedKeys(ctx, cb)
}

func (rt *runtime) handleFixedKeys(ctx Ctx, cb func([]byte)) error {
	if !rt.fixedKeysParsed {
		rt.fixedKeys = rt.fixedKeys[:0]
		for _, kv := range rt.fixed {
			v, err := kv.Key(ctx)
			if err != nil {
				return err
			}

			rt.fixedKeys = append(rt.fixedKeys, v)
		}

		rt.fixedKeysParsed = true

	}

	for _, key := range rt.fixedKeys {
		cb(key)
	}

	return nil
}

func (rt *runtime) handleDynaKeys(ctx Ctx, resetDyna bool, cb func([]byte)) error {
	if resetDyna {
		rt.dynaKeysParsed = false
		rt.dynaKeys = rt.dynaKeys[:0]
	}

	if !rt.dynaKeysParsed {
		for _, kv := range rt.dynas {
			v, err := kv.DynaKey(ctx)
			if err != nil {
				return err
			}

			rt.dynaKeys = append(rt.dynaKeys, v)
		}

		rt.dynaKeysParsed = true
	}

	for _, key := range rt.dynaKeys {
		cb(key)
	}

	return nil
}
