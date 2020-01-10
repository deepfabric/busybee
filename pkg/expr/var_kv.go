package expr

import (
	"fmt"

	"github.com/RoaringBitmap/roaring"
	"github.com/deepfabric/busybee/pkg/util"
	engine "github.com/fagongzi/expr"
	"github.com/fagongzi/log"
	"github.com/fagongzi/util/format"
	"github.com/fagongzi/util/hack"
)

var (
	emptyBM = roaring.New()
)

type kvVar struct {
	attr      []byte
	valueType string
}

func newKVVar(attr []byte, valueType string) engine.Expr {
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
