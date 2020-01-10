package expr

import (
	"bytes"
	"fmt"
	engine "github.com/fagongzi/expr"
	"github.com/fagongzi/log"
	"github.com/fagongzi/util/format"
	"github.com/fagongzi/util/hack"
)

type eventVar struct {
	attr      []byte
	valueType string
}

func newEventVar(attr []byte, valueType string) engine.Expr {
	return &eventVar{
		attr:      attr,
		valueType: valueType,
	}
}

func (v *eventVar) Exec(value interface{}) (interface{}, error) {
	ctx, ok := value.(Ctx)
	if !ok {
		log.Fatalf("BUG: invalid expr ctx type %T", ctx)
	}

	valueIndex := -1
	src := ctx.Event()
	for idx, kv := range src.Data {
		if bytes.Compare(kv.Key, v.attr) == 0 {
			valueIndex = idx
			break
		}
	}

	switch v.valueType {
	case stringVar:
		if valueIndex == -1 {
			return "", nil
		}

		return hack.SliceToString(src.Data[valueIndex].Value), nil
	case int64Var:
		if valueIndex == -1 {
			return 0, nil
		}

		return format.ParseStrInt64(hack.SliceToString(src.Data[valueIndex].Value))
	case bitmapVar:
		return nil, fmt.Errorf("bitmap can not with event var")
	default:
		return nil, fmt.Errorf("not support var type %s", v.valueType)
	}
}
