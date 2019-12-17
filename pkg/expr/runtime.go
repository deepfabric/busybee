package expr

import (
	"fmt"
	"regexp"

	"github.com/deepfabric/busybee/pkg/pb/metapb"
	"github.com/deepfabric/busybee/pkg/util"
	"github.com/fagongzi/util/format"
	"github.com/fagongzi/util/hack"
)

// Runtime expr runtime
type Runtime interface {
	// Exec calc result using src event
	Exec(src *metapb.Event) (bool, interface{}, error)
}

type runtime struct {
	expr    metapb.Expr
	fetcher ValueFetcher

	pattern           *regexp.Regexp
	expectUint64Value uint64
}

// NewRuntime create a expr runtime
func NewRuntime(expr metapb.Expr, fetcher ValueFetcher) (Runtime, error) {
	var err error

	rt := &runtime{
		fetcher: fetcher,
		expr:    expr,
	}

	if expr.Cmp == metapb.Match {
		rt.pattern, err = regexp.Compile(expr.Expect)
		if err != nil {
			return nil, err
		}
	}

	if expr.Type != metapb.String {
		rt.expectUint64Value, err = format.ParseStrUInt64(expr.Expect)
		if err != nil {
			return nil, err
		}
	}

	return rt, nil
}

func (rt *runtime) Exec(src *metapb.Event) (bool, interface{}, error) {
	var ok bool
	var returnValue interface{}
	var actulValue interface{}
	var values []interface{}
	var err error

	ok, values, err = rt.fetchValue(src)
	if err != nil {
		return false, nil, err
	}

	if !ok {
		return false, nil, nil
	}

	switch rt.expr.Op {
	case metapb.Empty:
		ok, actulValue = rt.doFunc(values, src)
		returnValue = actulValue
	case metapb.BMAnd:
		values[0] = util.BMAndInterface(values...)
		ok, actulValue = rt.doFunc(values[:1], src)
		returnValue = values[0]
	case metapb.BMOr:
		values[0] = util.BMOrInterface(values...)
		ok, actulValue = rt.doFunc(values[:1], src)
		returnValue = values[0]
	case metapb.BMXor:
		values[0] = util.BMXOrInterface(values...)
		ok, actulValue = rt.doFunc(values[:1], src)
		returnValue = values[0]
	case metapb.BMAndNot:
		values[0] = util.BMAndnotInterface(values...)
		ok, actulValue = rt.doFunc(values[:1], src)
		returnValue = values[0]
	}

	if !ok {
		return false, nil, nil
	}

	switch actulValue.(type) {
	case string:
		ok, err = compareString(actulValue.(string), rt)
		return ok, returnValue, err
	case uint64:
		ok, err = compareUint64(actulValue.(uint64), rt)
		return ok, returnValue, err
	}

	return false, nil, fmt.Errorf("not support cmp type %+v and %T",
		actulValue,
		actulValue)
}

func (rt *runtime) fetchValue(src *metapb.Event) (bool, []interface{}, error) {
	values := make([]interface{}, 0, len(rt.expr.Sources))

	switch rt.expr.Type {
	case metapb.Bitmap:
		for _, src := range rt.expr.Sources {
			bm, err := rt.fetcher.Bitmap(hack.StringToSlice(src))
			if err != nil {
				return false, nil, err
			}

			if bm == nil {
				return false, nil, nil
			}
			values = append(values, bm)
		}

		return true, values, nil
	default:
		ok := false
		for _, key := range rt.expr.Sources {
			ok = false
			for _, kv := range src.Data {
				if hack.SliceToString(kv.Key) == key {
					ok = true
					values = append(values, hack.SliceToString(kv.Value))
				}
			}
		}

		return ok, values, nil
	}
}
