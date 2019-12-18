package expr

import (
	"github.com/RoaringBitmap/roaring"
	"github.com/deepfabric/busybee/pkg/pb/metapb"
	"github.com/fagongzi/util/format"
)

func (rt *runtime) doFunc(value []interface{}, src *metapb.Event) (bool, interface{}) {
	switch rt.expr.Func {
	case metapb.First:
		return rt.first(value, src)
	case metapb.Max:
		return rt.max(value, src)
	case metapb.Min:
		return rt.min(value, src)
	case metapb.Count:
		return rt.count(value, src)
	case metapb.Avg:
		return rt.avg(value, src)
	}

	return false, nil
}

func (rt *runtime) first(value []interface{}, src *metapb.Event) (bool, interface{}) {
	switch rt.expr.Type {
	case metapb.String:
		return true, value[0]
	case metapb.Number:
		return true, format.MustParseStrUInt64(value[0].(string))
	case metapb.Bitmap:
		return false, nil
	}

	return false, nil
}

func (rt *runtime) max(value []interface{}, src *metapb.Event) (bool, interface{}) {
	switch rt.expr.Type {
	case metapb.String:
		max := ""
		for _, vv := range value {
			v := vv.(string)
			if v > max {
				max = v
			}
		}

		return true, max
	case metapb.Number:
		max := uint64(0)
		for _, vv := range value {
			v := format.MustParseStrUInt64(vv.(string))
			if v > max {
				max = v
			}
		}

		return true, max
	case metapb.Bitmap:
		max := uint32(0)
		for _, vv := range value {
			v := vv.(*roaring.Bitmap).Maximum()
			if v > max {
				max = v
			}
		}

		return true, max
	}

	return false, nil
}

func (rt *runtime) min(value []interface{}, src *metapb.Event) (bool, interface{}) {
	switch rt.expr.Type {
	case metapb.String:
		min := ""
		for _, vv := range value {
			v := vv.(string)
			if min == "" || v < min {
				min = v

			}
		}

		return true, min
	case metapb.Number:
		min := uint64(0)
		for _, vv := range value {
			v := format.MustParseStrUInt64(vv.(string))
			if min == 0 || v < min {
				min = v
			}
		}

		return true, min
	case metapb.Bitmap:
		min := uint32(0)
		for _, vv := range value {
			v := vv.(*roaring.Bitmap).Minimum()
			if min == 0 || v < min {
				min = v
			}
		}

		return true, min
	}

	return false, nil
}

func (rt *runtime) count(value []interface{}, src *metapb.Event) (bool, interface{}) {
	switch rt.expr.Type {
	case metapb.String:
		return false, nil
	case metapb.Number:
		return false, nil
	case metapb.Bitmap:
		c := uint64(0)
		for _, vv := range value {
			c += uint64(vv.(*roaring.Bitmap).GetCardinality())

		}

		return true, c
	}

	return false, nil
}

func (rt *runtime) avg(value []interface{}, src *metapb.Event) (bool, interface{}) {
	switch rt.expr.Type {
	case metapb.String:
		return false, nil
	case metapb.Number:
		sum := uint64(0)
		for _, v := range value {
			sum += format.MustParseStrUInt64(v.(string))
		}

		return true, sum / uint64(len(value))
	case metapb.Bitmap:
		return false, nil
	}

	return false, nil
}
