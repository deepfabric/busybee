package expr

import (
	"fmt"
	"regexp"

	"github.com/RoaringBitmap/roaring"
	"github.com/deepfabric/busybee/pkg/util"
	engine "github.com/fagongzi/expr"
	"github.com/fagongzi/util/format"
	"github.com/fagongzi/util/hack"
)

var (
	emptyBM = roaring.New()
)

func byValueType(value []byte, valueType engine.VarType) (interface{}, error) {
	switch valueType {
	case engine.Str:
		if len(value) == 0 {
			return "", nil
		}

		return hack.SliceToString(value), nil
	case engine.Num:
		if len(value) == 0 {
			return int64(0), nil
		}

		return format.ParseStrInt64(hack.SliceToString(value))
	case engine.Regexp:
		if len(value) == 0 {
			return regexp.MustCompile(".*"), nil
		}

		return regexp.Compile(hack.SliceToString(value))
	case bmType:
		if len(value) == 0 {
			return emptyBM, nil
		}

		bm := util.AcquireBitmap()
		util.MustParseBMTo(value, bm)
		return bm, nil
	default:
		return nil, fmt.Errorf("not support value type %d", valueType)
	}
}

func convertByType(value interface{}, valueType engine.VarType) (interface{}, error) {
	switch valueType {
	case engine.Str:
		return toString(value)
	case engine.Num:
		return toInt64(value)
	case engine.Regexp:
		return toRegexp(value)
	case bmType:
		return toBitmap(value)
	default:
		return nil, fmt.Errorf("not support value type %d", valueType)
	}
}

func toRegexp(value interface{}) (*regexp.Regexp, error) {
	if v, ok := value.(string); ok {
		return regexp.Compile(v)
	} else if v, ok := value.(*regexp.Regexp); ok {
		return v, nil
	}

	return nil, fmt.Errorf("expect string but %T", value)
}

func toString(value interface{}) (string, error) {
	if v, ok := value.(string); ok {
		return v, nil
	} else if v, ok := value.(int64); ok {
		return hack.SliceToString(format.Int64ToString(v)), nil
	}

	return "", fmt.Errorf("expect string but %T", value)
}

func toInt64(value interface{}) (int64, error) {
	if v, ok := value.(int64); ok {
		return v, nil
	} else if v, ok := value.([]byte); ok {
		if len(v) == 0 {
			return 0, nil
		}
		return format.ParseStrInt64(hack.SliceToString(v))
	} else if v, ok := value.(string); ok {
		return format.ParseStrInt64(v)
	}

	return 0, fmt.Errorf("expect int64 but %T", value)
}

func toBitmap(value interface{}) (*roaring.Bitmap, error) {
	if v, ok := value.(*roaring.Bitmap); ok {
		return v, nil
	} else if v, ok := value.(string); ok {
		if len(v) == 0 {
			return emptyBM, nil
		}

		bm := util.AcquireBitmap()
		util.MustParseBMTo(hack.StringToSlice(v), bm)
		return bm, nil
	} else if v, ok := value.([]byte); ok {
		if len(v) == 0 {
			return emptyBM, nil
		}

		bm := util.AcquireBitmap()
		util.MustParseBMTo(v, bm)
		return bm, nil
	}

	return nil, fmt.Errorf("expect bitmap/string/[]byte but %T", value)
}
