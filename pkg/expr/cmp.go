package expr

import (
	"fmt"
	"github.com/RoaringBitmap/roaring"
	"github.com/deepfabric/busybee/pkg/util"
	engine "github.com/fagongzi/expr"
	"regexp"
)

func add(left interface{}, right engine.Expr, ctx interface{}) (interface{}, error) {
	value, err := right.Exec(ctx)
	if err != nil {
		return nil, err
	}

	if v1, ok := left.(string); ok {
		v2, err := toString(value)
		if err != nil {
			return nil, err
		}

		return v1 + v2, nil
	} else if v1, ok := left.(int64); ok {
		v2, err := toInt64(value)
		if err != nil {
			return nil, err
		}

		return v1 + v2, nil
	}

	return nil, fmt.Errorf("+ not support var type %T", left)
}

func minus(left interface{}, right engine.Expr, ctx interface{}) (interface{}, error) {
	value, err := right.Exec(ctx)
	if err != nil {
		return nil, err
	}

	if v1, ok := left.(int64); ok {
		v2, err := toInt64(value)
		if err != nil {
			return nil, err
		}

		return v1 - v2, nil
	} else if v1, ok := left.(*roaring.Bitmap); ok {
		switch value.(type) {
		case int64:
			v1.Remove(uint32(value.(int64)))
		case *roaring.Bitmap:
			tmp := v1.Clone()
			tmp.And(value.(*roaring.Bitmap))
			v1.Xor(tmp)
		default:
			return nil, fmt.Errorf("- with bitmap not support %T", value)
		}

		return v1, nil
	}

	return nil, fmt.Errorf("- not support var type %T", left)
}

func equal(left interface{}, right engine.Expr, ctx interface{}) (interface{}, error) {
	value, err := right.Exec(ctx)
	if err != nil {
		return nil, err
	}

	if v1, ok := left.(string); ok {
		v2, err := toString(value)
		if err != nil {
			return nil, err
		}

		return v1 == v2, nil
	} else if v1, ok := left.(int64); ok {
		v2, err := toInt64(value)
		if err != nil {
			return nil, err
		}

		return v1 == v2, nil
	}

	return nil, fmt.Errorf("== not support var type %T", left)
}

func notEqual(left interface{}, right engine.Expr, ctx interface{}) (interface{}, error) {
	value, err := right.Exec(ctx)
	if err != nil {
		return nil, err
	}

	if v1, ok := left.(string); ok {
		v2, err := toString(value)
		if err != nil {
			return nil, err
		}

		return v1 != v2, nil
	} else if v1, ok := left.(int64); ok {
		v2, err := toInt64(value)
		if err != nil {
			return nil, err
		}

		return v1 != v2, nil
	}

	return nil, fmt.Errorf("!= not support var type %T", left)
}

func lt(left interface{}, right engine.Expr, ctx interface{}) (interface{}, error) {
	value, err := right.Exec(ctx)
	if err != nil {
		return nil, err
	}

	if v1, ok := left.(string); ok {
		v2, err := toString(value)
		if err != nil {
			return nil, err
		}

		return v1 < v2, nil
	} else if v1, ok := left.(int64); ok {
		v2, err := toInt64(value)
		if err != nil {
			return nil, err
		}

		return v1 < v2, nil
	}

	return nil, fmt.Errorf("< not support var type %T", left)
}

func le(left interface{}, right engine.Expr, ctx interface{}) (interface{}, error) {
	value, err := right.Exec(ctx)
	if err != nil {
		return nil, err
	}

	if v1, ok := left.(string); ok {
		v2, err := toString(value)
		if err != nil {
			return nil, err
		}

		return v1 <= v2, nil
	} else if v1, ok := left.(int64); ok {
		v2, err := toInt64(value)
		if err != nil {
			return nil, err
		}

		return v1 <= v2, nil
	}

	return nil, fmt.Errorf("<= not support var type %T", left)
}

func gt(left interface{}, right engine.Expr, ctx interface{}) (interface{}, error) {
	value, err := right.Exec(ctx)
	if err != nil {
		return nil, err
	}

	if v1, ok := left.(string); ok {
		v2, err := toString(value)
		if err != nil {
			return nil, err
		}

		return v1 > v2, nil
	} else if v1, ok := left.(int64); ok {
		v2, err := toInt64(value)
		if err != nil {
			return nil, err
		}

		return v1 > v2, nil
	}

	return nil, fmt.Errorf("> not support var type %T", left)
}

func ge(left interface{}, right engine.Expr, ctx interface{}) (interface{}, error) {
	value, err := right.Exec(ctx)
	if err != nil {
		return nil, err
	}

	if v1, ok := left.(string); ok {
		v2, err := toString(value)
		if err != nil {
			return nil, err
		}

		return v1 >= v2, nil
	} else if v1, ok := left.(int64); ok {
		v2, err := toInt64(value)
		if err != nil {
			return nil, err
		}

		return v1 <= v2, nil
	}

	return nil, fmt.Errorf(">= not support var type %T", left)
}

func match(left interface{}, right engine.Expr, ctx interface{}) (interface{}, error) {
	value, err := right.Exec(ctx)
	if err != nil {
		return nil, err
	}

	if v1, ok := left.(string); ok {
		v2, err := toRegexp(value)
		if err != nil {
			return nil, err
		}

		return v2.MatchString(v1), nil
	}

	return nil, fmt.Errorf("~ not support var type %T", left)
}

func notMatch(left interface{}, right engine.Expr, ctx interface{}) (interface{}, error) {
	value, err := right.Exec(ctx)
	if err != nil {
		return nil, err
	}

	if v1, ok := left.(string); ok {
		v2, err := toString(value)
		if err != nil {
			return nil, err
		}

		pattern, err := regexp.Compile(v2)
		if err != nil {
			return nil, err
		}

		return !pattern.MatchString(v1), nil
	}

	return nil, fmt.Errorf("!~ not support var type %T", left)
}

func andnot(left interface{}, right engine.Expr, ctx interface{}) (interface{}, error) {
	value, err := right.Exec(ctx)
	if err != nil {
		return nil, err
	}

	if v1, ok := left.(*roaring.Bitmap); ok {
		v2, err := toBitmap(value)
		if err != nil {
			return nil, err
		}

		return util.BMAndnot(v1, v2), nil
	}

	return nil, fmt.Errorf("andnot not support var type %T", left)
}

func xor(left interface{}, right engine.Expr, ctx interface{}) (interface{}, error) {
	value, err := right.Exec(ctx)
	if err != nil {
		return nil, err
	}

	if v1, ok := left.(*roaring.Bitmap); ok {
		v2, err := toBitmap(value)
		if err != nil {
			return nil, err
		}

		return util.BMXOr(v1, v2), nil
	}

	return nil, fmt.Errorf("andnot not support var type %T", left)
}

func multiplication(left interface{}, right engine.Expr, ctx interface{}) (interface{}, error) {
	value, err := right.Exec(ctx)
	if err != nil {
		return nil, err
	}

	if v1, ok := left.(int64); ok {
		v2, err := toInt64(value)
		if err != nil {
			return nil, err
		}

		return v1 * v2, nil
	}

	return nil, fmt.Errorf("* not support var type %T", left)
}

func division(left interface{}, right engine.Expr, ctx interface{}) (interface{}, error) {
	value, err := right.Exec(ctx)
	if err != nil {
		return nil, err
	}

	if v1, ok := left.(int64); ok {
		v2, err := toInt64(value)
		if err != nil {
			return nil, err
		}

		return v1 / v2, nil
	}

	return nil, fmt.Errorf("/ not support var type %T", left)
}

func logicAnd(left interface{}, right engine.Expr, ctx interface{}) (interface{}, error) {
	if v1, ok := left.(bool); ok {
		if !v1 {
			return false, nil
		}

		value, err := right.Exec(ctx)
		if err != nil {
			return nil, err
		}

		if v2, ok := value.(bool); ok {
			return v2, nil
		}

		return nil, fmt.Errorf("&& not support var type %T", value)
	} else if v1, ok := left.(*roaring.Bitmap); ok {
		value, err := right.Exec(ctx)
		if err != nil {
			return nil, err
		}

		if v2, ok := value.(*roaring.Bitmap); ok {
			v1.And(v2)
			return v1, nil
		}

		return nil, fmt.Errorf("&& not support bitmap && %T", value)
	}

	return nil, fmt.Errorf("&& not support var type %T", left)
}

func logicOr(left interface{}, right engine.Expr, ctx interface{}) (interface{}, error) {
	if v1, ok := left.(bool); ok {
		if v1 {
			return true, nil
		}

		value, err := right.Exec(ctx)
		if err != nil {
			return nil, err
		}

		if v2, ok := value.(bool); ok {
			return v2, nil
		}

		return nil, fmt.Errorf("|| not support var type %T", value)
	} else if v1, ok := left.(*roaring.Bitmap); ok {
		value, err := right.Exec(ctx)
		if err != nil {
			return nil, err
		}

		if v2, ok := value.(*roaring.Bitmap); ok {
			v1.Or(v2)
			return v1, nil
		}

		return nil, fmt.Errorf("|| not support bitmap || %T", value)
	}

	return nil, fmt.Errorf("|| not support var type %T", left)
}
