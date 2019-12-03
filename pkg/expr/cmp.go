package expr

import (
	"errors"
	"github.com/deepfabric/busybee/pkg/pb/metapb"
)

func compareString(value string, rt *runtime) (bool, error) {
	switch rt.expr.Cmp {
	case metapb.Equal:
		return value == rt.expr.Expect, nil
	case metapb.LT:
		return value < rt.expr.Expect, nil
	case metapb.LE:
		return value <= rt.expr.Expect, nil
	case metapb.GT:
		return value > rt.expr.Expect, nil
	case metapb.GE:
		return value >= rt.expr.Expect, nil
	case metapb.Match:
		return rt.pattern.MatchString(value), nil
	}

	return false, nil
}

func compareUint64(value uint64, rt *runtime) (bool, error) {
	switch rt.expr.Cmp {
	case metapb.Equal:
		return value == rt.expectUint64Value, nil
	case metapb.LT:
		return value < rt.expectUint64Value, nil
	case metapb.LE:
		return value <= rt.expectUint64Value, nil
	case metapb.GT:
		return value > rt.expectUint64Value, nil
	case metapb.GE:
		return value >= rt.expectUint64Value, nil
	case metapb.Match:
		return false, errors.New("number type not support regexp")
	}

	return false, nil
}
