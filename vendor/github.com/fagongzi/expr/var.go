package expr

import (
	"fmt"
	"regexp"

	"github.com/fagongzi/util/format"
	"github.com/fagongzi/util/hack"
)

// VarType var type like str, num, regexp, array.
type VarType int

var (
	// Str str var type
	Str = VarType(0)
	// Num num var type
	Num = VarType(1)
	// Regexp regexp type
	Regexp = VarType(2)
)

// ValueByType returns the value by type
func ValueByType(value []byte, varType VarType) (interface{}, error) {
	switch varType {
	case Str:
		if len(value) == 0 {
			return defaultValue(Str), nil
		}

		return hack.SliceToString(value), nil
	case Num:
		if len(value) == 0 {
			return defaultValue(Str), nil
		}

		return format.ParseStrInt64(hack.SliceToString(value))
	case Regexp:
		if len(value) == 0 {
			return defaultValue(Str), nil
		}

		return regexp.Compile(hack.SliceToString(value))
	default:
		return nil, fmt.Errorf("%d var type not support", varType)
	}
}
