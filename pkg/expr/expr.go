package expr

import (
	"fmt"
	engine "github.com/fagongzi/expr"
	"strings"
)

const (
	stringVar = "str:"
	int64Var  = "num:"
	bitmapVar = "bm:"

	eventVarType   = "event"
	profileVarType = "profile"
	kvVarType      = "kv"
	dynaKVVarType  = "dyna"
	funcVarType    = "func"
)

var parser engine.Parser

func init() {
	parser = engine.NewParser(varExprFactory)
	parser.AddOP("+", add)
	parser.AddOP("-", minus)
	parser.AddOP("*", multiplication)
	parser.AddOP("/", division)
	parser.AddOP(">", gt)
	parser.AddOP(">=", ge)
	parser.AddOP("<", lt)
	parser.AddOP("<=", le)
	parser.AddOP("==", equal)
	parser.AddOP("!=", notEqual)
	parser.AddOP("~", match)
	parser.AddOP("!~", notMatch)
	parser.AddOP("&&", logicAnd)
	parser.AddOP("||", logicOr)
	parser.AddOP("!&&", andnot)
	parser.AddOP("^||", xor)
	parser.ValueType("str:", "num:", "bm:")
}

func varExprFactory(data []byte, valueType string) (engine.Expr, error) {
	values := strings.Split(string(data), ".")
	switch values[0] {
	case eventVarType:
		if len(values[1:]) != 1 {
			return nil, fmt.Errorf("event var expect event.attr")
		}

		return newEventVar([]byte(values[1]), valueType), nil
	case profileVarType:
		if len(values[1:]) != 1 {
			return nil, fmt.Errorf("profile var expect profile.attr")
		}

		return newProfileVar([]byte(values[1]), valueType), nil
	case kvVarType:
		if len(values[1:]) != 1 {
			return nil, fmt.Errorf("kv var expect kv.key")
		}

		return newKVVar([]byte(values[1]), valueType), nil
	case dynaKVVarType:
		if len(values[1:]) < 2 {
			return nil, fmt.Errorf("dyna var expect dyna.pattern.[year|month|day]")
		}

		return newDynamicKVVar(values[1], values[2:], valueType)
	case funcVarType:
		if len(values[1:]) != 1 {
			return nil, fmt.Errorf("func var expect dyna.pattern.[year|month|day]")
		}

		return newFuncVar(values[1], valueType)
	}

	return nil, fmt.Errorf("%s var not support", values[0])
}
