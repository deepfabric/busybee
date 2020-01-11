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

var (
	bmType = engine.VarType(3)
)

var parser engine.Parser

func init() {
	parser = engine.NewParser(varExprFactory,
		engine.WithOp("+", add),
		engine.WithOp("-", minus),
		engine.WithOp("*", multiplication),
		engine.WithOp("/", division),
		engine.WithOp(">", gt),
		engine.WithOp(">=", ge),
		engine.WithOp("<", lt),
		engine.WithOp("<=", le),
		engine.WithOp("==", equal),
		engine.WithOp("!=", notEqual),
		engine.WithOp("~", match),
		engine.WithOp("!~", notMatch),
		engine.WithOp("&&", logicAnd),
		engine.WithOp("||", logicOr),
		engine.WithOp("!&", andnot),
		engine.WithOp("^|", xor),
		engine.WithVarType("str:", engine.Str),
		engine.WithVarType("num:", engine.Num),
		engine.WithVarType("reg:", engine.Regexp),
		engine.WithVarType("bm:", bmType),
	)
}

func varExprFactory(data []byte, valueType engine.VarType) (engine.Expr, error) {
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
