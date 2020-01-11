package expr

import (
	"regexp"
)

var (
	defaultValues = make(map[VarType]interface{})
)

// RegisterDefaultValue register default value
func RegisterDefaultValue(varType VarType, value interface{}) {
	defaultValues[varType] = value
}

func init() {
	initDefaultValues()
}

func initDefaultValues() {
	defaultValues[Str] = ""
	defaultValues[Num] = int64(0)
	defaultValues[Regexp] = regexp.MustCompile(".*")
}

func defaultValue(varType VarType) interface{} {
	return defaultValues[varType]
}
