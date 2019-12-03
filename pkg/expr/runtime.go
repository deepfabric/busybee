package expr

import (
	"fmt"
	"regexp"
	"strconv"

	"github.com/deepfabric/busybee/pkg/pb/metapb"
	"github.com/fagongzi/util/format"
)

const (
	fieldUserID     = "userId"
	fieldTenantID   = "tenantID"
	fieldWorkflowID = "workflowId"
	fieldInstanceID = "instanceId"
)

// Runtime expr runtime
type Runtime interface {
	// Exec calc result using src event
	Exec(src metapb.Event) (bool, error)
}

type runtime struct {
	expr metapb.Expr

	pattern           *regexp.Regexp
	expectUint64Value uint64
}

// NewRuntime create a expr runtime
func NewRuntime(expr metapb.Expr) (Runtime, error) {
	var err error

	rt := &runtime{
		expr: expr,
	}

	if expr.Cmp == metapb.Match {
		rt.pattern, err = regexp.Compile(expr.Expect)
		if err != nil {
			return nil, err
		}
	}

	if expr.Type == metapb.Number {
		rt.expectUint64Value, err = format.ParseStrUInt64(expr.Expect)
		if err != nil {
			return nil, err
		}
	}

	return rt, nil
}

func (rt *runtime) Exec(src metapb.Event) (bool, error) {
	var actulValue interface{}
	var ok bool
	if rt.expr.Field != nil {
		ok, actulValue = fieldValue(*rt.expr.Field, src)
	} else if rt.expr.Key != nil {
		ok, actulValue = kvValue(*rt.expr.Key, src)
	}

	if !ok {
		return false, nil
	}

	switch rt.expr.Type {
	case metapb.String:
		value, err := toString(actulValue)
		if err != nil {
			return false, err
		}

		return compareString(value, rt)
	case metapb.Number:
		value, err := toUint64(actulValue)
		if err != nil {
			return false, err
		}

		return compareUint64(value, rt)
	}

	return false, fmt.Errorf("not support cmp type %+v and %T",
		rt.expr.Type,
		actulValue)
}

func toString(src interface{}) (string, error) {
	if value, ok := src.(string); ok {
		return value, nil
	} else if value, ok := src.(uint64); ok {
		return strconv.FormatUint(value, 10), nil
	}

	return "", fmt.Errorf("[%T] can not convert to string value", src)
}

func toUint64(src interface{}) (uint64, error) {
	if value, ok := src.(string); ok {
		return format.ParseStrUInt64(value)
	} else if value, ok := src.(uint64); ok {
		return value, nil
	}

	return 0, fmt.Errorf("[%T] can not convert to uint64 value", src)
}

func fieldValue(field string, value metapb.Event) (bool, interface{}) {
	if field == fieldUserID {
		return true, value.UserID
	} else if field == fieldTenantID {
		return true, value.TenantID
	} else if field == fieldWorkflowID {
		return true, value.WorkflowID
	} else if field == fieldInstanceID {
		return true, value.InstanceID
	}

	return false, nil
}

func kvValue(key string, value metapb.Event) (bool, interface{}) {
	for _, kv := range value.Data {
		if kv.Key == key {
			return true, kv.Value
		}
	}

	return false, nil
}
