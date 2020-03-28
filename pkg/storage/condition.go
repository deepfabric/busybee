package storage

import (
	"bytes"

	"github.com/deepfabric/busybee/pkg/pb/rpcpb"
)

func matchConditionGroups(value []byte, groups []rpcpb.ConditionGroup) bool {
	for _, g := range groups {
		if matchConditionGroup(value, g) {
			return true
		}
	}

	return false
}

func matchConditionGroup(value []byte, g rpcpb.ConditionGroup) bool {
	for _, cond := range g.Conditions {
		if !matchCondition(value, cond) {
			return false
		}
	}
	return true
}

func matchCondition(value []byte, cond rpcpb.Condition) bool {
	switch cond.Cmp {
	case rpcpb.NotExists:
		return len(value) == 0
	case rpcpb.Exists:
		return len(value) > 0
	case rpcpb.Equal:
		return bytes.Compare(value, cond.Value) == 0
	case rpcpb.NotEqual:
		return bytes.Compare(value, cond.Value) == 0
	case rpcpb.GE:
		return bytes.Compare(value, cond.Value) >= 0
	case rpcpb.GT:
		return bytes.Compare(value, cond.Value) > 0
	case rpcpb.LE:
		return bytes.Compare(value, cond.Value) <= 0
	case rpcpb.LT:
		return bytes.Compare(value, cond.Value) < 0
	}

	return false
}
