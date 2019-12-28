package client

import (
	"github.com/deepfabric/busybee/pkg/pb/metapb"
)

type result interface {
	GetCode() int
	GetError() string
}

type codeResult struct {
	Code  int    `json:"code"`
	Error string `json:"err"`
}

func (r *codeResult) GetCode() int {
	return r.Code
}

func (r *codeResult) GetError() string {
	return r.Error
}

type uint64Result struct {
	codeResult
	Value uint64 `json:"value"`
}

type bytesResult struct {
	codeResult
	Value []byte `json:"value"`
}

type instanceCountStateResult struct {
	codeResult
	Value metapb.InstanceCountState `json:"value"`
}

type instanceStepStateResult struct {
	codeResult
	Value metapb.StepState `json:"value"`
}
