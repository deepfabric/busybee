package client

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
