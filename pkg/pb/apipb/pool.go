package apipb

import (
	"sync"
)

var (
	requestPool = sync.Pool{
		New: func() interface{} {
			return Request{}
		},
	}

	responsePool = sync.Pool{
		New: func() interface{} {
			return Response{}
		},
	}
)

// AcquireRequest returns value from pool
func AcquireRequest() *Request {
	return requestPool.Get().(*Request)
}

// ReleaseRequest returns the value to pool
func ReleaseRequest(value *Request) {
	value.Reset()
	requestPool.Put(value)
}

// AcquireResponse returns value from pool
func AcquireResponse() *Response {
	return responsePool.Get().(*Response)
}

// ReleaseResponse returns the value to pool
func ReleaseResponse(value *Response) {
	value.Reset()
	responsePool.Put(value)
}
