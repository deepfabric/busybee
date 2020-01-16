package api

import (
	"net/http"
	"sync"

	"github.com/deepfabric/busybee/pkg/pb/metapb"
	"github.com/fagongzi/util/format"
	"github.com/labstack/echo"
)

var (
	queueAddPool   sync.Pool
	queueFetchPool sync.Pool
)

func acquireQueueAdd() *QueueAdd {
	value := queueAddPool.Get()
	if value == nil {
		return &QueueAdd{}
	}
	return value.(*QueueAdd)
}

func releaseQueueAdd(value *QueueAdd) {
	value.reset()
	queueAddPool.Put(value)
}

func acquireQueueFetch() *QueueFetch {
	value := queueFetchPool.Get()
	if value == nil {
		return &QueueFetch{}
	}
	return value.(*QueueFetch)
}

func releaseQueueFetch(value *QueueFetch) {
	value.reset()
	queueFetchPool.Put(value)
}

// QueueAdd queue add
type QueueAdd struct {
	Items [][]byte `json:"items"`
}

func (qa *QueueAdd) reset() {
	qa.Items = qa.Items[:0]
}

// QueueFetch queue add
type QueueFetch struct {
	LastOffset uint64 `json:"lastOffset"`
	Count      uint64 `json:"count"`
}

func (qa *QueueFetch) reset() {
	qa.LastOffset = 0
	qa.Count = 0
}

// QueueFetchResult queue fetch result
type QueueFetchResult struct {
	LastOffset uint64   `json:"lastOffset"`
	Items      [][]byte `json:"items"`
}

func (s *httpServer) initQueueAPI() {
	s.http.POST("/queues/:id/tenant", s.queueTenantCreate)
	s.http.POST("/queues/:id/event", s.queueEventCreate)
	s.http.POST("/queues/:id/notify", s.queueNotifyCreate)
	s.http.PUT("/queues/:id/tenant/add", s.queueTenantAdd)
	s.http.PUT("/queues/:id/event/add", s.queueEventAdd)
	s.http.PUT("/queues/:id/notify/add", s.queueNotifyAdd)
	s.http.PUT("/queues/:id/tenant/fetch", s.queueTenantFetch)
	s.http.PUT("/queues/:id/event/fetch", s.queueEventFetch)
	s.http.PUT("/queues/:id/notify/fetch", s.queueNotifyFetch)
}

func (s *httpServer) queueTenantAdd(c echo.Context) error {
	return s.doQueueAdd(metapb.TenantGroup, c)
}

func (s *httpServer) queueEventAdd(c echo.Context) error {
	return s.doQueueAdd(metapb.EventGroup, c)
}

func (s *httpServer) queueTenantFetch(c echo.Context) error {
	return s.doQueueFetch(metapb.TenantGroup, c)
}

func (s *httpServer) queueEventFetch(c echo.Context) error {
	return s.doQueueFetch(metapb.EventGroup, c)
}

func (s *httpServer) queueNotifyAdd(c echo.Context) error {
	return s.doQueueAdd(metapb.NotifyGroup, c)
}

func (s *httpServer) queueNotifyFetch(c echo.Context) error {
	return s.doQueueFetch(metapb.NotifyGroup, c)
}

func (s *httpServer) queueTenantCreate(c echo.Context) error {
	return s.doQueueCreate(metapb.TenantGroup, c)
}

func (s *httpServer) queueEventCreate(c echo.Context) error {
	return s.doQueueCreate(metapb.EventGroup, c)
}

func (s *httpServer) queueNotifyCreate(c echo.Context) error {
	return s.doQueueCreate(metapb.NotifyGroup, c)
}

func (s *httpServer) doQueueCreate(group metapb.Group, c echo.Context) error {
	id, err := format.ParseStrUInt64(c.Param("id"))
	if err != nil {
		return c.JSON(http.StatusOK, &JSONResult{
			Code:  codeFailed,
			Error: err.Error(),
		})
	}

	err = s.engine.Storage().CreateQueue(id, group)
	if err != nil {
		return c.JSON(http.StatusOK, &JSONResult{
			Code:  codeFailed,
			Error: err.Error(),
		})
	}

	return c.JSON(http.StatusOK, &JSONResult{})
}

func (s *httpServer) doQueueAdd(group metapb.Group, c echo.Context) error {
	id, err := format.ParseStrUInt64(c.Param("id"))
	if err != nil {
		return c.JSON(http.StatusOK, &JSONResult{
			Code:  codeFailed,
			Error: err.Error(),
		})
	}

	value := acquireQueueAdd()
	err = readJSONFromBody(c, value)
	if err != nil {
		releaseQueueAdd(value)
		return c.JSON(http.StatusOK, &JSONResult{
			Code:  codeFailed,
			Error: err.Error(),
		})
	}

	offset, err := s.engine.Storage().QueueAdd(id, group, value.Items...)
	releaseQueueAdd(value)
	if err != nil {
		return c.JSON(http.StatusOK, &JSONResult{
			Code:  codeFailed,
			Error: err.Error(),
		})
	}

	return c.JSON(http.StatusOK, &JSONResult{
		Value: offset,
	})
}

func (s *httpServer) doQueueFetch(group metapb.Group, c echo.Context) error {
	id, err := format.ParseStrUInt64(c.Param("id"))
	if err != nil {
		return c.JSON(http.StatusOK, &JSONResult{
			Code:  codeFailed,
			Error: err.Error(),
		})
	}

	value := acquireQueueFetch()
	err = readJSONFromBody(c, value)
	if err != nil {
		releaseQueueFetch(value)
		return c.JSON(http.StatusOK, &JSONResult{
			Code:  codeFailed,
			Error: err.Error(),
		})
	}

	offset, items, err := s.engine.Storage().QueueFetch(id, group, value.LastOffset, value.Count)
	releaseQueueFetch(value)
	if err != nil {
		return c.JSON(http.StatusOK, &JSONResult{
			Code:  codeFailed,
			Error: err.Error(),
		})
	}

	return c.JSON(http.StatusOK, &JSONResult{
		Value: QueueFetchResult{
			LastOffset: offset,
			Items:      items,
		},
	})
}
