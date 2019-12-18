package api

import (
	"net/http"
	"sync"

	"github.com/deepfabric/busybee/pkg/storage"
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
	s.http.POST("/queues/:id/event", s.queueEventCreate)
	s.http.POST("/queues/:id/notify", s.queueNotifyCreate)
	s.http.PUT("/queues/:id/event/add", s.queueEventAdd)
	s.http.PUT("/queues/:id/notify/add", s.queueNotifyAdd)
	s.http.PUT("/queues/:id/event/fetch", s.queueEventFetch)
	s.http.PUT("/queues/:id/notify/fetch", s.queueNotifyFetch)
}

func (s *httpServer) queueEventAdd(c echo.Context) error {
	return s.doQueueAdd(storage.EventQueueGroup, c)
}

func (s *httpServer) queueEventFetch(c echo.Context) error {
	return s.doQueueFetch(storage.EventQueueGroup, c)
}

func (s *httpServer) queueNotifyAdd(c echo.Context) error {
	return s.doQueueAdd(storage.NotifyQueueGroup, c)
}

func (s *httpServer) queueNotifyFetch(c echo.Context) error {
	return s.doQueueFetch(storage.NotifyQueueGroup, c)
}

func (s *httpServer) queueEventCreate(c echo.Context) error {
	id, err := format.ParseStrUInt64(c.Param("id"))
	if err != nil {
		return c.JSON(http.StatusOK, &JSONResult{
			Code: codeFailed,
			Data: err.Error(),
		})
	}

	err = s.engine.Storage().CreateEventQueue(id)
	if err != nil {
		return c.JSON(http.StatusOK, &JSONResult{
			Code: codeFailed,
			Data: err.Error(),
		})
	}

	return c.JSON(http.StatusOK, &JSONResult{})
}

func (s *httpServer) queueNotifyCreate(c echo.Context) error {
	id, err := format.ParseStrUInt64(c.Param("id"))
	if err != nil {
		return c.JSON(http.StatusOK, &JSONResult{
			Code: codeFailed,
			Data: err.Error(),
		})
	}

	err = s.engine.Storage().CreateNotifyQueue(id)
	if err != nil {
		return c.JSON(http.StatusOK, &JSONResult{
			Code: codeFailed,
			Data: err.Error(),
		})
	}

	return c.JSON(http.StatusOK, &JSONResult{})
}

func (s *httpServer) doQueueAdd(group uint64, c echo.Context) error {
	id, err := format.ParseStrUInt64(c.Param("id"))
	if err != nil {
		return c.JSON(http.StatusOK, &JSONResult{
			Code: codeFailed,
			Data: err.Error(),
		})
	}

	value := acquireQueueAdd()
	err = readJSONFromBody(c, value)
	if err != nil {
		releaseQueueAdd(value)
		return c.JSON(http.StatusOK, &JSONResult{
			Code: codeFailed,
			Data: err.Error(),
		})
	}

	offset, err := s.engine.Storage().QueueAdd(id, group, value.Items...)
	releaseQueueAdd(value)
	if err != nil {
		return c.JSON(http.StatusOK, &JSONResult{
			Code: codeFailed,
			Data: err.Error(),
		})
	}

	return c.JSON(http.StatusOK, &JSONResult{
		Data: offset,
	})
}

func (s *httpServer) doQueueFetch(group uint64, c echo.Context) error {
	id, err := format.ParseStrUInt64(c.Param("id"))
	if err != nil {
		return c.JSON(http.StatusOK, &JSONResult{
			Code: codeFailed,
			Data: err.Error(),
		})
	}

	value := acquireQueueFetch()
	err = readJSONFromBody(c, value)
	if err != nil {
		releaseQueueFetch(value)
		return c.JSON(http.StatusOK, &JSONResult{
			Code: codeFailed,
			Data: err.Error(),
		})
	}

	offset, items, err := s.engine.Storage().QueueFetch(id, group, value.LastOffset, value.Count)
	releaseQueueFetch(value)
	if err != nil {
		return c.JSON(http.StatusOK, &JSONResult{
			Code: codeFailed,
			Data: err.Error(),
		})
	}

	return c.JSON(http.StatusOK, &JSONResult{
		Data: QueueFetchResult{
			LastOffset: offset,
			Items:      items,
		},
	})
}
