package api

import (
	"net/http"

	"github.com/deepfabric/busybee/pkg/pb/metapb"
	"github.com/fagongzi/util/format"
	"github.com/labstack/echo"
)

// CreateInstance create instance
type CreateInstance struct {
	ID          uint64 `json:"instance"`
	Crowd       []byte `json:"crowd"`
	MaxPerShard uint64 `json:"maxPerShard"`
}

func (s *httpServer) initWorkflowAPI() {
	s.http.POST("/workflows", s.createWorkflow)
	s.http.PUT("/workflows/:id", s.updateWorkflow)
	s.http.POST("/workflows/instance", s.createInstance)
	s.http.DELETE("/workflows/instance/:id", s.deleteInstance)
	s.http.POST("/workflows/instance/:id/start", s.startInstance)
	s.http.PUT("/workflows/instance/:id/step", s.stepInstance)
	s.http.GET("/workflows/instance/:id/state/count", s.instanceCount)
	s.http.GET("/workflows/instance/:id/state/step/:name", s.instanceStep)
}

func (s *httpServer) createWorkflow(c echo.Context) error {
	wf := metapb.Workflow{}
	err := readJSONFromBody(c, &wf)
	if err != nil {
		return c.JSON(http.StatusOK, &JSONResult{
			Code:  codeFailed,
			Error: err.Error(),
		})
	}

	id, err := s.engine.Create(wf)
	if err != nil {
		return c.JSON(http.StatusOK, &JSONResult{
			Code:  codeFailed,
			Error: err.Error(),
		})
	}

	return c.JSON(http.StatusOK, &JSONResult{
		Value: id,
	})
}

func (s *httpServer) updateWorkflow(c echo.Context) error {
	id, err := format.ParseStrUInt64(c.Param("id"))
	if err != nil {
		return c.JSON(http.StatusOK, &JSONResult{
			Code:  codeFailed,
			Error: err.Error(),
		})
	}

	wf := metapb.Workflow{}
	err = readJSONFromBody(c, &wf)
	if err != nil {
		return c.JSON(http.StatusOK, &JSONResult{
			Code:  codeFailed,
			Error: err.Error(),
		})
	}

	wf.ID = id
	err = s.engine.Update(wf)
	if err != nil {
		return c.JSON(http.StatusOK, &JSONResult{
			Code:  codeFailed,
			Error: err.Error(),
		})
	}

	return c.JSON(http.StatusOK, &JSONResult{})
}

func (s *httpServer) createInstance(c echo.Context) error {
	value := CreateInstance{}
	err := readJSONFromBody(c, &value)
	if err != nil {
		return c.JSON(http.StatusOK, &JSONResult{
			Code:  codeFailed,
			Error: err.Error(),
		})
	}

	id, err := s.engine.CreateInstance(value.ID, value.Crowd, value.MaxPerShard)
	if err != nil {
		return c.JSON(http.StatusOK, &JSONResult{
			Code:  codeFailed,
			Error: err.Error(),
		})
	}

	return c.JSON(http.StatusOK, &JSONResult{
		Value: id,
	})
}

func (s *httpServer) deleteInstance(c echo.Context) error {
	id, err := format.ParseStrUInt64(c.Param("id"))
	if err != nil {
		return c.JSON(http.StatusOK, &JSONResult{
			Code:  codeFailed,
			Error: err.Error(),
		})
	}

	err = s.engine.DeleteInstance(id)
	if err != nil {
		return c.JSON(http.StatusOK, &JSONResult{
			Code:  codeFailed,
			Error: err.Error(),
		})
	}

	return c.JSON(http.StatusOK, &JSONResult{})
}

func (s *httpServer) startInstance(c echo.Context) error {
	id, err := format.ParseStrUInt64(c.Param("id"))
	if err != nil {
		return c.JSON(http.StatusOK, &JSONResult{
			Code:  codeFailed,
			Error: err.Error(),
		})
	}

	err = s.engine.StartInstance(id)
	if err != nil {
		return c.JSON(http.StatusOK, &JSONResult{
			Code:  codeFailed,
			Error: err.Error(),
		})
	}

	return c.JSON(http.StatusOK, &JSONResult{})
}

func (s *httpServer) stepInstance(c echo.Context) error {
	id, err := format.ParseStrUInt64(c.Param("id"))
	if err != nil {
		return c.JSON(http.StatusOK, &JSONResult{
			Code:  codeFailed,
			Error: err.Error(),
		})
	}

	var event metapb.Event
	err = readJSONFromBody(c, &event)
	if err != nil {
		return c.JSON(http.StatusOK, &JSONResult{
			Code:  codeFailed,
			Error: err.Error(),
		})
	}

	event.InstanceID = id
	err = s.engine.Step(event)
	if err != nil {
		return c.JSON(http.StatusOK, &JSONResult{
			Code:  codeFailed,
			Error: err.Error(),
		})
	}

	return c.JSON(http.StatusOK, &JSONResult{})
}

func (s *httpServer) instanceCount(c echo.Context) error {
	id, err := format.ParseStrUInt64(c.Param("id"))
	if err != nil {
		return c.JSON(http.StatusOK, &JSONResult{
			Code:  codeFailed,
			Error: err.Error(),
		})
	}

	state, err := s.engine.InstanceCountState(id)
	if err != nil {
		return c.JSON(http.StatusOK, &JSONResult{
			Code:  codeFailed,
			Error: err.Error(),
		})
	}

	return c.JSON(http.StatusOK, &JSONResult{
		Value: state,
	})
}

func (s *httpServer) instanceStep(c echo.Context) error {
	id, err := format.ParseStrUInt64(c.Param("id"))
	if err != nil {
		return c.JSON(http.StatusOK, &JSONResult{
			Code:  codeFailed,
			Error: err.Error(),
		})
	}

	state, err := s.engine.InstanceStepState(id, c.Param("step"))
	if err != nil {
		return c.JSON(http.StatusOK, &JSONResult{
			Code:  codeFailed,
			Error: err.Error(),
		})
	}

	return c.JSON(http.StatusOK, &JSONResult{
		Value: state,
	})
}
