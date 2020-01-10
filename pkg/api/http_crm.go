package api

import (
	"net/http"

	"github.com/deepfabric/busybee/pkg/pb/metapb"
	"github.com/fagongzi/util/format"
	"github.com/labstack/echo"
)

func (s *httpServer) initCRMAPI() {
	s.http.PUT("/crm/mapping/:tid", s.updateMapping)
	s.http.GET("/crm/mapping/:tid", s.getMapping)
	s.http.PUT("/crm/profile/:tid/:id", s.updateProfile)
}

func (s *httpServer) updateMapping(c echo.Context) error {
	tid, err := format.ParseStrUInt64(c.Param("tid"))
	if err != nil {
		return c.JSON(http.StatusOK, &JSONResult{
			Code:  codeFailed,
			Error: err.Error(),
		})
	}

	values := metapb.IDValues{}
	err = readJSONFromBody(c, &values)
	if err != nil {
		return c.JSON(http.StatusOK, &JSONResult{
			Code:  codeFailed,
			Error: err.Error(),
		})
	}

	err = s.engine.Service().UpdateMapping(tid, values)
	if err != nil {
		return c.JSON(http.StatusOK, &JSONResult{
			Code:  codeFailed,
			Error: err.Error(),
		})
	}

	return c.JSON(http.StatusOK, &JSONResult{})
}

func (s *httpServer) getMapping(c echo.Context) error {
	tid, err := format.ParseStrUInt64(c.Param("tid"))
	if err != nil {
		return c.JSON(http.StatusOK, &JSONResult{
			Code:  codeFailed,
			Error: err.Error(),
		})
	}

	fromValue := c.QueryParam("fromValue")
	if fromValue == "" {
		return c.JSON(http.StatusOK, &JSONResult{
			Code:  codeFailed,
			Error: "missing fromValue query param",
		})
	}

	fromType, err := format.ParseStrUInt64(c.QueryParam("fromType"))
	if err != nil {
		return c.JSON(http.StatusOK, &JSONResult{
			Code:  codeFailed,
			Error: err.Error(),
		})
	}

	toType, err := format.ParseStrUInt64(c.QueryParam("toType"))
	if err != nil {
		return c.JSON(http.StatusOK, &JSONResult{
			Code:  codeFailed,
			Error: err.Error(),
		})
	}

	value, err := s.engine.Service().GetIDValue(tid, metapb.IDValue{
		Value: fromValue,
		Type:  uint32(fromType),
	}, uint32(toType))
	if err != nil {
		return c.JSON(http.StatusOK, &JSONResult{
			Code:  codeFailed,
			Error: err.Error(),
		})
	}

	return c.JSON(http.StatusOK, &JSONResult{
		Value: metapb.IDValue{
			Type:  uint32(toType),
			Value: value,
		},
	})
}

func (s *httpServer) updateProfile(c echo.Context) error {
	tid, err := format.ParseStrUInt64(c.Param("tid"))
	if err != nil {
		return c.JSON(http.StatusOK, &JSONResult{
			Code:  codeFailed,
			Error: err.Error(),
		})
	}

	uid, err := format.ParseStrUInt64(c.Param("id"))
	if err != nil {
		return c.JSON(http.StatusOK, &JSONResult{
			Code:  codeFailed,
			Error: err.Error(),
		})
	}

	value, err := readBody(c)
	if err != nil {
		return c.JSON(http.StatusOK, &JSONResult{
			Code:  codeFailed,
			Error: err.Error(),
		})
	}

	err = s.engine.Service().UpdateProfile(tid, uint32(uid), value)
	if err != nil {
		return c.JSON(http.StatusOK, &JSONResult{
			Code:  codeFailed,
			Error: err.Error(),
		})
	}

	return c.JSON(http.StatusOK, &JSONResult{})
}
