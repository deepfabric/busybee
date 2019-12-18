package api

import (
	"fmt"
	"net/http"

	"github.com/deepfabric/busybee/pkg/pb/rpcpb"
	"github.com/fagongzi/goetty"
	"github.com/fagongzi/util/format"
	"github.com/fagongzi/util/hack"
	"github.com/fagongzi/util/protoc"
	"github.com/labstack/echo"
)

func (s *httpServer) initStore() {
	s.http.PUT("/store/:key", s.putKV)
	s.http.GET("/store/:key", s.getKV)
	s.http.DELETE("/store/:key", s.deleteKV)

	s.http.PUT("/bitmap/:key", s.bmCreate)
	s.http.PUT("/bitmap/:key/add", s.bmAdd)
	s.http.PUT("/bitmap/:key/remove", s.bmRemove)
	s.http.PUT("/bitmap/:key/clear", s.bmClear)
	s.http.GET("/bitmap/:key/range", s.bmRange)
}

func (s *httpServer) putKV(c echo.Context) error {
	value, err := readBody(c)
	if err != nil {
		return c.JSON(http.StatusOK, &JSONResult{
			Code: codeFailed,
			Data: err.Error(),
		})
	}

	req := rpcpb.AcquireSetRequest()
	req.Key = hack.StringToSlice(c.Param("key"))
	req.Value = value

	_, err = s.engine.Storage().ExecCommand(req)
	rpcpb.ReleaseSetRequest(req)
	if err != nil {
		return c.JSON(http.StatusOK, &JSONResult{
			Code: codeFailed,
			Data: err.Error(),
		})
	}

	return c.JSON(http.StatusOK, &JSONResult{})
}

func (s *httpServer) getKV(c echo.Context) error {
	req := rpcpb.AcquireGetRequest()
	req.Key = hack.StringToSlice(c.Param("key"))

	value, err := s.engine.Storage().ExecCommand(req)
	rpcpb.ReleaseGetRequest(req)
	if err != nil {
		return c.JSON(http.StatusOK, &JSONResult{
			Code: codeFailed,
			Data: err.Error(),
		})
	}

	if len(value) == 0 {
		return c.JSON(http.StatusOK, &JSONResult{})
	}

	resp := rpcpb.AcquireGetResponse()
	err = c.JSON(http.StatusOK, &JSONResult{
		Data: resp.Value,
	})
	rpcpb.ReleaseGetResponse(resp)
	return err
}

func (s *httpServer) deleteKV(c echo.Context) error {
	req := rpcpb.AcquireDeleteRequest()
	req.Key = hack.StringToSlice(c.Param("key"))

	_, err := s.engine.Storage().ExecCommand(req)
	rpcpb.ReleaseDeleteRequest(req)
	if err != nil {
		return c.JSON(http.StatusOK, &JSONResult{
			Code: codeFailed,
			Data: err.Error(),
		})
	}

	return c.JSON(http.StatusOK, &JSONResult{})
}

func (s *httpServer) bmCreate(c echo.Context) error {
	data, err := readBody(c)
	if err != nil {
		return c.JSON(http.StatusOK, &JSONResult{
			Code: codeFailed,
			Data: err.Error(),
		})
	}

	value, err := readUint32(data)
	if err != nil {
		return c.JSON(http.StatusOK, &JSONResult{
			Code: codeFailed,
			Data: err.Error(),
		})
	}

	req := rpcpb.AcquireBMCreateRequest()
	req.Key = hack.StringToSlice(c.Param("key"))
	req.Value = value

	_, err = s.engine.Storage().ExecCommand(req)
	rpcpb.ReleaseBMCreateRequest(req)
	if err != nil {
		return c.JSON(http.StatusOK, &JSONResult{
			Code: codeFailed,
			Data: err.Error(),
		})
	}

	return c.JSON(http.StatusOK, &JSONResult{})
}

func (s *httpServer) bmAdd(c echo.Context) error {
	data, err := readBody(c)
	if err != nil {
		return c.JSON(http.StatusOK, &JSONResult{
			Code: codeFailed,
			Data: err.Error(),
		})
	}

	value, err := readUint32(data)
	if err != nil {
		return c.JSON(http.StatusOK, &JSONResult{
			Code: codeFailed,
			Data: err.Error(),
		})
	}

	req := rpcpb.AcquireBMAddRequest()
	req.Key = hack.StringToSlice(c.Param("key"))
	req.Value = value

	_, err = s.engine.Storage().ExecCommand(req)
	rpcpb.ReleaseBMAddRequest(req)
	if err != nil {
		return c.JSON(http.StatusOK, &JSONResult{
			Code: codeFailed,
			Data: err.Error(),
		})
	}

	return c.JSON(http.StatusOK, &JSONResult{})
}

func (s *httpServer) bmRemove(c echo.Context) error {
	data, err := readBody(c)
	if err != nil {
		return c.JSON(http.StatusOK, &JSONResult{
			Code: codeFailed,
			Data: err.Error(),
		})
	}

	value, err := readUint32(data)
	if err != nil {
		return c.JSON(http.StatusOK, &JSONResult{
			Code: codeFailed,
			Data: err.Error(),
		})
	}

	req := rpcpb.AcquireBMRemoveRequest()
	req.Key = hack.StringToSlice(c.Param("key"))
	req.Value = value

	_, err = s.engine.Storage().ExecCommand(req)
	rpcpb.ReleaseBMRemoveRequest(req)
	if err != nil {
		return c.JSON(http.StatusOK, &JSONResult{
			Code: codeFailed,
			Data: err.Error(),
		})
	}

	return c.JSON(http.StatusOK, &JSONResult{})
}

func (s *httpServer) bmClear(c echo.Context) error {
	req := rpcpb.AcquireBMClearRequest()
	req.Key = hack.StringToSlice(c.Param("key"))

	_, err := s.engine.Storage().ExecCommand(req)
	rpcpb.ReleaseBMClearRequest(req)
	if err != nil {
		return c.JSON(http.StatusOK, &JSONResult{
			Code: codeFailed,
			Data: err.Error(),
		})
	}

	return c.JSON(http.StatusOK, &JSONResult{})
}

func (s *httpServer) bmRange(c echo.Context) error {
	start, err := format.ParseStrUInt64(c.Param("start"))
	if err != nil {
		return c.JSON(http.StatusOK, &JSONResult{
			Code: codeFailed,
			Data: err.Error(),
		})
	}

	limit, err := format.ParseStrUInt64(c.Param("limit"))
	if err != nil {
		return c.JSON(http.StatusOK, &JSONResult{
			Code: codeFailed,
			Data: err.Error(),
		})
	}

	req := rpcpb.AcquireBMRangeRequest()
	req.Key = hack.StringToSlice(c.Param("key"))
	req.Start = uint32(start)
	req.Limit = limit

	value, err := s.engine.Storage().ExecCommand(req)
	rpcpb.ReleaseBMRangeRequest(req)
	if err != nil {
		return c.JSON(http.StatusOK, &JSONResult{
			Code: codeFailed,
			Data: err.Error(),
		})
	}

	resp := rpcpb.AcquireBMRangeResponse()
	protoc.MustUnmarshal(resp, value)
	if len(resp.Values) == 0 {
		return c.JSON(http.StatusOK, &JSONResult{})
	}

	buf := goetty.NewByteBuf(4 * len(resp.Values))
	for _, value := range resp.Values {
		buf.WriteUInt32(value)
	}
	_, data, _ := buf.ReadAll()
	err = c.JSON(http.StatusOK, &JSONResult{
		Data: data,
	})
	buf.Release()
	return err
}

func readUint32(data []byte) ([]uint32, error) {
	n := len(data)
	if n == 0 {
		return nil, nil
	}

	if n%4 != 0 {
		return nil, fmt.Errorf("invalid data len %d", n)
	}

	size := n / 4
	value := make([]uint32, size, size)
	for i := 0; i < size; i++ {
		value = append(value, goetty.Byte2UInt32(data[size*4:]))
	}
	return value, nil
}
