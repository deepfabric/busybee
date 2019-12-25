package api

import (
	"encoding/base64"
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
			Code:  codeFailed,
			Error: err.Error(),
		})
	}

	key, err := parseKey(hack.StringToSlice(c.Param("key")))
	if err != nil {
		return c.JSON(http.StatusOK, &JSONResult{
			Code:  codeFailed,
			Error: err.Error(),
		})
	}

	req := rpcpb.AcquireSetRequest()
	req.Key = key
	req.Value = value

	_, err = s.engine.Storage().ExecCommand(req)
	rpcpb.ReleaseSetRequest(req)
	if err != nil {
		return c.JSON(http.StatusOK, &JSONResult{
			Code:  codeFailed,
			Error: err.Error(),
		})
	}

	return c.JSON(http.StatusOK, &JSONResult{})
}

func (s *httpServer) getKV(c echo.Context) error {
	key, err := parseKey(hack.StringToSlice(c.Param("key")))
	if err != nil {
		return c.JSON(http.StatusOK, &JSONResult{
			Code:  codeFailed,
			Error: err.Error(),
		})
	}

	req := rpcpb.AcquireGetRequest()
	req.Key = key

	value, err := s.engine.Storage().ExecCommand(req)
	rpcpb.ReleaseGetRequest(req)
	if err != nil {
		return c.JSON(http.StatusOK, &JSONResult{
			Code:  codeFailed,
			Error: err.Error(),
		})
	}

	if len(value) == 0 {
		return c.JSON(http.StatusOK, &JSONResult{})
	}

	resp := rpcpb.AcquireGetResponse()
	err = c.JSON(http.StatusOK, &JSONResult{
		Value: resp.Value,
	})
	rpcpb.ReleaseGetResponse(resp)
	return err
}

func (s *httpServer) deleteKV(c echo.Context) error {
	key, err := parseKey(hack.StringToSlice(c.Param("key")))
	if err != nil {
		return c.JSON(http.StatusOK, &JSONResult{
			Code:  codeFailed,
			Error: err.Error(),
		})
	}

	req := rpcpb.AcquireDeleteRequest()
	req.Key = key

	_, err = s.engine.Storage().ExecCommand(req)
	rpcpb.ReleaseDeleteRequest(req)
	if err != nil {
		return c.JSON(http.StatusOK, &JSONResult{
			Code:  codeFailed,
			Error: err.Error(),
		})
	}

	return c.JSON(http.StatusOK, &JSONResult{})
}

func (s *httpServer) bmCreate(c echo.Context) error {
	data, err := readBody(c)
	if err != nil {
		return c.JSON(http.StatusOK, &JSONResult{
			Code:  codeFailed,
			Error: err.Error(),
		})
	}

	value, err := DecodeUint32Slice(data)
	if err != nil {
		return c.JSON(http.StatusOK, &JSONResult{
			Code:  codeFailed,
			Error: err.Error(),
		})
	}

	key, err := parseKey(hack.StringToSlice(c.Param("key")))
	if err != nil {
		return c.JSON(http.StatusOK, &JSONResult{
			Code:  codeFailed,
			Error: err.Error(),
		})
	}

	req := rpcpb.AcquireBMCreateRequest()
	req.Key = key
	req.Value = value

	_, err = s.engine.Storage().ExecCommand(req)
	rpcpb.ReleaseBMCreateRequest(req)
	if err != nil {
		return c.JSON(http.StatusOK, &JSONResult{
			Code:  codeFailed,
			Error: err.Error(),
		})
	}

	return c.JSON(http.StatusOK, &JSONResult{})
}

func (s *httpServer) bmAdd(c echo.Context) error {
	data, err := readBody(c)
	if err != nil {
		return c.JSON(http.StatusOK, &JSONResult{
			Code:  codeFailed,
			Error: err.Error(),
		})
	}

	value, err := DecodeUint32Slice(data)
	if err != nil {
		return c.JSON(http.StatusOK, &JSONResult{
			Code:  codeFailed,
			Error: err.Error(),
		})
	}

	key, err := parseKey(hack.StringToSlice(c.Param("key")))
	if err != nil {
		return c.JSON(http.StatusOK, &JSONResult{
			Code:  codeFailed,
			Error: err.Error(),
		})
	}

	req := rpcpb.AcquireBMAddRequest()
	req.Key = key
	req.Value = value

	_, err = s.engine.Storage().ExecCommand(req)
	rpcpb.ReleaseBMAddRequest(req)
	if err != nil {
		return c.JSON(http.StatusOK, &JSONResult{
			Code:  codeFailed,
			Error: err.Error(),
		})
	}

	return c.JSON(http.StatusOK, &JSONResult{})
}

func (s *httpServer) bmRemove(c echo.Context) error {
	data, err := readBody(c)
	if err != nil {
		return c.JSON(http.StatusOK, &JSONResult{
			Code:  codeFailed,
			Error: err.Error(),
		})
	}

	value, err := DecodeUint32Slice(data)
	if err != nil {
		return c.JSON(http.StatusOK, &JSONResult{
			Code:  codeFailed,
			Error: err.Error(),
		})
	}

	key, err := parseKey(hack.StringToSlice(c.Param("key")))
	if err != nil {
		return c.JSON(http.StatusOK, &JSONResult{
			Code:  codeFailed,
			Error: err.Error(),
		})
	}

	req := rpcpb.AcquireBMRemoveRequest()
	req.Key = key
	req.Value = value

	_, err = s.engine.Storage().ExecCommand(req)
	rpcpb.ReleaseBMRemoveRequest(req)
	if err != nil {
		return c.JSON(http.StatusOK, &JSONResult{
			Code:  codeFailed,
			Error: err.Error(),
		})
	}

	return c.JSON(http.StatusOK, &JSONResult{})
}

func (s *httpServer) bmClear(c echo.Context) error {
	key, err := parseKey(hack.StringToSlice(c.Param("key")))
	if err != nil {
		return c.JSON(http.StatusOK, &JSONResult{
			Code:  codeFailed,
			Error: err.Error(),
		})
	}

	req := rpcpb.AcquireBMClearRequest()
	req.Key = key

	_, err = s.engine.Storage().ExecCommand(req)
	rpcpb.ReleaseBMClearRequest(req)
	if err != nil {
		return c.JSON(http.StatusOK, &JSONResult{
			Code:  codeFailed,
			Error: err.Error(),
		})
	}

	return c.JSON(http.StatusOK, &JSONResult{})
}

func (s *httpServer) bmRange(c echo.Context) error {
	start, err := format.ParseStrUInt64(c.Param("start"))
	if err != nil {
		return c.JSON(http.StatusOK, &JSONResult{
			Code:  codeFailed,
			Error: err.Error(),
		})
	}

	limit, err := format.ParseStrUInt64(c.Param("limit"))
	if err != nil {
		return c.JSON(http.StatusOK, &JSONResult{
			Code:  codeFailed,
			Error: err.Error(),
		})
	}

	key, err := parseKey(hack.StringToSlice(c.Param("key")))
	if err != nil {
		return c.JSON(http.StatusOK, &JSONResult{
			Code:  codeFailed,
			Error: err.Error(),
		})
	}

	req := rpcpb.AcquireBMRangeRequest()
	req.Key = key
	req.Start = uint32(start)
	req.Limit = limit

	value, err := s.engine.Storage().ExecCommand(req)
	rpcpb.ReleaseBMRangeRequest(req)
	if err != nil {
		return c.JSON(http.StatusOK, &JSONResult{
			Code:  codeFailed,
			Error: err.Error(),
		})
	}

	resp := rpcpb.AcquireBMRangeResponse()
	protoc.MustUnmarshal(resp, value)
	if len(resp.Values) == 0 {
		return c.JSON(http.StatusOK, &JSONResult{})
	}

	return c.JSON(http.StatusOK, &JSONResult{
		Value: EncodeUint32Slice(resp.Values),
	})
}

// DecodeUint32Slice decode []byte as []uint32
func DecodeUint32Slice(data []byte) ([]uint32, error) {
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

// EncodeUint32Slice encode []uint32 as []byte
func EncodeUint32Slice(src []uint32) []byte {
	buf := goetty.NewByteBuf(4 * len(src))
	for _, value := range src {
		buf.WriteUInt32(value)
	}
	_, data, _ := buf.ReadAll()
	buf.Release()
	return data
}

func parseKey(key []byte) ([]byte, error) {
	value := make([]byte, base64.StdEncoding.DecodedLen(len(key)))
	_, err := base64.StdEncoding.Decode(value, key)
	if err != nil {
		return nil, err
	}

	return value, nil
}
