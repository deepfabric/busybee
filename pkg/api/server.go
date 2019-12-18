package api

import (
	"context"
	"github.com/deepfabric/busybee/pkg/core"
	"github.com/fagongzi/log"
	"github.com/labstack/echo"
)

const (
	codeOK     = 0
	codeFailed = 1
)

// Server is the api server
type Server interface {
	Start() error
	Stop() error
}

type httpServer struct {
	addr   string
	http   *echo.Echo
	engine core.Engine
}

// NewHTTPServer create a http restful server
func NewHTTPServer(addr string, engine core.Engine) (Server, error) {
	return &httpServer{
		addr:   addr,
		http:   echo.New(),
		engine: engine,
	}, nil
}

func (s *httpServer) Start() error {
	s.initQueueAPI()
	s.initWorkflowAPI()
	s.initStore()

	log.Infof("api server start at %s", s.addr)
	return s.http.Start(s.addr)
}

func (s *httpServer) Stop() error {
	return s.http.Shutdown(context.Background())
}
