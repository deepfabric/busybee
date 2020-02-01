package api

import (
	"io"
	"sync"

	"github.com/deepfabric/beehive/util"
	"github.com/deepfabric/busybee/pkg/core"
	"github.com/deepfabric/busybee/pkg/pb/rpcpb"
	"github.com/fagongzi/goetty"
	"github.com/fagongzi/log"
)

// Server is the api server
type Server interface {
	Start() error
	Stop() error
}

type server struct {
	addr         string
	engine       core.Engine
	tenantQueues sync.Map // tenanId -> *tenantQueue

	svr      *goetty.Server
	sessions sync.Map // interface{} -> *util.Session
}

// NewAPIServer create a server
func NewAPIServer(addr string, engine core.Engine) (Server, error) {
	return &server{
		addr: addr,
		svr: goetty.NewServer(addr,
			goetty.WithServerDecoder(decoder),
			goetty.WithServerEncoder(encoder)),
		engine: engine,
	}, nil
}

func (s *server) Start() error {
	log.Infof("api server start at %s", s.addr)
	c := make(chan error)
	go func() {
		c <- s.svr.Start(s.doConnection)
	}()

	select {
	case <-s.svr.Started():
		return nil
	case err := <-c:
		return err
	}
}

func (s *server) Stop() error {
	s.svr.Stop()
	return nil
}

func (s *server) doConnection(conn goetty.IOSession) error {
	rs := util.NewSession(conn, releaseResponse)
	s.sessions.Store(rs.ID, rs)
	log.Infof("session %d[%s] connected",
		rs.ID,
		rs.Addr)

	defer func() {
		s.sessions.Delete(rs.ID)
		rs.Close()
	}()

	for {
		value, err := conn.Read()
		if err != nil {
			if err == io.EOF {
				return nil
			}

			log.Errorf("session %d[%s] read failed with %+v",
				rs.ID,
				rs.Addr,
				err)
			return err
		}

		req := value.(*rpcpb.Request)
		err = s.onReq(rs.ID, req)
		if err != nil {
			rsp := rpcpb.AcquireResponse()
			rsp.ID = req.ID
			rsp.Error.Error = err.Error()
			rs.OnResp(rsp)
		}
	}
}

func releaseResponse(resp interface{}) {
	rpcpb.ReleaseResponse(resp.(*rpcpb.Response))
}
