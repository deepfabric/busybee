package api

import (
	"fmt"

	"github.com/deepfabric/busybee/pkg/pb/apipb"
	"github.com/fagongzi/goetty"
	"github.com/fagongzi/util/protoc"
)

var (
	c       = &codec{}
	decoder = goetty.NewIntLengthFieldBasedDecoder(c)
	encoder = goetty.NewIntLengthFieldBasedEncoder(c)
)

type codec struct {
}

func (c *codec) Decode(in *goetty.ByteBuf) (bool, interface{}, error) {
	data := in.GetMarkedRemindData()
	req := apipb.AcquireRequest()
	err := req.Unmarshal(data)
	if err != nil {
		return false, nil, err
	}

	in.MarkedBytesReaded()
	return true, req, nil
}

func (c *codec) Encode(data interface{}, out *goetty.ByteBuf) error {
	if resp, ok := data.(*apipb.Response); ok {
		index := out.GetWriteIndex()
		size := resp.Size()
		out.Expansion(size)
		protoc.MustMarshalTo(resp, out.RawBuf()[index:index+size])
		out.SetWriterIndex(index + size)
		apipb.ReleaseResponse(resp)
		return nil
	}

	return fmt.Errorf("not support %T %+v", data, data)
}
