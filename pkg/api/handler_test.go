package api

import (
	"fmt"
	"testing"
	"time"

	"github.com/deepfabric/busybee/pkg/core"
	"github.com/deepfabric/busybee/pkg/pb/metapb"
	"github.com/deepfabric/busybee/pkg/pb/rpcpb"
	"github.com/deepfabric/busybee/pkg/queue"
	"github.com/deepfabric/busybee/pkg/storage"
	"github.com/deepfabric/busybee/pkg/util"
	"github.com/fagongzi/goetty"
	"github.com/fagongzi/util/protoc"
	"github.com/stretchr/testify/assert"
)

func newTestServer(t *testing.T) (core.Engine, func()) {
	store, deferFunc := storage.NewTestStorage(t, false)

	eng, err := core.NewEngine(store, nil)
	assert.NoError(t, err, "newTestServer failed")

	eng.Start()
	s, err := NewAPIServer("127.0.0.1:12345", eng)
	assert.NoError(t, err, "newTestServer failed")
	assert.NoError(t, s.Start(), "newTestServer failed")

	return eng, func() {
		s.Stop()
		eng.Stop()
		deferFunc()
	}
}

type cliCodec struct {
}

func (c *cliCodec) Decode(in *goetty.ByteBuf) (bool, interface{}, error) {
	data := in.GetMarkedRemindData()
	req := rpcpb.AcquireResponse()
	err := req.Unmarshal(data)
	if err != nil {
		return false, nil, err
	}

	in.MarkedBytesReaded()
	return true, req, nil
}

func (c *cliCodec) Encode(data interface{}, out *goetty.ByteBuf) error {
	if req, ok := data.(*rpcpb.Request); ok {
		index := out.GetWriteIndex()
		size := req.Size()
		out.Expansion(size)
		protoc.MustMarshalTo(req, out.RawBuf()[index:index+size])
		out.SetWriterIndex(index + size)
		rpcpb.ReleaseRequest(req)
		return nil
	}

	return fmt.Errorf("not support %T %+v", data, data)
}

func createConn(t *testing.T) goetty.IOSession {
	c := &cliCodec{}
	conn := goetty.NewConnector("127.0.0.1:12345",
		goetty.WithClientDecoder(goetty.NewIntLengthFieldBasedDecoder(c)),
		goetty.WithClientEncoder(goetty.NewIntLengthFieldBasedEncoder(c)))
	_, err := conn.Connect()
	assert.NoError(t, err, "createConn failed")
	return conn
}

func TestGetAndSetAndDelete(t *testing.T) {
	_, deferFunc := newTestServer(t)
	defer deferFunc()

	conn := createConn(t)
	defer conn.Close()

	key := []byte("key")
	value := []byte("value")

	req := rpcpb.AcquireRequest()
	req.Type = rpcpb.Set
	req.Set.Key = key
	req.Set.Value = value
	assert.NoError(t, conn.WriteAndFlush(req), "TestGetAndSetAndDelete failed")
	data, err := conn.ReadTimeout(time.Second * 10)
	assert.NoError(t, err, "TestGetAndSetAndDelete failed")
	resp := data.(*rpcpb.Response)
	assert.Empty(t, resp.Error.Error, "TestGetAndSetAndDelete failed")

	req = rpcpb.AcquireRequest()
	req.Type = rpcpb.Get
	req.Get.Key = key
	assert.NoError(t, conn.WriteAndFlush(req), "TestGetAndSetAndDelete failed")
	data, err = conn.ReadTimeout(time.Second * 10)
	assert.NoError(t, err, "TestGetAndSetAndDelete failed")
	resp = data.(*rpcpb.Response)
	assert.Empty(t, resp.Error.Error, "TestGetAndSetAndDelete failed")
	assert.Equal(t, string(value), string(resp.BytesResp.Value), "TestGetAndSetAndDelete failed")

	req = rpcpb.AcquireRequest()
	req.Type = rpcpb.Delete
	req.Delete.Key = key
	assert.NoError(t, conn.WriteAndFlush(req), "TestGetAndSetAndDelete failed")
	_, err = conn.ReadTimeout(time.Second * 10)
	assert.NoError(t, err, "TestGetAndSetAndDelete failed")

	req = rpcpb.AcquireRequest()
	req.Type = rpcpb.Get
	req.Get.Key = key
	assert.NoError(t, conn.WriteAndFlush(req), "TestGetAndSetAndDelete failed")
	data, err = conn.ReadTimeout(time.Second * 10)
	assert.NoError(t, err, "TestGetAndSetAndDelete failed")
	resp = data.(*rpcpb.Response)
	assert.Empty(t, resp.Error.Error, "TestGetAndSetAndDelete failed")
	assert.Equal(t, "", string(resp.BytesResp.Value), "TestGetAndSetAndDelete failed")
}

func TestBMCreate(t *testing.T) {
	_, deferFunc := newTestServer(t)
	defer deferFunc()

	conn := createConn(t)
	defer conn.Close()

	key := []byte("key")

	req := rpcpb.AcquireRequest()
	req.Type = rpcpb.BMCreate
	req.BmCreate.Key = key
	req.BmCreate.Value = []uint32{1, 2, 3}
	assert.NoError(t, conn.WriteAndFlush(req), "TestBMCreate failed")
	data, err := conn.ReadTimeout(time.Second * 10)
	assert.NoError(t, err, "TestBMCreate failed")
	resp := data.(*rpcpb.Response)
	assert.Empty(t, resp.Error.Error, "TestBMCreate failed")

	req = rpcpb.AcquireRequest()
	req.Type = rpcpb.Get
	req.Get.Key = key
	assert.NoError(t, conn.WriteAndFlush(req), "TestBMCreate failed")
	data, err = conn.ReadTimeout(time.Second * 10)
	assert.NoError(t, err, "TestBMCreate failed")
	resp = data.(*rpcpb.Response)
	assert.Empty(t, resp.Error.Error, "TestBMCreate failed")

	bm := util.MustParseBM(resp.BytesResp.Value)
	assert.Equal(t, uint64(3), bm.GetCardinality(), "TestBMCreate failed")
}

func TestBMAdd(t *testing.T) {
	_, deferFunc := newTestServer(t)
	defer deferFunc()

	conn := createConn(t)
	defer conn.Close()

	key := []byte("key")

	req := rpcpb.AcquireRequest()
	req.Type = rpcpb.BMAdd
	req.BmAdd.Key = key
	req.BmAdd.Value = []uint32{1, 2, 3}
	assert.NoError(t, conn.WriteAndFlush(req), "TestBMAdd failed")
	data, err := conn.ReadTimeout(time.Second * 10)
	assert.NoError(t, err, "TestBMAdd failed")
	resp := data.(*rpcpb.Response)
	assert.Empty(t, resp.Error.Error, "TestBMAdd failed")

	req = rpcpb.AcquireRequest()
	req.Type = rpcpb.Get
	req.Get.Key = key
	assert.NoError(t, conn.WriteAndFlush(req), "TestBMAdd failed")
	data, err = conn.ReadTimeout(time.Second * 10)
	assert.NoError(t, err, "TestBMAdd failed")
	resp = data.(*rpcpb.Response)
	assert.Empty(t, resp.Error.Error, "TestBMAdd failed")

	bm := util.MustParseBM(resp.BytesResp.Value)
	assert.Equal(t, uint64(3), bm.GetCardinality(), "TestBMAdd failed")
}

func TestBMRemove(t *testing.T) {
	_, deferFunc := newTestServer(t)
	defer deferFunc()

	conn := createConn(t)
	defer conn.Close()

	key := []byte("key")

	req := rpcpb.AcquireRequest()
	req.Type = rpcpb.BMAdd
	req.BmAdd.Key = key
	req.BmAdd.Value = []uint32{1, 2, 3}
	assert.NoError(t, conn.WriteAndFlush(req), "TestBMRemove failed")
	conn.ReadTimeout(time.Second * 10)

	req = rpcpb.AcquireRequest()
	req.Type = rpcpb.BMRemove
	req.BmRemove.Key = key
	req.BmRemove.Value = []uint32{1}
	assert.NoError(t, conn.WriteAndFlush(req), "TestBMRemove failed")
	data, err := conn.ReadTimeout(time.Second * 10)
	assert.NoError(t, err, "TestBMRemove failed")
	resp := data.(*rpcpb.Response)
	assert.Empty(t, resp.Error.Error, "TestBMRemove failed")

	req = rpcpb.AcquireRequest()
	req.Type = rpcpb.Get
	req.Get.Key = key
	assert.NoError(t, conn.WriteAndFlush(req), "TestBMRemove failed")
	data, err = conn.ReadTimeout(time.Second * 10)
	assert.NoError(t, err, "TestBMRemove failed")
	resp = data.(*rpcpb.Response)
	assert.Empty(t, resp.Error.Error, "TestBMRemove failed")

	bm := util.MustParseBM(resp.BytesResp.Value)
	assert.Equal(t, uint64(2), bm.GetCardinality(), "TestBMRemove failed")
}

func TestBMClear(t *testing.T) {
	_, deferFunc := newTestServer(t)
	defer deferFunc()

	conn := createConn(t)
	defer conn.Close()

	key := []byte("key")

	req := rpcpb.AcquireRequest()
	req.Type = rpcpb.BMAdd
	req.BmAdd.Key = key
	req.BmAdd.Value = []uint32{1, 2, 3}
	assert.NoError(t, conn.WriteAndFlush(req), "TestBMClear failed")
	conn.ReadTimeout(time.Second * 10)

	req = rpcpb.AcquireRequest()
	req.Type = rpcpb.BMClear
	req.BmClear.Key = key
	assert.NoError(t, conn.WriteAndFlush(req), "TestBMClear failed")
	data, err := conn.ReadTimeout(time.Second * 10)
	assert.NoError(t, err, "TestBMClear failed")
	resp := data.(*rpcpb.Response)
	assert.Empty(t, resp.Error.Error, "TestBMClear failed")

	req = rpcpb.AcquireRequest()
	req.Type = rpcpb.Get
	req.Get.Key = key
	assert.NoError(t, conn.WriteAndFlush(req), "TestBMClear failed")
	data, err = conn.ReadTimeout(time.Second * 10)
	assert.NoError(t, err, "TestBMClear failed")
	resp = data.(*rpcpb.Response)
	assert.Empty(t, resp.Error.Error, "TestBMClear failed")

	bm := util.MustParseBM(resp.BytesResp.Value)
	assert.Equal(t, uint64(0), bm.GetCardinality(), "TestBMClear failed")
}

func TestBMRange(t *testing.T) {
	_, deferFunc := newTestServer(t)
	defer deferFunc()

	conn := createConn(t)
	defer conn.Close()

	key := []byte("key")

	req := rpcpb.AcquireRequest()
	req.Type = rpcpb.BMCreate
	req.BmCreate.Key = key
	req.BmCreate.Value = []uint32{1, 2, 3, 4, 5, 6}
	assert.NoError(t, conn.WriteAndFlush(req), "TestBMRange failed")
	conn.ReadTimeout(time.Second * 10)

	req = rpcpb.AcquireRequest()
	req.Type = rpcpb.BMRange
	req.BmRange.Key = key
	req.BmRange.Start = 1
	req.BmRange.Limit = 3
	assert.NoError(t, conn.WriteAndFlush(req), "TestBMRange failed")

	data, err := conn.ReadTimeout(time.Second * 10)
	assert.NoError(t, err, "TestBMRange failed")
	resp := data.(*rpcpb.Response)
	assert.Equal(t, 3, len(resp.Uint32SliceResp.Values), "TestBMRange failed")
	assert.Equal(t, uint32(1), resp.Uint32SliceResp.Values[0], "TestBMRange failed")
	assert.Equal(t, uint32(3), resp.Uint32SliceResp.Values[2], "TestBMRange failed")
}

func TestBMCount(t *testing.T) {
	_, deferFunc := newTestServer(t)
	defer deferFunc()

	conn := createConn(t)
	defer conn.Close()

	key := []byte("key")

	req := rpcpb.AcquireRequest()
	req.Type = rpcpb.BMCreate
	req.BmCreate.Key = key
	req.BmCreate.Value = []uint32{1, 2, 3}
	assert.NoError(t, conn.WriteAndFlush(req), "TestBMCount failed")
	conn.ReadTimeout(time.Second * 10)

	req = rpcpb.AcquireRequest()
	req.Type = rpcpb.BMCount
	req.BmCount.Key = key
	assert.NoError(t, conn.WriteAndFlush(req), "TestBMCount failed")

	data, err := conn.ReadTimeout(time.Second * 10)
	assert.NoError(t, err, "TestBMCount failed")
	resp := data.(*rpcpb.Response)
	assert.Equal(t, uint64(3), resp.Uint64Resp.Value, "TestBMCount failed")
}

func TestBMContains(t *testing.T) {
	_, deferFunc := newTestServer(t)
	defer deferFunc()

	conn := createConn(t)
	defer conn.Close()

	key := []byte("key")

	req := rpcpb.AcquireRequest()
	req.Type = rpcpb.BMCreate
	req.BmCreate.Key = key
	req.BmCreate.Value = []uint32{1, 2, 3}
	assert.NoError(t, conn.WriteAndFlush(req), "TestBMContains failed")
	conn.ReadTimeout(time.Second * 10)

	req = rpcpb.AcquireRequest()
	req.Type = rpcpb.BMContains
	req.BmContains.Key = key
	req.BmContains.Value = []uint32{1, 2}
	assert.NoError(t, conn.WriteAndFlush(req), "TestBMContains failed")

	data, err := conn.ReadTimeout(time.Second * 10)
	assert.NoError(t, err, "TestBMContains failed")
	resp := data.(*rpcpb.Response)
	assert.True(t, resp.BoolResp.Value, "TestBMCount failed")
}

func TestProfile(t *testing.T) {
	_, deferFunc := newTestServer(t)
	defer deferFunc()

	conn := createConn(t)
	defer conn.Close()

	tid := uint64(1)
	uid := uint32(100)
	value := []byte(`{"name":"zhangsan", "age": 18}`)

	req := rpcpb.AcquireRequest()
	req.Type = rpcpb.UpdateProfile
	req.UpdateProfile.ID = tid
	req.UpdateProfile.UserID = uid
	req.UpdateProfile.Value = value
	assert.NoError(t, conn.WriteAndFlush(req), "TestProfile failed")
	data, err := conn.ReadTimeout(time.Second * 10)
	assert.NoError(t, err, "TestProfile failed")
	resp := data.(*rpcpb.Response)
	assert.Empty(t, resp.Error.Error, "TestProfile failed")

	req = rpcpb.AcquireRequest()
	req.Type = rpcpb.GetProfile
	req.GetProfile.ID = tid
	req.GetProfile.UserID = uid
	assert.NoError(t, conn.WriteAndFlush(req), "TestProfile failed")
	data, err = conn.ReadTimeout(time.Second * 10)
	assert.NoError(t, err, "TestProfile failed")
	resp = data.(*rpcpb.Response)
	assert.Empty(t, resp.Error.Error, "TestProfile failed")
	assert.Equal(t, string(value), string(resp.BytesResp.Value), "TestProfile failed")

	req = rpcpb.AcquireRequest()
	req.Type = rpcpb.GetProfile
	req.GetProfile.ID = tid
	req.GetProfile.UserID = uid
	req.GetProfile.Field = "name"
	assert.NoError(t, conn.WriteAndFlush(req), "TestProfile failed")
	data, err = conn.ReadTimeout(time.Second * 10)
	assert.NoError(t, err, "TestProfile failed")
	resp = data.(*rpcpb.Response)
	assert.Empty(t, resp.Error.Error, "TestProfile failed")
	assert.Equal(t, "zhangsan", string(resp.BytesResp.Value), "TestProfile failed")

	req = rpcpb.AcquireRequest()
	req.Type = rpcpb.GetProfile
	req.GetProfile.ID = tid
	req.GetProfile.UserID = uid
	req.GetProfile.Field = "age"
	assert.NoError(t, conn.WriteAndFlush(req), "TestProfile failed")
	data, err = conn.ReadTimeout(time.Second * 10)
	assert.NoError(t, err, "TestProfile failed")
	resp = data.(*rpcpb.Response)
	assert.Empty(t, resp.Error.Error, "TestProfile failed")
	assert.Equal(t, "18", string(resp.BytesResp.Value), "TestProfile failed")
}

func TestUpdateAndScanMapping(t *testing.T) {
	_, deferFunc := newTestServer(t)
	defer deferFunc()

	conn := createConn(t)
	defer conn.Close()

	tid := uint64(1)
	userID := uint32(100)

	req := rpcpb.AcquireRequest()
	req.Type = rpcpb.UpdateMapping
	req.UpdateMapping.ID = tid
	req.UpdateMapping.UserID = userID
	req.UpdateMapping.Set.Values = []metapb.IDValue{
		metapb.IDValue{Value: "v1", Type: "1"},
		metapb.IDValue{Value: "v2", Type: "2"},
		metapb.IDValue{Value: "v3", Type: "3"},
	}
	assert.NoError(t, conn.WriteAndFlush(req), "TestMapping failed")
	data, err := conn.ReadTimeout(time.Second * 10)
	assert.NoError(t, err, "TestMapping failed")
	resp := data.(*rpcpb.Response)
	assert.Empty(t, resp.Error.Error, "TestMapping failed")

	req.Reset()
	req.Type = rpcpb.GetIDSet
	req.GetIDSet.ID = tid
	req.GetIDSet.UserID = userID
	assert.NoError(t, conn.WriteAndFlush(req), "TestMapping failed")
	data, err = conn.ReadTimeout(time.Second * 10)
	assert.NoError(t, err, "TestMapping failed")
	resp = data.(*rpcpb.Response)
	assert.Empty(t, resp.Error.Error, "TestMapping failed")
	set := &metapb.IDSet{}
	protoc.MustUnmarshal(set, resp.BytesResp.Value)
	assert.Equal(t, 3, len(set.Values), "TestMapping failed")

	for i := uint32(1); i <= 3; i++ {
		value := fmt.Sprintf("v%d", i)

		for j := uint32(1); j <= 3; j++ {
			if i != j {
				expect := fmt.Sprintf("v%d", j)

				req = rpcpb.AcquireRequest()
				req.Type = rpcpb.GetMapping
				req.GetMapping.ID = tid
				req.GetMapping.From.Value = value
				req.GetMapping.From.Type = fmt.Sprintf("%d", i)
				req.GetMapping.To = fmt.Sprintf("%d", j)

				assert.NoError(t, conn.WriteAndFlush(req), "TestMapping failed")
				data, err = conn.ReadTimeout(time.Second * 10)
				assert.NoError(t, err, "TestMapping failed")
				resp = data.(*rpcpb.Response)
				assert.Empty(t, resp.Error.Error, "TestMapping failed")
				assert.Equal(t, expect, string(resp.BytesResp.Value), "TestMapping failed")
			}
		}
	}

	req.Reset()
	req.Type = rpcpb.UpdateMapping
	req.UpdateMapping.ID = tid
	req.UpdateMapping.UserID = userID
	req.UpdateMapping.Set.Values = []metapb.IDValue{
		metapb.IDValue{Value: "v11", Type: "1"},
		metapb.IDValue{Value: "v22", Type: "2"},
		metapb.IDValue{Value: "v33", Type: "3"},
	}
	assert.NoError(t, conn.WriteAndFlush(req), "TestMapping failed")
	data, err = conn.ReadTimeout(time.Second * 10)
	assert.NoError(t, err, "TestMapping failed")
	resp = data.(*rpcpb.Response)
	assert.Empty(t, resp.Error.Error, "TestMapping failed")

	for i := uint32(1); i <= 3; i++ {
		value := fmt.Sprintf("v%d%d", i, i)

		for j := uint32(1); j <= 3; j++ {
			if i != j {
				expect := fmt.Sprintf("v%d%d", j, j)

				req = rpcpb.AcquireRequest()
				req.Type = rpcpb.GetMapping
				req.GetMapping.ID = tid
				req.GetMapping.From.Value = value
				req.GetMapping.From.Type = fmt.Sprintf("%d", i)
				req.GetMapping.To = fmt.Sprintf("%d", j)

				assert.NoError(t, conn.WriteAndFlush(req), "TestMapping failed")
				data, err = conn.ReadTimeout(time.Second * 10)
				assert.NoError(t, err, "TestMapping failed")
				resp = data.(*rpcpb.Response)
				assert.Empty(t, resp.Error.Error, "TestMapping failed")
				assert.Equal(t, expect, string(resp.BytesResp.Value), "TestMapping failed")
			}
		}
	}

	req = rpcpb.AcquireRequest()
	req.Type = rpcpb.ScanMapping
	req.ScanMapping.ID = tid
	req.ScanMapping.From = userID
	req.ScanMapping.To = userID + 1
	req.ScanMapping.Limit = 10
	assert.NoError(t, conn.WriteAndFlush(req), "TestMapping failed")
	data, err = conn.ReadTimeout(time.Second * 10)
	assert.NoError(t, err, "TestMapping failed")
	resp = data.(*rpcpb.Response)
	assert.Empty(t, resp.Error.Error, "TestMapping failed")
	assert.Equal(t, 1, len(resp.BytesSliceResp.Values), "TestMapping failed")
	idValue := &metapb.IDSet{}
	protoc.MustUnmarshal(idValue, resp.BytesSliceResp.Values[0])
	assert.Equal(t, 3, len(idValue.Values), "TestMapping failed")
}

func TestConcurrencyFetchOutput(t *testing.T) {
	eng, deferFunc := newTestServer(t)
	defer deferFunc()

	conn := createConn(t)
	defer conn.Close()

	tid := uint64(1)

	req := rpcpb.AcquireRequest()
	req.Type = rpcpb.TenantInit
	req.TenantInit.ID = tid
	req.TenantInit.InputQueuePartitions = 1
	assert.NoError(t, conn.WriteAndFlush(req), "TestConcurrencyFetchOutput failed")

	_, err := conn.ReadTimeout(time.Second * 10)
	assert.NoError(t, err, "TestConcurrencyFetchOutput failed")

	time.Sleep(time.Second)

	_, err = eng.Storage().ExecCommandWithGroup(&rpcpb.QueueAddRequest{
		Key: queue.PartitionKey(tid, 0),
		Items: [][]byte{[]byte("1"), []byte("2"), []byte("3"),
			[]byte("4"), []byte("5"), []byte("6"),
			[]byte("7"), []byte("8"), []byte("9")},
	}, metapb.TenantOutputGroup)
	assert.NoError(t, err, "TestConcurrencyFetchOutput failed")

	req.Reset()
	req.Type = rpcpb.FetchNotify
	req.FetchNotify.ID = tid
	req.FetchNotify.CompletedOffset = 0
	req.FetchNotify.Concurrency = 3
	req.FetchNotify.Count = 10
	req.FetchNotify.Consumer = "c1"
	assert.NoError(t, conn.WriteAndFlush(req), "TestConcurrencyFetchOutput failed")
	data, err := conn.ReadTimeout(time.Second * 10)
	assert.NoError(t, err, "TestConcurrencyFetchOutput failed")
	resp := data.(*rpcpb.Response)
	assert.Equal(t, 3, len(resp.BytesSliceResp.Values), "TestConcurrencyFetchOutput failed")
	assert.Equal(t, uint64(3), resp.BytesSliceResp.LastValue, "TestConcurrencyFetchOutput failed")

	req.Reset()
	req.Type = rpcpb.FetchNotify
	req.FetchNotify.ID = tid
	req.FetchNotify.CompletedOffset = 0
	req.FetchNotify.Concurrency = 3
	req.FetchNotify.Count = 10
	req.FetchNotify.Consumer = "c1"
	assert.NoError(t, conn.WriteAndFlush(req), "TestConcurrencyFetchOutput failed")
	data, err = conn.ReadTimeout(time.Second * 10)
	assert.NoError(t, err, "TestConcurrencyFetchOutput failed")
	resp = data.(*rpcpb.Response)
	assert.Equal(t, 3, len(resp.BytesSliceResp.Values), "TestConcurrencyFetchOutput failed")
	assert.Equal(t, uint64(6), resp.BytesSliceResp.LastValue, "TestConcurrencyFetchOutput failed")

	req.Reset()
	req.Type = rpcpb.FetchNotify
	req.FetchNotify.ID = tid
	req.FetchNotify.CompletedOffset = 0
	req.FetchNotify.Concurrency = 3
	req.FetchNotify.Count = 10
	req.FetchNotify.Consumer = "c1"
	assert.NoError(t, conn.WriteAndFlush(req), "TestConcurrencyFetchOutput failed")
	data, err = conn.ReadTimeout(time.Second * 10)
	assert.NoError(t, err, "TestConcurrencyFetchOutput failed")
	resp = data.(*rpcpb.Response)
	assert.Equal(t, 3, len(resp.BytesSliceResp.Values), "TestConcurrencyFetchOutput failed")
	assert.Equal(t, uint64(9), resp.BytesSliceResp.LastValue, "TestConcurrencyFetchOutput failed")

	req.Reset()
	req.Type = rpcpb.FetchNotify
	req.FetchNotify.ID = tid
	req.FetchNotify.CompletedOffset = 0
	req.FetchNotify.Concurrency = 3
	req.FetchNotify.Count = 10
	req.FetchNotify.Consumer = "c1"
	assert.NoError(t, conn.WriteAndFlush(req), "TestConcurrencyFetchOutput failed")
	data, err = conn.ReadTimeout(time.Second * 10)
	assert.NoError(t, err, "TestConcurrencyFetchOutput failed")
	resp = data.(*rpcpb.Response)
	assert.Equal(t, 0, len(resp.BytesSliceResp.Values), "TestConcurrencyFetchOutput failed")
	assert.Equal(t, uint64(0), resp.BytesSliceResp.LastValue, "TestConcurrencyFetchOutput failed")

}
