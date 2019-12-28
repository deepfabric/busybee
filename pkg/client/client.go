package client

import (
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"

	"github.com/deepfabric/busybee/pkg/api"
	"github.com/deepfabric/busybee/pkg/pb/metapb"
)

const (
	jsonContextType = "application/json"
)

// Client engine client
type Client interface {
	// CreateWorkflow create a workflow definition
	CreateWorkflow(metapb.Workflow) (uint64, error)
	// UpdateWorkflow update a workflow definition
	UpdateWorkflow(metapb.Workflow) error
	// CreateInstance create a workflow instance with a crowd
	CreateInstance(uint64, []byte, uint64) (uint64, error)
	// DeleteInstance delete a workflow instance
	DeleteInstance(uint64) error
	// StartInstance start the instance
	StartInstance(uint64) error
	// StepInstance step instance
	StepInstance(metapb.Event) error
	// InstanceCount return count info of all steps
	InstanceCountState(uint64) (metapb.InstanceCountState, error)
	// InstanceStep return count info of all steps
	InstanceStepState(uint64, string) (metapb.StepState, error)

	// CreateEventQueue create event queue
	CreateEventQueue(id uint64) error
	// CreateNotifyQueue create notify queue
	CreateNotifyQueue(id uint64) error
	// AddToEventQueue Adds the item to the queue and returns the last offset
	AddToEventQueue(id uint64, items [][]byte) (uint64, error)
	// AddToNotifyQueue Adds the item to the queue and returns the last offset
	AddToNotifyQueue(id uint64, items [][]byte) (uint64, error)

	// Set put a key, value paire to the store
	Set(key, value []byte) error
	// Get returns the value of the key
	Get(key []byte) ([]byte, error)
	// Delete delete the key from the store
	Delete(key []byte) error
	// BMCreate create a bitmap of init values
	BMCreate(key []byte, values ...uint32) error
	// BMAdd adds []uint32 to the bitmap
	BMAdd(key []byte, values ...uint32) error
	// BMRemove remove []uint32 from the bitmap
	BMRemove(key []byte, values ...uint32) error
	// BMClear clear the bitmap
	BMClear(key []byte) error
	// BMRange returns a []uint32 from the bitmap that >= start
	BMRange(key []byte, start uint32, limit uint64) ([]uint32, error)
}

type httpClient struct {
	opts *options
	addr string
	cli  *http.Client
}

// NewClient returns a http client
func NewClient(addr string, opts ...Option) Client {
	cli := &http.Client{}
	*cli = *http.DefaultClient
	c := &httpClient{
		addr: fmt.Sprintf("http://%s", addr),
		opts: &options{},
		cli:  cli,
	}

	for _, opt := range opts {
		opt(c.opts)
	}

	c.opts.adjust()
	return c
}

func (c *httpClient) CreateWorkflow(value metapb.Workflow) (uint64, error) {
	data, err := json.Marshal(&value)
	if err != nil {
		return 0, err
	}

	resp, err := c.doPost(fmt.Sprintf("%s/workflows", c.addr), data)
	if err != nil {
		return 0, err
	}

	return readUint64Result(resp)
}

func (c *httpClient) UpdateWorkflow(value metapb.Workflow) error {
	if value.ID == 0 {
		return errors.New("missing workflow id")
	}

	data, err := json.Marshal(&value)
	if err != nil {
		return err
	}

	resp, err := c.doPut(fmt.Sprintf("%s/workflows/%d", c.addr, value.ID), data)
	if err != nil {
		return err
	}

	return readEmptyResult(resp)
}

func (c *httpClient) CreateInstance(workflowID uint64, crowd []byte, maxPerShard uint64) (uint64, error) {
	value := api.CreateInstance{
		ID:          workflowID,
		Crowd:       crowd,
		MaxPerShard: maxPerShard,
	}

	data, err := json.Marshal(&value)
	if err != nil {
		return 0, err
	}

	resp, err := c.doPost(fmt.Sprintf("%s/workflows/instance", c.addr), data)
	if err != nil {
		return 0, err
	}

	return readUint64Result(resp)
}

func (c *httpClient) DeleteInstance(instanceID uint64) error {
	resp, err := c.doDelete(fmt.Sprintf("%s/workflows/instance/%d", c.addr, instanceID))
	if err != nil {
		return err
	}

	return readEmptyResult(resp)
}

func (c *httpClient) StartInstance(instanceID uint64) error {
	resp, err := c.doPost(fmt.Sprintf("%s/workflows/instance/%d/start", c.addr, instanceID), nil)
	if err != nil {
		return err
	}

	return readEmptyResult(resp)
}

func (c *httpClient) StepInstance(event metapb.Event) error {
	data, err := json.Marshal(&event)
	if err != nil {
		return err
	}

	resp, err := c.doPut(fmt.Sprintf("%s/workflows/instance/%d/step", c.addr, event.InstanceID), data)
	if err != nil {
		return err
	}

	return readEmptyResult(resp)
}

func (c *httpClient) InstanceCountState(id uint64) (metapb.InstanceCountState, error) {
	resp, err := c.cli.Get(fmt.Sprintf("%s/workflows/instance/%d/state/count", c.addr, id))
	if err != nil {
		return metapb.InstanceCountState{}, err
	}

	return readInstanceCountStateResultResult(resp)
}

func (c *httpClient) InstanceStepState(id uint64, step string) (metapb.StepState, error) {
	resp, err := c.cli.Get(fmt.Sprintf("%s/workflows/instance/%d/state/step/%s", c.addr, id, step))
	if err != nil {
		return metapb.StepState{}, err
	}

	return readInstanceStepStateResultResult(resp)
}

func (c *httpClient) CreateEventQueue(id uint64) error {
	resp, err := c.doPost(fmt.Sprintf("%s/queues/%d/event", c.addr, id), nil)
	if err != nil {
		return err
	}

	return readEmptyResult(resp)
}

func (c *httpClient) CreateNotifyQueue(id uint64) error {
	resp, err := c.doPost(fmt.Sprintf("%s/queues/%d/notify", c.addr, id), nil)
	if err != nil {
		return err
	}

	return readEmptyResult(resp)
}

func (c *httpClient) AddToEventQueue(id uint64, items [][]byte) (uint64, error) {
	value, _ := json.Marshal(&api.QueueAdd{
		Items: items,
	})

	resp, err := c.doPut(fmt.Sprintf("%s/queues/%d/event/add", c.addr, id), value)
	if err != nil {
		return 0, err
	}

	return readUint64Result(resp)
}

func (c *httpClient) AddToNotifyQueue(id uint64, items [][]byte) (uint64, error) {
	value, _ := json.Marshal(&api.QueueAdd{
		Items: items,
	})

	resp, err := c.doPut(fmt.Sprintf("%s/queues/%d/notify/add", c.addr, id), value)
	if err != nil {
		return 0, err
	}

	return readUint64Result(resp)
}

func (c *httpClient) Set(key, value []byte) error {
	resp, err := c.doPut(fmt.Sprintf("%s/store/%s", c.addr, base64.StdEncoding.EncodeToString(key)), value)
	if err != nil {
		return err
	}

	return readEmptyResult(resp)
}

func (c *httpClient) Get(key []byte) ([]byte, error) {
	resp, err := c.cli.Get(fmt.Sprintf("%s/store/%s", c.addr, base64.StdEncoding.EncodeToString(key)))
	if err != nil {
		return nil, err
	}

	return readBytesResult(resp)
}

func (c *httpClient) Delete(key []byte) error {
	resp, err := c.doDelete(fmt.Sprintf("%s/store/%s", c.addr, base64.StdEncoding.EncodeToString(key)))
	if err != nil {
		return err
	}

	return readEmptyResult(resp)
}

func (c *httpClient) BMCreate(key []byte, values ...uint32) error {
	resp, err := c.doPut(fmt.Sprintf("%s/bitmap/%s", c.addr, base64.StdEncoding.EncodeToString(key)), api.EncodeUint32Slice(values))
	if err != nil {
		return err
	}

	return readEmptyResult(resp)
}

func (c *httpClient) BMAdd(key []byte, values ...uint32) error {
	resp, err := c.doPut(fmt.Sprintf("%s/bitmap/%s/add", c.addr, base64.StdEncoding.EncodeToString(key)), api.EncodeUint32Slice(values))
	if err != nil {
		return err
	}

	return readEmptyResult(resp)
}

func (c *httpClient) BMRemove(key []byte, values ...uint32) error {
	resp, err := c.doPut(fmt.Sprintf("%s/bitmap/%s/remove", c.addr, base64.StdEncoding.EncodeToString(key)), api.EncodeUint32Slice(values))
	if err != nil {
		return err
	}

	return readEmptyResult(resp)
}

func (c *httpClient) BMClear(key []byte) error {
	resp, err := c.doPut(fmt.Sprintf("%s/bitmap/%s/clear", c.addr, base64.StdEncoding.EncodeToString(key)), nil)
	if err != nil {
		return err
	}

	return readEmptyResult(resp)
}

func (c *httpClient) BMRange(key []byte, start uint32, limit uint64) ([]uint32, error) {
	resp, err := c.cli.Get(fmt.Sprintf("%s/bitmap/%s/range?start=%d&limit=%d",
		c.addr,
		base64.StdEncoding.EncodeToString(key),
		start,
		limit))
	if err != nil {
		return nil, err
	}

	value, err := readBytesResult(resp)
	if err != nil {
		return nil, err
	}

	return api.DecodeUint32Slice(value)
}
