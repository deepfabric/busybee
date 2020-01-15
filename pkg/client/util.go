package client

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"

	"github.com/deepfabric/busybee/pkg/api"
	"github.com/deepfabric/busybee/pkg/pb/metapb"
)

func (c *httpClient) doPost(url string, data []byte) (*http.Response, error) {
	req, err := http.NewRequest("POST", url, bytes.NewBuffer(data))
	if err != nil {
		return nil, err
	}

	req.Header.Set("Content-Type", jsonContextType)
	return c.cli.Do(req)
}

func (c *httpClient) doPut(url string, data []byte) (*http.Response, error) {
	req, err := http.NewRequest("PUT", url, bytes.NewBuffer(data))
	if err != nil {
		return nil, err
	}

	req.Header.Set("Content-Type", jsonContextType)
	return c.cli.Do(req)
}

func (c *httpClient) doDelete(url string) (*http.Response, error) {
	req, err := http.NewRequest("DELETE", url, nil)
	if err != nil {
		return nil, err
	}

	req.Header.Set("Content-Type", jsonContextType)
	return c.cli.Do(req)
}

func readUint64Result(resp *http.Response) (uint64, error) {
	result := &uint64Result{}
	err := readResult(resp, result)
	if err != nil {
		return 0, err
	}

	return result.Value, nil
}

func readBytesResult(resp *http.Response) ([]byte, error) {
	result := &bytesResult{}
	err := readResult(resp, result)
	if err != nil {
		return nil, err
	}

	return result.Value, nil
}

func readQueueFetchResult(resp *http.Response) (api.QueueFetchResult, error) {
	result := &queueFetchResult{}
	err := readResult(resp, result)
	if err != nil {
		return api.QueueFetchResult{}, err
	}

	return result.Value, nil
}

func readEmptyResult(resp *http.Response) error {
	return readResult(resp, &codeResult{})
}

func readInstanceCountStateResultResult(resp *http.Response) (metapb.InstanceCountState, error) {
	result := &instanceCountStateResult{}
	err := readResult(resp, result)
	if err != nil {
		return metapb.InstanceCountState{}, err
	}

	return result.Value, nil
}

func readInstanceStepStateResultResult(resp *http.Response) (metapb.StepState, error) {
	result := &instanceStepStateResult{}
	err := readResult(resp, result)
	if err != nil {
		return metapb.StepState{}, err
	}

	return result.Value, nil
}

func readResult(resp *http.Response, result result) error {
	data, err := ioutil.ReadAll(resp.Body)
	resp.Body.Close()
	if err != nil {
		return err
	}

	err = json.Unmarshal(data, result)
	if err != nil {
		return err
	}

	if result.GetCode() != 0 {
		return fmt.Errorf("%+v", result.GetError())
	}

	return nil
}
