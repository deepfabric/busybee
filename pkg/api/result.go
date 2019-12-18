package api

// JSONResult json result
type JSONResult struct {
	Code int         `json:"code"`
	Data interface{} `json:"data"`
}
