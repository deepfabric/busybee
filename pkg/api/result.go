package api

// JSONResult json result
type JSONResult struct {
	Code  int         `json:"code"`
	Error string      `json:"err"`
	Value interface{} `json:"value"`
}
