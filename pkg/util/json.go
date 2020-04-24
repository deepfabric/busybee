package util

import (
	"github.com/buger/jsonparser"
)

// ExtractJSONField returns the field value in the json
func ExtractJSONField(src []byte, paths ...string) []byte {
	value, _, _, err := jsonparser.Get(src, paths...)
	if err != nil {
		return nil
	}

	return value
}
