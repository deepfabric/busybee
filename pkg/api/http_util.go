package api

import (
	"encoding/json"
	"io/ioutil"

	"github.com/labstack/echo"
)

func readJSONFromBody(c echo.Context, value interface{}) error {
	data, err := readBody(c)
	if err != nil {
		return err
	}

	if len(data) > 0 {
		err = json.Unmarshal(data, value)
		if err != nil {
			return err
		}
	}

	return nil
}

func readBody(c echo.Context) ([]byte, error) {
	data, err := ioutil.ReadAll(c.Request().Body)
	c.Request().Body.Close()
	if err != nil {
		return nil, err
	}

	return data, nil
}
