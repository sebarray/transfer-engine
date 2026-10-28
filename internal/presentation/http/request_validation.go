package httpapi

import (
	"encoding/json"
	"io"
	"mime"
	"net/url"
	"strings"
	"unicode/utf8"

	"github.com/labstack/echo/v5"
)

func parsePagination(c *echo.Context) ([2]string, error) {
	var result [2]string
	values, err := url.ParseQuery(c.Request().URL.RawQuery)
	if err != nil {
		return result, echo.ErrBadRequest
	}
	for k, v := range values {
		if len(v) != 1 || (k != "limit" && k != "cursor") {
			return result, echo.ErrBadRequest
		}
		if k == "limit" {
			if v[0] == "" {
				return result, echo.ErrBadRequest
			}
			result[0] = v[0]
		} else {
			result[1] = v[0]
		}
	}
	return result, nil
}

// All current write DTOs are flat objects. Validate exact field names and reject
// duplicates/nulls before decoding the typed DTO or invoking any domain effect.
func strictBody(c *echo.Context, target any, fields map[string]bool) error {
	media, params, err := mime.ParseMediaType(c.Request().Header.Get("Content-Type"))
	if err != nil || media != "application/json" || c.Request().Header.Get("Content-Encoding") != "" {
		return echo.ErrUnsupportedMediaType
	}
	for k, v := range params {
		if k != "charset" || !strings.EqualFold(v, "utf-8") {
			return echo.ErrUnsupportedMediaType
		}
	}
	body, err := io.ReadAll(c.Request().Body)
	if err != nil {
		return err
	}
	if !utf8.Valid(body) {
		return echo.ErrBadRequest
	}
	d := json.NewDecoder(strings.NewReader(string(body)))
	start, err := d.Token()
	if err != nil || start != json.Delim('{') {
		return echo.ErrBadRequest
	}
	seen := map[string]bool{}
	for d.More() {
		key, err := d.Token()
		if err != nil {
			return echo.ErrBadRequest
		}
		name, ok := key.(string)
		if !ok || !fields[name] || seen[name] {
			return echo.ErrBadRequest
		}
		seen[name] = true
		var value json.RawMessage
		if d.Decode(&value) != nil || string(value) == "null" {
			return echo.ErrBadRequest
		}
	}
	if _, err = d.Token(); err != nil {
		return echo.ErrBadRequest
	}
	var extra any
	if d.Decode(&extra) != io.EOF {
		return echo.ErrBadRequest
	}
	if json.Unmarshal(body, target) != nil {
		return echo.ErrBadRequest
	}
	return nil
}
