package httpapi

import (
	"context"
	"encoding/json"
	"io"
	"mime"
	"net/url"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/labstack/echo/v5"
	"transfer-engine/internal/business/identity"
	"transfer-engine/internal/business/transfers"
)

type actorKey struct{}

func actor(c *echo.Context) string {
	v, _ := c.Request().Context().Value(actorKey{}).(string)
	return v
}

// RegisterFinancial must run before serving. Funding requires both the service
// guard and this route guard; no request can select an actor or environment.
func RegisterFinancial(e *echo.Echo, service *transfers.Service, queries *transfers.Queries, auth identity.Authenticator, environment string, funding bool) {
	protect := func(next echo.HandlerFunc) echo.HandlerFunc {
		return func(c *echo.Context) error {
			values := c.Request().Header.Values("Authorization")
			if len(values) != 1 {
				return echo.ErrUnauthorized
			}
			parts := strings.Fields(values[0])
			if len(parts) != 2 || !strings.EqualFold(parts[0], "Bearer") {
				return echo.ErrUnauthorized
			}
			sub, err := auth.Authenticate(c.Request().Context(), parts[1])
			if err != nil {
				return echo.ErrUnauthorized
			}
			ctx, cancel := context.WithTimeout(context.WithValue(c.Request().Context(), actorKey{}, sub), 13*time.Second)
			defer cancel()
			c.SetRequest(c.Request().WithContext(ctx))
			return next(c)
		}
	}
	g := e.Group("/v1", protect)
	g.POST("/accounts", func(c *echo.Context) error {
		var dto struct {
			Currency string `json:"currency"`
		}
		if err := strictBody(c, &dto, map[string]bool{"currency": true}); err != nil {
			return err
		}
		v, err := service.CreateAccount(c.Request().Context(), actor(c), dto.Currency)
		if err != nil {
			return err
		}
		return c.JSON(201, v)
	})
	g.GET("/accounts/:id", func(c *echo.Context) error {
		v, err := service.Account(c.Request().Context(), actor(c), c.Param("id"))
		if err != nil {
			return err
		}
		return c.JSON(200, v)
	})
	g.GET("/accounts/:id/balance", func(c *echo.Context) error {
		v, err := service.Account(c.Request().Context(), actor(c), c.Param("id"))
		if err != nil {
			return err
		}
		return c.JSON(200, struct {
			ID       string `json:"account_id"`
			Currency string `json:"currency"`
			Balance  int64  `json:"balance_minor"`
		}{v.ID, v.Currency, v.Balance})
	})
	g.GET("/accounts/:id/movements", func(c *echo.Context) error {
		limit := 50
		params, err := parsePagination(c)
		if err != nil {
			return err
		}
		if params[0] != "" {
			limit, err = strconv.Atoi(params[0])
			if err != nil {
				return echo.ErrBadRequest
			}
		}
		v, err := queries.Movements(c.Request().Context(), actor(c), c.Param("id"), limit, params[1])
		if err != nil {
			return err
		}
		return c.JSON(200, v)
	})
	g.GET("/transfers/:id", func(c *echo.Context) error {
		v, err := queries.Transfer(c.Request().Context(), actor(c), c.Param("id"))
		if err != nil {
			return err
		}
		return c.JSON(200, v)
	})
	movement := func(fund bool) echo.HandlerFunc {
		return func(c *echo.Context) error {
			var dto struct {
				Source      string `json:"source_account_id"`
				Destination string `json:"destination_account_id"`
				Currency    string `json:"currency"`
				Amount      int64  `json:"amount"`
			}
			fields := map[string]bool{"destination_account_id": true, "currency": true, "amount": true}
			if !fund {
				fields["source_account_id"] = true
			}
			if err := strictBody(c, &dto, fields); err != nil {
				return err
			}
			keys := c.Request().Header.Values("Idempotency-Key")
			if len(keys) != 1 {
				return echo.ErrBadRequest
			}
			cmd := transfers.Command{ActorID: actor(c), SourceID: dto.Source, DestinationID: dto.Destination, Currency: dto.Currency, Amount: dto.Amount, Key: keys[0]}
			var v transfers.Result
			var err error
			if fund {
				v, err = service.Fund(c.Request().Context(), cmd)
			} else {
				v, err = service.Transfer(c.Request().Context(), cmd)
			}
			if err != nil {
				return err
			}
			status := 201
			if v.Replayed {
				status = 200
			}
			return c.JSON(status, v)
		}
	}
	g.POST("/transfers", movement(false))
	if funding && (environment == "development" || environment == "test") {
		g.POST("/test-funds", movement(true))
	}
}

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
