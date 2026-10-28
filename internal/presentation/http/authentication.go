package httpapi

import (
	"context"
	"strings"
	"time"

	"github.com/labstack/echo/v5"

	"transfer-engine/internal/business/identity"
)

type actorKey struct{}

func actor(c *echo.Context) string {
	v, _ := c.Request().Context().Value(actorKey{}).(string)
	return v
}
func authenticate(auth identity.Authenticator) echo.MiddlewareFunc {
	return func(next echo.HandlerFunc) echo.HandlerFunc {
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
			ctx := context.WithValue(c.Request().Context(), actorKey{}, sub)
			c.SetRequest(c.Request().WithContext(ctx))
			return next(c)
		}
	}
}
func requestTimeout(timeout time.Duration) echo.MiddlewareFunc {
	return func(next echo.HandlerFunc) echo.HandlerFunc {
		return func(c *echo.Context) error {
			ctx, cancel := context.WithTimeout(c.Request().Context(), timeout)
			defer cancel()
			c.SetRequest(c.Request().WithContext(ctx))
			return next(c)
		}
	}
}
