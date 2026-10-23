// Package httpapi adapts business operations to HTTP.
package httpapi

import (
	"log/slog"
	"net/http"

	"github.com/labstack/echo/v5"
	"github.com/labstack/echo/v5/middleware"

	"transfer-engine/internal/business/health"
)

// New constructs the HTTP adapter. Configure routes before serving requests.
// Health probes are public; RegisterFinancial adds authenticated routes when enabled.
func New(service *health.Service, logger *slog.Logger, bodyLimit int64) *echo.Echo {
	e := echo.NewWithConfig(echo.Config{
		Logger:           logger,
		HTTPErrorHandler: handleError,
	})
	e.Pre(requestLogging(logger))
	e.Use(recoverPanic, middleware.BodyLimit(bodyLimit))
	registerDocumentation(e)
	e.GET("/livez", func(c *echo.Context) error {
		return c.JSON(http.StatusOK, statusResponse{Status: "ok"})
	})
	e.GET("/readyz", func(c *echo.Context) error {
		if err := service.Ready(c.Request().Context()); err != nil {
			return echo.ErrServiceUnavailable
		}
		return c.JSON(http.StatusOK, statusResponse{Status: "ready"})
	})
	return e
}

type statusResponse struct {
	Status string `json:"status"`
}
