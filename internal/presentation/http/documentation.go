package httpapi

import (
	"embed"
	"net/http"

	"github.com/labstack/echo/v5"
	"transfer-engine/docs"
)

//go:embed swagger/*
var swaggerAssets embed.FS

func registerDocumentation(e *echo.Echo) {
	e.GET("/swagger", func(c *echo.Context) error {
		return c.Redirect(http.StatusPermanentRedirect, "/swagger/")
	})
	e.GET("/openapi.json", func(c *echo.Context) error {
		c.Response().Header().Set("X-Content-Type-Options", "nosniff")
		return c.Blob(http.StatusOK, "application/json; charset=utf-8", []byte(docs.OpenAPI))
	})
	for _, asset := range []struct{ path, file, contentType string }{
		{"/swagger/", "index.html", "text/html; charset=utf-8"},
		{"/swagger/swagger-ui.css", "swagger-ui.css", "text/css; charset=utf-8"},
		{"/swagger/swagger-ui-bundle.js", "swagger-ui-bundle.js", "text/javascript; charset=utf-8"},
		{"/swagger/initializer.js", "initializer.js", "text/javascript; charset=utf-8"},
	} {
		data, err := swaggerAssets.ReadFile("swagger/" + asset.file)
		if err != nil {
			panic(err) // Embedded files are build-time requirements.
		}
		e.GET(asset.path, func(c *echo.Context) error {
			h := c.Response().Header()
			h.Set("X-Content-Type-Options", "nosniff")
			h.Set("Referrer-Policy", "no-referrer")
			h.Set("Content-Security-Policy", "default-src 'none'; script-src 'self'; style-src 'self' 'unsafe-inline'; img-src 'self' data:; font-src 'self' data:; connect-src 'self'; frame-ancestors 'none'; base-uri 'none'")
			return c.Blob(http.StatusOK, asset.contentType, data)
		})
	}
}
