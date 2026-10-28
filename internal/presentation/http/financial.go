package httpapi

import (
	"time"

	"github.com/labstack/echo/v5"

	"transfer-engine/internal/business/identity"
	"transfer-engine/internal/business/transfers"
)

// RegisterFinancial must run before serving. Funding requires both the service
// guard and this route guard; no request can select an actor or environment.
func RegisterFinancial(e *echo.Echo, service transfers.Operations, queries transfers.ReadQueries, auth identity.Authenticator, environment string, funding bool) {
	accounts := accountHandlers{operations: service, queries: queries}
	transfersHTTP := transferHandlers{operations: service, queries: queries}
	g := e.Group("/v1", authenticate(auth), requestTimeout(13*time.Second))
	g.POST("/accounts", accounts.create)
	g.GET("/accounts/:id", accounts.get)
	g.GET("/accounts/:id/balance", accounts.balance)
	g.GET("/accounts/:id/movements", accounts.movements)
	g.POST("/transfers", transfersHTTP.create)
	g.GET("/transfers/:id", transfersHTTP.get)
	if funding && (environment == "development" || environment == "test") {
		funds := fundingHandler{operations: service}
		g.POST("/test-funds", funds.create)
	}
}
