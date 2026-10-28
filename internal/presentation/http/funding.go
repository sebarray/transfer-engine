package httpapi

import (
	"github.com/labstack/echo/v5"

	"transfer-engine/internal/business/transfers"
)

type fundingHandler struct{ operations transfers.FundingOperations }

func (h fundingHandler) create(c *echo.Context) error {
	var dto fundRequest
	if err := strictBody(c, &dto, map[string]bool{"destination_account_id": true, "currency": true, "amount": true}); err != nil {
		return err
	}
	key, err := idempotencyKey(c)
	if err != nil {
		return err
	}
	result, err := h.operations.Fund(c.Request().Context(), transfers.Command{
		ActorID: actor(c), DestinationID: dto.Destination, Currency: dto.Currency,
		Amount: dto.Amount, Key: key,
	})
	if err != nil {
		return err
	}
	return writeMovementResult(c, result)
}
