package httpapi

import (
	"net/http"

	"github.com/labstack/echo/v5"

	"transfer-engine/internal/business/transfers"
)

type transferHandlers struct {
	operations transfers.TransferOperations
	queries    transfers.TransferQueries
}

func (h transferHandlers) create(c *echo.Context) error {
	var dto createTransferRequest
	if err := strictBody(c, &dto, map[string]bool{"source_account_id": true, "destination_account_id": true, "currency": true, "amount": true}); err != nil {
		return err
	}
	key, err := idempotencyKey(c)
	if err != nil {
		return err
	}
	result, err := h.operations.Transfer(c.Request().Context(), transfers.Command{
		ActorID: actor(c), SourceID: dto.Source, DestinationID: dto.Destination,
		Currency: dto.Currency, Amount: dto.Amount, Key: key,
	})
	if err != nil {
		return err
	}
	return writeMovementResult(c, result)
}
func (h transferHandlers) get(c *echo.Context) error {
	v, err := h.queries.Transfer(c.Request().Context(), actor(c), c.Param("id"))
	if err != nil {
		return err
	}
	return c.JSON(http.StatusOK, transferDetailsDTO(v))
}
func idempotencyKey(c *echo.Context) (string, error) {
	keys := c.Request().Header.Values("Idempotency-Key")
	if len(keys) != 1 {
		return "", echo.ErrBadRequest
	}
	return keys[0], nil
}
func writeMovementResult(c *echo.Context, result transfers.Result) error {
	status := http.StatusCreated
	if result.Replayed {
		status = http.StatusOK
	}
	return c.JSON(status, movementResultResponse{ID: result.ID, Status: result.Status})
}
