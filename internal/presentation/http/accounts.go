package httpapi

import (
	"net/http"
	"strconv"

	"github.com/labstack/echo/v5"

	"transfer-engine/internal/business/transfers"
)

type accountHandlers struct {
	operations transfers.AccountOperations
	queries    transfers.MovementQueries
}

func (h accountHandlers) create(c *echo.Context) error {
	var dto createAccountRequest
	if err := strictBody(c, &dto, map[string]bool{"currency": true}); err != nil {
		return err
	}
	v, err := h.operations.CreateAccount(c.Request().Context(), actor(c), dto.Currency)
	if err != nil {
		return err
	}
	return c.JSON(http.StatusCreated, accountDTO(v))
}

func (h accountHandlers) get(c *echo.Context) error {
	v, err := h.operations.Account(c.Request().Context(), actor(c), c.Param("id"))
	if err != nil {
		return err
	}
	return c.JSON(http.StatusOK, accountDTO(v))
}

func (h accountHandlers) balance(c *echo.Context) error {
	v, err := h.operations.Account(c.Request().Context(), actor(c), c.Param("id"))
	if err != nil {
		return err
	}
	return c.JSON(http.StatusOK, balanceResponse{ID: v.ID, Currency: v.Currency, Balance: v.Balance})
}

func (h accountHandlers) movements(c *echo.Context) error {
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
	v, err := h.queries.Movements(c.Request().Context(), actor(c), c.Param("id"), limit, params[1])
	if err != nil {
		return err
	}
	return c.JSON(http.StatusOK, movementsDTO(v))
}
