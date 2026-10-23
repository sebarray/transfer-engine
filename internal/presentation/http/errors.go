package httpapi

import (
	"context"
	"errors"
	"net/http"
	"transfer-engine/internal/business/transfers"

	"github.com/labstack/echo/v5"
)

type errorResponse struct {
	Error errorDetails `json:"error"`
}

type errorDetails struct {
	Code      string `json:"code"`
	Message   string `json:"message"`
	RequestID string `json:"request_id"`
}

func handleError(c *echo.Context, err error) {
	response, status := echo.ResolveResponseStatus(c.Response(), err)
	code := ""
	switch {
	case errors.Is(err, transfers.ErrNotFound), errors.Is(err, transfers.ErrUnauthorized), errors.Is(err, transfers.ErrFundingDisabled):
		status = 404
		code = "not_found"
	case errors.Is(err, transfers.ErrInvalidInput), errors.Is(err, transfers.ErrInvalidAmount), errors.Is(err, transfers.ErrInvalidCurrency), errors.Is(err, transfers.ErrSameAccount):
		status = 400
		code = "invalid_request"
	case errors.Is(err, transfers.ErrMismatch):
		status = 409
		code = "idempotency_conflict"
	case errors.Is(err, transfers.ErrConflict):
		status = 409
		code = "concurrency_conflict"
	case errors.Is(err, transfers.ErrInsufficientFunds):
		status = 422
		code = "insufficient_funds"
	case errors.Is(err, transfers.ErrInactive):
		status = 422
		code = "inactive_account"
	case errors.Is(err, transfers.ErrOverflow):
		status = 422
		code = "balance_overflow"
	case errors.Is(err, transfers.ErrCommitUnknown):
		status = 503
		code = "commit_unknown"
	case errors.Is(err, transfers.ErrInFlight):
		status = 503
		code = "operation_in_flight"
	case errors.Is(err, transfers.ErrUnavailable), errors.Is(err, context.Canceled), errors.Is(err, context.DeadlineExceeded):
		status = 503
		code = "service_unavailable"
	}
	if response != nil && response.Committed {
		return
	}
	if status < 400 || status > 599 {
		status = http.StatusInternalServerError
	}
	// Error strings can contain credentials, SQL or request input. Only expose
	// the standard status text and an adapter-owned error code.
	if code == "" {
		code = errorCode(status)
	}
	if status == 401 {
		c.Response().Header().Set("WWW-Authenticate", "Bearer")
	}
	if status == 503 {
		c.Response().Header().Set("Retry-After", "1")
	}
	body := errorResponse{Error: errorDetails{
		Code:      code,
		Message:   http.StatusText(status),
		RequestID: RequestID(c.Request().Context()),
	}}
	if writeErr := c.JSON(status, body); writeErr != nil {
		c.Logger().ErrorContext(c.Request().Context(), "error response write failed")
	}
}

func errorCode(status int) string {
	switch status {
	case http.StatusUnauthorized:
		return "unauthenticated"
	case http.StatusUnsupportedMediaType:
		return "unsupported_media_type"
	case http.StatusBadRequest:
		return "invalid_request"
	case http.StatusNotFound:
		return "not_found"
	case http.StatusMethodNotAllowed:
		return "method_not_allowed"
	case http.StatusRequestEntityTooLarge:
		return "body_too_large"
	case http.StatusServiceUnavailable:
		return "service_unavailable"
	case http.StatusInternalServerError:
		return "internal_error"
	default:
		return "http_error"
	}
}
