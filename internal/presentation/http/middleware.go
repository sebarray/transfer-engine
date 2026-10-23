package httpapi

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"log/slog"
	"net/http"
	"time"

	"github.com/labstack/echo/v5"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"
)

type requestIDKey struct{}

// RequestID returns the server-generated identifier in a request context.
func RequestID(ctx context.Context) string {
	id, _ := ctx.Value(requestIDKey{}).(string)
	return id
}

func requestLogging(logger *slog.Logger) echo.MiddlewareFunc {
	counter, _ := otel.Meter("transfer-engine/http").Int64Counter("http.server.requests")
	duration, _ := otel.Meter("transfer-engine/http").Float64Histogram("http.server.duration", metric.WithUnit("s"))
	return func(next echo.HandlerFunc) echo.HandlerFunc {
		return func(c *echo.Context) error {
			var randomID [16]byte
			if _, err := rand.Read(randomID[:]); err != nil {
				return echo.ErrInternalServerError
			}
			id := hex.EncodeToString(randomID[:])
			ctx := context.WithValue(c.Request().Context(), requestIDKey{}, id)
			ctx = otel.GetTextMapPropagator().Extract(ctx, propagation.HeaderCarrier(c.Request().Header))
			ctx, span := otel.Tracer("transfer-engine/http").Start(ctx, "http.request", trace.WithSpanKind(trace.SpanKindServer))
			defer span.End()
			c.SetRequest(c.Request().WithContext(ctx))
			c.Response().Header().Set("X-Request-ID", id)
			c.Response().Header().Set("Cache-Control", "no-store")
			requestLogger := logger.With("request_id", id)
			if sc := span.SpanContext(); sc.IsValid() {
				requestLogger = requestLogger.With("trace_id", sc.TraceID().String(), "span_id", sc.SpanID().String())
			}
			c.SetLogger(requestLogger)
			started := time.Now()
			if err := next(c); err != nil {
				c.Echo().HTTPErrorHandler(c, err)
			}
			_, status := echo.ResolveResponseStatus(c.Response(), nil)
			// Log the registered route only. Raw URLs, query strings, headers,
			// bodies and untrusted incoming request IDs are deliberately omitted.
			route := c.Path()
			if route == "" {
				route = "unmatched"
			}
			method := c.Request().Method
			switch method {
			case "GET", "POST", "PUT", "PATCH", "DELETE", "HEAD", "OPTIONS":
			default:
				method = "OTHER"
			}
			attrs := []attribute.KeyValue{attribute.String("http.request.method", method), attribute.String("http.route", route), attribute.Int("http.response.status_code", status)}
			span.SetName(method + " " + route)
			span.SetAttributes(attrs...)
			counter.Add(ctx, 1, metric.WithAttributes(attrs...))
			duration.Record(ctx, time.Since(started).Seconds(), metric.WithAttributes(attrs...))
			requestLogger.InfoContext(ctx, "http request",
				"method", method,
				"route", route,
				"status", status,
				"duration_ms", time.Since(started).Milliseconds(),
			)
			return nil
		}
	}
}

func recoverPanic(next echo.HandlerFunc) echo.HandlerFunc {
	return func(c *echo.Context) (err error) {
		defer func() {
			if recovered := recover(); recovered != nil {
				if recovered == http.ErrAbortHandler {
					panic(recovered)
				}
				// Panic payloads and stacks can include sensitive request data.
				c.Logger().ErrorContext(c.Request().Context(), "request panic recovered")
				err = echo.ErrInternalServerError
			}
		}()
		return next(c)
	}
}
