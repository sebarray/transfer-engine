package httpapi_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/labstack/echo/v5"

	"transfer-engine/internal/business/health"
	httpapi "transfer-engine/internal/presentation/http"
)

type dependencyFunc func(context.Context) error

func (f dependencyFunc) Ping(ctx context.Context) error { return f(ctx) }

func newRouter(dependency dependencyFunc, logs io.Writer, limit int64) (*echo.Echo, *health.Service) {
	service := health.New(dependency, time.Second)
	return httpapi.New(service, slog.New(slog.NewJSONHandler(logs, nil)), limit), service
}

func TestHealthEndpoints(t *testing.T) {
	for _, tt := range []struct {
		name    string
		path    string
		err     error
		drain   bool
		status  int
		payload string
	}{
		{name: "live", path: "/livez", status: 200, payload: `{"status":"ok"}`},
		{name: "live during outage", path: "/livez", err: errors.New("database down"), status: 200, payload: `{"status":"ok"}`},
		{name: "live while draining", path: "/livez", drain: true, status: 200, payload: `{"status":"ok"}`},
		{name: "ready", path: "/readyz", status: 200, payload: `{"status":"ready"}`},
		{name: "unavailable", path: "/readyz", err: errors.New("postgres://user:secret@database"), status: 503},
		{name: "draining", path: "/readyz", drain: true, status: 503},
	} {
		t.Run(tt.name, func(t *testing.T) {
			var logs bytes.Buffer
			router, service := newRouter(func(context.Context) error {
				if tt.path == "/livez" {
					t.Fatal("liveness must not probe dependencies")
				}
				return tt.err
			}, &logs, 1024)
			if tt.drain {
				service.Drain()
			}
			response := httptest.NewRecorder()
			router.ServeHTTP(response, httptest.NewRequest(http.MethodGet, tt.path, nil))
			if response.Code != tt.status {
				t.Fatalf("status = %d, want %d: %s", response.Code, tt.status, response.Body)
			}
			if tt.payload != "" && strings.TrimSpace(response.Body.String()) != tt.payload {
				t.Fatalf("body = %s, want %s", response.Body, tt.payload)
			}
			if tt.status == 503 {
				assertError(t, response, "service_unavailable")
			}
			if strings.Contains(response.Body.String()+logs.String(), "secret") {
				t.Fatal("dependency credentials leaked to response or logs")
			}
			if got := response.Header().Get("Cache-Control"); got != "no-store" {
				t.Fatalf("Cache-Control = %q, want no-store", got)
			}
		})
	}
}

func TestCanceledReadiness(t *testing.T) {
	router, _ := newRouter(func(context.Context) error {
		t.Fatal("canceled request must not reach the dependency")
		return nil
	}, io.Discard, 1024)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	response := httptest.NewRecorder()
	router.ServeHTTP(response, httptest.NewRequestWithContext(ctx, http.MethodGet, "/readyz", nil))
	if response.Code != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want 503", response.Code)
	}
}

func TestRequestIDPropagationAndSafeLogging(t *testing.T) {
	var logs bytes.Buffer
	var observedID string
	router, _ := newRouter(func(ctx context.Context) error {
		observedID = httpapi.RequestID(ctx)
		return nil
	}, &logs, 1024)
	ids := make(map[string]bool)
	for range 2 {
		request := httptest.NewRequest(http.MethodGet, "/readyz?token=query-secret", nil)
		request.Header.Set("Authorization", "Bearer bearer-secret")
		request.Header.Set("Cookie", "session=cookie-secret")
		request.Header.Set("X-Request-ID", "untrusted-secret\nforged-log")
		response := httptest.NewRecorder()
		router.ServeHTTP(response, request)
		id := response.Header().Get("X-Request-ID")
		if !regexp.MustCompile(`^[0-9a-f]{32}$`).MatchString(id) || ids[id] {
			t.Fatalf("request ID = %q, want unique 128-bit hex identifier", id)
		}
		ids[id] = true
		if observedID != id {
			t.Fatalf("business context request ID = %q, want %q", observedID, id)
		}
	}
	if strings.Contains(logs.String(), "secret") || strings.Contains(logs.String(), "forged-log") {
		t.Fatalf("sensitive request fields leaked: %s", logs.String())
	}
	for _, line := range strings.Split(strings.TrimSpace(logs.String()), "\n") {
		var entry struct {
			RequestID string `json:"request_id"`
			Route     string `json:"route"`
			Status    int    `json:"status"`
		}
		if err := json.Unmarshal([]byte(line), &entry); err != nil {
			t.Fatal(err)
		}
		if !ids[entry.RequestID] || entry.Route != "/readyz" || entry.Status != 200 {
			t.Fatalf("incorrect request log: %s", line)
		}
	}
}

func TestErrorsAndRecovery(t *testing.T) {
	for _, tt := range []struct {
		name   string
		method string
		path   string
		status int
		code   string
	}{
		{name: "unknown route", method: "GET", path: "/secret-account?token=query-secret", status: 404, code: "not_found"},
		{name: "wrong method", method: "POST", path: "/livez", status: 405, code: "method_not_allowed"},
		{name: "internal error", method: "GET", path: "/error", status: 500, code: "internal_error"},
		{name: "http error", method: "GET", path: "/bad-request", status: 400, code: "invalid_request"},
		{name: "panic", method: "GET", path: "/panic", status: 500, code: "internal_error"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			var logs bytes.Buffer
			router, _ := newRouter(func(context.Context) error { return nil }, &logs, 1024)
			router.GET("/error", func(*echo.Context) error { return errors.New("internal-secret") })
			router.GET("/bad-request", func(*echo.Context) error {
				return echo.NewHTTPError(http.StatusBadRequest, "untrusted-secret")
			})
			router.GET("/panic", func(*echo.Context) error { panic("panic-secret") })
			response := httptest.NewRecorder()
			router.ServeHTTP(response, httptest.NewRequest(tt.method, tt.path, nil))
			if response.Code != tt.status {
				t.Fatalf("status = %d, want %d", response.Code, tt.status)
			}
			assertError(t, response, tt.code)
			if strings.Contains(response.Body.String()+logs.String(), "secret") {
				t.Fatalf("sensitive data leaked: body=%s logs=%s", response.Body, logs.String())
			}
			if tt.status == http.StatusMethodNotAllowed && !strings.Contains(response.Header().Get("Allow"), "GET") {
				t.Fatal("405 response must preserve allowed methods")
			}
		})
	}
}

func TestBodyLimit(t *testing.T) {
	for _, tt := range []struct {
		name          string
		body          string
		unknownLength bool
		status        int
	}{
		{name: "at limit", body: "1234", status: 204},
		{name: "known too large", body: "12345", status: 413},
		{name: "unknown at limit", body: "1234", unknownLength: true, status: 204},
		{name: "unknown too large", body: "12345", unknownLength: true, status: 413},
	} {
		t.Run(tt.name, func(t *testing.T) {
			router, _ := newRouter(func(context.Context) error { return nil }, io.Discard, 4)
			sideEffect := false
			router.POST("/read-body", func(c *echo.Context) error {
				if _, err := io.Copy(io.Discard, c.Request().Body); err != nil {
					return err
				}
				sideEffect = true
				return c.NoContent(http.StatusNoContent)
			})
			request := httptest.NewRequest(http.MethodPost, "/read-body", strings.NewReader(tt.body))
			if tt.unknownLength {
				request.ContentLength = -1
			}
			response := httptest.NewRecorder()
			router.ServeHTTP(response, request)
			if response.Code != tt.status {
				t.Fatalf("status = %d, want %d", response.Code, tt.status)
			}
			if tt.status == http.StatusRequestEntityTooLarge {
				assertError(t, response, "body_too_large")
				if sideEffect {
					t.Fatal("oversized input caused a handler side effect")
				}
			}
		})
	}
}

func assertError(t *testing.T, response *httptest.ResponseRecorder, wantCode string) {
	t.Helper()
	var body struct {
		Error struct {
			Code      string `json:"code"`
			Message   string `json:"message"`
			RequestID string `json:"request_id"`
		} `json:"error"`
	}
	if err := json.Unmarshal(response.Body.Bytes(), &body); err != nil {
		t.Fatalf("invalid error JSON: %v", err)
	}
	if body.Error.Code != wantCode || body.Error.Message != http.StatusText(response.Code) {
		t.Fatalf("error body = %s, want %q and standard status message", response.Body, wantCode)
	}
	if body.Error.RequestID == "" || body.Error.RequestID != response.Header().Get("X-Request-ID") {
		t.Fatal("error response must correlate with server-generated request ID")
	}
	if got := response.Header().Get("Content-Type"); !strings.HasPrefix(got, "application/json") {
		t.Fatalf("Content-Type = %q, want application/json", got)
	}
}
