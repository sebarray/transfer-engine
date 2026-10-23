package httpapi

import (
	"io"
	"log/slog"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"

	"transfer-engine/internal/business/health"
)

func TestDocumentationServedWithoutFinancialAPI(t *testing.T) {
	e := New(health.New(healthy{}, time.Second), slog.New(slog.NewTextHandler(io.Discard, nil)), 1024)
	for _, tc := range []struct{ path, contentType, content string }{
		{"/swagger/", "text/html", "./initializer.js"},
		{"/swagger/swagger-ui.css", "text/css", ".swagger-ui"},
		{"/swagger/swagger-ui-bundle.js", "text/javascript", "SwaggerUIBundle"},
		{"/swagger/initializer.js", "text/javascript", "persistAuthorization: false"},
		{"/openapi.json", "application/json", `"bearerAuth"`},
	} {
		t.Run(tc.path, func(t *testing.T) {
			r := httptest.NewRecorder()
			e.ServeHTTP(r, httptest.NewRequest("GET", tc.path, nil))
			if r.Code != 200 || !strings.HasPrefix(r.Header().Get("Content-Type"), tc.contentType) || !strings.Contains(r.Body.String(), tc.content) {
				t.Fatalf("documentation response: status=%d type=%s", r.Code, r.Header().Get("Content-Type"))
			}
			if tc.path == "/openapi.json" {
				data, err := os.ReadFile("../../../docs/openapi.json")
				if err != nil || string(data) != r.Body.String() {
					t.Fatal("served contract differs from source", err)
				}
			}
		})
	}
	r := httptest.NewRecorder()
	e.ServeHTTP(r, httptest.NewRequest("GET", "/swagger", nil))
	if r.Code != 308 || r.Header().Get("Location") != "/swagger/" {
		t.Fatal("missing canonical redirect")
	}
	for _, path := range []string{"/v1/accounts", "/v1/test-funds", "/swagger/unknown.js"} {
		r := httptest.NewRecorder()
		e.ServeHTTP(r, httptest.NewRequest("POST", path, nil))
		if r.Code != 404 && r.Code != 405 {
			t.Fatalf("unexpected route enabled: %s status=%d", path, r.Code)
		}
	}
}
