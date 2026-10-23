package httpapi

import (
	"encoding/json"
	"io"
	"log/slog"
	"os"
	"strings"
	"testing"
	"time"
	"transfer-engine/internal/business/health"
	"transfer-engine/internal/business/transfers"
)

func TestOpenAPIDocumentsRegisteredOperations(t *testing.T) {
	data, err := os.ReadFile("../../../docs/openapi.json")
	if err != nil {
		t.Fatal(err)
	}
	var spec struct {
		OpenAPI string                                `json:"openapi"`
		Paths   map[string]map[string]json.RawMessage `json:"paths"`
	}
	if err = json.Unmarshal(data, &spec); err != nil || spec.OpenAPI != "3.1.0" {
		t.Fatal("invalid OpenAPI document")
	}
	store := &financialStore{}
	service, _ := transfers.New(store, "test", true)
	e := New(health.New(healthy{}, time.Second), slog.New(slog.NewJSONHandler(io.Discard, nil)), 1024)
	RegisterFinancial(e, service, transfers.NewQueries(store), testAuth{}, "test", true)
	for path, operations := range spec.Paths {
		for method := range operations {
			if method == "parameters" {
				continue
			}
			if _, err := e.Router().Routes().FindByMethodPath(strings.ToUpper(method), strings.ReplaceAll(path, "{id}", ":id")); err != nil {
				t.Fatalf("documented route absent: %s %s", method, path)
			}
		}
	}
	for _, route := range e.Router().Routes() {
		// Documentation assets are not business API operations.
		if route.Path == "/openapi.json" || route.Path == "/swagger" || strings.HasPrefix(route.Path, "/swagger/") {
			continue
		}
		if route.Method != "GET" && route.Method != "POST" {
			continue
		}
		path := strings.ReplaceAll(route.Path, ":id", "{id}")
		if strings.Contains(path, "*") {
			continue
		}
		if _, ok := spec.Paths[path][strings.ToLower(route.Method)]; !ok {
			t.Fatalf("route undocumented: %s %s", route.Method, path)
		}
	}
}
