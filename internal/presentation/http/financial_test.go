package httpapi

import (
	"context"
	"io"
	"log/slog"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
	"transfer-engine/internal/business/health"
	"transfer-engine/internal/business/transfers"
)

type testAuth struct{}

func (testAuth) Authenticate(context.Context, string) (string, error) { return "alice", nil }

type financialStore struct{ calls int }

func (s *financialStore) CreateAccount(context.Context, string, string) (transfers.Account, error) {
	s.calls++
	return transfers.Account{ID: "account"}, nil
}
func (s *financialStore) Account(context.Context, string, string) (transfers.Account, error) {
	return transfers.Account{}, transfers.ErrNotFound
}
func (s *financialStore) WithinTransaction(context.Context, func(transfers.Tx) error) error {
	s.calls++
	return transfers.ErrUnavailable
}
func (s *financialStore) Resolve(context.Context, transfers.Claim) (*transfers.Result, error) {
	return nil, nil
}
func (s *financialStore) TransferDetails(context.Context, string, string) (transfers.TransferDetails, error) {
	return transfers.TransferDetails{}, transfers.ErrNotFound
}
func (s *financialStore) Movements(context.Context, string, string, int, int64) ([]transfers.Movement, error) {
	return nil, transfers.ErrNotFound
}

type healthy struct{}

func (healthy) Ping(context.Context) error { return nil }
func TestStrictFinancialBoundary(t *testing.T) {
	for _, tc := range []struct {
		name, body, media string
		status            int
	}{
		{"valid", `{"currency":"ARS"}`, "application/json", 201},
		{"duplicate", `{"currency":"ARS","currency":"USD"}`, "application/json", 400},
		{"case variant", `{"Currency":"ARS"}`, "application/json", 400},
		{"actor spoof", `{"currency":"ARS","actor_id":"bob"}`, "application/json", 400},
		{"trailing", `{"currency":"ARS"} {}`, "application/json", 400},
		{"null", `null`, "application/json", 400},
		{"null field", `{"currency":null}`, "application/json", 400},
		{"empty", "", "application/json", 400},
		{"media", `{"currency":"ARS"}`, "text/plain", 415},
		{"limit", `{"currency":"` + strings.Repeat("x", 300) + `"}`, "application/json", 413},
	} {
		t.Run(tc.name, func(t *testing.T) {
			store := &financialStore{}
			service, _ := transfers.New(store, "test", false)
			e := New(health.New(healthy{}, time.Second), slog.New(slog.NewJSONHandler(io.Discard, nil)), 256)
			RegisterFinancial(e, service, transfers.NewQueries(store), testAuth{}, "test", false)
			r := httptest.NewRequest("POST", "/v1/accounts", strings.NewReader(tc.body))
			r.Header.Set("Authorization", "Bearer token")
			r.Header.Set("Content-Type", tc.media)
			w := httptest.NewRecorder()
			e.ServeHTTP(w, r)
			if w.Code != tc.status {
				t.Fatalf("status=%d body=%s", w.Code, w.Body.String())
			}
			if tc.status != 201 && store.calls != 0 {
				t.Fatal("invalid request reached business effect")
			}
		})
	}
}
func TestProductionNeverRegistersFunding(t *testing.T) {
	store := &financialStore{}
	service, _ := transfers.New(store, "production", false)
	e := New(health.New(healthy{}, time.Second), slog.New(slog.NewJSONHandler(io.Discard, nil)), 256)
	RegisterFinancial(e, service, transfers.NewQueries(store), testAuth{}, "production", true)
	r := httptest.NewRequest("POST", "/v1/test-funds", strings.NewReader(`{}`))
	r.Header.Set("Authorization", "Bearer token")
	w := httptest.NewRecorder()
	e.ServeHTTP(w, r)
	if w.Code != 404 || store.calls != 0 {
		t.Fatal("production funding exposed")
	}
}
