package httpapi

import (
	"context"
	"io"
	"log/slog"
	"net/http/httptest"
	"strings"
	"testing"

	"transfer-engine/internal/business/transfers"
)

type readinessFunc func(context.Context) error

func (f readinessFunc) Ready(ctx context.Context) error { return f(ctx) }

// Unused methods fail immediately if a handler calls the wrong use case.
type accountOperations struct {
	transfers.Operations
	create func(context.Context, string, string) (transfers.Account, error)
}

func (s accountOperations) CreateAccount(ctx context.Context, actor, currency string) (transfers.Account, error) {
	return s.create(ctx, actor, currency)
}

type transferQueries struct {
	transfers.ReadQueries
	read func(context.Context, string, string) (transfers.TransferDetails, error)
}

func (q transferQueries) Transfer(ctx context.Context, actor, id string) (transfers.TransferDetails, error) {
	return q.read(ctx, actor, id)
}

func TestHandlersAcceptInjectedUseCases(t *testing.T) {
	type contextKey struct{}
	checkContext := func(ctx context.Context) {
		t.Helper()
		if ctx.Value(contextKey{}) != "request" {
			t.Fatal("request context was not propagated")
		}
	}
	var readyCalls, createCalls, readCalls int
	e := New(readinessFunc(func(ctx context.Context) error {
		checkContext(ctx)
		readyCalls++
		return nil
	}), slog.New(slog.NewJSONHandler(io.Discard, nil)), 1024)
	operations := accountOperations{create: func(ctx context.Context, actor, currency string) (transfers.Account, error) {
		checkContext(ctx)
		createCalls++
		if actor != "alice" || currency != "ARS" {
			t.Fatalf("unexpected account arguments: %q %q", actor, currency)
		}
		return transfers.Account{ID: "created-account"}, nil
	}}
	queries := transferQueries{read: func(ctx context.Context, actor, id string) (transfers.TransferDetails, error) {
		checkContext(ctx)
		readCalls++
		if actor != "alice" || id != "transfer-id" {
			t.Fatalf("unexpected query arguments: %q %q", actor, id)
		}
		return transfers.TransferDetails{}, transfers.ErrNotFound
	}}
	RegisterFinancial(e, operations, queries, testAuth{}, "test", false)
	for _, tc := range []struct {
		method, path, body string
		status             int
	}{
		{"GET", "/readyz", "", 200},
		{"POST", "/v1/accounts", `{"currency":"ARS"}`, 201},
		{"GET", "/v1/transfers/transfer-id", "", 404},
	} {
		r := httptest.NewRequest(tc.method, tc.path, strings.NewReader(tc.body))
		r = r.WithContext(context.WithValue(r.Context(), contextKey{}, "request"))
		r.Header.Set("Authorization", "Bearer token")
		r.Header.Set("Content-Type", "application/json")
		w := httptest.NewRecorder()
		e.ServeHTTP(w, r)
		if w.Code != tc.status {
			t.Fatalf("%s: status=%d body=%s", tc.path, w.Code, w.Body.String())
		}
		if tc.status == 201 && !strings.Contains(w.Body.String(), `"id":"created-account"`) {
			t.Fatalf("injected result missing: %s", w.Body.String())
		}
	}
	if readyCalls != 1 || createCalls != 1 || readCalls != 1 {
		t.Fatalf("use case calls: readiness=%d create=%d read=%d", readyCalls, createCalls, readCalls)
	}
}
