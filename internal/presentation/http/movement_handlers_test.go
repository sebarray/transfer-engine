package httpapi

import (
	"context"
	"io"
	"log/slog"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"transfer-engine/internal/business/transfers"
)

type transferFunc func(context.Context, transfers.Command) (transfers.Result, error)

func (f transferFunc) Transfer(ctx context.Context, cmd transfers.Command) (transfers.Result, error) {
	return f(ctx, cmd)
}

type fundingFunc func(context.Context, transfers.Command) (transfers.Result, error)

func (f fundingFunc) Fund(ctx context.Context, cmd transfers.Command) (transfers.Result, error) {
	return f(ctx, cmd)
}

func TestMovementHandlers(t *testing.T) {
	for _, tc := range []struct {
		name, path, body, source string
		funding, replay          bool
		status, calls            int
	}{
		{"transfer", "/transfers", `{"source_account_id":"source","destination_account_id":"target","currency":"ARS","amount":25}`, "source", false, false, 201, 1},
		{"transfer replay", "/transfers", `{"source_account_id":"source","destination_account_id":"target","currency":"ARS","amount":25}`, "source", false, true, 200, 1},
		{"fund", "/test-funds", `{"destination_account_id":"target","currency":"ARS","amount":25}`, "", true, false, 201, 1},
		{"fund replay", "/test-funds", `{"destination_account_id":"target","currency":"ARS","amount":25}`, "", true, true, 200, 1},
		{"fund rejects source", "/test-funds", `{"source_account_id":"source","destination_account_id":"target","currency":"ARS","amount":25}`, "", true, false, 400, 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			calls := 0
			var received context.Context
			operation := func(ctx context.Context, cmd transfers.Command) (transfers.Result, error) {
				calls++
				received = ctx
				if cmd != (transfers.Command{ActorID: "alice", SourceID: tc.source, DestinationID: "target", Currency: "ARS", Amount: 25, Key: "key"}) {
					t.Fatalf("unexpected command: %+v", cmd)
				}
				if _, ok := ctx.Deadline(); !ok {
					t.Fatal("missing request deadline")
				}
				return transfers.Result{ID: "result", Status: "completed", Replayed: tc.replay}, nil
			}
			e := New(readinessFunc(func(context.Context) error { return nil }), slog.New(slog.NewTextHandler(io.Discard, nil)), 1024)
			g := e.Group("/v1", authenticate(testAuth{}), requestTimeout(time.Second))
			if tc.funding {
				h := fundingHandler{operations: fundingFunc(operation)}
				g.POST(tc.path, h.create)
			} else {
				h := transferHandlers{operations: transferFunc(operation)}
				g.POST(tc.path, h.create)
			}
			r := httptest.NewRequest("POST", "/v1"+tc.path, strings.NewReader(tc.body))
			r.Header.Set("Authorization", "Bearer token")
			r.Header.Set("Content-Type", "application/json")
			r.Header.Set("Idempotency-Key", "key")
			w := httptest.NewRecorder()
			e.ServeHTTP(w, r)
			if w.Code != tc.status || calls != tc.calls {
				t.Fatalf("status=%d calls=%d body=%s", w.Code, calls, w.Body.String())
			}
			if calls > 0 {
				if strings.TrimSpace(w.Body.String()) != `{"id":"result","status":"completed"}` {
					t.Fatalf("unexpected response: %s", w.Body.String())
				}
				if received.Err() != context.Canceled {
					t.Fatal("request context not released")
				}
			}
		})
	}
}

func TestAuthenticationRejectsBeforeHandler(t *testing.T) {
	for _, headers := range [][]string{nil, {"Basic token"}, {"Bearer"}, {"Bearer token", "Bearer other"}} {
		e := New(readinessFunc(func(context.Context) error { return nil }), slog.New(slog.NewTextHandler(io.Discard, nil)), 1024)
		h := fundingHandler{operations: fundingFunc(func(context.Context, transfers.Command) (transfers.Result, error) {
			t.Fatal("unauthenticated request reached use case")
			return transfers.Result{}, nil
		})}
		e.Group("/v1", authenticate(testAuth{})).POST("/test-funds", h.create)
		r := httptest.NewRequest("POST", "/v1/test-funds", strings.NewReader(`{}`))
		for _, value := range headers {
			r.Header.Add("Authorization", value)
		}
		w := httptest.NewRecorder()
		e.ServeHTTP(w, r)
		if w.Code != 401 {
			t.Fatalf("headers=%v status=%d", headers, w.Code)
		}
	}
}
