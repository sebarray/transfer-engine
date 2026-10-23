//go:build integration

package integration_test

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"encoding/json"
	"github.com/golang-jwt/jwt/v5"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"
	"transfer-engine/internal/business/health"
	"transfer-engine/internal/business/transfers"
	"transfer-engine/internal/infrastructure/auth"
	postgresadapter "transfer-engine/internal/infrastructure/postgres"
	httpapi "transfer-engine/internal/presentation/http"
)

func TestAuthenticatedFinancialHTTP(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	pool := transferDatabase(t, ctx)
	store := postgresadapter.NewTransferStore(pool)
	if store.Ping(ctx) == nil {
		t.Fatal("unversioned schema accepted")
	}
	if err := postgresadapter.Migrate(ctx, pool, os.DirFS("../../migrations"), 1); err != nil {
		t.Fatal(err)
	}
	if err := store.Ping(ctx); err != nil {
		t.Fatal(err)
	}
	pub, private, _ := ed25519.GenerateKey(rand.Reader)
	validator, _ := auth.NewJWT(pub, "test-issuer", "test-api")
	token := func(actor string) string {
		raw, err := jwt.NewWithClaims(jwt.SigningMethodEdDSA, jwt.RegisteredClaims{Subject: actor, Issuer: "test-issuer", Audience: jwt.ClaimStrings{"test-api"}, ExpiresAt: jwt.NewNumericDate(time.Now().Add(time.Hour))}).SignedString(private)
		if err != nil {
			t.Fatal(err)
		}
		return raw
	}
	previous := otel.GetTracerProvider()
	prop := otel.GetTextMapPropagator()
	recorder := tracetest.NewSpanRecorder()
	provider := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(recorder))
	otel.SetTracerProvider(provider)
	otel.SetTextMapPropagator(propagation.TraceContext{})
	defer func() {
		otel.SetTracerProvider(previous)
		otel.SetTextMapPropagator(prop)
		_ = provider.Shutdown(context.Background())
	}()
	service, _ := transfers.New(store, "test", true)
	router := httpapi.New(health.New(store, time.Second), slog.New(slog.NewJSONHandler(io.Discard, nil)), 1024)
	httpapi.RegisterFinancial(router, service, transfers.NewQueries(store), validator, "test", true)
	request := func(method, path, actor, body, key string) *httptest.ResponseRecorder {
		r := httptest.NewRequest(method, path, strings.NewReader(body))
		r.Header.Set("Content-Type", "application/json")
		if actor != "" {
			r.Header.Set("Authorization", "Bearer "+token(actor))
		}
		if key != "" {
			r.Header.Set("Idempotency-Key", key)
		}
		r.Header.Set("traceparent", "00-11111111111111111111111111111111-2222222222222222-01")
		w := httptest.NewRecorder()
		router.ServeHTTP(w, r)
		return w
	}
	check := func(w *httptest.ResponseRecorder, status int) {
		t.Helper()
		if w.Code != status {
			t.Fatalf("status=%d want=%d body=%s", w.Code, status, w.Body.String())
		}
	}
	check(request("POST", "/v1/accounts", "", `{"currency":"ARS"}`, ""), 401)
	account := func(actor string) transfers.Account {
		w := request("POST", "/v1/accounts", actor, `{"currency":"ARS"}`, "")
		check(w, 201)
		var a transfers.Account
		if err := json.Unmarshal(w.Body.Bytes(), &a); err != nil {
			t.Fatal(err)
		}
		return a
	}
	a, b := account("alice"), account("bob")
	check(request("POST", "/v1/test-funds", "alice", `{"destination_account_id":"`+a.ID+`","currency":"ARS","amount":100}`, "fund"), 201)
	body := `{"source_account_id":"` + a.ID + `","destination_account_id":"` + b.ID + `","currency":"ARS","amount":10}`
	w := request("POST", "/v1/transfers", "alice", body, "transfer")
	check(w, 201)
	var result transfers.Result
	_ = json.Unmarshal(w.Body.Bytes(), &result)
	retry := request("POST", "/v1/transfers", "alice", body, "transfer")
	check(retry, 200)
	if retry.Body.String() != w.Body.String() {
		t.Fatal("replay changed logical response")
	}
	check(request("POST", "/v1/transfers", "alice", strings.Replace(body, `"amount":10`, `"amount":11`, 1), "transfer"), 409)
	check(request("POST", "/v1/transfers", "bob", body, "attack"), 404)
	check(request("GET", "/v1/accounts/"+a.ID, "bob", "", ""), 404)
	check(request("GET", "/v1/accounts/"+a.ID+"/balance", "alice", "", ""), 200)
	check(request("GET", "/v1/accounts/"+a.ID+"/movements", "bob", "", ""), 404)
	check(request("GET", "/v1/transfers/"+result.ID, "charlie", "", ""), 404)
	check(request("GET", "/v1/transfers/"+result.ID, "bob", "", ""), 200)
	p1 := request("GET", "/v1/accounts/"+a.ID+"/movements?limit=1", "alice", "", "")
	check(p1, 200)
	var page transfers.Page
	_ = json.Unmarshal(p1.Body.Bytes(), &page)
	if len(page.Items) != 1 || page.NextCursor == "" {
		t.Fatal("pagination missing")
	}
	p2 := request("GET", "/v1/accounts/"+a.ID+"/movements?limit=1&cursor="+page.NextCursor, "alice", "", "")
	check(p2, 200)
	var next transfers.Page
	_ = json.Unmarshal(p2.Body.Bytes(), &next)
	if len(next.Items) != 1 || next.Items[0].ID == page.Items[0].ID || next.NextCursor != "" {
		t.Fatal("unstable pagination")
	}
	var traceparent string
	if err := pool.QueryRow(ctx, `SELECT traceparent FROM outbox_events WHERE aggregate_id=$1`, result.ID).Scan(&traceparent); err != nil || !strings.HasPrefix(traceparent, "00-11111111111111111111111111111111-") {
		t.Fatalf("trace lost in transaction: %q %v", traceparent, err)
	}
	if len(recorder.Ended()) == 0 {
		t.Fatal("HTTP spans absent")
	}
	// Execute through HTTP, then close the connection after commit without sending
	// the recorded response. The retry must observe the same durable result.
	lost := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		buffer := httptest.NewRecorder()
		router.ServeHTTP(buffer, r)
		conn, _, err := w.(http.Hijacker).Hijack()
		if err == nil {
			_ = conn.Close()
		}
	}))
	r, _ := http.NewRequestWithContext(ctx, "POST", lost.URL+"/v1/transfers", strings.NewReader(body))
	r.Header.Set("Authorization", "Bearer "+token("alice"))
	r.Header.Set("Content-Type", "application/json")
	r.Header.Set("Idempotency-Key", "lost-response")
	client := &http.Client{Timeout: 5 * time.Second}
	res, err := client.Do(r)
	if res != nil {
		_ = res.Body.Close()
	}
	lost.Close()
	if err == nil {
		t.Fatal("expected missing HTTP response")
	}
	check(request("POST", "/v1/transfers", "alice", body, "lost-response"), 200)
	checkBalances(t, ctx, pool, a.ID, b.ID, 80, 20)
	if _, err := pool.Exec(ctx, `INSERT INTO schema_migrations(version,checksum) VALUES(99,'future')`); err != nil {
		t.Fatal(err)
	}
	check(request("GET", "/readyz", "", "", ""), 503)
	if _, err := pool.Exec(ctx, `DELETE FROM schema_migrations WHERE version=99`); err != nil {
		t.Fatal(err)
	}
	check(request("GET", "/readyz", "", "", ""), 200)
}
