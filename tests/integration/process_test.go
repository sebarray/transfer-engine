//go:build integration

package integration_test

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/golang-jwt/jwt/v5"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
	"transfer-engine/internal/business/events"
	"transfer-engine/internal/business/transfers"
	"transfer-engine/internal/infrastructure/messaging"
	postgresadapter "transfer-engine/internal/infrastructure/postgres"
)

// This proof runs the real composition root and CLI, not an in-process router.
// All data, keys, ports and containers are disposable and local to this test.
func TestProcessRecoveryDemo(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	binary := filepath.Join(t.TempDir(), "api")
	if runtime.GOOS == "windows" {
		binary += ".exe"
	}
	build := exec.CommandContext(ctx, "go", "build", "-o", binary, "../../cmd/api")
	if output, err := build.CombinedOutput(); err != nil {
		t.Fatalf("build: %v\n%s", err, output)
	}
	pool := transferDatabase(t, ctx)
	if err := postgresadapter.Migrate(ctx, pool, os.DirFS("../../migrations"), 1); err != nil {
		t.Fatal(err)
	}
	rabbit, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{ContainerRequest: testcontainers.ContainerRequest{
		Image: "rabbitmq:4.3.6-management-alpine", ExposedPorts: []string{"5672/tcp"},
		Env:        map[string]string{"RABBITMQ_DEFAULT_USER": "test", "RABBITMQ_DEFAULT_PASS": "test"},
		WaitingFor: wait.ForAll(wait.ForLog("Server startup complete"), wait.ForListeningPort("5672/tcp")).WithDeadline(90 * time.Second),
	}, Started: true})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		cleanup, stop := context.WithTimeout(context.Background(), 30*time.Second)
		defer stop()
		if err := rabbit.Terminate(cleanup); err != nil {
			t.Error(err)
		}
	})
	host, err := rabbit.Host(ctx)
	if err != nil {
		t.Fatal(err)
	}
	port, err := rabbit.MappedPort(ctx, "5672/tcp")
	if err != nil {
		t.Fatal(err)
	}
	broker := messaging.Broker{URL: fmt.Sprintf("amqp://test:test@%s/", net.JoinHostPort(host, port.Port()))}
	if err := broker.Declare(ctx); err != nil {
		t.Fatal(err)
	}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	address := listener.Addr().String()
	_ = listener.Close()
	public, private, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	env := demoEnvironment(map[string]string{
		"APP_ENV": "test", "HTTP_ADDR": address, "DATABASE_URL": pool.Config().ConnString(),
		"FINANCIAL_API_ENABLED": "true", "JWT_ISSUER": "process-demo", "JWT_AUDIENCE": "transfer-engine",
		"JWT_PUBLIC_KEY": base64.StdEncoding.EncodeToString(public), "TEST_FUNDS_ENABLED": "true",
		"MESSAGING_ENABLED": "true", "RABBITMQ_URL": broker.URL, "MESSAGE_WORKERS": "2", "MESSAGE_PREFETCH": "1",
		"RECONCILIATION_ENABLED": "false", "TELEMETRY_OTLP_ENDPOINT": "", "DLQ_REPLAY_ENABLED": "false",
	})
	processCtx, stopProcess := context.WithCancel(ctx)
	process := exec.CommandContext(processCtx, binary)
	process.Env = env
	// Do not capture signed tokens or environment values in test artifacts.
	process.Stdout = io.Discard
	process.Stderr = io.Discard
	if err := process.Start(); err != nil {
		stopProcess()
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { done <- process.Wait() }()
	t.Cleanup(func() {
		stopProcess()
		select {
		case <-done:
		case <-time.After(10 * time.Second):
			t.Error("API process did not stop")
		}
	})
	client := &http.Client{Timeout: 5 * time.Second}
	baseURL := "http://" + address
	awaitDemo(t, ctx, "financial API readiness", 30*time.Second, func() bool {
		res, err := client.Get(baseURL + "/readyz")
		if err != nil {
			return false
		}
		defer res.Body.Close()
		return res.StatusCode == 200
	})
	token := func(actor string) string {
		raw, err := jwt.NewWithClaims(jwt.SigningMethodEdDSA, jwt.RegisteredClaims{Issuer: "process-demo", Audience: jwt.ClaimStrings{"transfer-engine"}, Subject: actor, ExpiresAt: jwt.NewNumericDate(time.Now().Add(time.Hour))}).SignedString(private)
		if err != nil {
			t.Fatal(err)
		}
		return raw
	}
	request := func(method, path, actor string, body any, key string, status int) []byte {
		t.Helper()
		payload, err := json.Marshal(body)
		if err != nil {
			t.Fatal(err)
		}
		r, err := http.NewRequestWithContext(ctx, method, baseURL+path, bytes.NewReader(payload))
		if err != nil {
			t.Fatal(err)
		}
		r.Header.Set("Content-Type", "application/json")
		r.Header.Set("Authorization", "Bearer "+token(actor))
		if key != "" {
			r.Header.Set("Idempotency-Key", key)
		}
		res, err := client.Do(r)
		if err != nil {
			t.Fatal(err)
		}
		defer res.Body.Close()
		data, err := io.ReadAll(res.Body)
		if err != nil {
			t.Fatal(err)
		}
		if res.StatusCode != status {
			t.Fatalf("%s %s: status %d, expected %d", method, path, res.StatusCode, status)
		}
		return data
	}
	account := func(actor string) transfers.Account {
		var a transfers.Account
		if err := json.Unmarshal(request("POST", "/v1/accounts", actor, map[string]string{"currency": "ARS"}, "", 201), &a); err != nil {
			t.Fatal(err)
		}
		return a
	}
	a, b := account("alice"), account("bob")
	request("POST", "/v1/test-funds", "alice", map[string]any{"destination_account_id": a.ID, "currency": "ARS", "amount": 100}, "fund", 201)
	brokerCommand := func(command string) {
		t.Helper()
		code, output, err := rabbit.Exec(ctx, []string{"rabbitmqctl", command})
		if output != nil {
			_, _ = io.Copy(io.Discard, output)
		}
		if err != nil || code != 0 {
			t.Fatalf("rabbitmqctl %s failed: exit=%d error=%v", command, code, err)
		}
	}
	brokerCommand("stop_app")
	if err := broker.Declare(ctx); err == nil {
		t.Fatal("broker remained reachable after stop_app")
	}
	body := map[string]any{"source_account_id": a.ID, "destination_account_id": b.ID, "currency": "ARS", "amount": 25}
	var transfer transfers.Result
	if err := json.Unmarshal(request("POST", "/v1/transfers", "alice", body, "broker-outage", 201), &transfer); err != nil {
		t.Fatal(err)
	}
	checkBalances(t, ctx, pool, a.ID, b.ID, 75, 25)
	var pending bool
	if err := pool.QueryRow(ctx, `SELECT published_at IS NULL FROM outbox_events WHERE aggregate_id=$1`, transfer.ID).Scan(&pending); err != nil || !pending {
		t.Fatal("committed transfer did not retain pending outbox")
	}
	t.Log("PROOF broker stopped: HTTP 201, balances 75/25, durable outbox pending")
	brokerCommand("start_app")
	awaitDemo(t, ctx, "outbox recovery and notification", 60*time.Second, func() bool {
		var ok bool
		err := pool.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM transfer_notifications n JOIN outbox_events o ON o.event_id=n.event_id WHERE n.transfer_id=$1 AND o.published_at IS NOT NULL)`, transfer.ID).Scan(&ok)
		return err == nil && ok
	})
	var replay transfers.Result
	if err := json.Unmarshal(request("POST", "/v1/transfers", "alice", body, "broker-outage", 200), &replay); err != nil || replay.ID != transfer.ID {
		t.Fatal("HTTP retry changed durable result")
	}
	t.Log("PROOF broker recovered: outbox published, notification persisted, identical HTTP retry returns 200")
	// A fixture with a new event ID references an already notified transfer. Its
	// new inbox row proves replay consumption, while the unique transfer effect
	// proves deduplication. Same-ID redelivery/exhaustion are covered separately.
	var envelope events.Envelope
	var payload []byte
	if err := pool.QueryRow(ctx, `SELECT event_id::text,event_type,event_version,aggregate_id::text,occurred_at,payload FROM outbox_events WHERE aggregate_id=$1`, transfer.ID).Scan(&envelope.ID, &envelope.Type, &envelope.Version, &envelope.AggregateID, &envelope.OccurredAt, &payload); err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(payload, &envelope.Payload); err != nil {
		t.Fatal(err)
	}
	var eventID [16]byte
	if _, err := rand.Read(eventID[:]); err != nil {
		t.Fatal(err)
	}
	eventID[6] = (eventID[6] & 15) | 64
	eventID[8] = (eventID[8] & 63) | 128
	envelope.ID = fmt.Sprintf("%x-%x-%x-%x-%x", eventID[:4], eventID[4:6], eventID[6:8], eventID[8:10], eventID[10:])
	encoded, err := json.Marshal(envelope)
	if err != nil {
		t.Fatal(err)
	}
	if err := broker.Publish(ctx, events.DeadQueue, events.Packet{ID: envelope.ID, Body: encoded, Attempt: 3, Reason: "demo_duplicate"}); err != nil {
		t.Fatal(err)
	}
	cli := func(environment []string, args ...string) ([]byte, error) {
		command := exec.CommandContext(ctx, binary, args...)
		command.Env = environment
		return command.CombinedOutput()
	}
	if _, err := cli(env, "-replay-dlq", "-dry-run=false", "-operator=demo", "-reason=dedup proof"); err == nil {
		t.Fatal("CLI replay bypassed enablement guard")
	}
	preview, err := cli(env, "-replay-dlq", "-limit=1")
	if err != nil {
		t.Fatal("CLI dry run failed")
	}
	var previews []messaging.ReplayPreview
	if err := json.Unmarshal(preview, &previews); err != nil || len(previews) != 1 || previews[0].EventID != envelope.ID {
		t.Fatal("CLI preview missing event")
	}
	var audits int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM dlq_replay_audit`).Scan(&audits); err != nil || audits != 0 {
		t.Fatal("dry run persisted replay audit")
	}
	active := append([]string(nil), env...)
	for i, v := range active {
		if strings.HasPrefix(v, "DLQ_REPLAY_ENABLED=") {
			active[i] = "DLQ_REPLAY_ENABLED=true"
		}
	}
	if _, err := cli(active, "-replay-dlq", "-dry-run=false", "-limit=1", "-operator=demo", "-reason=dedup proof"); err != nil {
		t.Fatal("CLI active replay failed")
	}
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM dlq_replay_audit WHERE event_id=$1 AND confirmed_at IS NOT NULL AND operator_id='demo'`, envelope.ID).Scan(&audits); err != nil || audits != 1 {
		t.Fatal("confirmed replay audit missing")
	}
	awaitDemo(t, ctx, "replayed event inbox commit", 30*time.Second, func() bool {
		var found bool
		err := pool.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM processed_events WHERE consumer_name='transfer_notification_v1' AND event_id=$1)`, envelope.ID).Scan(&found)
		return err == nil && found
	})
	var effects int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM transfer_notifications WHERE transfer_id=$1`, transfer.ID).Scan(&effects); err != nil || effects != 1 {
		t.Fatal("replay duplicated effect")
	}
	checkBalances(t, ctx, pool, a.ID, b.ID, 75, 25)
	t.Log("PROOF CLI: guarded writes, dry run, confirmed audit, one notification and unchanged balances")
}

func awaitDemo(t *testing.T, ctx context.Context, name string, timeout time.Duration, condition func() bool) {
	t.Helper()
	deadline, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()
	for {
		if condition() {
			return
		}
		select {
		case <-deadline.Done():
			t.Fatalf("timed out waiting for %s", name)
		case <-ticker.C:
		}
	}
}

func demoEnvironment(overrides map[string]string) []string {
	// Keep platform/toolchain variables, remove inherited application settings.
	var env []string
	for _, entry := range os.Environ() {
		key, _, _ := strings.Cut(entry, "=")
		upper := strings.ToUpper(key)
		drop := false
		for _, prefix := range []string{"APP_ENV", "HTTP_", "DATABASE_URL", "DB_", "HEALTH_", "SHUTDOWN_", "TEST_FUNDS_", "LOG_LEVEL", "RECONCILIATION_", "REDIS_", "MESSAGING_", "RABBITMQ_", "MESSAGE_", "DLQ_", "FINANCIAL_", "JWT_", "TELEMETRY_", "OTEL_"} {
			if strings.HasPrefix(upper, prefix) {
				drop = true
				break
			}
		}
		if !drop {
			env = append(env, entry)
		}
	}
	for key, value := range overrides {
		env = append(env, key+"="+value)
	}
	return env
}
