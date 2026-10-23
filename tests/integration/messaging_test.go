//go:build integration

package integration_test

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http/httptest"
	"os"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/golang-jwt/jwt/v5"
	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/trace"
	"transfer-engine/internal/business/events"
	"transfer-engine/internal/business/health"
	"transfer-engine/internal/business/transfers"
	"transfer-engine/internal/infrastructure/auth"
	"transfer-engine/internal/infrastructure/messaging"
	postgresadapter "transfer-engine/internal/infrastructure/postgres"
	httpapi "transfer-engine/internal/presentation/http"
)

type rawDelivery struct{ amqp.Delivery }

func (d rawDelivery) Ack() error { return d.Delivery.Ack(false) }
func packet(d amqp.Delivery) events.Packet {
	integer := func(v any) int {
		switch x := v.(type) {
		case int32:
			return int(x)
		case int64:
			return int(x)
		}
		return 0
	}
	reason, _ := d.Headers["failure_reason"].(string)
	return events.Packet{ID: d.MessageId, Body: d.Body, Attempt: integer(d.Headers["attempt"]), Replays: integer(d.Headers["replays"]), Reason: reason}
}
func receive(t *testing.T, url, queue string) (amqp.Delivery, func()) {
	t.Helper()
	conn, err := amqp.DialConfig(url, amqp.Config{Dial: amqp.DefaultDial(5 * time.Second)})
	if err != nil {
		t.Fatal(err)
	}
	closeConnection := func() { _ = conn.CloseDeadline(time.Now().Add(time.Second)) }
	t.Cleanup(closeConnection)
	ch, err := conn.Channel()
	if err != nil {
		t.Fatal(err)
	}
	if err = ch.Qos(1, 0, false); err != nil {
		t.Fatal(err)
	}
	messages, err := ch.Consume(queue, "probe", false, false, false, false, nil)
	if err != nil {
		t.Fatal(err)
	}
	select {
	case d, ok := <-messages:
		if !ok {
			t.Fatal("delivery channel closed")
		}
		if err = ch.Cancel("probe", false); err != nil {
			t.Fatal(err)
		}
		return d, closeConnection
	case <-time.After(15 * time.Second):
		t.Fatalf("no delivery in %s", queue)
		return amqp.Delivery{}, closeConnection
	}
}

type failureEffect struct{ err error }

func (f failureEffect) Apply(context.Context, events.Envelope) error { return f.err }

type lostConfirm struct{ events.Sender }

func (s lostConfirm) Publish(ctx context.Context, q string, p events.Packet) error {
	if err := s.Sender.Publish(ctx, q, p); err != nil {
		return err
	}
	return events.ErrHandoff
}

type blockingEffect struct {
	entered     chan struct{}
	release     chan struct{}
	active, max atomic.Int32
	real        events.Effects
}

func (e *blockingEffect) Apply(ctx context.Context, event events.Envelope) error {
	active := e.active.Add(1)
	defer e.active.Add(-1)
	for current := e.max.Load(); active > current; current = e.max.Load() {
		if e.max.CompareAndSwap(current, active) {
			break
		}
	}
	e.entered <- struct{}{}
	select {
	case <-e.release:
		return e.real.Apply(ctx, event)
	case <-ctx.Done():
		return ctx.Err()
	}
}
func TestReliableMessaging(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	pool := transferDatabase(t, ctx)
	if err := postgresadapter.Migrate(ctx, pool, os.DirFS("../../migrations"), 1); err != nil {
		t.Fatal(err)
	}
	ctr, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{ContainerRequest: testcontainers.ContainerRequest{Image: "rabbitmq:4.3.6-management-alpine", ExposedPorts: []string{"5672/tcp"}, Env: map[string]string{"RABBITMQ_DEFAULT_USER": "test", "RABBITMQ_DEFAULT_PASS": "test"}, WaitingFor: wait.ForAll(wait.ForLog("Server startup complete"), wait.ForListeningPort("5672/tcp")).WithDeadline(90 * time.Second)}, Started: true})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		cleanup, stop := context.WithTimeout(context.Background(), 30*time.Second)
		defer stop()
		if err := ctr.Terminate(cleanup); err != nil {
			t.Error(err)
		}
	})
	host, err := ctr.Host(ctx)
	if err != nil {
		t.Fatal(err)
	}
	port, err := ctr.MappedPort(ctx, "5672/tcp")
	if err != nil {
		t.Fatal(err)
	}
	broker := messaging.Broker{URL: fmt.Sprintf("amqp://test:test@%s:%s/", host, port.Port())}
	if err = broker.Declare(ctx); err != nil {
		t.Fatal(err)
	}
	store := postgresadapter.NewEventStore(pool)
	consumer := events.Consumer{Effects: store, Sender: broker, Policy: events.DefaultPolicy()}
	money, _ := transfers.New(postgresadapter.NewTransferStore(pool), "test", true)
	a, err := money.CreateAccount(ctx, "alice", "ARS")
	if err != nil {
		t.Fatal(err)
	}
	b, err := money.CreateAccount(ctx, "bob", "ARS")
	if err != nil {
		t.Fatal(err)
	}
	_, err = money.Fund(ctx, transfers.Command{ActorID: "alice", DestinationID: a.ID, Amount: 100, Currency: "ARS", Key: "fund"})
	if err != nil {
		t.Fatal(err)
	}
	makeTransfer := func(t *testing.T, key string) transfers.Result {
		t.Helper()
		r, err := money.Transfer(ctx, transfers.Command{ActorID: "alice", SourceID: a.ID, DestinationID: b.ID, Currency: "ARS", Amount: 1, Key: key})
		if err != nil {
			t.Fatal(err)
		}
		return r
	}
	flush := func(t *testing.T) {
		t.Helper()
		if found, err := events.PublishNext(ctx, store, broker); err != nil || !found {
			t.Fatalf("publish found=%v err=%v", found, err)
		}
	}
	effects := func(t *testing.T, id string, want int) {
		t.Helper()
		var n int
		if err := pool.QueryRow(ctx, `SELECT count(*) FROM transfer_notifications WHERE transfer_id=$1`, id).Scan(&n); err != nil || n != want {
			t.Fatalf("notifications=%d want=%d err=%v", n, want, err)
		}
	}
	t.Run("mandatory unroutable publication fails", func(t *testing.T) {
		if err := broker.Publish(ctx, "missing.queue", events.Packet{Body: []byte(`{}`)}); !errors.Is(err, events.ErrHandoff) {
			t.Fatalf("unroutable accepted: %v", err)
		}
	})
	t.Run("broker failure preserves committed transfer and outbox", func(t *testing.T) {
		r := makeTransfer(t, "broker-down")
		bad := messaging.Broker{URL: "amqp://test:test@127.0.0.1:1/"}
		if _, err := events.PublishNext(ctx, store, bad); err == nil {
			t.Fatal("unavailable broker accepted publish")
		}
		var pending bool
		if err := pool.QueryRow(ctx, `SELECT published_at IS NULL FROM outbox_events WHERE aggregate_id=$1`, r.ID).Scan(&pending); err != nil || !pending {
			t.Fatal("outbox lost")
		}
		if _, err := pool.Exec(ctx, `UPDATE outbox_events SET available_at=clock_timestamp() WHERE aggregate_id=$1`, r.ID); err != nil {
			t.Fatal(err)
		}
		flush(t)
		d, closeConn := receive(t, broker.URL, events.MainQueue)
		defer closeConn()
		if err := consumer.Handle(ctx, packet(d), rawDelivery{d}); err != nil {
			t.Fatal(err)
		}
		effects(t, r.ID, 1)
	})
	t.Run("crash after confirm before mark duplicates safely", func(t *testing.T) {
		r := makeTransfer(t, "publisher-crash")
		_, err := pool.Exec(ctx, `CREATE FUNCTION fail_mark() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'injected mark failure'; END $$; CREATE TRIGGER fail_mark BEFORE UPDATE ON outbox_events FOR EACH ROW WHEN (NEW.published_at IS NOT NULL) EXECUTE FUNCTION fail_mark()`)
		if err != nil {
			t.Fatal(err)
		}
		if _, err = events.PublishNext(ctx, store, broker); err == nil {
			t.Fatal("mark failure ignored")
		}
		if _, err = pool.Exec(ctx, `DROP TRIGGER fail_mark ON outbox_events; DROP FUNCTION fail_mark()`); err != nil {
			t.Fatal(err)
		}
		flush(t)
		id := ""
		for i := 0; i < 2; i++ {
			d, closeConn := receive(t, broker.URL, events.MainQueue)
			if id != "" && d.MessageId != id {
				t.Fatal("duplicate changed event ID")
			}
			id = d.MessageId
			if err := consumer.Handle(ctx, packet(d), rawDelivery{d}); err != nil {
				t.Fatal(err)
			}
			closeConn()
		}
		effects(t, r.ID, 1)
	})
	t.Run("consumer dies after effect commit before ACK", func(t *testing.T) {
		r := makeTransfer(t, "consumer-crash")
		flush(t)
		d, closeConn := receive(t, broker.URL, events.MainQueue)
		e, err := events.Decode(d.Body)
		if err != nil {
			t.Fatal(err)
		}
		if err = store.Apply(ctx, e); err != nil {
			t.Fatal(err)
		}
		closeConn()
		repeated, closeAgain := receive(t, broker.URL, events.MainQueue)
		defer closeAgain()
		if !repeated.Redelivered || repeated.MessageId != d.MessageId {
			t.Fatal("unacknowledged message was not redelivered")
		}
		if err = consumer.Handle(ctx, packet(repeated), rawDelivery{repeated}); err != nil {
			t.Fatal(err)
		}
		effects(t, r.ID, 1)
	})
	t.Run("effect failure rolls back inbox", func(t *testing.T) {
		r := makeTransfer(t, "effect-rollback")
		flush(t)
		d, closeConn := receive(t, broker.URL, events.MainQueue)
		defer closeConn()
		e, _ := events.Decode(d.Body)
		_, err := pool.Exec(ctx, `CREATE FUNCTION fail_notification() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'injected effect failure'; END $$; CREATE TRIGGER fail_notification BEFORE INSERT ON transfer_notifications FOR EACH ROW EXECUTE FUNCTION fail_notification()`)
		if err != nil {
			t.Fatal(err)
		}
		if err = store.Apply(ctx, e); !errors.Is(err, events.ErrTransient) {
			t.Fatal(err)
		}
		var n int
		if err = pool.QueryRow(ctx, `SELECT count(*) FROM processed_events WHERE event_id=$1`, e.ID).Scan(&n); err != nil || n != 0 {
			t.Fatal("inbox committed without effect")
		}
		if _, err = pool.Exec(ctx, `DROP TRIGGER fail_notification ON transfer_notifications; DROP FUNCTION fail_notification()`); err != nil {
			t.Fatal(err)
		}
		if err = consumer.Handle(ctx, packet(d), rawDelivery{d}); err != nil {
			t.Fatal(err)
		}
		effects(t, r.ID, 1)
	})
	t.Run("poison version and ambiguous DLQ confirm keep original", func(t *testing.T) {
		r := makeTransfer(t, "poison")
		flush(t)
		d, closeConn := receive(t, broker.URL, events.MainQueue)
		e, _ := events.Decode(d.Body)
		e.Version = 99
		body, _ := json.Marshal(e)
		_ = d.Ack(false)
		closeConn()
		p := events.Packet{ID: e.ID, Body: body}
		if err = broker.Publish(ctx, events.MainQueue, p); err != nil {
			t.Fatal(err)
		}
		original, closeOriginal := receive(t, broker.URL, events.MainQueue)
		broken := consumer
		broken.Sender = lostConfirm{broker}
		if err = broken.Handle(ctx, packet(original), rawDelivery{original}); !errors.Is(err, events.ErrHandoff) {
			t.Fatal(err)
		}
		closeOriginal()
		again, closeAgain := receive(t, broker.URL, events.MainQueue)
		if !again.Redelivered {
			t.Fatal("original lost on ambiguous handoff")
		}
		if err = consumer.Handle(ctx, packet(again), rawDelivery{again}); err != nil {
			t.Fatal(err)
		}
		closeAgain()
		for i := 0; i < 2; i++ {
			dead, closeDead := receive(t, broker.URL, events.DeadQueue)
			if packet(dead).Reason != "invalid_event" {
				t.Fatal("missing poison metadata")
			}
			if err = dead.Ack(false); err != nil {
				t.Fatal(err)
			}
			closeDead()
		}
		effects(t, r.ID, 0)
	})
	t.Run("retry TTL exhaustion DLQ and audited replay", func(t *testing.T) {
		r := makeTransfer(t, "exhaustion")
		flush(t)
		failing := events.Consumer{Effects: failureEffect{events.ErrTransient}, Sender: broker, Policy: events.RetryPolicy{Delays: [3]time.Duration{30 * time.Millisecond, 60 * time.Millisecond, 90 * time.Millisecond}, Jitter: func(d time.Duration) time.Duration { return d }}}
		for attempt := 0; attempt <= 3; attempt++ {
			d, closeConn := receive(t, broker.URL, events.MainQueue)
			if packet(d).Attempt != attempt {
				t.Fatalf("attempt %d", packet(d).Attempt)
			}
			if err = failing.Handle(ctx, packet(d), rawDelivery{d}); err != nil {
				t.Fatal(err)
			}
			closeConn()
		}
		dead, closeDead := receive(t, broker.URL, events.DeadQueue)
		p := packet(dead)
		if p.Reason != "retry_exhausted" || p.Attempt != 3 {
			t.Fatalf("DLQ metadata %+v", p)
		}
		request := events.ReplayRequest{Operator: "integration-operator", Reason: "dependency recovered", DryRun: true}
		if err = events.Replay(ctx, p, rawDelivery{dead}, broker, store, request); err != nil {
			t.Fatal(err)
		}
		closeDead()
		previews, previewErr := broker.ReplayBatch(ctx, 1, request, store)
		if previewErr != nil || len(previews) != 1 || previews[0].EventID != p.ID || !previews[0].Valid {
			t.Fatalf("dry run preview: %+v %v", previews, previewErr)
		}
		request.DryRun = false
		previews, replayErr := broker.ReplayBatch(ctx, 1, request, store)
		if replayErr != nil || len(previews) != 1 || previews[0].EventID != p.ID {
			t.Fatalf("active replay: %+v %v", previews, replayErr)
		}
		replay, closeReplay := receive(t, broker.URL, events.MainQueue)
		defer closeReplay()
		if replay.MessageId != p.ID || packet(replay).Replays != 1 || packet(replay).Attempt != 0 {
			t.Fatal("replay changed identity or budget")
		}
		if err = consumer.Handle(ctx, packet(replay), rawDelivery{replay}); err != nil {
			t.Fatal(err)
		}
		effects(t, r.ID, 1)
		var n int
		if err = pool.QueryRow(ctx, `SELECT count(*) FROM dlq_replay_audit WHERE event_id=$1 AND confirmed_at IS NOT NULL AND reason='dependency recovered'`, p.ID).Scan(&n); err != nil || n != 1 {
			t.Fatal("replay audit missing")
		}
	})
	t.Run("trace crosses authenticated HTTP outbox RabbitMQ consumer", func(t *testing.T) {
		previous, prop := otel.GetTracerProvider(), otel.GetTextMapPropagator()
		provider := sdktrace.NewTracerProvider()
		otel.SetTracerProvider(provider)
		otel.SetTextMapPropagator(propagation.TraceContext{})
		defer func() {
			otel.SetTracerProvider(previous)
			otel.SetTextMapPropagator(prop)
			_ = provider.Shutdown(context.Background())
		}()
		pub, private, _ := ed25519.GenerateKey(rand.Reader)
		validator, _ := auth.NewJWT(pub, "issuer", "api")
		token, _ := jwt.NewWithClaims(jwt.SigningMethodEdDSA, jwt.RegisteredClaims{Subject: "alice", Issuer: "issuer", Audience: jwt.ClaimStrings{"api"}, ExpiresAt: jwt.NewNumericDate(time.Now().Add(time.Hour))}).SignedString(private)
		router := httpapi.New(health.New(pool, time.Second), slog.New(slog.NewJSONHandler(io.Discard, nil)), 1024)
		httpapi.RegisterFinancial(router, money, transfers.NewQueries(postgresadapter.NewTransferStore(pool)), validator, "test", false)
		body := fmt.Sprintf(`{"source_account_id":%q,"destination_account_id":%q,"currency":"ARS","amount":1}`, a.ID, b.ID)
		r := httptest.NewRequest("POST", "/v1/transfers", strings.NewReader(body))
		r.Header.Set("Authorization", "Bearer "+token)
		r.Header.Set("Content-Type", "application/json")
		r.Header.Set("Idempotency-Key", "trace-http")
		r.Header.Set("traceparent", "00-11111111111111111111111111111111-2222222222222222-01")
		w := httptest.NewRecorder()
		router.ServeHTTP(w, r)
		if w.Code != 201 {
			t.Fatal(w.Body.String())
		}
		flush(t)
		consumeCtx, stop := context.WithTimeout(ctx, 10*time.Second)
		defer stop()
		observed := make(chan string, 1)
		done := make(chan error, 1)
		go func() {
			done <- broker.Consume(consumeCtx, 1, func(work context.Context, p events.Packet, d events.Delivery) error {
				err := consumer.Handle(work, p, d)
				if err == nil {
					observed <- trace.SpanContextFromContext(work).TraceID().String()
				}
				return err
			})
		}()
		select {
		case got := <-observed:
			stop()
			if got != "11111111111111111111111111111111" {
				t.Fatalf("consumer trace=%s", got)
			}
		case <-consumeCtx.Done():
			t.Fatal("traced event not consumed")
		}
		select {
		case <-done:
		case <-time.After(6 * time.Second):
			t.Fatal("consumer failed to stop")
		}
	})
	t.Run("bounded workers drain on shutdown", func(t *testing.T) {
		makeTransfer(t, "worker-1")
		makeTransfer(t, "worker-2")
		makeTransfer(t, "worker-3")
		blocker := &blockingEffect{entered: make(chan struct{}, 8), release: make(chan struct{}), real: store}
		workerCtx, stop := context.WithCancel(ctx)
		done := make(chan struct{})
		go func() {
			defer close(done)
			messaging.Run(workerCtx, broker, store, blocker, 2, 1, slog.New(slog.NewJSONHandler(io.Discard, nil)))
		}()
		for i := 0; i < 2; i++ {
			select {
			case <-blocker.entered:
			case <-time.After(15 * time.Second):
				stop()
				t.Fatal("workers did not start")
			}
		}
		stop()
		close(blocker.release)
		select {
		case <-done:
		case <-time.After(10 * time.Second):
			t.Fatal("workers failed to drain")
		}
		if blocker.max.Load() != 2 || blocker.active.Load() != 0 {
			t.Fatalf("max=%d active=%d", blocker.max.Load(), blocker.active.Load())
		}
	})
	var mismatches int
	if err = pool.QueryRow(ctx, `SELECT count(*) FROM account_balance_reconciliation WHERE difference_minor<>0`).Scan(&mismatches); err != nil || mismatches != 0 {
		t.Fatalf("consumer changed financial state: %d %v", mismatches, err)
	}
}
