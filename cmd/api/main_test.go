package main

import (
	"context"
	"io"
	"log/slog"
	"net"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	"transfer-engine/internal/business/events"
	"transfer-engine/internal/business/health"
)

type healthyDependency struct{}

func (healthyDependency) Ping(context.Context) error { return nil }

type observedListener struct {
	net.Listener
	closed chan struct{}
	once   sync.Once
}

func (l *observedListener) Close() error {
	err := l.Listener.Close()
	l.once.Do(func() { close(l.closed) })
	return err
}

func TestShutdownDrainsAcceptedRequest(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	l := &observedListener{Listener: listener, closed: make(chan struct{})}
	t.Cleanup(func() { _ = l.Close() })
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	entered, release := make(chan context.Context, 1), make(chan struct{})
	var releaseOnce sync.Once
	finish := func() { releaseOnce.Do(func() { close(release) }) }
	t.Cleanup(finish)
	server := &http.Server{Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		entered <- r.Context()
		select {
		case <-release:
			_, _ = io.WriteString(w, "finished")
		case <-r.Context().Done():
		}
	})}
	status := health.New(healthyDependency{}, time.Second)
	stopped := make(chan error, 1)
	go func() { stopped <- serve(ctx, server, l, status, 2*time.Second, discardLogger()) }()
	response := make(chan string, 1)
	client := &http.Client{Timeout: 5 * time.Second}
	go func() {
		res, err := client.Get("http://" + l.Addr().String())
		if err != nil {
			response <- err.Error()
			return
		}
		defer res.Body.Close()
		body, _ := io.ReadAll(res.Body)
		response <- string(body)
	}()
	requestCtx := await(t, entered)
	cancel()
	await(t, l.closed) // Shutdown has closed admission; accepted handler is still running.
	if err := status.Ready(context.Background()); err == nil {
		t.Fatal("draining server reported ready")
	}
	if requestCtx.Err() != nil {
		t.Fatal("termination signal canceled an accepted request before drain")
	}
	finish()
	if body := await(t, response); body != "finished" {
		t.Fatalf("accepted request lost its response: %q", body)
	}
	if err := await(t, stopped); err != nil {
		t.Fatalf("graceful shutdown failed: %v", err)
	}
}

func TestShutdownDeadlineCancelsAcceptedRequest(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = listener.Close() })
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	entered, canceled := make(chan struct{}), make(chan struct{})
	server := &http.Server{Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		close(entered)
		<-r.Context().Done()
		close(canceled)
	})}
	stopped := make(chan error, 1)
	go func() {
		stopped <- serve(ctx, server, listener, health.New(healthyDependency{}, time.Second), 25*time.Millisecond, discardLogger())
	}()
	clientDone := make(chan struct{})
	go func() {
		defer close(clientDone)
		client := &http.Client{Timeout: 5 * time.Second}
		res, err := client.Get("http://" + listener.Addr().String())
		if err == nil {
			_ = res.Body.Close()
		}
	}()
	await(t, entered)
	cancel()
	if err := await(t, stopped); err == nil || !strings.Contains(err.Error(), "deadline exceeded") {
		t.Fatalf("expected forced shutdown error, got %v", err)
	}
	await(t, canceled)
	await(t, clientDone)
}

func TestRunValidatesBeforeConnecting(t *testing.T) {
	t.Setenv("APP_ENV", "production")
	t.Setenv("DATABASE_URL", "postgres://user:secret@localhost/db")
	t.Setenv("TEST_FUNDS_ENABLED", "true")
	err := run(context.Background(), io.Discard)
	if err == nil || !strings.Contains(err.Error(), "cannot be enabled in production") {
		t.Fatalf("expected test funds guard, got %v", err)
	}
}

func discardLogger() *slog.Logger { return slog.New(slog.NewJSONHandler(io.Discard, nil)) }

func TestReplayWritesRequireExplicitEnablement(t *testing.T) {
	t.Setenv("DATABASE_URL", "postgres://user:secret@127.0.0.1:1/db")
	t.Setenv("DLQ_REPLAY_ENABLED", "false")
	err := runReplay(context.Background(), 1, events.ReplayRequest{Operator: "test", Reason: "test"}, io.Discard)
	if err == nil || !strings.Contains(err.Error(), "DLQ_REPLAY_ENABLED") {
		t.Fatalf("expected replay guard before connection, got %v", err)
	}
}

func await[T any](t *testing.T, ch <-chan T) T {
	t.Helper()
	select {
	case value := <-ch:
		return value
	case <-time.After(10 * time.Second):
		t.Fatal("timed out waiting for test synchronization")
		var zero T
		return zero
	}
}
