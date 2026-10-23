package telemetry

import (
	"context"
	"go.opentelemetry.io/otel"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"
)

func TestOTLPExportsBothSignalsAndShutdownIsBounded(t *testing.T) {
	previousTrace, previousMeter, previousProp, previousError := otel.GetTracerProvider(), otel.GetMeterProvider(), otel.GetTextMapPropagator(), otel.GetErrorHandler()
	defer func() {
		otel.SetTracerProvider(previousTrace)
		otel.SetMeterProvider(previousMeter)
		otel.SetTextMapPropagator(previousProp)
		otel.SetErrorHandler(previousError)
	}()
	var mu sync.Mutex
	paths := map[string]int{}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.Copy(io.Discard, r.Body)
		mu.Lock()
		paths[r.URL.Path]++
		mu.Unlock()
		w.Header().Set("Content-Type", "application/x-protobuf")
		w.WriteHeader(200)
	}))
	defer server.Close()
	shutdown, err := Start(context.Background(), server.URL, slog.New(slog.NewJSONHandler(io.Discard, nil)))
	if err != nil {
		t.Fatal(err)
	}
	ctx, span := otel.Tracer("test").Start(context.Background(), "test operation")
	counter, _ := otel.Meter("test").Int64Counter("test.operations")
	counter.Add(ctx, 1)
	span.End()
	cleanup, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err = shutdown(cleanup); err != nil {
		t.Fatal(err)
	}
	mu.Lock()
	defer mu.Unlock()
	if paths["/v1/traces"] == 0 || paths["/v1/metrics"] == 0 {
		t.Fatalf("OTLP paths=%v", paths)
	}
}

func TestCollectorFailureDoesNotBlockRequestsOrShutdown(t *testing.T) {
	previousTrace, previousMeter, previousProp, previousError := otel.GetTracerProvider(), otel.GetMeterProvider(), otel.GetTextMapPropagator(), otel.GetErrorHandler()
	defer func() {
		otel.SetTracerProvider(previousTrace)
		otel.SetMeterProvider(previousMeter)
		otel.SetTextMapPropagator(previousProp)
		otel.SetErrorHandler(previousError)
	}()
	entered, release := make(chan struct{}, 2), make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		entered <- struct{}{}
		select {
		case <-release:
		case <-r.Context().Done():
		}
	}))
	defer server.Close()
	defer close(release)
	shutdown, err := Start(context.Background(), server.URL, slog.New(slog.NewJSONHandler(io.Discard, nil)))
	if err != nil {
		t.Fatal(err)
	}
	_, span := otel.Tracer("test").Start(context.Background(), "nonblocking")
	span.End()
	done := make(chan error, 1)
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		done <- shutdown(ctx)
	}()
	select {
	case <-entered:
	case <-time.After(3 * time.Second):
		t.Fatal("export never attempted")
	}
	select {
	case err := <-done:
		if err == nil {
			t.Fatal("stalled exporter incorrectly succeeded")
		}
	case <-time.After(3 * time.Second):
		t.Fatal("shutdown blocked on collector")
	}
}
