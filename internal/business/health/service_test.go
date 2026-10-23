package health_test

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"transfer-engine/internal/business/health"
)

type dependencyFunc func(context.Context) error

func (f dependencyFunc) Ping(ctx context.Context) error { return f(ctx) }

func TestReady(t *testing.T) {
	unavailable := errors.New("dependency unavailable")
	for _, tt := range []struct {
		name string
		err  error
	}{
		{name: "available"},
		{name: "unavailable", err: unavailable},
	} {
		t.Run(tt.name, func(t *testing.T) {
			svc := health.New(dependencyFunc(func(context.Context) error { return tt.err }), time.Second)
			if err := svc.Ready(context.Background()); !errors.Is(err, tt.err) {
				t.Fatalf("Ready() error = %v, want %v", err, tt.err)
			}
		})
	}
}

func TestReadyPropagatesContextAndBoundsDeadline(t *testing.T) {
	type contextKey struct{}
	parent := context.WithValue(context.Background(), contextKey{}, "request")
	before := time.Now()
	svc := health.New(dependencyFunc(func(ctx context.Context) error {
		if ctx.Value(contextKey{}) != "request" {
			t.Fatal("request context value was not propagated")
		}
		deadline, ok := ctx.Deadline()
		if !ok || deadline.Before(before) || deadline.After(time.Now().Add(time.Second)) {
			t.Fatalf("probe deadline = %v, want bounded by readiness timeout", deadline)
		}
		return nil
	}), time.Second)
	if err := svc.Ready(parent); err != nil {
		t.Fatal(err)
	}
}

func TestReadyRespectsEarlierParentDeadline(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Hour)
	defer cancel()
	want, _ := ctx.Deadline()
	svc := health.New(dependencyFunc(func(ctx context.Context) error {
		if got, ok := ctx.Deadline(); !ok || !got.Equal(want) {
			t.Fatalf("probe deadline = %v, want parent deadline %v", got, want)
		}
		return nil
	}), 2*time.Hour)
	if err := svc.Ready(ctx); err != nil {
		t.Fatal(err)
	}
}

func TestReadyCanceledBeforeProbe(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	svc := health.New(dependencyFunc(func(context.Context) error {
		t.Fatal("canceled request must not probe dependency")
		return nil
	}), time.Second)
	if err := svc.Ready(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("Ready() error = %v, want cancellation", err)
	}
}

func TestReadyCancellationDuringProbe(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	svc := health.New(dependencyFunc(func(ctx context.Context) error {
		cancel()
		<-ctx.Done()
		return ctx.Err()
	}), time.Second)
	if err := svc.Ready(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("Ready() error = %v, want cancellation", err)
	}
}

func TestReadyExpiredDeadline(t *testing.T) {
	ctx, cancel := context.WithDeadline(context.Background(), time.Now().Add(-time.Second))
	defer cancel()
	svc := health.New(dependencyFunc(func(context.Context) error {
		t.Fatal("expired request must not probe dependency")
		return nil
	}), time.Second)
	if err := svc.Ready(ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Ready() error = %v, want expired deadline", err)
	}
}

func TestReadyOwnTimeout(t *testing.T) {
	svc := health.New(dependencyFunc(func(ctx context.Context) error {
		<-ctx.Done()
		return ctx.Err()
	}), time.Nanosecond)
	if err := svc.Ready(context.Background()); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Ready() error = %v, want timeout", err)
	}
}

func TestDrainRejectsNewAndInFlightProbe(t *testing.T) {
	started := make(chan struct{})
	release := make(chan struct{})
	result := make(chan error, 1)
	svc := health.New(dependencyFunc(func(context.Context) error {
		close(started)
		<-release
		return nil
	}), time.Hour)
	go func() { result <- svc.Ready(context.Background()) }()
	<-started
	svc.Drain()
	if err := svc.Ready(context.Background()); !errors.Is(err, health.ErrDraining) {
		t.Fatalf("new probe error = %v, want draining", err)
	}
	close(release)
	if err := <-result; !errors.Is(err, health.ErrDraining) {
		t.Fatalf("in-flight probe error = %v, want draining", err)
	}
}

func TestConcurrentDrainAndReadiness(t *testing.T) {
	svc := health.New(dependencyFunc(func(context.Context) error { return nil }), time.Hour)
	var group sync.WaitGroup
	for range 30 {
		group.Add(1)
		go func() {
			defer group.Done()
			svc.Drain()
			if err := svc.Ready(context.Background()); !errors.Is(err, health.ErrDraining) {
				t.Errorf("Ready() after drain = %v, want draining", err)
			}
		}()
	}
	group.Wait()
}
