package redis

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"transfer-engine/internal/business/reconciliation"
)

type fakeLease struct {
	acquire  func(context.Context) (string, error)
	releases int
}

func (f *fakeLease) Acquire(ctx context.Context, _ time.Duration) (string, error) {
	return f.acquire(ctx)
}
func (f *fakeLease) Release(context.Context, string) error { f.releases++; return nil }

func TestBreakerOpenSingleProbeAndRecovery(t *testing.T) {
	var calls atomic.Int32
	f := &fakeLease{acquire: func(context.Context) (string, error) { calls.Add(1); return "", reconciliation.ErrUnavailable }}
	b := NewProtectedLease(f)
	now := time.Unix(100, 0)
	b.now = func() time.Time { return now }
	for i := 0; i < 20; i++ {
		_, _ = b.Acquire(context.Background(), time.Second)
	}
	if calls.Load() != 3 {
		t.Fatalf("open circuit called dependency %d times", calls.Load())
	}
	if err := b.Release(context.Background(), "old-token"); err != nil || f.releases != 1 {
		t.Fatal("cleanup suppressed")
	}
	now = now.Add(10 * time.Second)
	entered, release := make(chan struct{}), make(chan struct{})
	f.acquire = func(ctx context.Context) (string, error) {
		calls.Add(1)
		if _, ok := ctx.Deadline(); !ok {
			t.Error("missing call deadline")
		}
		close(entered)
		<-release
		return "", reconciliation.ErrBusy
	}
	done := make(chan struct{})
	go func() { defer close(done); _, _ = b.Acquire(context.Background(), time.Second) }()
	<-entered
	var group sync.WaitGroup
	for i := 0; i < 100; i++ {
		group.Add(1)
		go func() {
			defer group.Done()
			_, err := b.Acquire(context.Background(), time.Second)
			if !errors.Is(err, reconciliation.ErrUnavailable) {
				t.Error("extra probe admitted")
			}
		}()
	}
	group.Wait()
	close(release)
	<-done
	f.acquire = func(context.Context) (string, error) { calls.Add(1); return "token", nil }
	if token, err := b.Acquire(context.Background(), time.Second); err != nil || token != "token" || calls.Load() != 5 {
		t.Fatalf("recovery failed: %s %v calls=%d", token, err, calls.Load())
	}
}

func TestLateSuccessCannotCloseNewCircuit(t *testing.T) {
	entered, release := make(chan struct{}), make(chan struct{})
	var calls atomic.Int32
	f := &fakeLease{acquire: func(context.Context) (string, error) {
		if calls.Add(1) == 1 {
			close(entered)
			<-release
			return "old-token", nil
		}
		return "", reconciliation.ErrUnavailable
	}}
	b := NewProtectedLease(f)
	done := make(chan struct{})
	go func() { defer close(done); _, _ = b.Acquire(context.Background(), time.Second) }()
	<-entered
	for i := 0; i < 3; i++ {
		_, _ = b.Acquire(context.Background(), time.Second)
	}
	close(release)
	<-done
	_, _ = b.Acquire(context.Background(), time.Second)
	if calls.Load() != 4 {
		t.Fatal("late success closed circuit")
	}
}

func TestCancellationAndProbeFailure(t *testing.T) {
	calls := 0
	f := &fakeLease{acquire: func(context.Context) (string, error) { calls++; return "", reconciliation.ErrUnavailable }}
	b := NewProtectedLease(f)
	now := time.Unix(100, 0)
	b.now = func() time.Time { return now }
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := b.Acquire(ctx, time.Second); !errors.Is(err, context.Canceled) || calls != 0 {
		t.Fatal("canceled call admitted")
	}
	for i := 0; i < 3; i++ {
		_, _ = b.Acquire(context.Background(), time.Second)
	}
	now = now.Add(10 * time.Second)
	_, _ = b.Acquire(context.Background(), time.Second) // Failed probe reopens for a full cooldown.
	_, _ = b.Acquire(context.Background(), time.Second)
	if calls != 4 {
		t.Fatal("failed probe did not reopen circuit")
	}
}
