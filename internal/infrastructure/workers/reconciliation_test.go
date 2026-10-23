package workers

import (
	"context"
	"io"
	"log/slog"
	"testing"
	"time"
	"transfer-engine/internal/business/reconciliation"
)

type blockingAudit struct {
	started chan struct{}
	saves   int
}

func (s *blockingAudit) Settings(context.Context) (reconciliation.Settings, error) {
	return reconciliation.Settings{Enabled: true, IntervalSeconds: 60, Version: 1}, nil
}
func (s *blockingAudit) UpdateSettings(context.Context, reconciliation.Settings) (reconciliation.Settings, error) {
	panic("unexpected call")
}
func (s *blockingAudit) Begin(context.Context, time.Duration) (int64, error) { return 1, nil }
func (s *blockingAudit) Snapshot(ctx context.Context) (reconciliation.Report, error) {
	close(s.started)
	<-ctx.Done()
	return reconciliation.Report{}, ctx.Err()
}
func (s *blockingAudit) Save(context.Context, int64, reconciliation.Report) error {
	s.saves++
	return nil
}

type auditLease struct{ released bool }

func (l *auditLease) Acquire(context.Context, time.Duration) (string, error) { return "token", nil }
func (l *auditLease) Release(context.Context, string) error                  { l.released = true; return nil }
func TestSchedulerCancellationDrainsAndReleases(t *testing.T) {
	store := &blockingAudit{started: make(chan struct{})}
	lease := &auditLease{}
	service, _ := reconciliation.New(store, lease, time.Minute)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan struct{})
	go func() {
		defer close(done)
		RunReconciliation(ctx, service, slog.New(slog.NewJSONHandler(io.Discard, nil)))
	}()
	select {
	case <-store.started:
	case <-time.After(time.Second):
		t.Fatal("audit did not start")
	}
	cancel()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("worker failed to drain")
	}
	if !lease.released || store.saves != 0 {
		t.Fatal("canceled snapshot was published or lease not released")
	}
}

type failingSettings struct {
	blockingAudit
	calls int
	err   error
}

func (s *failingSettings) Settings(context.Context) (reconciliation.Settings, error) {
	s.calls++
	if s.calls <= 2 {
		return reconciliation.Settings{}, s.err
	}
	return reconciliation.Settings{Enabled: false, IntervalSeconds: 60, Version: 1}, nil
}
func TestSchedulerRetriesOnlyAvailabilityFailuresAndResets(t *testing.T) {
	for _, failure := range []error{reconciliation.ErrUnavailable, reconciliation.ErrInvalid} {
		store := &failingSettings{err: failure}
		service, _ := reconciliation.New(store, &auditLease{}, time.Second)
		var delays []time.Duration
		runReconciliation(context.Background(), service, slog.New(slog.NewJSONHandler(io.Discard, nil)), func(_ context.Context, d time.Duration) bool {
			delays = append(delays, d)
			return len(delays) < 3
		})
		if len(delays) != 3 || delays[2] != time.Minute {
			t.Fatal("healthy interval not restored")
		}
		if failure == reconciliation.ErrUnavailable {
			for i, d := range delays[:2] {
				base := time.Second << i
				if d < base*8/10 || d > base*12/10 {
					t.Fatal("unbounded or missing backoff")
				}
			}
		} else if delays[0] != time.Minute || delays[1] != time.Minute {
			t.Fatal("permanent error retried eagerly")
		}
	}
}
