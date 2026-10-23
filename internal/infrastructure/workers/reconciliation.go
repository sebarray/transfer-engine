package workers

import (
	"context"
	"errors"
	"log/slog"
	"time"
	"transfer-engine/internal/business/events"
	"transfer-engine/internal/business/reconciliation"
)

// RunReconciliation runs at most one audit per process at a time. The service's
// leases coordinate independent replicas. Cancellation drains the current call.
func RunReconciliation(ctx context.Context, service *reconciliation.Service, logger *slog.Logger) {
	runReconciliation(ctx, service, logger, waitReconciliation)
}

func runReconciliation(ctx context.Context, service *reconciliation.Service, logger *slog.Logger, wait func(context.Context, time.Duration) bool) {
	failures := 0
	for {
		if ctx.Err() != nil {
			return
		}
		settings, err := service.Settings(ctx)
		delay := time.Minute
		if err == nil {
			delay = time.Duration(settings.IntervalSeconds) * time.Second
			if settings.Enabled {
				err = service.RunOnce(ctx)
			}
		}
		if err != nil && ctx.Err() == nil && !errors.Is(err, reconciliation.ErrBusy) {
			logger.Warn("reconciliation_failed")
		}
		if errors.Is(err, reconciliation.ErrUnavailable) {
			// Retry the next auxiliary audit, never an individual ambiguous write.
			// Bound outage retries independently of the configured healthy interval.
			delay = events.Jitter(time.Second << min(failures, 5))
			failures = min(failures+1, 5)
		} else {
			failures = 0
		}
		if !wait(ctx, delay) {
			return
		}
	}
}

func waitReconciliation(ctx context.Context, delay time.Duration) bool {
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}
