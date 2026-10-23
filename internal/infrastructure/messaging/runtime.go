package messaging

import (
	"context"
	"log/slog"
	"sync"
	"time"
	"transfer-engine/internal/business/events"
)

// Run owns a fixed publisher plus a bounded number of workers. Outages never
// enter the financial path. Sessions reconnect with bounded jittered backoff.
func Run(ctx context.Context, b Broker, outbox events.Outbox, effects events.Effects, workers, prefetch int, logger *slog.Logger) {
	if workers < 1 || workers > 16 || prefetch < 1 || prefetch > 64 {
		logger.Error("invalid_messaging_limits")
		return
	}
	for failures := 0; ctx.Err() == nil; failures++ {
		if err := b.Declare(ctx); err != nil {
			logger.Warn("broker_unavailable")
			if !pause(ctx, events.Jitter(time.Second<<min(failures, 5))) {
				return
			}
			continue
		}
		failures = 0
		session, cancel := context.WithCancel(ctx)
		results := make(chan error, workers+1)
		var group sync.WaitGroup
		group.Add(1)
		go func() {
			defer group.Done()
			for session.Err() == nil {
				found, err := events.PublishNext(session, outbox, b)
				if err != nil {
					results <- err
					return
				}
				if !found && !pause(session, 250*time.Millisecond) {
					return
				}
			}
		}()
		consumer := events.Consumer{Effects: effects, Sender: b, Policy: events.DefaultPolicy()}
		for i := 0; i < workers; i++ {
			group.Add(1)
			go func() { defer group.Done(); results <- b.Consume(session, prefetch, consumer.Handle) }()
		}
		select {
		case <-ctx.Done():
		case <-results:
			logger.Warn("messaging_session_restarting")
		}
		cancel()
		group.Wait()
		if !pause(ctx, events.Jitter(time.Second)) {
			return
		}
	}
}
func pause(ctx context.Context, d time.Duration) bool {
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}
