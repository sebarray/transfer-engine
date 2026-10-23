package redis

import (
	"context"
	"errors"
	"sync"
	"time"

	"transfer-engine/internal/business/reconciliation"
)

// ProtectedLease suppresses acquisitions after three dependency failures. It
// never retries a possibly successful SET NX and never suppresses token cleanup.
// It is process-local; PostgreSQL fencing remains the correctness boundary.
type ProtectedLease struct {
	lease      reconciliation.Lease
	mu         sync.Mutex
	now        func() time.Time
	failures   int
	generation uint64
	openUntil  time.Time
	probe      bool
}

func NewProtectedLease(lease reconciliation.Lease) *ProtectedLease {
	return &ProtectedLease{lease: lease, now: time.Now}
}

func (l *ProtectedLease) Acquire(ctx context.Context, ttl time.Duration) (string, error) {
	if ttl < time.Millisecond || ttl > time.Minute {
		return "", reconciliation.ErrInvalid
	}
	if err := ctx.Err(); err != nil {
		return "", err
	}
	l.mu.Lock()
	if !l.openUntil.IsZero() {
		if l.now().Before(l.openUntil) || l.probe {
			l.mu.Unlock()
			return "", reconciliation.ErrUnavailable
		}
		l.probe = true
	}
	generation := l.generation
	probe := l.probe
	l.mu.Unlock()
	call, cancel := context.WithTimeout(ctx, time.Second)
	defer cancel()
	token, err := l.lease.Acquire(call, ttl)
	l.mu.Lock()
	defer l.mu.Unlock()
	// Results from calls admitted before an opening cannot close a newer circuit.
	if generation != l.generation {
		return token, err
	}
	if ctx.Err() != nil {
		if probe {
			l.probe = false
		}
		return token, err
	}
	if errors.Is(err, reconciliation.ErrUnavailable) || errors.Is(err, context.DeadlineExceeded) {
		l.failures++
		if probe || l.failures >= 3 {
			l.openUntil = l.now().Add(10 * time.Second)
			l.probe = false
			l.generation++
		}
	} else {
		// Contention is evidence of a reachable dependency, not an outage.
		l.failures = 0
		l.openUntil = time.Time{}
		l.probe = false
		if probe {
			l.generation++
		}
	}
	return token, err
}

func (l *ProtectedLease) Release(ctx context.Context, token string) error {
	call, cancel := context.WithTimeout(ctx, time.Second)
	defer cancel()
	return l.lease.Release(call, token)
}
