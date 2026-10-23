// Package reconciliation coordinates read-only ledger audits. It never changes balances.
package reconciliation

import (
	"context"
	"errors"
	"time"
)

var (
	ErrBusy        = errors.New("reconciliation already leased")
	ErrStale       = errors.New("reconciliation lease expired or superseded")
	ErrConflict    = errors.New("settings version conflict")
	ErrInvalid     = errors.New("invalid reconciliation settings")
	ErrUnavailable = errors.New("reconciliation dependency unavailable")
)

type Settings struct {
	Enabled         bool
	IntervalSeconds int
	Version         int64
}
type Report struct{ Accounts, Mismatches int64 }

// Lease is an efficiency mechanism; Store independently fences persisted reports.
type Lease interface {
	Acquire(context.Context, time.Duration) (string, error)
	Release(context.Context, string) error
}
type Store interface {
	Settings(context.Context) (Settings, error)
	UpdateSettings(context.Context, Settings) (Settings, error)
	Begin(context.Context, time.Duration) (int64, error)
	Snapshot(context.Context) (Report, error)
	Save(context.Context, int64, Report) error
}
type Service struct {
	store Store
	lease Lease
	ttl   time.Duration
}

func New(store Store, lease Lease, ttl time.Duration) (*Service, error) {
	if ttl < time.Second || ttl > time.Minute {
		return nil, ErrInvalid
	}
	return &Service{store: store, lease: lease, ttl: ttl}, nil
}
func (s *Service) Settings(ctx context.Context) (Settings, error) {
	ctx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	return s.store.Settings(ctx)
}
func (s *Service) UpdateSettings(ctx context.Context, settings Settings) (Settings, error) {
	ctx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	if settings.Version < 1 || settings.IntervalSeconds < 1 || settings.IntervalSeconds > 86400 {
		return Settings{}, ErrInvalid
	}
	return s.store.UpdateSettings(ctx, settings)
}
func (s *Service) RunOnce(ctx context.Context) error {
	ctx, cancel := context.WithTimeout(ctx, s.ttl)
	defer cancel()
	token, err := s.lease.Acquire(ctx, s.ttl)
	if err != nil {
		return err
	}
	defer func() {
		cleanup, stop := context.WithTimeout(context.WithoutCancel(ctx), time.Second)
		defer stop()
		_ = s.lease.Release(cleanup, token)
	}()
	// PostgreSQL enforces an independent lease and monotonically increasing fence.
	// Redis loss or takeover cannot bypass this predicate at the persistence boundary.
	fence, err := s.store.Begin(ctx, s.ttl)
	if err != nil {
		return err
	}
	report, err := s.store.Snapshot(ctx)
	if err != nil {
		return err
	}
	return s.store.Save(ctx, fence, report)
}
