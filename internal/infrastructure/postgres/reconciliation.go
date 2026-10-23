package postgres

import (
	"context"
	"errors"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"time"
	"transfer-engine/internal/business/reconciliation"
)

type ReconciliationStore struct{ pool *pgxpool.Pool }

func NewReconciliationStore(pool *pgxpool.Pool) *ReconciliationStore {
	return &ReconciliationStore{pool: pool}
}
func (s *ReconciliationStore) Settings(ctx context.Context) (reconciliation.Settings, error) {
	ctx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	var settings reconciliation.Settings
	err := s.pool.QueryRow(ctx, `SELECT enabled,interval_seconds,version FROM reconciliation_settings WHERE id`).Scan(&settings.Enabled, &settings.IntervalSeconds, &settings.Version)
	return settings, reconciliationError(err)
}
func (s *ReconciliationStore) UpdateSettings(ctx context.Context, v reconciliation.Settings) (reconciliation.Settings, error) {
	ctx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	var updated reconciliation.Settings
	err := s.pool.QueryRow(ctx, `UPDATE reconciliation_settings SET enabled=$1,interval_seconds=$2,version=version+1 WHERE id AND version=$3 RETURNING enabled,interval_seconds,version`, v.Enabled, v.IntervalSeconds, v.Version).Scan(&updated.Enabled, &updated.IntervalSeconds, &updated.Version)
	if errors.Is(err, pgx.ErrNoRows) {
		return updated, reconciliation.ErrConflict
	}
	return updated, reconciliationError(err)
}
func (s *ReconciliationStore) Begin(ctx context.Context, ttl time.Duration) (int64, error) {
	if ttl <= 0 || ttl > time.Minute {
		return 0, reconciliation.ErrInvalid
	}
	var fence int64
	err := s.pool.QueryRow(ctx, `UPDATE reconciliation_state SET fence=fence+1,lease_until=clock_timestamp()+($1::bigint*interval '1 millisecond') WHERE id AND lease_until<=clock_timestamp() RETURNING fence`, ttl.Milliseconds()).Scan(&fence)
	if errors.Is(err, pgx.ErrNoRows) {
		return 0, reconciliation.ErrBusy
	}
	return fence, reconciliationError(err)
}
func (s *ReconciliationStore) Snapshot(ctx context.Context) (reconciliation.Report, error) {
	var report reconciliation.Report
	// One statement gives balances and ledger the same MVCC snapshot.
	err := s.pool.QueryRow(ctx, `SELECT count(*),count(*) FILTER(WHERE difference_minor<>0) FROM account_balance_reconciliation`).Scan(&report.Accounts, &report.Mismatches)
	return report, reconciliationError(err)
}
func (s *ReconciliationStore) Save(ctx context.Context, fence int64, report reconciliation.Report) error {
	ctx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return reconciliationError(err)
	}
	defer func() {
		cleanup, stop := context.WithTimeout(context.Background(), time.Second)
		defer stop()
		_ = tx.Rollback(cleanup)
	}()
	// Check expiry after obtaining the row lock, never before a potentially
	// long lock wait. New owners serialize on this same PostgreSQL row.
	if _, err = tx.Exec(ctx, `SELECT fence FROM reconciliation_state WHERE id FOR UPDATE`); err != nil {
		return reconciliationError(err)
	}
	tag, err := tx.Exec(ctx, `UPDATE reconciliation_state SET accounts=$2,mismatches=$3,checked_at=clock_timestamp(),lease_until='-infinity' WHERE id AND fence=$1 AND lease_until>clock_timestamp()`, fence, report.Accounts, report.Mismatches)
	if err != nil {
		return reconciliationError(err)
	}
	if tag.RowsAffected() != 1 {
		return reconciliation.ErrStale
	}
	return reconciliationError(tx.Commit(ctx))
}
func reconciliationError(err error) error {
	if err == nil {
		return nil
	}
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return err
	}
	return reconciliation.ErrUnavailable
}
