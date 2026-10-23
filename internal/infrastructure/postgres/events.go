package postgres

import (
	"context"
	"encoding/json"
	"errors"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"transfer-engine/internal/business/events"
)

type EventStore struct{ pool *pgxpool.Pool }

func NewEventStore(pool *pgxpool.Pool) *EventStore { return &EventStore{pool: pool} }
func rollback(tx pgx.Tx) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	_ = tx.Rollback(ctx)
}
func (s *EventStore) Check(ctx context.Context) error {
	var ok bool
	err := s.pool.QueryRow(ctx, `SELECT to_regclass('transfer_notifications') IS NOT NULL AND to_regclass('dlq_replay_audit') IS NOT NULL`).Scan(&ok)
	if err != nil || !ok {
		return events.ErrTransient
	}
	return nil
}

// DispatchOne holds a single row lock through a bounded publish/confirm. A
// process crash releases the transaction; publishing before mark may duplicate.
func (s *EventStore) DispatchOne(ctx context.Context, send func(context.Context, events.Envelope) error) (bool, error) {
	ctx, cancel := context.WithTimeout(ctx, 8*time.Second)
	defer cancel()
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return false, events.ErrTransient
	}
	defer rollback(tx)
	var e events.Envelope
	var payload []byte
	var attempts int
	err = tx.QueryRow(ctx, `SELECT event_id::text,event_type,event_version,aggregate_id::text,occurred_at,COALESCE(traceparent,''),payload,attempts FROM outbox_events WHERE published_at IS NULL AND available_at<=clock_timestamp() ORDER BY available_at,event_id FOR UPDATE SKIP LOCKED LIMIT 1`).Scan(&e.ID, &e.Type, &e.Version, &e.AggregateID, &e.OccurredAt, &e.Traceparent, &payload, &attempts)
	if errors.Is(err, pgx.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, events.ErrTransient
	}
	// Invalid payload still becomes an invalid envelope and is routed to DLQ.
	_ = json.Unmarshal(payload, &e.Payload)
	sendErr := send(ctx, e)
	if sendErr == nil {
		_, err = tx.Exec(ctx, `UPDATE outbox_events SET published_at=clock_timestamp(),attempts=LEAST(attempts,2147483646)+1 WHERE event_id=$1`, e.ID)
	} else {
		delay := time.Second << min(attempts, 5)
		delay = events.Jitter(delay)
		_, err = tx.Exec(ctx, `UPDATE outbox_events SET attempts=LEAST(attempts,2147483646)+1,available_at=clock_timestamp()+($2::bigint*interval '1 millisecond') WHERE event_id=$1`, e.ID, delay.Milliseconds())
	}
	if err != nil {
		return true, events.ErrTransient
	}
	if err = tx.Commit(ctx); err != nil {
		return true, events.ErrTransient
	}
	return true, sendErr
}
func (s *EventStore) Apply(ctx context.Context, e events.Envelope) error {
	ctx, cancel := context.WithTimeout(ctx, 4*time.Second)
	defer cancel()
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return events.ErrTransient
	}
	defer rollback(tx)
	tag, err := tx.Exec(ctx, `INSERT INTO processed_events(consumer_name,event_id) VALUES('transfer_notification_v1',$1) ON CONFLICT DO NOTHING`, e.ID)
	if err != nil {
		return events.ErrTransient
	}
	if tag.RowsAffected() == 0 {
		return nil
	}
	// Trust only an event matching the authoritative transfer; this effect is
	// an internal simulated notification, not a balance update or external send.
	tag, err = tx.Exec(ctx, `INSERT INTO transfer_notifications(event_id,transfer_id,currency,amount_minor) SELECT $1,id,currency,amount_minor FROM transfers WHERE id=$2 AND currency=$3 AND amount_minor=$4 ON CONFLICT(transfer_id) DO NOTHING`, e.ID, e.Payload.TransferID, e.Payload.Currency, e.Payload.Amount)
	if err != nil {
		return events.ErrTransient
	}
	if tag.RowsAffected() == 0 {
		var exists bool
		err = tx.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM transfer_notifications WHERE transfer_id=$1 AND currency=$2 AND amount_minor=$3)`, e.Payload.TransferID, e.Payload.Currency, e.Payload.Amount).Scan(&exists)
		if err != nil {
			return events.ErrTransient
		}
		if !exists {
			return events.ErrPermanent
		}
	}
	if err = tx.Commit(ctx); err != nil {
		return events.ErrTransient
	}
	return nil
}
func (s *EventStore) BeginReplay(ctx context.Context, event, operator, reason string) (string, error) {
	var id string
	err := s.pool.QueryRow(ctx, `INSERT INTO dlq_replay_audit(event_id,operator_id,reason) VALUES($1,$2,$3) RETURNING id::text`, event, operator, reason).Scan(&id)
	if err != nil {
		return "", events.ErrTransient
	}
	return id, nil
}
func (s *EventStore) ConfirmReplay(ctx context.Context, id string) error {
	tag, err := s.pool.Exec(ctx, `UPDATE dlq_replay_audit SET confirmed_at=clock_timestamp() WHERE id=$1`, id)
	if err != nil || tag.RowsAffected() != 1 {
		return events.ErrTransient
	}
	return nil
}
