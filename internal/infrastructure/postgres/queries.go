package postgres

import (
	"context"
	"time"
	"transfer-engine/internal/business/transfers"
)

func (s *TransferStore) TransferDetails(ctx context.Context, actor, id string) (transfers.TransferDetails, error) {
	ctx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	var v transfers.TransferDetails
	err := s.pool.QueryRow(ctx, `SELECT t.id::text,t.source_account_id::text,t.destination_account_id::text,t.currency,t.amount_minor,t.status,t.created_at FROM transfers t WHERE t.id=$1 AND EXISTS(SELECT 1 FROM accounts a WHERE a.id IN(t.source_account_id,t.destination_account_id) AND a.owner_id=$2 AND a.kind='customer')`, id, actor).Scan(&v.ID, &v.SourceID, &v.DestinationID, &v.Currency, &v.Amount, &v.Status, &v.CreatedAt)
	return v, translate(err)
}
func (s *TransferStore) Movements(ctx context.Context, actor, id string, limit int, before int64) ([]transfers.Movement, error) {
	ctx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	if _, err := s.Account(ctx, actor, id); err != nil {
		return nil, err
	}
	rows, err := s.pool.Query(ctx, `SELECT p.id,p.journal_entry_id::text,p.currency,p.amount_minor,p.created_at FROM postings p JOIN accounts a ON a.id=p.account_id WHERE a.id=$1 AND a.owner_id=$2 AND a.kind='customer' AND ($3::bigint=0 OR p.id<$3) ORDER BY p.id DESC LIMIT $4`, id, actor, before, limit)
	if err != nil {
		return nil, translate(err)
	}
	defer rows.Close()
	items := []transfers.Movement{}
	for rows.Next() {
		var v transfers.Movement
		if err := rows.Scan(&v.ID, &v.JournalID, &v.Currency, &v.Amount, &v.CreatedAt); err != nil {
			return nil, translate(err)
		}
		items = append(items, v)
	}
	return items, translate(rows.Err())
}

// Ping gates financial readiness on the exact supported migration history.
func (s *TransferStore) Ping(ctx context.Context) error {
	var compatible bool
	err := s.pool.QueryRow(ctx, `SELECT count(*)=3 AND min(version)=1 AND max(version)=3 FROM schema_migrations`).Scan(&compatible)
	if err != nil || !compatible {
		return transfers.ErrUnavailable
	}
	_, err = s.pool.Exec(ctx, `SELECT t.id,p.id,o.event_id,n.event_id FROM transfers t,postings p,outbox_events o,transfer_notifications n LIMIT 0`)
	return translate(err)
}
