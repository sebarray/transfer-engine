package postgres

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
	"transfer-engine/internal/business/transfers"
)

type TransferStore struct{ pool *pgxpool.Pool }

func NewTransferStore(pool *pgxpool.Pool) *TransferStore { return &TransferStore{pool: pool} }

const accountColumns = `id::text, owner_id, currency, status, kind, balance_minor`

func scanAccount(row pgx.Row) (transfers.Account, error) {
	var a transfers.Account
	err := row.Scan(&a.ID, &a.OwnerID, &a.Currency, &a.Status, &a.Kind, &a.Balance)
	return a, translate(err)
}
func (s *TransferStore) CreateAccount(ctx context.Context, actor, currency string) (transfers.Account, error) {
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	return scanAccount(s.pool.QueryRow(ctx, `INSERT INTO accounts(owner_id,currency) VALUES($1,$2) RETURNING `+accountColumns, actor, currency))
}
func (s *TransferStore) Account(ctx context.Context, actor, id string) (transfers.Account, error) {
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	return scanAccount(s.pool.QueryRow(ctx, `SELECT `+accountColumns+` FROM accounts WHERE id=$1 AND owner_id=$2 AND kind='customer'`, id, actor))
}
func (s *TransferStore) WithinTransaction(ctx context.Context, fn func(transfers.Tx) error) error {
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	if err != nil {
		return translate(err)
	}
	defer func() {
		cleanup, stop := context.WithTimeout(context.Background(), 3*time.Second)
		defer stop()
		_ = tx.Rollback(cleanup)
	}()
	if _, err = tx.Exec(ctx, `SET LOCAL lock_timeout='3s'; SET LOCAL statement_timeout='8s'`); err != nil {
		return translate(err)
	}
	if err = fn(&transferTx{tx: tx}); err != nil {
		return err
	}
	err = tx.Commit(ctx)
	if err != nil {
		var pgErr *pgconn.PgError
		if errors.As(err, &pgErr) || errors.Is(err, pgx.ErrTxCommitRollback) {
			return translate(err)
		}
		return transfers.ErrCommitUnknown
	}
	return nil
}

type transferTx struct{ tx pgx.Tx }

func (s *TransferStore) Resolve(ctx context.Context, c transfers.Claim) (*transfers.Result, error) {
	ctx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	return readResult(s.pool.QueryRow(ctx, `SELECT request_hash,state,result FROM idempotency_records WHERE actor_id=$1 AND operation=$2 AND idempotency_key=$3`, c.Actor, c.Operation, c.Key), c)
}

func (t *transferTx) Claim(ctx context.Context, c transfers.Claim) (*transfers.Result, error) {
	tag, err := t.tx.Exec(ctx, `INSERT INTO idempotency_records(actor_id,operation,idempotency_key,request_hash) VALUES($1,$2,$3,$4) ON CONFLICT DO NOTHING`, c.Actor, c.Operation, c.Key, c.Hash[:])
	if err != nil {
		return nil, translate(err)
	}
	if tag.RowsAffected() == 1 {
		return nil, nil
	}
	return readResult(t.tx.QueryRow(ctx, `SELECT request_hash,state,result FROM idempotency_records WHERE actor_id=$1 AND operation=$2 AND idempotency_key=$3`, c.Actor, c.Operation, c.Key), c)
}

func readResult(row pgx.Row, c transfers.Claim) (*transfers.Result, error) {
	var hash, result []byte
	var state string
	err := row.Scan(&hash, &state, &result)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, translate(err)
	}
	if !bytes.Equal(hash, c.Hash[:]) {
		return nil, transfers.ErrMismatch
	}
	if state != "completed" {
		return nil, transfers.ErrInFlight
	}
	var r transfers.Result
	if json.Unmarshal(result, &r) != nil || r.ID == "" || r.Status != "completed" {
		return nil, transfers.ErrUnavailable
	}
	return &r, nil
}
func (t *transferTx) Treasury(ctx context.Context, currency string) (string, error) {
	_, err := t.tx.Exec(ctx, `INSERT INTO accounts(owner_id,kind,currency) VALUES('test-system','test_treasury',$1) ON CONFLICT DO NOTHING`, currency)
	if err != nil {
		return "", translate(err)
	}
	var id string
	err = t.tx.QueryRow(ctx, `SELECT id::text FROM accounts WHERE kind='test_treasury' AND currency=$1`, currency).Scan(&id)
	return id, translate(err)
}
func (t *transferTx) LockAccounts(ctx context.Context, source, target string) (transfers.Account, transfers.Account, error) {
	rows, err := t.tx.Query(ctx, `SELECT `+accountColumns+` FROM accounts WHERE id IN ($1::uuid,$2::uuid) ORDER BY id FOR UPDATE`, source, target)
	if err != nil {
		return transfers.Account{}, transfers.Account{}, translate(err)
	}
	defer rows.Close()
	var a, b transfers.Account
	count := 0
	for rows.Next() {
		value, err := scanAccount(rows)
		if err != nil {
			return a, b, err
		}
		count++
		if value.ID == source {
			a = value
		} else {
			b = value
		}
	}
	if err = rows.Err(); err != nil {
		return a, b, translate(err)
	}
	if count != 2 {
		return a, b, transfers.ErrNotFound
	}
	return a, b, nil
}
func (t *transferTx) Post(ctx context.Context, c transfers.Command, funding bool) (transfers.Result, error) {
	kind := "transfer"
	if funding {
		kind = "test_issuance"
	}
	var journal string
	err := t.tx.QueryRow(ctx, `INSERT INTO journal_entries(kind,currency) VALUES($1,$2) RETURNING id::text`, kind, c.Currency).Scan(&journal)
	if err != nil {
		return transfers.Result{}, translate(err)
	}
	result := transfers.Result{ID: journal, Status: "completed"}
	if !funding {
		err = t.tx.QueryRow(ctx, `INSERT INTO transfers(actor_id,source_account_id,destination_account_id,currency,amount_minor,journal_entry_id) VALUES($1,$2,$3,$4,$5,$6) RETURNING id::text`, c.ActorID, c.SourceID, c.DestinationID, c.Currency, c.Amount, journal).Scan(&result.ID)
		if err != nil {
			return transfers.Result{}, translate(err)
		}
	}
	_, err = t.tx.Exec(ctx, `INSERT INTO postings(journal_entry_id,account_id,currency,amount_minor) VALUES($1,$2,$4,-$5::bigint),($1,$3,$4,$5::bigint)`, journal, c.SourceID, c.DestinationID, c.Currency, c.Amount)
	if err != nil {
		return transfers.Result{}, translate(err)
	}
	_, err = t.tx.Exec(ctx, `UPDATE accounts SET balance_minor=balance_minor + CASE WHEN id=$1::uuid THEN -$3::bigint ELSE $3::bigint END,version=version+1 WHERE id IN ($1::uuid,$2::uuid)`, c.SourceID, c.DestinationID, c.Amount)
	if err != nil {
		return transfers.Result{}, translate(err)
	}
	if !funding {
		carrier := propagation.MapCarrier{}
		otel.GetTextMapPropagator().Inject(ctx, carrier)
		_, err = t.tx.Exec(ctx, `INSERT INTO outbox_events(event_type,event_version,aggregate_id,payload,traceparent) VALUES('transfer.completed',1,$1::uuid,jsonb_build_object('transfer_id',$1::uuid::text,'currency',$2::text,'amount_minor',$3::bigint),$4)`, result.ID, c.Currency, c.Amount, carrier.Get("traceparent"))
	}
	return result, translate(err)
}
func (t *transferTx) Complete(ctx context.Context, c transfers.Claim, r transfers.Result) error {
	payload, err := json.Marshal(r)
	if err != nil {
		return transfers.ErrUnavailable
	}
	tag, err := t.tx.Exec(ctx, `UPDATE idempotency_records SET state='completed',result=$4,completed_at=CURRENT_TIMESTAMP WHERE actor_id=$1 AND operation=$2 AND idempotency_key=$3 AND state='in_progress'`, c.Actor, c.Operation, c.Key, payload)
	if err != nil {
		return translate(err)
	}
	if tag.RowsAffected() != 1 {
		return transfers.ErrConflict
	}
	return nil
}
func translate(err error) error {
	if err == nil {
		return nil
	}
	if errors.Is(err, pgx.ErrNoRows) {
		return transfers.ErrNotFound
	}
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return err
	}
	var e *pgconn.PgError
	if errors.As(err, &e) {
		switch e.Code {
		case "55P03":
			return transfers.ErrInFlight
		case "40001", "40P01", "23505":
			return transfers.ErrConflict
		case "22003":
			return transfers.ErrOverflow
		}
	}
	return transfers.ErrUnavailable
}
