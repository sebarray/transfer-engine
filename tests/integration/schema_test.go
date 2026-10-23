//go:build integration

package integration_test

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

const (
	sourceID   = "00000000-0000-0000-0000-000000000001"
	targetID   = "00000000-0000-0000-0000-000000000002"
	treasuryID = "00000000-0000-0000-0000-000000000003"
	thirdID    = "00000000-0000-0000-0000-000000000004"
	usdID      = "00000000-0000-0000-0000-000000000005"
	issuanceID = "10000000-0000-0000-0000-000000000001"
	journalID  = "10000000-0000-0000-0000-000000000002"
	transferID = "20000000-0000-0000-0000-000000000001"
	eventID    = "30000000-0000-0000-0000-000000000001"
)

// These tests intentionally fail when Docker is unavailable: requesting the
// integration suite must not silently turn database assertions into skips.
func TestPostgresSchema(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	container, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: testcontainers.ContainerRequest{
			Image:        "postgres:17.11-alpine3.24",
			ExposedPorts: []string{"5432/tcp"},
			Env: map[string]string{
				"POSTGRES_DB": "transfer_engine", "POSTGRES_USER": "test", "POSTGRES_PASSWORD": "test",
			},
			WaitingFor: wait.ForAll(
				wait.ForLog("database system is ready to accept connections").WithOccurrence(2),
				wait.ForListeningPort("5432/tcp"),
			).WithDeadline(90 * time.Second),
		},
		Started: true,
	})
	if err != nil {
		t.Fatalf("start PostgreSQL Testcontainer (Docker is required): %v", err)
	}
	t.Cleanup(func() {
		cleanup, stop := context.WithTimeout(context.Background(), 20*time.Second)
		defer stop()
		if err := container.Terminate(cleanup); err != nil {
			t.Errorf("terminate PostgreSQL Testcontainer: %v", err)
		}
	})
	host, err := container.Host(ctx)
	if err != nil {
		t.Fatal(err)
	}
	port, err := container.MappedPort(ctx, "5432/tcp")
	if err != nil {
		t.Fatal(err)
	}
	conn, err := pgx.Connect(ctx, fmt.Sprintf("postgres://test:test@%s:%s/transfer_engine?sslmode=disable", host, port.Port()))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		cleanup, stop := context.WithTimeout(context.Background(), 5*time.Second)
		defer stop()
		if err := conn.Close(cleanup); err != nil {
			t.Errorf("close PostgreSQL connection: %v", err)
		}
	})

	t.Run("migrations up down up", func(t *testing.T) {
		for _, suffix := range []string{"up", "down", "up"} {
			sql, err := os.ReadFile(filepath.Join("..", "..", "migrations", "000001_initial."+suffix+".sql"))
			if err != nil {
				t.Fatal(err)
			}
			mustExec(t, ctx, conn, string(sql))
		}
	})
	if t.Failed() {
		t.FailNow()
	}
	mustExec(t, ctx, conn, `INSERT INTO accounts (id, owner_id, kind, currency) VALUES
		($1, 'alice', 'customer', 'ARS'), ($2, 'bob', 'customer', 'ARS'),
		($3, 'test-system', 'test_treasury', 'ARS'), ($4, 'carol', 'customer', 'ARS'),
		($5, 'alice', 'customer', 'USD')`, sourceID, targetID, treasuryID, thirdID, usdID)

	t.Run("account constraints", func(t *testing.T) {
		for _, tc := range []struct{ name, sql, code string }{
			{"unsupported currency", `INSERT INTO accounts (owner_id, currency) VALUES ('alice', 'ZZZ')`, "23503"},
			{"nonzero opening balance", `INSERT INTO accounts (owner_id, currency, balance_minor) VALUES ('alice', 'ARS', 1)`, "23514"},
			{"negative customer balance", `UPDATE accounts SET balance_minor = -1 WHERE owner_id = 'bob'`, "23514"},
			{"immutable account kind", `UPDATE accounts SET kind = 'test_treasury' WHERE owner_id = 'bob'`, "23514"},
			{"immutable ownership", `UPDATE accounts SET owner_id = 'bob' WHERE owner_id = 'carol'`, "23514"},
			{"one treasury per currency", `INSERT INTO accounts (owner_id, kind, currency) VALUES ('test-system', 'test_treasury', 'ARS')`, "23505"},
		} {
			t.Run(tc.name, func(t *testing.T) {
				_, err := conn.Exec(ctx, tc.sql)
				requireSQLState(t, err, tc.code)
			})
		}
	})

	t.Run("balanced issuance commits and reconciles", func(t *testing.T) {
		tx := begin(t, ctx, conn)
		mustExec(t, ctx, tx, `INSERT INTO journal_entries (id, kind, currency) VALUES ($1, 'test_issuance', 'ARS')`, issuanceID)
		mustExec(t, ctx, tx, `INSERT INTO postings (journal_entry_id, account_id, currency, amount_minor)
			VALUES ($1, $2, 'ARS', -100), ($1, $3, 'ARS', 100)`, issuanceID, treasuryID, sourceID)
		mustExec(t, ctx, tx, `UPDATE accounts SET balance_minor = CASE WHEN id = $1 THEN -100 ELSE 100 END
			WHERE id IN ($1, $2)`, treasuryID, sourceID)
		if err := tx.Commit(ctx); err != nil {
			t.Fatal(err)
		}
		assertReconciled(t, ctx, conn)
	})

	t.Run("invalid journals fail at commit", func(t *testing.T) {
		for _, tc := range []struct {
			name     string
			accounts []string
			amounts  []int64
		}{
			{"empty", nil, nil},
			{"single posting", []string{treasuryID}, []int64{-10}},
			{"unbalanced", []string{treasuryID, sourceID}, []int64{-10, 9}},
			{"issuance without treasury", []string{sourceID, targetID}, []int64{-10, 10}},
			{"reversed issuance", []string{treasuryID, sourceID}, []int64{10, -10}},
		} {
			t.Run(tc.name, func(t *testing.T) {
				tx := begin(t, ctx, conn)
				mustExec(t, ctx, tx, `INSERT INTO journal_entries (id, kind, currency) VALUES ($1, 'test_issuance', 'ARS')`, journalID)
				for i, account := range tc.accounts {
					mustExec(t, ctx, tx, `INSERT INTO postings (journal_entry_id, account_id, currency, amount_minor) VALUES ($1, $2, 'ARS', $3)`, journalID, account, tc.amounts[i])
				}
				requireSQLState(t, tx.Commit(ctx), "23514")
				assertCount(t, ctx, conn, "journal_entries", 1)
			})
		}
	})

	t.Run("cross currency posting rejected", func(t *testing.T) {
		tx := begin(t, ctx, conn)
		mustExec(t, ctx, tx, `INSERT INTO journal_entries (id, kind, currency) VALUES ($1, 'test_issuance', 'ARS')`, journalID)
		_, err := tx.Exec(ctx, `INSERT INTO postings (journal_entry_id, account_id, currency, amount_minor) VALUES ($1, $2, 'ARS', 10)`, journalID, usdID)
		requireSQLState(t, err, "23503")
	})

	t.Run("third posting rejected even if total would balance", func(t *testing.T) {
		tx := begin(t, ctx, conn)
		mustExec(t, ctx, tx, `INSERT INTO journal_entries (id, kind, currency) VALUES ($1, 'test_issuance', 'ARS')`, journalID)
		_, err := tx.Exec(ctx, `INSERT INTO postings (journal_entry_id, account_id, currency, amount_minor)
			VALUES ($1, $2, 'ARS', -10), ($1, $3, 'ARS', 5), ($1, $4, 'ARS', 5)`, journalID, treasuryID, sourceID, targetID)
		requireSQLState(t, err, "23505")
	})

	t.Run("transfer constraints", func(t *testing.T) {
		for _, tc := range []struct {
			name   string
			target string
			amount int64
			code   string
		}{
			{"zero amount", targetID, 0, "23514"},
			{"negative amount", targetID, -1, "23514"},
			{"same account", sourceID, 1, "23514"},
			{"currency mismatch", usdID, 1, "23503"},
		} {
			t.Run(tc.name, func(t *testing.T) {
				tx := begin(t, ctx, conn)
				mustExec(t, ctx, tx, `INSERT INTO journal_entries (id, kind, currency) VALUES ($1, 'transfer', 'ARS')`, journalID)
				_, err := tx.Exec(ctx, `INSERT INTO transfers (actor_id, source_account_id, destination_account_id, currency, amount_minor, journal_entry_id)
					VALUES ('alice', $1, $2, 'ARS', $3, $4)`, sourceID, tc.target, tc.amount, journalID)
				requireSQLState(t, err, tc.code)
			})
		}
	})

	t.Run("transfer must match its journal", func(t *testing.T) {
		tx := begin(t, ctx, conn)
		writeTransfer(t, ctx, tx)
		// The transfer describes 25 minor units, while its journal contains 30.
		mustExec(t, ctx, tx, `INSERT INTO postings (journal_entry_id, account_id, currency, amount_minor)
			VALUES ($1, $2, 'ARS', -30), ($1, $3, 'ARS', 30)`, journalID, sourceID, targetID)
		requireSQLState(t, tx.Commit(ctx), "23514")
	})

	t.Run("all financial records roll back together", func(t *testing.T) {
		tx := begin(t, ctx, conn)
		writeCompleteMovement(t, ctx, tx)
		if err := tx.Rollback(ctx); err != nil {
			t.Fatal(err)
		}
		for _, table := range []string{"transfers", "idempotency_records", "outbox_events"} {
			assertCount(t, ctx, conn, table, 0)
		}
		assertCount(t, ctx, conn, "journal_entries", 1)
		assertCount(t, ctx, conn, "postings", 2)
		assertReconciled(t, ctx, conn)
	})

	t.Run("complete transfer commits", func(t *testing.T) {
		tx := begin(t, ctx, conn)
		writeCompleteMovement(t, ctx, tx)
		if err := tx.Commit(ctx); err != nil {
			t.Fatal(err)
		}
		for _, table := range []string{"transfers", "idempotency_records", "outbox_events"} {
			assertCount(t, ctx, conn, table, 1)
		}
		assertReconciled(t, ctx, conn)
	})

	t.Run("financial history is immutable", func(t *testing.T) {
		for _, statement := range []string{
			`UPDATE postings SET amount_minor = amount_minor + 1`, `DELETE FROM postings`,
			`UPDATE journal_entries SET kind = 'test_issuance'`, `DELETE FROM journal_entries`,
			`UPDATE transfers SET amount_minor = 26`, `DELETE FROM transfers`,
			`TRUNCATE postings`, `TRUNCATE journal_entries CASCADE`, `TRUNCATE transfers`,
		} {
			t.Run(statement, func(t *testing.T) {
				_, err := conn.Exec(ctx, statement)
				requireSQLState(t, err, "23514")
			})
		}
		_, err := conn.Exec(ctx, `INSERT INTO postings (journal_entry_id, account_id, currency, amount_minor) VALUES ($1, $2, 'ARS', 1)`, journalID, thirdID)
		requireSQLState(t, err, "23505")
	})

	t.Run("idempotency and inbox uniqueness", func(t *testing.T) {
		_, err := conn.Exec(ctx, `INSERT INTO idempotency_records (actor_id, operation, idempotency_key, request_hash)
			VALUES ('alice', 'create_transfer', 'order-1', decode(repeat('00', 32), 'hex'))`)
		requireSQLState(t, err, "23505")
		mustExec(t, ctx, conn, `INSERT INTO idempotency_records (actor_id, operation, idempotency_key, request_hash)
			VALUES ('bob', 'create_transfer', 'order-1', decode(repeat('00', 32), 'hex')),
			('alice', 'test_issuance', 'order-1', decode(repeat('00', 32), 'hex'))`)
		mustExec(t, ctx, conn, `INSERT INTO processed_events (consumer_name, event_id) VALUES ('notification', $1)`, eventID)
		_, err = conn.Exec(ctx, `INSERT INTO processed_events (consumer_name, event_id) VALUES ('notification', $1)`, eventID)
		requireSQLState(t, err, "23505")
		mustExec(t, ctx, conn, `INSERT INTO processed_events (consumer_name, event_id) VALUES ('audit', $1)`, eventID)
	})
}

type executor interface {
	Exec(context.Context, string, ...any) (pgconn.CommandTag, error)
}

func mustExec(t *testing.T, ctx context.Context, conn executor, sql string, args ...any) {
	t.Helper()
	if _, err := conn.Exec(ctx, sql, args...); err != nil {
		t.Fatal(err)
	}
}

func begin(t *testing.T, ctx context.Context, conn *pgx.Conn) pgx.Tx {
	t.Helper()
	tx, err := conn.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		cleanup, stop := context.WithTimeout(context.Background(), 5*time.Second)
		defer stop()
		if err := tx.Rollback(cleanup); err != nil && !errors.Is(err, pgx.ErrTxClosed) {
			t.Errorf("rollback test transaction: %v", err)
		}
	})
	return tx
}

func requireSQLState(t *testing.T, err error, code string) {
	t.Helper()
	var pgErr *pgconn.PgError
	if !errors.As(err, &pgErr) || pgErr.Code != code {
		t.Fatalf("want SQLSTATE %s, got %v", code, err)
	}
}

func assertCount(t *testing.T, ctx context.Context, conn *pgx.Conn, table string, want int) {
	t.Helper()
	var count int
	if err := conn.QueryRow(ctx, "SELECT count(*) FROM "+pgx.Identifier{table}.Sanitize()).Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != want {
		t.Fatalf("%s count = %d, want %d", table, count, want)
	}
}

func assertReconciled(t *testing.T, ctx context.Context, conn *pgx.Conn) {
	t.Helper()
	var inconsistent int
	if err := conn.QueryRow(ctx, `SELECT count(*) FROM account_balance_reconciliation WHERE difference_minor <> 0`).Scan(&inconsistent); err != nil {
		t.Fatal(err)
	}
	if inconsistent != 0 {
		t.Fatalf("%d accounts do not reconcile with ledger", inconsistent)
	}
}

func writeTransfer(t *testing.T, ctx context.Context, tx pgx.Tx) {
	t.Helper()
	mustExec(t, ctx, tx, `INSERT INTO journal_entries (id, kind, currency) VALUES ($1, 'transfer', 'ARS')`, journalID)
	mustExec(t, ctx, tx, `INSERT INTO transfers (id, actor_id, source_account_id, destination_account_id, currency, amount_minor, journal_entry_id)
		VALUES ($1, 'alice', $2, $3, 'ARS', 25, $4)`, transferID, sourceID, targetID, journalID)
}

func writeCompleteMovement(t *testing.T, ctx context.Context, tx pgx.Tx) {
	t.Helper()
	writeTransfer(t, ctx, tx)
	mustExec(t, ctx, tx, `INSERT INTO postings (journal_entry_id, account_id, currency, amount_minor)
		VALUES ($1, $2, 'ARS', -25), ($1, $3, 'ARS', 25)`, journalID, sourceID, targetID)
	mustExec(t, ctx, tx, `UPDATE accounts SET balance_minor = balance_minor + CASE WHEN id = $1 THEN -25 ELSE 25 END
		WHERE id IN ($1, $2)`, sourceID, targetID)
	mustExec(t, ctx, tx, `INSERT INTO idempotency_records (actor_id, operation, idempotency_key, request_hash, state, result, completed_at)
		VALUES ('alice', 'create_transfer', 'order-1', decode(repeat('00', 32), 'hex'), 'completed', jsonb_build_object('transfer_id', $1::text), CURRENT_TIMESTAMP)`, transferID)
	mustExec(t, ctx, tx, `INSERT INTO outbox_events (event_id, event_type, event_version, aggregate_id, payload)
		VALUES ($1, 'transfer.completed', 1, $2::uuid, jsonb_build_object('transfer_id', $2::uuid::text, 'amount_minor', 25, 'currency', 'ARS'))`, eventID, transferID)
}
