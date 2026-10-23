//go:build integration

package integration_test

import (
	"context"
	"errors"
	"fmt"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
	"os"
	"sync"
	"testing"
	"time"
	"transfer-engine/internal/business/transfers"
	postgresadapter "transfer-engine/internal/infrastructure/postgres"
)

func TestAtomicTransfers(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	pool := transferDatabase(t, ctx)
	service, err := transfers.New(postgresadapter.NewTransferStore(pool), "test", true)
	if err != nil {
		t.Fatal(err)
	}
	testAtomicTransfers(t, ctx, pool, service)
}

func transferDatabase(t *testing.T, ctx context.Context) *pgxpool.Pool {
	t.Helper()
	ctr, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{ContainerRequest: testcontainers.ContainerRequest{Image: "postgres:17.11-alpine3.24", ExposedPorts: []string{"5432/tcp"}, Env: map[string]string{"POSTGRES_USER": "test", "POSTGRES_PASSWORD": "test", "POSTGRES_DB": "transfers"}, WaitingFor: wait.ForAll(wait.ForLog("database system is ready to accept connections").WithOccurrence(2), wait.ForListeningPort("5432/tcp"))}, Started: true})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		cleanup, stop := context.WithTimeout(context.Background(), 20*time.Second)
		defer stop()
		if err := ctr.Terminate(cleanup); err != nil {
			t.Error(err)
		}
	})
	host, err := ctr.Host(ctx)
	if err != nil {
		t.Fatal(err)
	}
	port, err := ctr.MappedPort(ctx, "5432/tcp")
	if err != nil {
		t.Fatal(err)
	}
	pool, err := pgxpool.New(ctx, fmt.Sprintf("postgres://test:test@%s:%s/transfers?sslmode=disable", host, port.Port()))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	sql, err := os.ReadFile("../../migrations/000001_initial.up.sql")
	if err != nil {
		t.Fatal(err)
	}
	if _, err = pool.Exec(ctx, string(sql)); err != nil {
		t.Fatal(err)
	}
	return pool
}

func testAtomicTransfers(t *testing.T, ctx context.Context, pool *pgxpool.Pool, service *transfers.Service) {
	a, err := service.CreateAccount(ctx, "alice", "ARS")
	if err != nil {
		t.Fatal(err)
	}
	b, err := service.CreateAccount(ctx, "bob", "ARS")
	if err != nil {
		t.Fatal(err)
	}
	if a.Balance != 0 {
		t.Fatal("nonzero opening balance")
	}
	if _, err = service.Account(ctx, "bob", a.ID); !errors.Is(err, transfers.ErrNotFound) {
		t.Fatalf("ownership: %v", err)
	}
	_, err = service.Fund(ctx, transfers.Command{ActorID: "alice", DestinationID: a.ID, Currency: "ARS", Amount: 50, Key: "fund"})
	if err != nil {
		t.Fatal(err)
	}
	command := func(key string) transfers.Command {
		return transfers.Command{ActorID: "alice", SourceID: a.ID, DestinationID: b.ID, Currency: "ARS", Amount: 1, Key: key}
	}
	t.Run("100 concurrent spends conserve money", func(t *testing.T) {
		start := make(chan struct{})
		results := make(chan error, 100)
		var wg sync.WaitGroup
		for i := 0; i < 100; i++ {
			wg.Add(1)
			go func(i int) {
				defer wg.Done()
				<-start
				_, err := service.Transfer(ctx, command(fmt.Sprintf("spend-%d", i)))
				results <- err
			}(i)
		}
		close(start)
		wg.Wait()
		close(results)
		success, rejected := 0, 0
		for err := range results {
			if err == nil {
				success++
			} else if errors.Is(err, transfers.ErrInsufficientFunds) {
				rejected++
			} else {
				t.Errorf("unexpected: %v", err)
			}
		}
		if success != 50 || rejected != 50 {
			t.Fatalf("success=%d rejected=%d", success, rejected)
		}
		checkBalances(t, ctx, pool, a.ID, b.ID, 0, 50)
	})
	t.Run("opposing transfers finish without deadlock", func(t *testing.T) {
		_, err := service.Fund(ctx, transfers.Command{ActorID: "alice", DestinationID: a.ID, Currency: "ARS", Amount: 50, Key: "fund-again"})
		if err != nil {
			t.Fatal(err)
		}
		start := make(chan struct{})
		results := make(chan error, 100)
		for i := 0; i < 100; i++ {
			go func(i int) {
				<-start
				c := command(fmt.Sprintf("opposing-%d", i))
				if i%2 == 1 {
					c.ActorID = "bob"
					c.SourceID = b.ID
					c.DestinationID = a.ID
				}
				_, err := service.Transfer(ctx, c)
				results <- err
			}(i)
		}
		close(start)
		for i := 0; i < 100; i++ {
			if err := <-results; err != nil {
				t.Error(err)
			}
		}
		checkBalances(t, ctx, pool, a.ID, b.ID, 50, 50)
	})
	t.Run("failure after balance changes rolls back everything", func(t *testing.T) {
		var before, after int
		countRecords := `SELECT (SELECT count(*) FROM transfers)+(SELECT count(*) FROM journal_entries)+(SELECT count(*) FROM postings)+(SELECT count(*) FROM outbox_events)`
		if err := pool.QueryRow(ctx, countRecords).Scan(&before); err != nil {
			t.Fatal(err)
		}
		_, err := pool.Exec(ctx, `CREATE FUNCTION fail_outbox() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'injected outbox failure'; END $$; CREATE TRIGGER fail_outbox BEFORE INSERT ON outbox_events FOR EACH ROW EXECUTE FUNCTION fail_outbox()`)
		if err != nil {
			t.Fatal(err)
		}
		c := command("rollback")
		_, err = service.Transfer(ctx, c)
		if !errors.Is(err, transfers.ErrUnavailable) {
			t.Fatalf("got %v", err)
		}
		checkBalances(t, ctx, pool, a.ID, b.ID, 50, 50)
		if err := pool.QueryRow(ctx, countRecords).Scan(&after); err != nil || before != after {
			t.Fatalf("partial records survived rollback: before=%d after=%d err=%v", before, after, err)
		}
		var count int
		if err = pool.QueryRow(ctx, `SELECT count(*) FROM idempotency_records WHERE idempotency_key='rollback'`).Scan(&count); err != nil || count != 0 {
			t.Fatalf("claim survived: %d %v", count, err)
		}
		if _, err = pool.Exec(ctx, `DROP TRIGGER fail_outbox ON outbox_events; DROP FUNCTION fail_outbox()`); err != nil {
			t.Fatal(err)
		}
		first, err := service.Transfer(ctx, c)
		if err != nil {
			t.Fatal(err)
		}
		replay, err := service.Transfer(ctx, c)
		if err != nil || !replay.Replayed || replay.ID != first.ID {
			t.Fatalf("replay: %+v %v", replay, err)
		}
		c.Amount = 2
		if _, err = service.Transfer(ctx, c); !errors.Is(err, transfers.ErrMismatch) {
			t.Fatalf("mismatch: %v", err)
		}
		checkBalances(t, ctx, pool, a.ID, b.ID, 49, 51)
	})
	var count int
	if err = pool.QueryRow(ctx, `SELECT count(*) FROM transfers t LEFT JOIN outbox_events o ON o.aggregate_id=t.id WHERE o.event_id IS NULL`).Scan(&count); err != nil || count != 0 {
		t.Fatalf("missing outbox: %d %v", count, err)
	}
}
func checkBalances(t *testing.T, ctx context.Context, pool *pgxpool.Pool, a, b string, wantA, wantB int64) {
	t.Helper()
	var gotA, gotB int64
	var differences int
	if err := pool.QueryRow(ctx, `SELECT balance_minor FROM accounts WHERE id=$1`, a).Scan(&gotA); err != nil {
		t.Fatal(err)
	}
	if err := pool.QueryRow(ctx, `SELECT balance_minor FROM accounts WHERE id=$1`, b).Scan(&gotB); err != nil {
		t.Fatal(err)
	}
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM account_balance_reconciliation WHERE difference_minor<>0`).Scan(&differences); err != nil {
		t.Fatal(err)
	}
	if gotA != wantA || gotB != wantB || differences != 0 {
		t.Fatalf("balances %d/%d want %d/%d, unreconciled=%d", gotA, gotB, wantA, wantB, differences)
	}
}
