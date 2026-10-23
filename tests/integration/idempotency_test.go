//go:build integration

package integration_test

import (
	"context"
	"errors"
	"github.com/jackc/pgx/v5/pgxpool"
	"strings"
	"testing"
	"time"
	"transfer-engine/internal/business/transfers"
	postgresadapter "transfer-engine/internal/infrastructure/postgres"
)

// uncertaintyStore injects loss of the commit acknowledgement while executing
// the real PostgreSQL transaction. Resolution still uses a real independent query.
type uncertaintyStore struct {
	transfers.Store
	committed bool
	cancel    context.CancelFunc
}

func (s *uncertaintyStore) WithinTransaction(ctx context.Context, fn func(transfers.Tx) error) error {
	err := s.Store.WithinTransaction(ctx, func(tx transfers.Tx) error {
		if err := fn(tx); err != nil {
			return err
		}
		if !s.committed {
			return errors.New("injected rollback")
		}
		return nil
	})
	if s.committed && err != nil {
		return err
	}
	if s.cancel != nil {
		s.cancel()
	}
	return transfers.ErrCommitUnknown
}
func TestDistributedIdempotency(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	pool := transferDatabase(t, ctx)
	other, err := pgxpool.NewWithConfig(ctx, pool.Config())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(other.Close)
	firstStore := postgresadapter.NewTransferStore(pool)
	first, _ := transfers.New(firstStore, "test", true)
	second, _ := transfers.New(postgresadapter.NewTransferStore(other), "test", true)
	a, err := first.CreateAccount(ctx, "alice", "ARS")
	if err != nil {
		t.Fatal(err)
	}
	b, err := first.CreateAccount(ctx, "bob", "ARS")
	if err != nil {
		t.Fatal(err)
	}
	_, err = first.Fund(ctx, transfers.Command{ActorID: "alice", DestinationID: a.ID, Amount: 100, Currency: "ARS", Key: "fund"})
	if err != nil {
		t.Fatal(err)
	}
	c := transfers.Command{ActorID: "alice", SourceID: a.ID, DestinationID: b.ID, Amount: 1, Currency: "ARS", Key: "same-key"}
	t.Run("100 identical requests through independent pools", func(t *testing.T) {
		type outcome struct {
			result transfers.Result
			err    error
		}
		outcomes := make(chan outcome, 100)
		start := make(chan struct{})
		for i := 0; i < 100; i++ {
			go func(i int) {
				<-start
				service := first
				if i%2 != 0 {
					service = second
				}
				command := c
				if i%3 == 0 {
					command.SourceID = strings.ToUpper(command.SourceID)
				}
				r, err := service.Transfer(ctx, command)
				outcomes <- outcome{r, err}
			}(i)
		}
		close(start)
		id := ""
		fresh := 0
		for i := 0; i < 100; i++ {
			o := <-outcomes
			if o.err != nil {
				t.Errorf("request: %v", o.err)
				continue
			}
			if id == "" {
				id = o.result.ID
			}
			if o.result.ID != id {
				t.Error("different logical result")
			}
			if !o.result.Replayed {
				fresh++
			}
		}
		if fresh != 1 {
			t.Fatalf("fresh transfers=%d", fresh)
		}
		checkBalances(t, ctx, pool, a.ID, b.ID, 99, 1)
		var n int
		for _, table := range []string{"transfers", "outbox_events"} {
			if err := pool.QueryRow(ctx, "SELECT count(*) FROM "+table).Scan(&n); err != nil || n != 1 {
				t.Fatalf("%s count=%d err=%v", table, n, err)
			}
		}
	})
	t.Run("lost response replay survives connection restart", func(t *testing.T) {
		other.Close()
		replacement, err := pgxpool.NewWithConfig(ctx, pool.Config())
		if err != nil {
			t.Fatal(err)
		}
		defer replacement.Close()
		restarted, _ := transfers.New(postgresadapter.NewTransferStore(replacement), "test", true)
		result, err := restarted.Transfer(ctx, c)
		if err != nil || !result.Replayed {
			t.Fatalf("replay=%+v error=%v", result, err)
		}
		checkBalances(t, ctx, pool, a.ID, b.ID, 99, 1)
	})
	t.Run("committed but acknowledgment lost and caller canceled", func(t *testing.T) {
		requestCtx, cancelRequest := context.WithCancel(ctx)
		defer cancelRequest()
		uncertain, _ := transfers.New(&uncertaintyStore{Store: firstStore, committed: true, cancel: cancelRequest}, "test", true)
		command := c
		command.Key = "commit-lost"
		result, err := uncertain.Transfer(requestCtx, command)
		if err != nil || !result.Replayed {
			t.Fatalf("resolution=%+v err=%v", result, err)
		}
		replay, err := first.Transfer(ctx, command)
		if err != nil || replay.ID != result.ID {
			t.Fatalf("retry=%+v err=%v", replay, err)
		}
		checkBalances(t, ctx, pool, a.ID, b.ID, 98, 2)
	})
	t.Run("absent result stays unknown and safe retry succeeds", func(t *testing.T) {
		uncertain, _ := transfers.New(&uncertaintyStore{Store: firstStore}, "test", true)
		command := c
		command.Key = "rollback-lost"
		if _, err := uncertain.Transfer(ctx, command); !errors.Is(err, transfers.ErrCommitUnknown) {
			t.Fatal(err)
		}
		checkBalances(t, ctx, pool, a.ID, b.ID, 98, 2)
		if _, err := first.Transfer(ctx, command); err != nil {
			t.Fatal(err)
		}
		checkBalances(t, ctx, pool, a.ID, b.ID, 97, 3)
	})
	t.Run("changed payload and actor cannot reuse result", func(t *testing.T) {
		changed := c
		changed.Amount = 2
		if _, err := first.Transfer(ctx, changed); !errors.Is(err, transfers.ErrMismatch) {
			t.Fatal(err)
		}
		changed = c
		changed.ActorID = "mallory"
		if _, err := first.Transfer(ctx, changed); !errors.Is(err, transfers.ErrNotFound) {
			t.Fatal(err)
		}
	})
	t.Run("inflight contention is bounded", func(t *testing.T) {
		tx, err := pool.Begin(ctx)
		if err != nil {
			t.Fatal(err)
		}
		defer tx.Rollback(ctx)
		_, err = tx.Exec(ctx, `INSERT INTO idempotency_records(actor_id,operation,idempotency_key,request_hash) VALUES('alice','create_transfer','pending',decode(repeat('00',32),'hex'))`)
		if err != nil {
			t.Fatal(err)
		}
		command := c
		command.Key = "pending"
		if _, err := first.Transfer(ctx, command); !errors.Is(err, transfers.ErrInFlight) {
			t.Fatalf("expected bounded lock timeout: %v", err)
		}
		if err := tx.Rollback(ctx); err != nil {
			t.Fatal(err)
		}
		if _, err := first.Transfer(ctx, command); err != nil {
			t.Fatal(err)
		}
		checkBalances(t, ctx, pool, a.ID, b.ID, 96, 4)
	})
}
