//go:build integration

package integration_test

import (
	"context"
	"errors"
	redisclient "github.com/redis/go-redis/v9"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
	"net"
	"os"
	"testing"
	"time"
	"transfer-engine/internal/business/reconciliation"
	"transfer-engine/internal/business/transfers"
	postgresadapter "transfer-engine/internal/infrastructure/postgres"
	redisadapter "transfer-engine/internal/infrastructure/redis"
)

func TestReconciliationCoordination(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	pool := transferDatabase(t, ctx)
	for _, suffix := range []string{"up", "down", "up"} {
		sql, err := os.ReadFile("../../migrations/000002_reconciliation." + suffix + ".sql")
		if err != nil {
			t.Fatal(err)
		}
		if _, err = pool.Exec(ctx, string(sql)); err != nil {
			t.Fatal(err)
		}
	}
	ctr, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{ContainerRequest: testcontainers.ContainerRequest{Image: "redis:8.10.2-alpine3.23", ExposedPorts: []string{"6379/tcp"}, WaitingFor: wait.ForListeningPort("6379/tcp")}, Started: true})
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
	port, err := ctr.MappedPort(ctx, "6379/tcp")
	if err != nil {
		t.Fatal(err)
	}
	address := net.JoinHostPort(host, port.Port())
	lease := redisadapter.New(address)
	defer lease.Close()
	raw := redisclient.NewClient(&redisclient.Options{Addr: address})
	defer raw.Close()
	store := postgresadapter.NewReconciliationStore(pool)
	service, _ := reconciliation.New(store, redisadapter.NewProtectedLease(lease), 5*time.Second)
	t.Run("optimistic settings reject concurrent stale updates", func(t *testing.T) {
		settings, err := service.Settings(ctx)
		if err != nil {
			t.Fatal(err)
		}
		settings.Enabled = true
		settings.IntervalSeconds = 10
		start := make(chan struct{})
		results := make(chan error, 2)
		for i := 0; i < 2; i++ {
			go func() { <-start; _, err := service.UpdateSettings(ctx, settings); results <- err }()
		}
		close(start)
		success, conflict := 0, 0
		for i := 0; i < 2; i++ {
			err := <-results
			if err == nil {
				success++
			} else if errors.Is(err, reconciliation.ErrConflict) {
				conflict++
			} else {
				t.Error(err)
			}
		}
		if success != 1 || conflict != 1 {
			t.Fatalf("success=%d conflict=%d", success, conflict)
		}
	})
	t.Run("expired Redis owner cannot release successor", func(t *testing.T) {
		old, err := lease.Acquire(ctx, time.Minute)
		if err != nil {
			t.Fatal(err)
		}
		if _, err = lease.Acquire(ctx, time.Minute); !errors.Is(err, reconciliation.ErrBusy) {
			t.Fatal(err)
		}
		// Force server-side expiry instead of timing an assertion with a sleep.
		if err = raw.PExpireAt(ctx, "transfer-engine:reconciliation:lease", time.Unix(1, 0)).Err(); err != nil {
			t.Fatal(err)
		}
		next, err := lease.Acquire(ctx, time.Minute)
		if err != nil {
			t.Fatal(err)
		}
		if err = lease.Release(ctx, old); err != nil {
			t.Fatal(err)
		}
		if _, err = lease.Acquire(ctx, time.Minute); !errors.Is(err, reconciliation.ErrBusy) {
			t.Fatal("stale release deleted successor")
		}
		if err = lease.Release(ctx, next); err != nil {
			t.Fatal(err)
		}
	})
	t.Run("expired and superseded database fences reject writes", func(t *testing.T) {
		old, err := store.Begin(ctx, time.Minute)
		if err != nil {
			t.Fatal(err)
		}
		if _, err = store.Begin(ctx, time.Minute); !errors.Is(err, reconciliation.ErrBusy) {
			t.Fatal(err)
		}
		if _, err = pool.Exec(ctx, `UPDATE reconciliation_state SET lease_until=clock_timestamp()-interval '1 second'`); err != nil {
			t.Fatal(err)
		}
		if err = store.Save(ctx, old, reconciliation.Report{Accounts: 999}); !errors.Is(err, reconciliation.ErrStale) {
			t.Fatal(err)
		}
		next, err := store.Begin(ctx, time.Minute)
		if err != nil || next <= old {
			t.Fatalf("fence %d -> %d, %v", old, next, err)
		}
		if err = store.Save(ctx, next, reconciliation.Report{Accounts: 2}); err != nil {
			t.Fatal(err)
		}
		if err = store.Save(ctx, old, reconciliation.Report{Accounts: 999}); !errors.Is(err, reconciliation.ErrStale) {
			t.Fatal(err)
		}
		var accounts int64
		if err = pool.QueryRow(ctx, `SELECT accounts FROM reconciliation_state`).Scan(&accounts); err != nil || accounts != 2 {
			t.Fatalf("stale write persisted: %d %v", accounts, err)
		}
	})
	money, _ := transfers.New(postgresadapter.NewTransferStore(pool), "test", true)
	a, err := money.CreateAccount(ctx, "alice", "ARS")
	if err != nil {
		t.Fatal(err)
	}
	b, err := money.CreateAccount(ctx, "bob", "ARS")
	if err != nil {
		t.Fatal(err)
	}
	_, err = money.Fund(ctx, transfers.Command{ActorID: "alice", DestinationID: a.ID, Currency: "ARS", Amount: 10, Key: "fund"})
	if err != nil {
		t.Fatal(err)
	}
	if err = service.RunOnce(ctx); err != nil {
		t.Fatal(err)
	}
	var mismatches int
	if err = pool.QueryRow(ctx, `SELECT mismatches FROM reconciliation_state`).Scan(&mismatches); err != nil || mismatches != 0 {
		t.Fatalf("report=%d %v", mismatches, err)
	}
	t.Run("Redis outage does not block transfers", func(t *testing.T) {
		if err := ctr.Stop(ctx, nil); err != nil {
			t.Fatal(err)
		}
		for i := 0; i < 4; i++ {
			if err := service.RunOnce(ctx); !errors.Is(err, reconciliation.ErrUnavailable) {
				t.Fatalf("Redis outage attempt %d=%v", i, err)
			}
		}
		_, err := money.Transfer(ctx, transfers.Command{ActorID: "alice", SourceID: a.ID, DestinationID: b.ID, Currency: "ARS", Amount: 1, Key: "redis-down"})
		if err != nil {
			t.Fatal(err)
		}
		checkBalances(t, ctx, pool, a.ID, b.ID, 9, 1)
	})
}
