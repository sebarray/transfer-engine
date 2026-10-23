//go:build integration

package integration_test

import (
	"context"
	"os"
	"testing"
	"testing/fstest"
	"time"
	postgresadapter "transfer-engine/internal/infrastructure/postgres"
)

func TestVersionedMigrations(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	pool := transferDatabase(t, ctx)
	// This fixture has the legacy initdb schema. Adopt only verified version 1.
	if err := postgresadapter.Migrate(ctx, pool, os.DirFS("../../migrations"), 1); err != nil {
		t.Fatal(err)
	}
	if err := postgresadapter.Migrate(ctx, pool, os.DirFS("../../migrations"), 0); err != nil {
		t.Fatal(err)
	}
	var count int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM schema_migrations`).Scan(&count); err != nil || count != 3 {
		t.Fatalf("history %d %v", count, err)
	}
	first, err := os.ReadFile("../../migrations/000001_initial.up.sql")
	if err != nil {
		t.Fatal(err)
	}
	second, err := os.ReadFile("../../migrations/000002_reconciliation.up.sql")
	if err != nil {
		t.Fatal(err)
	}
	third, err := os.ReadFile("../../migrations/000003_messaging.up.sql")
	if err != nil {
		t.Fatal(err)
	}
	changed := fstest.MapFS{"000001_initial.up.sql": &fstest.MapFile{Data: append(first, []byte("\n-- drift")...)}, "000002_reconciliation.up.sql": &fstest.MapFile{Data: second}, "000003_messaging.up.sql": &fstest.MapFile{Data: third}}
	if err := postgresadapter.Migrate(ctx, pool, changed, 0); err == nil {
		t.Fatal("changed migration accepted")
	}
	failed := fstest.MapFS{"000001_initial.up.sql": &fstest.MapFile{Data: first}, "000002_reconciliation.up.sql": &fstest.MapFile{Data: second}, "000003_messaging.up.sql": &fstest.MapFile{Data: third}, "000004_failure.up.sql": &fstest.MapFile{Data: []byte("BEGIN; CREATE TABLE must_rollback(id int); SELECT 1/0; COMMIT;")}}
	if err := postgresadapter.Migrate(ctx, pool, failed, 0); err == nil {
		t.Fatal("failed migration accepted")
	}
	var absent bool
	if err := pool.QueryRow(ctx, `SELECT to_regclass('must_rollback') IS NULL`).Scan(&absent); err != nil || !absent {
		t.Fatalf("DDL survived rollback %v", err)
	}
}
