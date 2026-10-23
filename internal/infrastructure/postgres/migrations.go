package postgres

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"github.com/jackc/pgx/v5/pgxpool"
	"io/fs"
	"sort"
	"strconv"
	"strings"
	"time"
)

// Migrate applies ordered scripts and records their checksums in the same
// transaction. Baseline explicitly adopts scripts already applied by initdb;
// it must only be used after an operator has verified that existing schema.
func Migrate(ctx context.Context, pool *pgxpool.Pool, files fs.FS, baseline int) error {
	ctx, cancel := context.WithTimeout(ctx, time.Minute)
	defer cancel()
	names, err := fs.Glob(files, "*.up.sql")
	if err != nil || len(names) == 0 {
		return errors.New("no migrations found")
	}
	sort.Strings(names)
	if baseline < 0 || baseline > len(names) {
		return errors.New("invalid migration baseline")
	}
	conn, err := pool.Acquire(ctx)
	if err != nil {
		return errors.New("migration connection unavailable")
	}
	defer conn.Release()
	if _, err = conn.Exec(ctx, `SELECT pg_advisory_lock(74633201)`); err != nil {
		return errors.New("migration lock unavailable")
	}
	defer func() {
		cleanup, stop := context.WithTimeout(context.Background(), time.Second)
		defer stop()
		if _, err := conn.Exec(cleanup, `SELECT pg_advisory_unlock(74633201)`); err != nil {
			_ = conn.Conn().Close(cleanup)
		}
	}()
	if _, err = conn.Exec(ctx, `CREATE TABLE IF NOT EXISTS schema_migrations(version integer PRIMARY KEY, checksum text NOT NULL, applied_at timestamptz NOT NULL DEFAULT CURRENT_TIMESTAMP)`); err != nil {
		return errors.New("migration metadata unavailable")
	}
	var applied int
	if err = conn.QueryRow(ctx, `SELECT count(*) FROM schema_migrations`).Scan(&applied); err != nil {
		return errors.New("migration metadata unavailable")
	}
	if baseline > 0 && applied > 0 {
		return errors.New("baseline requires empty migration history")
	}
	if applied > len(names) {
		return errors.New("database schema is newer than this binary")
	}
	for i, name := range names {
		version, err := strconv.Atoi(strings.SplitN(name, "_", 2)[0])
		if err != nil || version != i+1 {
			return errors.New("migration versions must be contiguous")
		}
		data, err := fs.ReadFile(files, name)
		if err != nil {
			return fmt.Errorf("read migration %d", version)
		}
		hash := sha256.Sum256(data)
		checksum := hex.EncodeToString(hash[:])
		if version <= applied {
			var stored string
			if err = conn.QueryRow(ctx, `SELECT checksum FROM schema_migrations WHERE version=$1`, version).Scan(&stored); err != nil || stored != checksum {
				return fmt.Errorf("migration %d checksum mismatch", version)
			}
			continue
		}
		tx, err := conn.Begin(ctx)
		if err != nil {
			return errors.New("begin migration failed")
		}
		body := strings.TrimSpace(string(data))
		// Project SQL scripts own their transaction when invoked with psql. Strip
		// only that outer wrapper here so schema + history commit atomically.
		begin := strings.Index(body, "BEGIN;")
		if begin < 0 || !strings.HasSuffix(body, "COMMIT;") {
			_ = tx.Rollback(ctx)
			return fmt.Errorf("migration %d lacks transaction wrapper", version)
		}
		body = body[:begin] + body[begin+len("BEGIN;"):len(body)-len("COMMIT;")]
		if version > baseline {
			_, err = tx.Exec(ctx, body)
		}
		if err == nil {
			_, err = tx.Exec(ctx, `INSERT INTO schema_migrations(version,checksum) VALUES($1,$2)`, version, checksum)
		}
		if err == nil {
			err = tx.Commit(ctx)
		}
		if err != nil {
			cleanup, stop := context.WithTimeout(context.Background(), time.Second)
			_ = tx.Rollback(cleanup)
			stop()
			return fmt.Errorf("migration %d failed", version)
		}
	}
	return nil
}
