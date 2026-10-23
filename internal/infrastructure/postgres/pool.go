// Package postgres owns PostgreSQL adapters. No driver types cross into business.
package postgres

import (
	"context"
	"errors"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

type Config struct {
	URL            string
	MaxConns       int32
	MinConns       int32
	ConnectTimeout time.Duration
}

// Open verifies connectivity before returning ownership of the pool to its caller.
// Errors deliberately omit driver details, which may contain the connection URL.
func Open(ctx context.Context, settings Config) (*pgxpool.Pool, error) {
	config, err := pgxpool.ParseConfig(settings.URL)
	if err != nil {
		return nil, errors.New("invalid PostgreSQL connection settings")
	}
	config.MaxConns = settings.MaxConns
	config.MinConns = settings.MinConns
	config.ConnConfig.ConnectTimeout = settings.ConnectTimeout
	config.ConnConfig.RuntimeParams["application_name"] = "transfer-engine"
	startupCtx, cancel := context.WithTimeout(ctx, settings.ConnectTimeout)
	defer cancel()
	pool, err := pgxpool.NewWithConfig(startupCtx, config)
	if err != nil {
		return nil, errors.New("could not create PostgreSQL pool")
	}
	if err := pool.Ping(startupCtx); err != nil {
		pool.Close()
		return nil, errors.New("PostgreSQL unavailable during startup")
	}
	return pool, nil
}
