package postgres

import (
	"context"
	"strings"
	"testing"
	"time"
)

func TestInvalidSettingsDoNotLeakCredentials(t *testing.T) {
	pool, err := Open(context.Background(), Config{
		URL: "postgres://user:secret%zz@localhost/db", MaxConns: 1, ConnectTimeout: time.Second,
	})
	if err == nil || pool != nil {
		t.Fatal("expected invalid URL to be rejected")
	}
	if strings.Contains(err.Error(), "secret") || strings.Contains(err.Error(), "user") {
		t.Fatal("connection error leaked credentials")
	}
}

func TestCanceledStartupDoesNotReturnPool(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	pool, err := Open(ctx, Config{
		URL: "postgres://user:secret@127.0.0.1:1/db?sslmode=disable", MaxConns: 1, ConnectTimeout: time.Second,
	})
	if err == nil || pool != nil {
		t.Fatal("canceled startup must not return a usable pool")
	}
	if strings.Contains(err.Error(), "secret") {
		t.Fatal("connection error leaked credentials")
	}
}
