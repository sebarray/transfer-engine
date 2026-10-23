package config

import (
	"log/slog"
	"strings"
	"testing"
	"time"
)

func fromMap(values map[string]string) (Config, error) {
	return load(func(key string) (string, bool) { value, ok := values[key]; return value, ok })
}

func TestDefaultsDisableTestFunds(t *testing.T) {
	c, err := fromMap(map[string]string{"DATABASE_URL": "postgres://test:secret@localhost:5432/transfer_engine"})
	if err != nil {
		t.Fatal(err)
	}
	if c.TestFundsEnabled || c.Environment != "development" || c.HTTPAddr != ":8080" ||
		c.DBMaxConns != 10 || c.DBMinConns != 0 || c.HealthTimeout != 2*time.Second ||
		c.HTTPBodyLimitBytes != 1<<20 || c.LogLevel != slog.LevelInfo {
		t.Fatal("unexpected default configuration")
	}
}

func TestValidationRejectsUnsafeSettingsWithoutEchoingValues(t *testing.T) {
	for _, tc := range []struct{ name, key, value string }{
		{"missing database", "DATABASE_URL", ""},
		{"malformed secret URL", "DATABASE_URL", "postgres://user:super-secret%zz@localhost/db"},
		{"wrong scheme", "DATABASE_URL", "https://localhost/db"},
		{"missing database path", "DATABASE_URL", "postgres://localhost"},
		{"invalid environment", "APP_ENV", "prod"},
		{"missing port", "HTTP_ADDR", "localhost"},
		{"zero port", "HTTP_ADDR", ":0"},
		{"large port", "HTTP_ADDR", ":65536"},
		{"zero timeout", "DB_CONNECT_TIMEOUT", "0s"},
		{"negative timeout", "HEALTH_TIMEOUT", "-1s"},
		{"large timeout", "SHUTDOWN_TIMEOUT", "1h"},
		{"malformed timeout", "HTTP_IDLE_TIMEOUT", "secret"},
		{"header exceeds read", "HTTP_READ_HEADER_TIMEOUT", "11s"},
		{"health exceeds write", "HTTP_WRITE_TIMEOUT", "1s"},
		{"pool zero", "DB_MAX_CONNS", "0"},
		{"pool overflow", "DB_MAX_CONNS", "2147483648"},
		{"pool min exceeds max", "DB_MIN_CONNS", "11"},
		{"negative body limit", "HTTP_BODY_LIMIT_BYTES", "-1"},
		{"large body limit", "HTTP_BODY_LIMIT_BYTES", "16777217"},
		{"boolean", "TEST_FUNDS_ENABLED", "enabled"},
		{"log level", "LOG_LEVEL", "trace"},
		{"reconciliation boolean", "RECONCILIATION_ENABLED", "enabled"},
		{"messaging boolean", "MESSAGING_ENABLED", "enabled"},
		{"replay boolean", "DLQ_REPLAY_ENABLED", "enabled"},
		{"financial boolean", "FINANCIAL_API_ENABLED", "enabled"},
		{"telemetry scheme", "TELEMETRY_OTLP_ENDPOINT", "file:///private"},
		{"telemetry credentials", "TELEMETRY_OTLP_ENDPOINT", "http://user:secret@localhost:4318"},
		{"telemetry query", "TELEMETRY_OTLP_ENDPOINT", "http://localhost:4318/?secret=hidden"},
		{"broker credentials", "RABBITMQ_URL", "amqp://user:secret%zz@localhost/"},
		{"broker scheme", "RABBITMQ_URL", "http://localhost/"},
		{"broker host", "RABBITMQ_URL", "amqp:///"},
		{"zero workers", "MESSAGE_WORKERS", "0"},
		{"excess workers", "MESSAGE_WORKERS", "17"},
		{"zero prefetch", "MESSAGE_PREFETCH", "0"},
		{"excess prefetch", "MESSAGE_PREFETCH", "65"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			values := map[string]string{"DATABASE_URL": "postgres://user:super-secret@localhost/db"}
			values[tc.key] = tc.value
			_, err := fromMap(values)
			if err == nil || !strings.Contains(err.Error(), tc.key) {
				t.Fatalf("expected validation failure for %s, got %v", tc.key, err)
			}
			if strings.Contains(err.Error(), "secret") {
				t.Fatal("validation leaked a configuration value")
			}
		})
	}
}

func TestReconciliationRedisAddress(t *testing.T) {
	for _, address := range []string{"", "localhost", ":6379", "localhost:0", "localhost:65536"} {
		_, err := fromMap(map[string]string{"DATABASE_URL": "postgres://localhost/db", "RECONCILIATION_ENABLED": "true", "REDIS_ADDR": address})
		if err == nil || !strings.Contains(err.Error(), "REDIS_ADDR") {
			t.Fatalf("invalid address accepted: %q %v", address, err)
		}
	}
}

func TestProductionRejectsTestFunding(t *testing.T) {
	for _, environment := range []string{"development", "test", "production"} {
		t.Run(environment, func(t *testing.T) {
			_, err := fromMap(map[string]string{
				"DATABASE_URL": "postgres://localhost/db", "APP_ENV": environment, "TEST_FUNDS_ENABLED": "true",
			})
			if (err != nil) != (environment == "production") {
				t.Fatalf("environment %s: %v", environment, err)
			}
		})
	}
}

func TestFinancialAPIRequiresExplicitTrustConfiguration(t *testing.T) {
	base := map[string]string{"DATABASE_URL": "postgres://localhost/db", "FINANCIAL_API_ENABLED": "true"}
	if _, err := fromMap(base); err == nil {
		t.Fatal("missing public key accepted")
	}
	base["JWT_PUBLIC_KEY"] = "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA="
	if _, err := fromMap(base); err == nil {
		t.Fatal("missing issuer/audience accepted")
	}
	base["JWT_ISSUER"] = "issuer"
	base["JWT_AUDIENCE"] = "api"
	if _, err := fromMap(base); err != nil {
		t.Fatal(err)
	}
	base["HTTP_WRITE_TIMEOUT"] = "10s"
	if _, err := fromMap(base); err == nil {
		t.Fatal("insufficient HTTP write budget accepted")
	}
}
