// Package config loads immutable process settings from the environment.
package config

import (
	"encoding/base64"
	"fmt"
	"log/slog"
	"net"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"
)

type Config struct {
	Environment                     string
	HTTPAddr                        string
	DatabaseURL                     string
	DBMaxConns                      int32
	DBMinConns                      int32
	DBConnectTimeout                time.Duration
	HealthTimeout                   time.Duration
	HTTPReadTimeout                 time.Duration
	HTTPReadHeaderTimeout           time.Duration
	HTTPWriteTimeout                time.Duration
	HTTPIdleTimeout                 time.Duration
	ShutdownTimeout                 time.Duration
	HTTPBodyLimitBytes              int64
	TestFundsEnabled                bool
	LogLevel                        slog.Level
	ReconciliationEnabled           bool
	RedisAddr                       string
	MessagingEnabled                bool
	RabbitMQURL                     string
	MessageWorkers, MessagePrefetch int
	DLQReplayEnabled                bool
	FinancialAPIEnabled             bool
	JWTPublicKey                    []byte
	JWTIssuer, JWTAudience          string
	TelemetryEndpoint               string
}

func Load() (Config, error) { return load(os.LookupEnv) }

func load(lookup func(string) (string, bool)) (Config, error) {
	get := func(key, fallback string) string {
		if value, ok := lookup(key); ok {
			return value
		}
		return fallback
	}
	c := Config{
		Environment: get("APP_ENV", "development"),
		HTTPAddr:    get("HTTP_ADDR", ":8080"),
		DatabaseURL: get("DATABASE_URL", ""),
	}
	switch c.Environment {
	case "development", "test", "production":
	default:
		return Config{}, fmt.Errorf("APP_ENV must be development, test or production")
	}
	_, port, err := net.SplitHostPort(c.HTTPAddr)
	portNumber, portErr := strconv.Atoi(port)
	if err != nil || portErr != nil || portNumber < 1 || portNumber > 65535 {
		return Config{}, fmt.Errorf("HTTP_ADDR must contain a host and a port between 1 and 65535")
	}
	dbURL, err := url.Parse(c.DatabaseURL)
	if err != nil || dbURL == nil || (dbURL.Scheme != "postgres" && dbURL.Scheme != "postgresql") ||
		dbURL.Hostname() == "" || strings.Trim(dbURL.Path, "/") == "" || dbURL.Fragment != "" {
		return Config{}, fmt.Errorf("DATABASE_URL must be a PostgreSQL URL with host and database")
	}
	for _, item := range []struct {
		key, fallback string
		target        *time.Duration
	}{
		{"DB_CONNECT_TIMEOUT", "5s", &c.DBConnectTimeout},
		{"HEALTH_TIMEOUT", "2s", &c.HealthTimeout},
		{"HTTP_READ_TIMEOUT", "10s", &c.HTTPReadTimeout},
		{"HTTP_READ_HEADER_TIMEOUT", "5s", &c.HTTPReadHeaderTimeout},
		{"HTTP_WRITE_TIMEOUT", "15s", &c.HTTPWriteTimeout},
		{"HTTP_IDLE_TIMEOUT", "60s", &c.HTTPIdleTimeout},
		{"SHUTDOWN_TIMEOUT", "10s", &c.ShutdownTimeout},
	} {
		value, err := time.ParseDuration(get(item.key, item.fallback))
		if err != nil || value <= 0 || value > 5*time.Minute {
			return Config{}, fmt.Errorf("%s must be a positive duration no greater than 5m", item.key)
		}
		*item.target = value
	}
	if c.HTTPReadHeaderTimeout > c.HTTPReadTimeout {
		return Config{}, fmt.Errorf("HTTP_READ_HEADER_TIMEOUT must not exceed HTTP_READ_TIMEOUT")
	}
	if c.HealthTimeout >= c.HTTPWriteTimeout {
		return Config{}, fmt.Errorf("HEALTH_TIMEOUT must be shorter than HTTP_WRITE_TIMEOUT")
	}
	maxConns, err := strconv.ParseInt(get("DB_MAX_CONNS", "10"), 10, 32)
	if err != nil || maxConns < 1 || maxConns > 1000 {
		return Config{}, fmt.Errorf("DB_MAX_CONNS must be between 1 and 1000")
	}
	minConns, err := strconv.ParseInt(get("DB_MIN_CONNS", "0"), 10, 32)
	if err != nil || minConns < 0 || minConns > maxConns {
		return Config{}, fmt.Errorf("DB_MIN_CONNS must be between 0 and DB_MAX_CONNS")
	}
	c.DBMaxConns, c.DBMinConns = int32(maxConns), int32(minConns)
	c.HTTPBodyLimitBytes, err = strconv.ParseInt(get("HTTP_BODY_LIMIT_BYTES", "1048576"), 10, 64)
	if err != nil || c.HTTPBodyLimitBytes < 1 || c.HTTPBodyLimitBytes > 16<<20 {
		return Config{}, fmt.Errorf("HTTP_BODY_LIMIT_BYTES must be between 1 and 16777216")
	}
	c.TestFundsEnabled, err = strconv.ParseBool(get("TEST_FUNDS_ENABLED", "false"))
	if err != nil {
		return Config{}, fmt.Errorf("TEST_FUNDS_ENABLED must be a boolean")
	}
	if c.Environment == "production" && c.TestFundsEnabled {
		return Config{}, fmt.Errorf("TEST_FUNDS_ENABLED cannot be enabled in production")
	}
	c.ReconciliationEnabled, err = strconv.ParseBool(get("RECONCILIATION_ENABLED", "false"))
	if err != nil {
		return Config{}, fmt.Errorf("RECONCILIATION_ENABLED must be a boolean")
	}
	c.MessagingEnabled, err = strconv.ParseBool(get("MESSAGING_ENABLED", "false"))
	if err != nil {
		return Config{}, fmt.Errorf("MESSAGING_ENABLED must be a boolean")
	}
	c.DLQReplayEnabled, err = strconv.ParseBool(get("DLQ_REPLAY_ENABLED", "false"))
	if err != nil {
		return Config{}, fmt.Errorf("DLQ_REPLAY_ENABLED must be a boolean")
	}
	c.RabbitMQURL = get("RABBITMQ_URL", "amqp://transfer:transfer_dev@localhost:5672/")
	brokerURL, parseErr := url.Parse(c.RabbitMQURL)
	if parseErr != nil || brokerURL == nil || (brokerURL.Scheme != "amqp" && brokerURL.Scheme != "amqps") || brokerURL.Hostname() == "" {
		return Config{}, fmt.Errorf("RABBITMQ_URL must be an AMQP URL with a host")
	}
	c.MessageWorkers, err = strconv.Atoi(get("MESSAGE_WORKERS", "4"))
	if err != nil || c.MessageWorkers < 1 || c.MessageWorkers > 16 {
		return Config{}, fmt.Errorf("MESSAGE_WORKERS must be between 1 and 16")
	}
	c.MessagePrefetch, err = strconv.Atoi(get("MESSAGE_PREFETCH", "1"))
	if err != nil || c.MessagePrefetch < 1 || c.MessagePrefetch > 64 {
		return Config{}, fmt.Errorf("MESSAGE_PREFETCH must be between 1 and 64")
	}
	c.RedisAddr = get("REDIS_ADDR", "localhost:6379")
	if c.ReconciliationEnabled {
		host, port, splitErr := net.SplitHostPort(c.RedisAddr)
		number, numberErr := strconv.Atoi(port)
		if splitErr != nil || host == "" || numberErr != nil || number < 1 || number > 65535 {
			return Config{}, fmt.Errorf("REDIS_ADDR must contain a host and a valid port")
		}
	}
	// Keep the accepted levels explicit; slog also accepts numeric offsets.
	switch level := get("LOG_LEVEL", "info"); level {
	case "debug", "info", "warn", "error":
		_ = c.LogLevel.UnmarshalText([]byte(level))
	default:
		return Config{}, fmt.Errorf("LOG_LEVEL must be debug, info, warn or error")
	}
	c.FinancialAPIEnabled, err = strconv.ParseBool(get("FINANCIAL_API_ENABLED", "false"))
	if err != nil {
		return Config{}, fmt.Errorf("FINANCIAL_API_ENABLED must be a boolean")
	}
	if c.FinancialAPIEnabled {
		c.JWTIssuer = get("JWT_ISSUER", "")
		c.JWTAudience = get("JWT_AUDIENCE", "")
		c.JWTPublicKey, err = base64.StdEncoding.DecodeString(get("JWT_PUBLIC_KEY", ""))
		if err != nil || len(c.JWTPublicKey) != 32 {
			return Config{}, fmt.Errorf("JWT_PUBLIC_KEY must be a base64 encoded Ed25519 public key")
		}
		if strings.TrimSpace(c.JWTIssuer) == "" || strings.TrimSpace(c.JWTAudience) == "" {
			return Config{}, fmt.Errorf("JWT_ISSUER and JWT_AUDIENCE are required")
		}
		if c.HTTPWriteTimeout < 15*time.Second {
			return Config{}, fmt.Errorf("HTTP_WRITE_TIMEOUT must be at least 15s for financial handlers")
		}
	}
	c.TelemetryEndpoint = get("TELEMETRY_OTLP_ENDPOINT", "")
	if c.TelemetryEndpoint != "" {
		u, e := url.Parse(c.TelemetryEndpoint)
		if e != nil || u.Hostname() == "" || (u.Scheme != "http" && u.Scheme != "https") || u.User != nil || u.RawQuery != "" || u.Fragment != "" {
			return Config{}, fmt.Errorf("TELEMETRY_OTLP_ENDPOINT must be an HTTP(S) endpoint without credentials or query")
		}
	}
	return c, nil
}
