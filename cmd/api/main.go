package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"io"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"transfer-engine/internal/business/events"
	"transfer-engine/internal/business/health"
	"transfer-engine/internal/business/reconciliation"
	"transfer-engine/internal/business/transfers"
	"transfer-engine/internal/infrastructure/auth"
	"transfer-engine/internal/infrastructure/config"
	"transfer-engine/internal/infrastructure/messaging"
	"transfer-engine/internal/infrastructure/postgres"
	redisadapter "transfer-engine/internal/infrastructure/redis"
	"transfer-engine/internal/infrastructure/telemetry"
	"transfer-engine/internal/infrastructure/workers"
	httpapi "transfer-engine/internal/presentation/http"
)

func main() {
	migrate := flag.Bool("migrate", false, "apply schema migrations and exit")
	baseline := flag.Int("baseline", 0, "adopt already applied migrations (requires verified existing schema)")
	migrationsDir := flag.String("migrations-dir", "migrations", "directory containing SQL migrations")
	replay := flag.Bool("replay-dlq", false, "inspect or replay DLQ messages (operator CLI)")
	dryRun := flag.Bool("dry-run", true, "inspect without republishing or acknowledging DLQ messages")
	limit := flag.Int("limit", 10, "maximum DLQ messages to inspect/replay (1-100)")
	operator := flag.String("operator", "", "operator identity for replay audit")
	reason := flag.String("reason", "", "operator reason for replay audit")
	flag.Parse()
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	var err error
	if *migrate && *replay {
		err = errors.New("migrate and replay-dlq are mutually exclusive")
	} else if *replay {
		err = runReplay(ctx, *limit, events.ReplayRequest{Operator: *operator, Reason: *reason, DryRun: *dryRun}, os.Stdout)
	} else if *migrate {
		err = runMigrations(ctx, *migrationsDir, *baseline)
	} else if *baseline != 0 {
		err = errors.New("baseline requires migrate")
	} else {
		err = run(ctx, os.Stdout)
	}
	stop()
	if err != nil {
		slog.New(slog.NewJSONHandler(os.Stderr, nil)).Error("api_stopped", "error", err.Error())
		os.Exit(1)
	}
}

func runReplay(ctx context.Context, limit int, request events.ReplayRequest, output io.Writer) error {
	cfg, err := config.Load()
	if err != nil {
		return err
	}
	if !request.DryRun && !cfg.DLQReplayEnabled {
		return errors.New("replay writes require DLQ_REPLAY_ENABLED=true and operator credentials")
	}
	ctx, cancel := context.WithTimeout(ctx, 2*time.Minute)
	defer cancel()
	pool, err := postgres.Open(ctx, postgres.Config{URL: cfg.DatabaseURL, MaxConns: 2, ConnectTimeout: cfg.DBConnectTimeout})
	if err != nil {
		return err
	}
	defer pool.Close()
	store := postgres.NewEventStore(pool)
	if err = store.Check(ctx); err != nil {
		return errors.New("messaging schema unavailable; apply migration 000003")
	}
	previews, err := (messaging.Broker{URL: cfg.RabbitMQURL}).ReplayBatch(ctx, limit, request, store)
	if encodeErr := json.NewEncoder(output).Encode(previews); encodeErr != nil {
		return encodeErr
	}
	return err
}

func runMigrations(ctx context.Context, directory string, baseline int) error {
	cfg, err := config.Load()
	if err != nil {
		return err
	}
	pool, err := postgres.Open(ctx, postgres.Config{URL: cfg.DatabaseURL, MaxConns: 1, ConnectTimeout: cfg.DBConnectTimeout})
	if err != nil {
		return err
	}
	defer pool.Close()
	return postgres.Migrate(ctx, pool, os.DirFS(directory), baseline)
}

func run(ctx context.Context, output io.Writer) error {
	cfg, err := config.Load()
	if err != nil {
		return err
	}
	logger := slog.New(slog.NewJSONHandler(output, &slog.HandlerOptions{Level: cfg.LogLevel}))
	shutdownTelemetry, err := telemetry.Start(ctx, cfg.TelemetryEndpoint, logger)
	if err != nil {
		return err
	}
	defer func() {
		cleanup, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		if shutdownTelemetry(cleanup) != nil {
			logger.Warn("telemetry_shutdown_failed")
		}
	}()
	pool, err := postgres.Open(ctx, postgres.Config{
		URL: cfg.DatabaseURL, MaxConns: cfg.DBMaxConns, MinConns: cfg.DBMinConns,
		ConnectTimeout: cfg.DBConnectTimeout,
	})
	if err != nil {
		return err
	}
	defer pool.Close()
	var dependency health.Dependency = pool
	store := postgres.NewTransferStore(pool)
	if cfg.FinancialAPIEnabled {
		check, cancel := context.WithTimeout(ctx, 3*time.Second)
		err := store.Ping(check)
		cancel()
		if err != nil {
			return errors.New("financial schema incompatible; migrate or verify and baseline schema before enabling API")
		}
		dependency = store
	}
	status := health.New(dependency, cfg.HealthTimeout)
	router := httpapi.New(status, logger, cfg.HTTPBodyLimitBytes)
	if cfg.FinancialAPIEnabled {
		validator, err := auth.NewJWT(cfg.JWTPublicKey, cfg.JWTIssuer, cfg.JWTAudience)
		if err != nil {
			return errors.New("JWT configuration invalid")
		}
		service, err := transfers.New(store, cfg.Environment, cfg.TestFundsEnabled)
		if err != nil {
			return err
		}
		httpapi.RegisterFinancial(router, service, transfers.NewQueries(store), validator, cfg.Environment, cfg.TestFundsEnabled)
	}
	server := newServer(cfg, router)
	listener, err := net.Listen("tcp", cfg.HTTPAddr)
	if err != nil {
		return errors.New("could not listen on HTTP_ADDR")
	}
	logger.Info("api_started", "address", listener.Addr().String(), "environment", cfg.Environment)
	if cfg.MessagingEnabled {
		store := postgres.NewEventStore(pool)
		checkCtx, stop := context.WithTimeout(ctx, 3*time.Second)
		err := store.Check(checkCtx)
		stop()
		if err != nil {
			_ = listener.Close()
			return errors.New("messaging schema unavailable; apply migration 000003")
		}
		workerCtx, cancelWorker := context.WithCancel(ctx)
		done := make(chan struct{})
		go func() {
			defer close(done)
			messaging.Run(workerCtx, messaging.Broker{URL: cfg.RabbitMQURL, Logger: logger}, store, store, cfg.MessageWorkers, cfg.MessagePrefetch, logger)
		}()
		defer func() { cancelWorker(); <-done }()
	}
	if cfg.ReconciliationEnabled {
		lease := redisadapter.New(cfg.RedisAddr)
		defer lease.Close()
		audit, err := reconciliation.New(postgres.NewReconciliationStore(pool), redisadapter.NewProtectedLease(lease), 5*time.Second)
		if err != nil {
			_ = listener.Close()
			return err
		}
		// Require migration 000002 before enabling this optional worker.
		if _, err = audit.Settings(ctx); err != nil {
			_ = listener.Close()
			return errors.New("reconciliation schema unavailable; apply migration 000002")
		}
		workerCtx, cancelWorker := context.WithCancel(ctx)
		done := make(chan struct{})
		go func() { defer close(done); workers.RunReconciliation(workerCtx, audit, logger) }()
		defer func() { cancelWorker(); <-done }()
	}
	return serve(ctx, server, listener, status, cfg.ShutdownTimeout, logger)
}

func newServer(cfg config.Config, handler http.Handler) *http.Server {
	return &http.Server{
		Addr: cfg.HTTPAddr, Handler: handler,
		ReadHeaderTimeout: cfg.HTTPReadHeaderTimeout, ReadTimeout: cfg.HTTPReadTimeout,
		WriteTimeout: cfg.HTTPWriteTimeout, IdleTimeout: cfg.HTTPIdleTimeout,
		MaxHeaderBytes: 16 << 10,
	}
}

func serve(ctx context.Context, server *http.Server, listener net.Listener, status *health.Service, shutdownTimeout time.Duration, logger *slog.Logger) error {
	// Requests get a dedicated cancellation root: a termination signal stops intake,
	// but accepted requests retain their contexts until draining completes or expires.
	requestCtx, cancelRequests := context.WithCancel(context.Background())
	defer cancelRequests()
	server.BaseContext = func(net.Listener) context.Context { return requestCtx }
	server.ErrorLog = slog.NewLogLogger(logger.Handler(), slog.LevelError)
	serveErr := make(chan error, 1)
	go func() { serveErr <- server.Serve(listener) }()
	select {
	case err := <-serveErr:
		status.Drain()
		cancelRequests()
		_ = server.Close()
		if errors.Is(err, http.ErrServerClosed) {
			return nil
		}
		return errors.New("HTTP server failed")
	case <-ctx.Done():
		status.Drain()
		logger.Info("api_draining")
	}
	shutdownCtx, cancel := context.WithTimeout(context.Background(), shutdownTimeout)
	defer cancel()
	if err := server.Shutdown(shutdownCtx); err != nil {
		cancelRequests()
		_ = server.Close()
		<-serveErr
		logger.Error("api_shutdown_forced")
		return errors.New("HTTP graceful shutdown deadline exceeded")
	}
	if err := <-serveErr; err != nil && !errors.Is(err, http.ErrServerClosed) {
		return errors.New("HTTP server failed during shutdown")
	}
	logger.Info("api_stopped")
	return nil
}
