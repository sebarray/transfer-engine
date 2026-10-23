# Testing strategy

## Unit tests
- Service tests with fake transaction manager/repositories: happy path; no funds; ownership fail; inactive accounts; invalid amount/currency; same account; idempotent replay; mismatched payload; domain error mapping; rollback triggering.
- Retry classification/backoff policy with injected clock/randomness; consumer version validation; queue routing; context cancellation.

## Integration tests (Testcontainers-Go)
- PostgreSQL: migrations, constraints, balanced journal, rollback, replay persisted across connection/process restart, duplicate idempotency unique constraint, deadlocks, transaction boundaries.
- Redis: lease ownership/expiry for auxiliary jobs; stale worker fencing where necessary.
- RabbitMQ: publisher confirms, routing, delayed retry, DLQ, redelivery, consumer dedup; use a supported image matching deployment config.

## Concurrency tests
- 100 simultaneous transfers from one source with limited initial funds: accepted+rejected predictable, source never negative, balances and ledger reconcile.
- 100 duplicate same-key requests across 2+ application instances/processes: one logical transfer, same result for replay.
- Opposing account transfers in contention: deterministic lock order reduces deadlocks, test liveness and consistency.
- Run `go test -race ./...` for Go races; database tests separately validate distributed races (race detector alone is insufficient).

## Observability tests
- Propagated context/trace IDs across HTTP -> business -> broker envelope and consumer; no secret leakage.
- Validate metric names/labels for bounded cardinality.

## CI checks
- `go test ./... -count=1`
- `go test -race ./...` (where runner budget permits)
- `go test -tags=integration ./tests/integration/...`
- lint, format, compile, Docker build and migration verification.

Do not claim benchmark results unless measured. Load testing and full benchmarks are explicitly optional/out of selected scope.

## Runnable acceptance proofs

See [DEMO.md](DEMO.md) for exact commands and assertion-to-requirement mapping.
`TestProcessRecoveryDemo` compiles the real `cmd/api` binary, exercises signed HTTP,
stops/restarts the disposable RabbitMQ application, and invokes the actual replay
CLI. It waits for persisted state with bounded polling, not fixed sleeps.
`make integration` includes this test and uses a 10-minute overall timeout.
GitHub Actions invokes that target; a configured workflow is not evidence of a
hosted run. The child binary is not race-instrumented; `make race` checks the
ordinary Go test suite, while database concurrency assertions run in integration.
