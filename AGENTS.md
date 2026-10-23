# Codex instructions — Transfer Engine

You are developing **Transfer Engine**, a portfolio-grade Go service for **simulated internal money transfers**. Read `docs/PRODUCT.md`, `docs/ARCHITECTURE.md`, `docs/CONTRACTS.md`, `docs/RELIABILITY.md`, `docs/TESTING.md`, and `IMPLEMENTATION_PLAN.md` before changes. Treat these as project requirements. Respond in Spanish; code and identifiers in English.

## Non-negotiable rules

1. **Exactly three application layers:** `internal/presentation` (HTTP adapters), `internal/business` (domain, use cases, ports), `internal/infrastructure` (databases, broker, Redis, background adapters). `cmd/api` is composition root. Dependencies point inward: business NEVER imports Echo, pgx, Redis, RabbitMQ or HTTP. Interfaces are defined by their consumers in business.
2. **Simulated financial system only.** No real banking integration, CVU, actual money, payment processing, PCI claims, or production-readiness claims. Test-money funding endpoints are never enabled in production configuration.
3. **Money:** use `int64` minor units plus validated ISO currency; never float. Amount > 0; accounts same currency; source != destination. Overflow-aware calculations. Account authorization scoped to actor.
4. **Atomic financial movement:** single PostgreSQL transaction including idempotency claim/result, transfer, balance changes, double-entry ledger and outbox event. All-or-nothing. Store a balanced immutable ledger; make balance reconciliation possible. No balance updates via RabbitMQ consumers.
5. **Concurrency:** order row locks deterministically for accounts; use `SELECT ... FOR UPDATE` (or demonstrably equivalent) for spend protection; optimistic version checks for suitable non-money paths. Redis lock only for auxiliary coordination, never as sole safeguard for money; expired leases must not allow stale writes (use fencing/protective transaction when necessary).
6. **Idempotency:** PostgreSQL unique (actor + operation + key) and canonical payload hash; identical retry returns same stable logical result; mismatched request returns conflict. Handle simultaneous requests, in-flight contention, crash after commit but before HTTP response, and ambiguous commit outcomes. Do not assume Redis provides correctness.
7. **Async messaging:** transactional outbox to RabbitMQ; publisher confirms and safe mark-as-published protocol; consumer at-least-once and durable inbox/dedup for side effects; explicit retry classification, bounded backoff+jitter, max attempts, DLQ, replay, ACK/NACK semantics, poison messages, message versioning, ordering strategy, prefetch/backpressure and graceful worker shutdown. Never ACK original unless outcome or reliable handoff persisted.
8. **Resilience:** context propagation; bounded timeouts; retries only where semantically safe; circuit breaker for external non-financial dependencies; worker pools with bounded concurrency.
9. **Observability:** OpenTelemetry traces and metrics, structured `slog` with request/trace identifiers; redact tokens and sensitive data. Do not use high-cardinality financial identifiers as metric labels.
10. **Security:** JWT validation and per-account ownership authorization, body limits/input validation, appropriate HTTP status mapping; health endpoints are a permitted small essential addition.
11. **Quality:** unit tests with fake ports; integration with Testcontainers for PostgreSQL, Redis, RabbitMQ; concurrency/race tests; CI lint/test/build; deterministic tests, no sleep-based correctness assertions where avoidable. Preserve failure injection tests.
12. **No placeholders silently presented as complete:** If a requirement is not implemented, mark it TODO with a testable acceptance criterion and record it in `IMPLEMENTATION_PLAN.md`. Prefer an executable vertical slice over skeleton sprawl. Make incremental commits/tasks; never claim tests passed unless run.

## Development protocol

- At each phase: state scope, implement minimal coherent code, add tests, run `go test ./...` and relevant integration suite, report results/limitations.
- Keep Go types and methods idiomatic. Use explicit interfaces only where boundary substitution is beneficial.
- Avoid over-engineering: one service, one PostgreSQL source of truth, no Kubernetes or CQRS requirement.
- Never use fake examples as real benchmark results.
- Work in the order of `IMPLEMENTATION_PLAN.md`, starting with the first incomplete phase.
