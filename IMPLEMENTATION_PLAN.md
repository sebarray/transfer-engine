# Implementation plan — execute sequentially

Mark a task `[x]` only after implementing and running relevant checks. If not possible, report blocker and keep `[ ]`.

## Phase 1 — Foundation
- [x] Initialize local Go module `transfer-engine` (no remote owner configured), pinned to Go 1.27.3.
- [x] Create 3-layer directories and minimal runnable Echo API (`/livez`, `/readyz`).
- [x] Env config validation, `slog`, graceful shutdown, dependency injection in `cmd/api`.
- [x] Docker Compose for PostgreSQL, Redis, RabbitMQ; Docker multi-stage image.
- [x] Initial schema migrations with test accounts, journals, postings, transfers, idempotency, outbox, processed_events.
- [x] Basic unit tests + CI baseline.

Verified locally on 2026-10-22: `go test ./... -count=1`, `go vet ./...`,
`go build -trimpath -o bin/api.exe ./cmd/api`, formatting, PostgreSQL Testcontainers
integration (including up/down/up), and `go test -race ./... -count=1` in Linux
Docker. Multi-stage Docker build and Compose startup passed; livez/readyz returned
200. Smoke test used API port 18080 and PostgreSQL port 15432 because the default
database port was occupied. GitHub Actions is configured; no hosted run is claimed.

Phase boundaries / testable TODOs:
- Phase 2 verified balances, ledger, transfer, idempotency and outbox commit or
  roll back together through the real application adapter, including overspend
  contention and an injected outbox failure after balance changes.
- Phase 3 verifies canonical fingerprints, concurrent stable replay and bounded
  ambiguous-commit resolution against authoritative PostgreSQL state.
- Phase 4 implements and failure-tests outbox publication, inbox handling,
  bounded retries, DLQ and audited replay. Redis coordinates only the optional
  auxiliary reconciliation scheduler.
- Phase 6 adds opt-in authenticated financial HTTP, ownership-scoped queries,
  OpenAPI and OpenTelemetry with propagation and redaction tests.
- Phase 3 adds versioned migration execution with checksums and explicit baseline
  adoption of an existing verified bootstrap schema. The optional reconciliation
  worker checks its required schema at startup. Phase 6 gates financial startup
  and readiness on migration history 1–3 and required tables; missing and newer
  schemas are integration-tested.

## Phase 2 — Atomic transfers
- [x] Domain types, money invariants, typed errors.
- [x] Account creation and owner-scoped balance query.
- [x] Test issuance with balanced treasury ledger, dev/test only.
- [x] Transaction manager and transaction-scoped ports.
- [x] Atomic transfer with ordered row locks, balanced immutable ledger, materialized balances and DB safeguards.
- [x] Integration tests for commit, rollback, overspend, opposite-direction contention.

Phase 2 verification: unit tests with fake transactional ports, PostgreSQL
Testcontainers integration, 100 simultaneous spends (50 accepted/50 insufficient
funds), 100 opposing transfers, balanced issuance, ownership, replay/conflict,
and outbox failure injection proving rollback of every financial record.
`go test ./... -count=1`, integration suite, `go vet ./...`, build and Linux
`go test -race ./... -count=1` passed. No schema changes were required.

These are application use cases in business plus a PostgreSQL adapter, exercised
directly by integration tests. Phase 6 wires authenticated handlers in cmd/api,
tests JWT issuer/audience/signature/expiry, owner-scoped HTTP reads and no funding
route in production. Actor identity comes only from a verified JWT subject.
Phase 3 resolves ErrCommitUnknown by querying durable idempotency state. There is
no automatic second financial attempt. Caller retries must reuse the original
actor, operation, key and payload.

## Phase 3 — Idempotency and distributed concurrency
- [x] Canonical request fingerprint; DB key scoped by actor+operation.
- [x] Atomic claim/result and concurrent same-key replay.
- [x] Recover from lost/ambiguous response after commit at the use-case boundary.
- [x] Optimistic versioning for reconciliation settings, a non-money entity.
- [x] Redis lock for auxiliary reconciliation scheduler only; expiry/fencing correctness.
- [x] Multi-instance concurrency integration test.

Phase 3 evidence: 100 identical requests through two independent PostgreSQL pools
produce one transfer/outbox and stable replay; connection restart, payload mismatch,
actor isolation and bounded in-flight contention are tested. Lost commit responses
are injected around real commits/rollbacks; resolution works with canceled caller
contexts and never repeats the financial write. An absent or unreachable result
remains unknown. Optimistic settings updates allow exactly one winner; PostgreSQL
leases/fences reject expired or superseded audit writes. Redis expiry/ownership
and Redis outage with successful financial transfer are integration-tested.
See docs/phase3.md for the tested boundary and activation/migration commands.

Verification includes unit tests, PostgreSQL/Redis Testcontainers integration,
Linux race detector, vet, build, Docker image and Compose validation. One repeat
hit a Docker cleanup timeout after functional assertions; this is an infrastructure
failure, not a passed run; the subsequent full rerun passed, including cleanup.
Phase 6 verifies actual HTTP response loss after commit and stable replay/status
mapping. The earlier multi-instance tests use two pools in one test process.

## Phase 4 — Reliable async events
- [x] Persist `transfer.completed` in outbox in same money transaction.
- [x] RabbitMQ topology: durable main queue, retry queues and DLQ; versioned event envelope.
- [x] Outbox publisher with confirms and crash recovery.
- [x] Bounded worker pool + prefetch; durable inbox/processed-event dedupe.
- [x] Error classification, bounded retries with jitter, poisoned messages, DLQ and safe replay path.
- [x] Tests for duplicate deliveries, retry exhaustion, ack-after-commit, broker downtime, worker crash.

Phase 4 verification on 2026-10-23: unit tests, full PostgreSQL/Redis/RabbitMQ
Testcontainers suite, Linux race detector, vet, executable build, Docker image
build and Compose configuration validation passed. Failure injection covers
confirm-before-mark, commit-before-ACK, effect/inbox rollback, unreachable broker
followed by recovery, poison version, ambiguous DLQ confirm, retry TTL exhaustion,
audited replay, bounded workers and graceful drain. No hosted CI run is claimed.

The effect is a simulated internal notification stored in PostgreSQL, never a
balance update or external send. See docs/phase4.md for migration 000003,
activation, administrative CLI, ordering, TTL and duplicate-delivery semantics.
The broker-outage test uses an unreachable endpoint; it does not demonstrate
cluster failover or disk-loss recovery. Phase 6 adds trace instrumentation;
the CLI relies on host/credential access, with declared operator audit identity.

## Phase 5 — Resilience
- [x] Context/deadline propagation; timeouts.
- [x] Safe retries for transients; backoff/jitter.
- [x] Circuit breaker for **auxiliary external interface**, not core PostgreSQL consistency path.
- [x] Graceful shutdown of HTTP and workers; backpressure.

Phase 5 verification on 2026-10-23: unit tests, full PostgreSQL/Redis/RabbitMQ
integration suite, Linux race detector, vet and executable build passed.
The Redis reconciliation lease uses a process-local breaker: three availability
failures, ten-second cooldown, one recovery probe, generation-protected late
results and bounded cleanup that bypasses the open circuit. PostgreSQL money
transactions and fencing remain outside the breaker. Scheduler availability
failures use capped exponential backoff with jitter; permanent failures retain
the normal interval. No ambiguous financial write is automatically repeated.

Tests cover concurrent probe admission, failed probe/recovery, stale completions,
cancellation and deadline propagation, retry classification/reset, a stalled
AMQP handshake, and real Redis outage with successful financial transfers.
Existing HTTP drain, worker bounds, inbox/outbox and failure-injection tests remain.
See docs/phase5.md for budgets, lifecycle, policies and test limitations. Breaker
recovery timing is verified with a controlled clock, not cluster failover.

## Phase 6 — Observability and security
- [x] OpenTelemetry traces + metrics; W3C context propagation through messages.
- [x] Structured `slog` with trace/request IDs and safe redaction.
- [x] JWT validation, resource authorization, request body/input limits.
- [x] OpenAPI docs and stable error mappings.

Phase 6 adds opt-in HTTP account creation, balances, keyset-paginated movements,
transfers, transfer reads and dev/test issuance. Ed25519 JWTs require issuer,
audience, expiration and subject; protected routes never trust body actor IDs.
Strict bounded JSON rejects duplicate/unknown fields, extra values and nulls.
Readiness rejects unversioned/newer schema when financial routes are enabled.

Verified on 2026-10-23: unit tests and full PostgreSQL/Redis/RabbitMQ integration
suite, including lost TCP response after commit with stable HTTP replay and
authenticated HTTP -> outbox -> RabbitMQ -> consumer trace correlation.
Linux race detector passed, including the final collector-stall tests. Tests
assert bounded metric labels, secret-free logs, real OTLP HTTP signal export,
and bounded shutdown with a blocked collector. See docs/phase6.md and
docs/openapi.json for activation, ownership, error codes and limitations.
Vet, executable/container builds, module checksum verification and Compose
validation passed. The OpenAPI operation list is checked against registered
routes. No hosted CI run or deployed collector/dashboard is claimed.

Telemetry export is optional and best-effort with a bounded dropping queue;
no durable telemetry delivery or per-drop counter is claimed. One configured
public key is supported; issuer service, dynamic JWKS and dashboards are outside
this implementation. The API financial flag remains false by default.

## Phase 7 — Polish
- [x] End-to-end Testcontainers suite integrated into GitHub Actions.
- [x] `go test -race` and concurrency tests.
- [x] README: architecture, tradeoffs, setup, proof demos, known limitations.
- [x] Demonstrate broker failure/outbox recovery and DLQ replay.

Phase 7 verification on 2026-10-23: `go test ./... -count=1`, vet, build,
formatting, complete Testcontainers integration including concurrent spends and
same-key contention, Linux `go test -race ./... -count=1`, Compose validation and
container build passed. The final complete integration run also verifies the
replayed fixture's committed inbox before asserting effect deduplication.
The process demo also passed independently in a Linux Go container, validating
the child-binary/CLI path used by CI as well as the local Windows execution.

`TestProcessRecoveryDemo` builds and executes cmd/api, creates/funds accounts via
signed HTTP, stops the disposable RabbitMQ application, confirms a transfer with
pending outbox, starts RabbitMQ and observes publication plus notification.
It invokes the real replay CLI for rejection, dry run and active audited replay;
balances remain 75/25 and one notification survives. The DLQ fixture has a fresh
event identity for an already-notified transfer so consumption is observable in
the inbox; same-event-ID redelivery and retry exhaustion remain covered by the
existing messaging suite. Polling is bounded and driven by observable state.

`make demo` runs the process proof, and `make integration` includes it with a
10-minute suite timeout. GitHub Actions invokes that target. This is configured
CI, not a claimed hosted run. README and docs/DEMO.md explain commands, evidence,
architecture, tradeoffs and known limitations. No production-readiness,
exactly-once delivery, cluster failover, benchmark or external notification claim
is made. All seven implementation phases are complete within the simulated scope.

## Follow-up — Interactive API documentation

- [x] Serve Swagger UI at `/swagger/`, redirect `/swagger`, and serve the canonical
  embedded contract at `/openapi.json`; include pinned local UI assets and license.
- [x] Support same-origin Try it out and JWT authorization without persistent
  token storage or an external validator; preserve financial route gates.
- [x] Test static routes, content types, source/served contract equality and
  unchanged disabled financial routes. Unit suite, vet and the authenticated
  PostgreSQL HTTP integration test passed on 2026-10-23.
- [x] Docker image build passed with embedded documentation (`transfer-engine:swagger`).

Browser rendering has not been manually verified. The complete contract remains
visible regardless of enabled routes; README explains activation and 404 behavior.

## Follow-up — Interface-based HTTP injection

- [x] Inject business use case interfaces into HTTP adapters, including readiness.
  Implemented contracts: `transfers.Operations`, `transfers.ReadQueries`, and
  `health.Readiness`; `identity.Authenticator` already existed. Acceptance:
  direct fake use cases exercise HTTP context/actor/argument propagation, responses
  and error mapping; `go test ./... -count=1` and the authenticated financial HTTP
  PostgreSQL integration test pass.

  Verification on 2026-10-27: `go test ./... -count=1 -timeout=90s` passed
  outside the sandbox, including the direct use case injection test. Sandboxed
  runs failed in local socket tests due to access restrictions. The authenticated
  HTTP integration test could not create a Docker provider; `docker info` confirmed
  the Docker Desktop Linux engine pipe was unavailable. TODO: start Docker and run
  `go test -tags=integration ./tests/integration/... -run '^TestAuthenticatedFinancialHTTP$' -count=1 -timeout=5m`.
  Resolved on 2026-10-28: the authenticated HTTP integration acceptance passed
  outside the sandbox with Docker available.

## Follow-up — Separate financial HTTP responsibilities

- [x] Separate route registration, authentication, request deadlines, account/
  transfer/test-funding handlers, strict request parsing and explicit response DTOs.
  Handlers consume narrow business interfaces; transfer and funding no longer
  share a boolean-driven handler. Preserve the public wire contract and both
  production funding guards.
- [x] Verify fake use case injection, actor/command propagation, transfer/funding
  replay status, funding source-field rejection, authentication rejection and
  request context cleanup. Full `go test ./... -count=1 -timeout=90s` passed
  outside the sandbox on 2026-10-28.
- [x] Verify authenticated PostgreSQL HTTP integration after the refactor.
  Acceptance: `go test -tags=integration ./tests/integration/... -run '^TestAuthenticatedFinancialHTTP$' -count=1 -timeout=5m` passes.
  Passed outside the sandbox on 2026-10-28 with Docker available.

## Deliberate non-goals from user's selection
No mandated fuzzing, microbenchmarks, contract testing suite, rate limiting, bulkheads, full Grafana dashboards/alerts/SLOs, security scanners, load testing, Kubernetes, multi-service CQRS. Input validation, race detector and health checks are essential small additions. Reliable outbox, at-least-once consumer and workers are required to meet selected event-driven behavior and explicit DLQ requirements.
