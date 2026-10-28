# Architecture — exactly three layers

```text
HTTP client
    |
    v
1) presentation/http: routing, DTO, middleware, validation, authentication, authorization boundary, status mapping
    |
    v
2) business: domain models, invariants, services/use cases, consumed ports/contracts
    |
    v (port interfaces)
3) infrastructure: postgres/pgx, redis, rabbitmq, transaction implementation, event consumers, worker pool, config/telemetry adapters
    |
    +--> PostgreSQL: authoritative balances + ledger + idempotency + outbox + processed_events
    +--> RabbitMQ: events, retry queues, dead-letter queue
    +--> Redis: optional auxiliary distributed coordination; never money consistency

cmd/api/main.go = dependency wiring/composition root
```

## Suggested repository tree
```text
cmd/api/main.go
internal/presentation/http/{handlers,dto,middleware,router.go,errors.go}
internal/business/{domain,ports,services}
internal/infrastructure/{postgres,redis,messaging,workers,telemetry,config}
migrations/
tests/{integration,concurrency,fixtures}
deployments/{Dockerfile,compose.yaml}
docs/{PRODUCT.md,ARCHITECTURE.md,CONTRACTS.md,RELIABILITY.md,TESTING.md,adr/}
.github/workflows/ci.yaml
Makefile
README.md
```

## Dependency rule
- `presentation` calls business use case interfaces and does not import a DB client.
  HTTP adapters receive `transfers.Operations`, `transfers.ReadQueries` and
  `health.Readiness`; authentication uses `identity.Authenticator`. These contracts
  live in business. `cmd/api` constructs the concrete implementations and injects
  them; constructors may return concrete types without coupling handlers to them.
  `financial.go` registers routes and middleware only. Account, transfer and
  test-funding handlers receive narrower business contracts and adapt requests
  independently. Authentication and request deadlines are separate middleware;
  explicit presentation DTOs preserve the public JSON contract. Business JSON
  representations used by persistence and idempotency remain unchanged.
- `business` only depends on Go standard library and domain-level packages.
- `infrastructure` implements business ports. Transaction-scoped repos share one `pgx.Tx` passed as implementation detail, not exposed to business.
- `main` constructs config/logger/tracer, pools/clients, implementations, services, handlers; sets timeouts and shutdown ordering.

## Money transaction
1. Validate request and actor permissions.
2. Compute canonical payload hash; claim/check idempotency key via database unique constraint and transaction.
3. Lock both account rows in deterministic order; re-check statuses, currencies, ownership and sufficient funds inside transaction.
4. Record transfer and balanced journal entries. Update both materialized balances; all with overflow checks, nonnegative constraints and appropriate isolation.
5. Persist outbox event; store idempotency result; COMMIT.
6. Return committed transfer. If commit outcome ambiguous, query idempotency record before deciding retry.

**Ledger design:** Prefer `journal_entries` + `postings` where each journal holds two or more immutable postings and is balanced by currency. This demonstrates true double-entry semantics; for V1 exactly two postings per transfer. A test issuance posts against test treasury account, not ex nihilo balance changes. SQL schema and tests must enforce/verify journal balance at commit (e.g., controlled posting API + database constraint trigger/appropriate design).

## Concurrency
- PostgreSQL row locks (sorted account IDs) are the primary spend-control mechanism.
- Optimistic locking on selected config/profile operations, not as competing safeguard for the same transfer path.
- Redis leases for **auxiliary scheduler / reconciliation leadership only**. Expiry and stale-owner effects must be mitigated, not assumed safe due to a lock token.
- Async side effects are outside the financial transaction but generated from the transactional outbox.

## Configuration and security
Validated env-based configuration, including HTTP request body limits, JWT issuer/audience/signature/expiry, DB pool, broker parameters, deadlines, retry budgets and test-only endpoint flag. Default to test-only endpoint disabled. Validate owner access before returning account/transfer details. Do not log bearer tokens or full account identifiers in metrics labels.
