# Phase 3: idempotency and auxiliary coordination

## Financial operations

The canonical fingerprint is SHA-256 over the command JSON with fixed field
order: actor_id, source_account_id, destination_account_id, currency, amount.
UUIDs normalize to lowercase before hashing. The key is excluded from the hash;
the PostgreSQL primary key scopes it by actor + operation + key. Invalid UTF-8
and NUL in identity/key inputs are rejected. Currencies remain the strict
ARS/USD/EUR catalog. These canonical fields and ordering are a storage contract;
changing them requires an explicit compatibility strategy.

Claim, result, balances, journal, transfer and outbox share one transaction.
Conflicting inserts wait on PostgreSQL uniqueness; lock_timeout bounds contention
to three seconds, with an earlier caller deadline taking precedence. Contention
returns ErrInFlight; a retry uses the same key. Financial transactions are never
automatically retried by the adapter.

When COMMIT has an uncertain outcome, the service queries the authoritative
idempotency record using an independent three-second recovery context. It does
this even if the caller has canceled. A matching committed result returns its
stable ID. Absence or an unavailable database remains ErrCommitUnknown because
absence alone does not prove rollback. No second write occurs during resolution.
Subsequent retries must retain actor, operation, key and payload; PostgreSQL's
unique claim serializes them with any original transaction still finishing.

Integration checks use two service instances with independent pools in one test
process. They cover 100 identical concurrent requests, canonical UUID casing,
one persisted transfer/outbox, connection restart, changed payload, actor
isolation and bounded in-flight waits. A wrapper around the real store injects
lost acknowledgements after a real commit or rollback and cancels the caller.
This validates recovery decisions; it is not a network packet-loss proxy test.
HTTP response-loss mapping will be exercised when financial HTTP routes arrive.

## Reconciliation

The optional scheduler reads materialized balances and immutable postings from
one PostgreSQL statement snapshot. It writes only an auxiliary summary with the
number of accounts and mismatches, never balances or postings.

Redis uses SET NX with a random owner token and TTL. Release uses compare-and-delete
Lua so an expired holder cannot delete its successor. Redis is an optimization:
PostgreSQL independently grants a timed lease and increasing fence. Publishing a
report locks that row, then checks fence and expiry using the database clock.
Expired or superseded workers cannot publish a report. Context deadlines bound
each run; a crashed run can be reclaimed after its database lease expires.
Redis outage skips auxiliary work and has no dependency path into transfers.

The scheduler uses at most one active run per process. Cancellation drains its
active operation before closing Redis/PostgreSQL. Failed attempts wait for the
configured interval; there is no immediate retry loop.

`reconciliation_settings` is a non-financial optimistic-version example.
UpdateSettings requires the caller's expected Version; two concurrent updates
from the same version yield one success and one ErrConflict. Financial movement
continues to rely on ordered row locks, not optimistic settings versions.

Integration tests use real PostgreSQL and Redis. They force server-side expiry
without sleep-based assertions, verify stale releases/fences, audit output,
optimistic conflict, and successful money movement while Redis is stopped.

## Migration and activation

Migration 000002 adds reconciliation settings/state; 000001 remains unchanged.
For an empty database, export DATABASE_URL then run:

```sh
go run ./cmd/api -migrate
```

For an existing, verified phase-1/phase-2 database created by the original
Compose bootstrap (000001 already applied, no migration history), run once:

```sh
go run ./cmd/api -migrate -baseline=1
```

Baseline is an explicit adoption assertion: it does not verify or repair the
existing schema. For a fresh Compose volume that already ran both init scripts,
adopt with baseline=2 instead. Once history exists, omit baseline. Migration
execution uses an advisory lock and records checksums atomically with each
script. Changed checksums and databases newer than the available files fail.
The runner applies up scripts only; down scripts remain disposable-test tools.

Enable the process worker with RECONCILIATION_ENABLED=true and a REDIS_ADDR in
host:port form. The optional worker checks its required schema at startup.
The default Redis adapter targets the local Compose environment; credentials
and TLS configuration are not provided in this phase. Enable the stored job
using an expected-version update (or the business UpdateSettings method):

```sql
UPDATE reconciliation_settings
SET enabled = true, version = version + 1
WHERE id AND version = 1;
```

The default interval is 60 seconds and each run has a five-second budget.
Inspect `reconciliation_state` for the latest checked_at/accounts/mismatches.
API liveness/readiness never depends on Redis.

References: [Redis lock ownership and release](https://redis.io/docs/latest/develop/clients/patterns/distributed-locks/),
[go-redis client](https://redis.io/docs/latest/develop/clients/go/).
