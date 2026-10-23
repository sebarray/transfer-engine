# Product requirements — Transfer Engine

## Goal
A single backend service showcasing advanced Go engineering through simulated transfers between internal accounts, with auditability and demonstrable correctness under concurrency and failures.

## Version 1 capabilities
- Create test accounts with owner, status, currency, zero starting balance.
- Inject **fictitious funds** only under explicit test/dev configuration using audited, balanced issuance ledger postings (a test treasury/counterparty account), not arbitrary balance mutation.
- Query account and balance (authorized owner), movements (paginated), and transfer by ID.
- Execute synchronous same-currency transfer between two accounts, with HTTP `Idempotency-Key`.
- Publish `transfer.completed` event to broker after the money transaction commits; auxiliary consumer demonstrates safe at-least-once processing.
- Expose traces/metrics/logs, JSON errors, OpenAPI, Docker Compose, CI, Testcontainers.

## Explicit non-goals
- Real-world transfers, settlement, clearing, banks/CVU, exchange rates, compliance/KYC, chargebacks, interest, multi-region active-active, multi-tenant banking platform, production financial institution use.
- Async debit/credit settlement; money movement stays in a single DB transaction.

## Business invariants
1. `amount > 0`, valid currency in supported catalog, all amounts represented in minor units `int64`.
2. Source != destination; both accounts active and have same currency.
3. Source is authorized for authenticated actor. Do not leak another user's balance or transfer.
4. Funds never become negative; SQL constraints defend the invariant.
5. Each completed transfer has exactly one balanced journal (one debit and one credit, same currency). No editing/deleting posted ledger entries; reversals through new entries in future phase only.
6. Balance changes, journal, transfer, idempotency result, and outbox record are one ACID commit.
7. Requests sharing key + same actor/operation + same normalized payload yield one transfer, even when concurrent or retried. Different payload with reused key is conflict.
8. Event duplicates do not duplicate financial movement or auxiliary effects.

## Public API

Implemented wire contract: [OpenAPI](openapi.json). Financial routes are opt-in;
see [activation and trust configuration](phase6.md).
- `POST /v1/accounts` (authorized)
- `GET /v1/accounts/{id}`
- `GET /v1/accounts/{id}/balance`
- `GET /v1/accounts/{id}/movements?limit=&cursor=`
- `POST /v1/transfers` (`Idempotency-Key` required)
- `GET /v1/transfers/{id}`
- `POST /v1/test-funds` (development/test only; disabled otherwise)
- `GET /livez`, `GET /readyz`

`POST /v1/transfers` example:
```http
POST /v1/transfers
Authorization: Bearer <JWT>
Idempotency-Key: transfer-order-123
Content-Type: application/json

{"source_account_id":"aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa","destination_account_id":"bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb","amount":25000,"currency":"ARS"}
```
Amount `25000` is 250.00 ARS in two-decimal minor units. New success: `201 Created`; identical replay: `200 OK` with same transfer ID; key with changed normalized request: `409 Conflict`. Validation 400, auth 401, ownership 403 (or 404 per consistent privacy policy), insufficient funds 422, infrastructure unavailable 503. In-flight duplicate: bounded wait or documented transient response; never double debit.

## Acceptance demo
- 100+ simultaneous transfer attempts with limited funds: never negative balance, aggregate money conserved, journal reconciles.
- Concurrent identical idempotency keys across independent service replicas: exactly one logical transfer.
- Crash after commit before response, retry: same transfer returned.
- Broker failure: transfer commits, outbox pending, eventual publish upon recovery.
- Duplicate consumer delivery: auxiliary side effect occurs once.
- Retry exhaustion: message appears in DLQ with useful metadata and can be replayed safely.
