# ADR-0001 — Core architectural decisions

Status: Accepted for project bootstrap

1. One microservice with three layers, business ports insulated from adapters.
2. Synchronous, atomic internal simulated transfer with PostgreSQL as source of truth.
3. Financial balances are materialized for reads but reconciled with immutable balanced postings.
4. PostgreSQL idempotency scoped by actor, operation and key; Redis does not guard money correctness.
5. RabbitMQ events are auxiliary and emitted via transactional outbox.
6. Redis coordination handles auxiliary work only; guard against expired lock ownership.
7. Async consumers accept at-least-once delivery, have durable dedupe, bounded retry and DLQ/replay.
8. Development-only test funding with traceable balanced treasury postings.

Tradeoff: A single database transaction simplifies correctness for internal transfers but is not a model of cross-bank settlement. More infrastructure raises teaching value but must not compromise clear failure semantics.
