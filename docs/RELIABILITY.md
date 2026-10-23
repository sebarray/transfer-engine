# Reliability, async processing and failure modes

## Delivery guarantee
RabbitMQ delivery is at-least-once (duplicates possible). The goal is **effectively-once protected business effects**, not a false claim of exactly-once message delivery. DB commit and broker publish are not atomic; use transactional outbox.

## Outbox
- Write outbox event with money transaction.
- Publisher scans pending events with safe concurrent claim, e.g., `FOR UPDATE SKIP LOCKED` with bounded batch/lease.
- Publish persistent message and await publisher confirm; only then record published state. Handle crash after publish before mark: duplicates are expected; consumer deduplicates.
- Clear lease/allow reclaim on publisher crash; no permanent stuck event.

## Consumer lifecycle
- Validate envelope + event version; identify permanent vs retryable failures.
- Handler performs auxiliary side effects with durable processed_event / inbox record in the same DB transaction when possible. For external effects, provide target idempotency keys and document weaker guarantee if provider lacks idempotency.
- ACK only on successful processing / already processed event, or after confirming safe transfer to an alternate durable queue. Do not ACK then attempt a potentially lossy republish.
- Use publisher confirms for retry/DLQ handoff; account for confirm loss and duplicates.
- Configure `prefetch`, finite worker pool, graceful cancellation/shutdown, and redelivery after worker death.

## Retry / DLQ policy
Suggested configurable sample: initial attempt, then retry after about 5s, 30s, 2m with jitter (exact scheduled delay buckets can be approximate if using TTL queues). Max attempts finite; after exhaustion publish with reason/attempt count to DLQ. **Do not use infinite immediate NACK requeue loops.** Permanent failures (invalid payload, unsupported schema version) go to DLQ without futile retries. Prefer RabbitMQ dead-letter exchanges and per-delay retry queues or quorum queue delivery-limit as relevant; document actual broker semantics, TTL ordering considerations and queue durability.

## Replay
- Protected/admin-only replay command or CLI that inspects DLQ payload and metadata, supports dry run, records operator reason, bounded batch, validates contract and republishes with same event ID for dedup.
- Avoid DLQ replay storms and infinite retries; replay can fail again and return to DLQ.

## Ordering
Do not promise global queue ordering with competing workers and retries. Design consumers to be order-independent or enforce aggregate version guard if needed. A `transfer.completed` notification consumer can be designed independent of ordering.

## Retry and circuit breaker
Only retry classified transient external calls and transient serialization/deadlock DB errors when operations are safely idempotent. Exponential backoff + jitter, bounded by request deadline. Circuit breaker only on external auxiliary dependencies; broker failure must NOT roll back an already committed transfer.

## Failure-injection matrix
1. Broker down before publish: outbox retains pending event.
2. Publisher dies after confirm before DB mark: duplicate publish safe.
3. Consumer dies after side-effect commit before ACK: duplicate delivery safe.
4. Poison schema: DLQ with diagnostic metadata, no tight retry loop.
5. Retry budget exhausted: DLQ.
6. DLQ publish confirmation ambiguous: may duplicate, must not lose original prematurely.
7. Duplicate/out-of-order delivery: no duplicate auxiliary side effects.
8. Redis down: core financial invariant still upheld.
9. Commit outcome unknown: query idempotency state before any second financial attempt.
10. Process termination: stop intake, drain workers under deadline, release DB/broker connections.
