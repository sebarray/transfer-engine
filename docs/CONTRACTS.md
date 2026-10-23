# Interfaces and contracts (design guidance)

Treat these as semantic contracts rather than frozen method names; evolve them coherently when implementing.

## Business use case
```go
type CreateTransferCommand struct {
    ActorID, SourceAccountID, DestinationAccountID string
    Currency, IdempotencyKey string
    Amount int64 // positive minor units
}
type TransferUseCase interface {
    Execute(ctx context.Context, cmd CreateTransferCommand) (TransferResult, error)
}
type TransferResult struct {
    TransferID string
    Replayed bool
    Status string
}
```

## Repository and transaction boundary
```go
type TransactionManager interface {
    WithinTransaction(ctx context.Context, fn func(context.Context, TransferTx) error) error
}
type TransferTx interface {
    Accounts() AccountRepository
    Transfers() TransferRepository
    Ledger() LedgerRepository
    Idempotency() IdempotencyRepository
    Outbox() OutboxRepository
}
```

Postgres implementations must bind all transaction-scoped repositories to the *same tx*. Never start nested independent transactions for one money movement.

## Domain errors
Typed / sentinel errors at least: invalid amount, invalid currency, same account, account not found, inactive account, unauthorized operation, insufficient funds, idempotency payload mismatch, in-flight operation, concurrency conflict, transient dependency failure. Presentation maps to stable HTTP codes/error IDs; domain does not import HTTP.

## Event envelope
```json
{
  "event_id": "uuid",
  "event_type": "transfer.completed",
  "event_version": 1,
  "aggregate_id": "transfer-id",
  "occurred_at": "RFC3339 timestamp",
  "traceparent": "W3C trace context",
  "payload": {"transfer_id":"...","currency":"ARS","amount_minor":25000}
}
```

No sensitive authentication data or unnecessary PII in event payload. Consumer must validate event schema/version, dedupe by event ID and manage safe ack/replay. Do not put unbounded IDs into metric labels.
