# Initial PostgreSQL schema

This schema stores simulated money only. The supported ISO currency subset is
ARS, USD, and EUR, each with two decimal minor units. Monetary columns are
`bigint`; journal validation and the reconciliation view use PostgreSQL's exact
`numeric` result of `sum(bigint)` to avoid overflow while summing.

Accounts begin at zero. Customer balances cannot become negative. One explicit
`test_treasury` account per currency may carry the negative counterpart of test
issuance. Account identity, ownership, kind and currency are immutable. A treasury
account must never be exposed as a customer account by future endpoints.

Each journal has exactly two postings: a negative debit and a positive credit.
Deferred constraint triggers validate the complete journal at commit, permitting
the application to insert its parts in the same transaction. Transfer journals
must match one transfer's accounts and amount. Issuance journals must debit the
test treasury and credit a customer. Composite foreign keys enforce the same
currency across accounts, journals, postings and transfers.

Unique partial indexes permit only one debit and one credit per journal. Once a
valid journal commits, these indexes prevent appending further postings. Triggers
reject updates, deletes and truncation of journals, postings and transfers. The
database owner can still change schema or disable triggers; these safeguards
protect normal DML and are not a substitute for database role separation.

`account_balance_reconciliation` exposes stored balance, summed postings and their
difference for every account, including the treasury. Application code must still
update balances and insert the journal, transfer, idempotency result and outbox in
one transaction. Phase 2 implements that business operation and verifies
rollback, overspend and opposing concurrent transfers through the real adapter.
Balance-to-ledger equality on arbitrary SQL account updates is not enforced;
application writes must use the transaction adapter.

The idempotency primary key is `(actor_id, operation, idempotency_key)` and its
fingerprint stores 32 bytes for SHA-256. Phase 2 includes canonical hashing,
bounded lock waits and basic replay. Phase 3 verifies independent-instance replay
and adds bounded ambiguous-commit resolution. `processed_events` scopes
deduplication by consumer and event. The outbox has a partial pending index;
publisher claim, confirms, fencing if leases are introduced, and delivery retry
semantics remain Phase 4 work. No asynchronous consumer may update balances.

Migration scripts own their `BEGIN`/`COMMIT` for psql invocation. The versioned
runner removes that outer wrapper and records the checksum inside its transaction.
Docker's initial database bootstrap runs up scripts only for a new volume.
The down migration drops all simulated records and is for disposable development
databases. Integration tests apply up/down/up and test real PostgreSQL constraints
using the same PostgreSQL image as Compose.

Migration 000002 adds optimistic reconciliation settings and a PostgreSQL-fenced
audit state. It never changes financial tables. See [phase 3](phase3.md) for
baseline adoption, migration history and worker activation.

PostgreSQL reference: [deferred constraint triggers](https://www.postgresql.org/docs/17/sql-createtrigger.html).
