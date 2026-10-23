-- Destructive development rollback. Run only against a disposable database.
BEGIN;
DROP VIEW account_balance_reconciliation;
DROP TABLE processed_events;
DROP TABLE outbox_events;
DROP TABLE idempotency_records;
DROP TABLE transfers;
DROP TABLE postings;
DROP TABLE journal_entries;
DROP TABLE accounts;
DROP TABLE currencies;
DROP FUNCTION validate_balanced_journal();
DROP FUNCTION reject_ledger_mutation();
DROP FUNCTION guard_account_identity();
COMMIT;
