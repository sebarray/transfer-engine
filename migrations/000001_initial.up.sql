-- Simulated balances only. This migration owns its transaction.
BEGIN;
CREATE TABLE currencies (
    code text PRIMARY KEY CHECK (code ~ '^[A-Z]{3}$'),
    minor_unit smallint NOT NULL CHECK (minor_unit BETWEEN 0 AND 4)
);
INSERT INTO currencies (code, minor_unit) VALUES ('ARS', 2), ('USD', 2), ('EUR', 2);

CREATE TABLE accounts (
    id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    owner_id text NOT NULL CHECK (length(btrim(owner_id)) > 0),
    kind text NOT NULL DEFAULT 'customer' CHECK (kind IN ('customer', 'test_treasury')),
    currency text NOT NULL REFERENCES currencies(code),
    status text NOT NULL DEFAULT 'active' CHECK (status IN ('active', 'closed')),
    balance_minor bigint NOT NULL DEFAULT 0,
    version bigint NOT NULL DEFAULT 1 CHECK (version > 0),
    created_at timestamptz NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT accounts_nonnegative_customer_balance CHECK (kind = 'test_treasury' OR balance_minor >= 0),
    UNIQUE (id, currency)
);
CREATE INDEX accounts_owner_id_idx ON accounts (owner_id, id);
CREATE INDEX accounts_currency_idx ON accounts (currency);
CREATE UNIQUE INDEX accounts_one_treasury_per_currency_idx ON accounts (currency) WHERE kind = 'test_treasury';

CREATE TABLE journal_entries (
    id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    kind text NOT NULL CHECK (kind IN ('transfer', 'test_issuance')),
    currency text NOT NULL REFERENCES currencies(code),
    created_at timestamptz NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE (id, currency)
);
CREATE INDEX journal_entries_currency_idx ON journal_entries (currency);

CREATE TABLE postings (
    id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    journal_entry_id uuid NOT NULL,
    account_id uuid NOT NULL,
    currency text NOT NULL,
    amount_minor bigint NOT NULL CHECK (amount_minor <> 0),
    created_at timestamptz NOT NULL DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (journal_entry_id, currency) REFERENCES journal_entries(id, currency),
    FOREIGN KEY (account_id, currency) REFERENCES accounts(id, currency),
    UNIQUE (journal_entry_id, account_id)
);
CREATE INDEX postings_account_movements_idx ON postings (account_id, id);
-- Every committed journal has one debit and one credit. These uniqueness
-- constraints plus immutability prevent appending even a balanced pair later.
CREATE UNIQUE INDEX postings_one_debit_per_journal_idx ON postings (journal_entry_id) WHERE amount_minor < 0;
CREATE UNIQUE INDEX postings_one_credit_per_journal_idx ON postings (journal_entry_id) WHERE amount_minor > 0;

CREATE TABLE transfers (
    id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    actor_id text NOT NULL CHECK (length(btrim(actor_id)) > 0),
    source_account_id uuid NOT NULL,
    destination_account_id uuid NOT NULL,
    currency text NOT NULL,
    amount_minor bigint NOT NULL CHECK (amount_minor > 0),
    journal_entry_id uuid NOT NULL UNIQUE,
    status text NOT NULL DEFAULT 'completed' CHECK (status = 'completed'),
    created_at timestamptz NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CHECK (source_account_id <> destination_account_id),
    FOREIGN KEY (source_account_id, currency) REFERENCES accounts(id, currency),
    FOREIGN KEY (destination_account_id, currency) REFERENCES accounts(id, currency),
    FOREIGN KEY (journal_entry_id, currency) REFERENCES journal_entries(id, currency)
);
CREATE INDEX transfers_source_idx ON transfers (source_account_id, created_at, id);
CREATE INDEX transfers_destination_idx ON transfers (destination_account_id, created_at, id);
CREATE INDEX transfers_actor_idx ON transfers (actor_id, created_at, id);

CREATE TABLE idempotency_records (
    actor_id text NOT NULL CHECK (length(btrim(actor_id)) > 0),
    operation text NOT NULL CHECK (length(btrim(operation)) > 0),
    idempotency_key text NOT NULL CHECK (length(idempotency_key) BETWEEN 1 AND 200),
    request_hash bytea NOT NULL CHECK (octet_length(request_hash) = 32),
    state text NOT NULL DEFAULT 'in_progress' CHECK (state IN ('in_progress', 'completed')),
    result jsonb,
    created_at timestamptz NOT NULL DEFAULT CURRENT_TIMESTAMP,
    completed_at timestamptz,
    PRIMARY KEY (actor_id, operation, idempotency_key),
    CHECK ((state = 'in_progress' AND result IS NULL AND completed_at IS NULL)
        OR (state = 'completed' AND result IS NOT NULL AND jsonb_typeof(result) = 'object' AND completed_at IS NOT NULL))
);

CREATE TABLE outbox_events (
    event_id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    event_type text NOT NULL CHECK (length(btrim(event_type)) > 0),
    event_version integer NOT NULL CHECK (event_version > 0),
    aggregate_id uuid NOT NULL,
    occurred_at timestamptz NOT NULL DEFAULT CURRENT_TIMESTAMP,
    traceparent text,
    payload jsonb NOT NULL CHECK (jsonb_typeof(payload) = 'object'),
    available_at timestamptz NOT NULL DEFAULT CURRENT_TIMESTAMP,
    published_at timestamptz,
    attempts integer NOT NULL DEFAULT 0 CHECK (attempts >= 0)
);
CREATE INDEX outbox_events_pending_idx ON outbox_events (available_at, event_id) WHERE published_at IS NULL;

CREATE TABLE processed_events (
    consumer_name text NOT NULL CHECK (length(btrim(consumer_name)) > 0),
    event_id uuid NOT NULL,
    processed_at timestamptz NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (consumer_name, event_id)
);

CREATE FUNCTION guard_account_identity() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF TG_OP = 'INSERT' THEN
        IF NEW.balance_minor <> 0 THEN
            RAISE EXCEPTION 'new accounts must start with zero balance' USING ERRCODE = '23514';
        END IF;
    ELSIF ROW(NEW.id, NEW.owner_id, NEW.kind, NEW.currency, NEW.created_at)
        IS DISTINCT FROM ROW(OLD.id, OLD.owner_id, OLD.kind, OLD.currency, OLD.created_at) THEN
        RAISE EXCEPTION 'account identity is immutable' USING ERRCODE = '23514';
    END IF;
    RETURN NEW;
END;
$$;
CREATE TRIGGER accounts_identity_guard BEFORE INSERT OR UPDATE ON accounts
    FOR EACH ROW EXECUTE FUNCTION guard_account_identity();

CREATE FUNCTION reject_ledger_mutation() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    RAISE EXCEPTION 'posted financial records are immutable' USING ERRCODE = '23514';
END;
$$;
CREATE TRIGGER journal_entries_immutable BEFORE UPDATE OR DELETE ON journal_entries
    FOR EACH ROW EXECUTE FUNCTION reject_ledger_mutation();
CREATE TRIGGER postings_immutable BEFORE UPDATE OR DELETE ON postings
    FOR EACH ROW EXECUTE FUNCTION reject_ledger_mutation();
CREATE TRIGGER transfers_immutable BEFORE UPDATE OR DELETE ON transfers
    FOR EACH ROW EXECUTE FUNCTION reject_ledger_mutation();
CREATE TRIGGER journal_entries_no_truncate BEFORE TRUNCATE ON journal_entries
    FOR EACH STATEMENT EXECUTE FUNCTION reject_ledger_mutation();
CREATE TRIGGER postings_no_truncate BEFORE TRUNCATE ON postings
    FOR EACH STATEMENT EXECUTE FUNCTION reject_ledger_mutation();
CREATE TRIGGER transfers_no_truncate BEFORE TRUNCATE ON transfers
    FOR EACH STATEMENT EXECUTE FUNCTION reject_ledger_mutation();

CREATE FUNCTION validate_balanced_journal() RETURNS trigger LANGUAGE plpgsql SET search_path = public, pg_temp AS $$
DECLARE
    journal_id uuid;
    journal_kind text;
    posting_count bigint;
    posting_total numeric;
BEGIN
    IF TG_TABLE_NAME = 'journal_entries' THEN
        journal_id := NEW.id;
    ELSE
        journal_id := NEW.journal_entry_id;
    END IF;
    SELECT kind INTO journal_kind FROM journal_entries WHERE id = journal_id;
    -- SUM(bigint) returns numeric, avoiding overflow in validation itself.
    SELECT count(*), sum(amount_minor) INTO posting_count, posting_total
        FROM postings WHERE journal_entry_id = journal_id;
    IF posting_count <> 2 OR posting_total IS DISTINCT FROM 0::numeric THEN
        RAISE EXCEPTION 'journal must contain exactly two balanced postings' USING ERRCODE = '23514';
    END IF;
    IF journal_kind = 'transfer' THEN
        IF NOT EXISTS (
            SELECT 1 FROM transfers t
            JOIN postings debit ON debit.journal_entry_id = t.journal_entry_id
                AND debit.account_id = t.source_account_id AND debit.amount_minor = -t.amount_minor
            JOIN postings credit ON credit.journal_entry_id = t.journal_entry_id
                AND credit.account_id = t.destination_account_id AND credit.amount_minor = t.amount_minor
            JOIN accounts source ON source.id = t.source_account_id AND source.kind = 'customer'
            JOIN accounts destination ON destination.id = t.destination_account_id AND destination.kind = 'customer'
            WHERE t.journal_entry_id = journal_id
        ) THEN
            RAISE EXCEPTION 'transfer must match its debit and credit postings' USING ERRCODE = '23514';
        END IF;
    ELSE
        IF EXISTS (SELECT 1 FROM transfers WHERE journal_entry_id = journal_id)
            OR NOT EXISTS (
                SELECT 1 FROM postings p JOIN accounts a ON a.id = p.account_id
                WHERE p.journal_entry_id = journal_id AND a.kind = 'test_treasury' AND p.amount_minor < 0
            ) OR NOT EXISTS (
                SELECT 1 FROM postings p JOIN accounts a ON a.id = p.account_id
                WHERE p.journal_entry_id = journal_id AND a.kind = 'customer' AND p.amount_minor > 0
            ) THEN
            RAISE EXCEPTION 'test issuance requires a treasury debit and customer credit' USING ERRCODE = '23514';
        END IF;
    END IF;
    RETURN NULL;
END;
$$;
CREATE CONSTRAINT TRIGGER journal_entries_balanced AFTER INSERT ON journal_entries
    DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION validate_balanced_journal();
CREATE CONSTRAINT TRIGGER postings_balanced AFTER INSERT ON postings
    DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION validate_balanced_journal();
CREATE CONSTRAINT TRIGGER transfers_balanced AFTER INSERT ON transfers
    DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION validate_balanced_journal();

-- Reconciliation is deliberately visible: application transactions must update
-- both materialized balances and postings together. Phase 2 supplies that path.
CREATE VIEW account_balance_reconciliation AS
    SELECT a.id AS account_id, a.currency, a.balance_minor,
        COALESCE(sum(p.amount_minor), 0::numeric) AS ledger_balance_minor,
        a.balance_minor::numeric - COALESCE(sum(p.amount_minor), 0::numeric) AS difference_minor
    FROM accounts a LEFT JOIN postings p ON p.account_id = a.id
    GROUP BY a.id;
COMMIT;
