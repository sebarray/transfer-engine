BEGIN;
CREATE TABLE transfer_notifications (
 event_id uuid PRIMARY KEY,
 transfer_id uuid NOT NULL UNIQUE REFERENCES transfers(id),
 currency text NOT NULL REFERENCES currencies(code),
 amount_minor bigint NOT NULL CHECK(amount_minor>0),
 created_at timestamptz NOT NULL DEFAULT CURRENT_TIMESTAMP
);
CREATE TABLE dlq_replay_audit (
 id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
 event_id uuid NOT NULL,
 operator_id text NOT NULL CHECK(length(operator_id) BETWEEN 1 AND 200),
 reason text NOT NULL CHECK(length(reason) BETWEEN 1 AND 500),
 requested_at timestamptz NOT NULL DEFAULT CURRENT_TIMESTAMP,
 confirmed_at timestamptz
);
CREATE INDEX dlq_replay_event_idx ON dlq_replay_audit(event_id,requested_at);
COMMIT;
