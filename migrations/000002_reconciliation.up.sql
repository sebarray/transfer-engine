BEGIN;
CREATE TABLE reconciliation_settings (
 id boolean PRIMARY KEY DEFAULT true CHECK (id),
 enabled boolean NOT NULL DEFAULT false,
 interval_seconds integer NOT NULL DEFAULT 60 CHECK (interval_seconds BETWEEN 1 AND 86400),
 version bigint NOT NULL DEFAULT 1 CHECK (version>0)
);
INSERT INTO reconciliation_settings DEFAULT VALUES;
CREATE TABLE reconciliation_state (
 id boolean PRIMARY KEY DEFAULT true CHECK (id),
 fence bigint NOT NULL DEFAULT 0 CHECK (fence>=0),
 lease_until timestamptz NOT NULL DEFAULT '-infinity',
 accounts bigint CHECK (accounts>=0),
 mismatches bigint CHECK (mismatches>=0),
 checked_at timestamptz
);
INSERT INTO reconciliation_state DEFAULT VALUES;
COMMIT;
