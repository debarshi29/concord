-- 0001_init: forward-only. Rollback story: restore from backup / PITR (docs/runbook.md §9).

CREATE TABLE events (
    id               uuid        PRIMARY KEY,
    seq              bigserial   NOT NULL UNIQUE,            -- arrival order
    idempotency_key  text,
    aggregate_id     text,
    version          integer,
    event_type       text,
    raw_body         text        NOT NULL,                   -- exactly what was received
    payload          jsonb,                                  -- parsed envelope, NULL if unparsable
    signature_status text        NOT NULL CHECK (signature_status IN ('valid', 'invalid', 'missing')),
    status           text        NOT NULL CHECK (status IN (
                         'received', 'buffered', 'parked', 'processed',
                         'duplicate', 'stale', 'dead', 'rejected')),
    reject_reason    text,
    attempts         integer     NOT NULL DEFAULT 0,
    next_attempt_at  timestamptz NOT NULL,
    last_error       text,
    traceparent      text,
    received_at      timestamptz NOT NULL,
    processed_at     timestamptz
);

-- the claim query: due, unprocessed events oldest-first
CREATE INDEX events_claim_idx ON events (next_attempt_at, seq) WHERE status = 'received';
-- draining an aggregate's buffer
CREATE INDEX events_buffered_idx ON events (aggregate_id, version) WHERE status IN ('buffered', 'parked');

CREATE TABLE idempotency (
    key        text        PRIMARY KEY,
    outcome    jsonb       NOT NULL,
    event_id   uuid        REFERENCES events (id),
    created_at timestamptz NOT NULL,
    expires_at timestamptz NOT NULL
);
CREATE INDEX idempotency_expires_idx ON idempotency (expires_at);

CREATE TABLE aggregates (
    id         text        PRIMARY KEY,
    state      jsonb       NOT NULL DEFAULT '{}'::jsonb,
    version    integer     NOT NULL DEFAULT 0,
    updated_at timestamptz NOT NULL
);

CREATE TABLE buffer (
    event_id         uuid        PRIMARY KEY REFERENCES events (id),
    aggregate_id     text        NOT NULL,
    awaiting_version integer     NOT NULL,                   -- version the aggregate needed when parked
    window_expires   timestamptz NOT NULL
);
CREATE INDEX buffer_aggregate_idx ON buffer (aggregate_id);
CREATE INDEX buffer_expiry_idx ON buffer (window_expires);

CREATE TABLE dlq (
    event_id    uuid        PRIMARY KEY REFERENCES events (id),
    attempts    integer     NOT NULL,
    last_error  text        NOT NULL,
    failed_at   timestamptz NOT NULL,
    resolved_at timestamptz,
    resolution  text        CHECK (resolution IN ('redriven', 'discarded'))
);
CREATE INDEX dlq_open_idx ON dlq (failed_at) WHERE resolved_at IS NULL;

-- The downstream "side effect" ledger. Deliberately NO unique constraint on (aggregate_id, version):
-- the duplicate-effect metric is count(*) - count(distinct (aggregate_id, version)), and a constraint
-- would turn the bug we are hunting into an exception instead of a measurement.
CREATE TABLE effect_log (
    id           bigserial   PRIMARY KEY,
    aggregate_id text        NOT NULL,
    version      integer     NOT NULL,
    event_id     uuid        NOT NULL,
    applied_at   timestamptz NOT NULL
);

CREATE TABLE corrections (
    id           bigserial   PRIMARY KEY,
    aggregate_id text        NOT NULL,
    kind         text        NOT NULL CHECK (kind IN ('missing', 'behind', 'divergent', 'local_ahead', 'orphan')),
    before       jsonb,
    after        jsonb,
    corrected_at timestamptz NOT NULL
);
CREATE INDEX corrections_time_idx ON corrections (corrected_at);

CREATE TABLE reconcile_runs (
    id           bigserial   PRIMARY KEY,
    started_at   timestamptz NOT NULL,
    finished_at  timestamptz NOT NULL,
    checked      integer     NOT NULL,
    corrected    integer     NOT NULL
);
