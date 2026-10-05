CREATE TABLE IF NOT EXISTS runs (
    id               text PRIMARY KEY,
    task_id          text NOT NULL,
    status           text NOT NULL CHECK (status IN ('queued', 'executing', 'completed', 'failed', 'cancelled')),
    payload          jsonb NOT NULL,
    output           jsonb,
    error            text,
    attempt          integer NOT NULL DEFAULT 0,
    queue            text NOT NULL,
    concurrency_key  text,
    idempotency_key  text,
    run_at           timestamptz NOT NULL DEFAULT now(),
    locked_by        text,
    lease_until      timestamptz,
    trace_id         text,
    span_id          text,
    created_at       timestamptz NOT NULL DEFAULT now(),
    updated_at       timestamptz NOT NULL DEFAULT now()
);

-- One run per idempotency key and task: triggering twice returns the first run.
CREATE UNIQUE INDEX IF NOT EXISTS runs_idempotency ON runs (task_id, idempotency_key) WHERE idempotency_key IS NOT NULL;
-- What the reaper scans for: executing runs whose worker stopped renewing the lease.
CREATE INDEX IF NOT EXISTS runs_expired_leases ON runs (lease_until) WHERE status = 'executing';
-- What reconciliation scans for: runs that should be in a Redis queue.
CREATE INDEX IF NOT EXISTS runs_queued ON runs (run_at) WHERE status = 'queued';

CREATE TABLE IF NOT EXISTS run_events (
    id       bigserial PRIMARY KEY,
    run_id   text NOT NULL REFERENCES runs (id) ON DELETE CASCADE,
    type     text NOT NULL,
    attempt  integer NOT NULL,
    detail   text,
    at       timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS run_events_run ON run_events (run_id, id);
