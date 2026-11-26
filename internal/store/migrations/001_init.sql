CREATE TABLE backup_jobs (
    id uuid PRIMARY KEY,
    source text NOT NULL,
    idempotency_key text UNIQUE,
    status text NOT NULL DEFAULT 'queued'
        CHECK (status IN ('queued', 'running', 'completed', 'failed', 'cancelled')),
    attempts integer NOT NULL DEFAULT 0 CHECK (attempts >= 0),
    max_attempts integer NOT NULL DEFAULT 3 CHECK (max_attempts BETWEEN 1 AND 10),
    next_attempt_at timestamptz NOT NULL DEFAULT now(),
    lease_until timestamptz,
    lease_token uuid,
    archive_name text,
    archive_size bigint,
    last_error text,
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now(),
    completed_at timestamptz
);

CREATE INDEX backup_jobs_ready_idx ON backup_jobs (next_attempt_at, created_at)
    WHERE status = 'queued';
CREATE INDEX backup_jobs_expired_idx ON backup_jobs (lease_until)
    WHERE status = 'running';
CREATE INDEX backup_jobs_created_idx ON backup_jobs (created_at DESC, id DESC);
