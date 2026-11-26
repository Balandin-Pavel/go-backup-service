package store

import (
	"context"
	"embed"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/Balandin-Pavel/go-backup-service/internal/backup"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

//go:embed migrations/*.sql
var migrations embed.FS

var ErrNotFound = errors.New("job not found")
var ErrIdempotencyConflict = errors.New("idempotency key belongs to another source")

const columns = `id::text, source, status, attempts, max_attempts, archive_name,
	archive_size, last_error, created_at, updated_at, completed_at, idempotency_key`
const qualifiedColumns = `j.id::text, j.source, j.status, j.attempts, j.max_attempts,
	j.archive_name, j.archive_size, j.last_error, j.created_at, j.updated_at,
	j.completed_at, j.idempotency_key`

type Store struct{ DB *pgxpool.Pool }

func (s *Store) Migrate(ctx context.Context) error {
	tx, err := s.DB.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	if _, err = tx.Exec(ctx, "SELECT pg_advisory_xact_lock(82971501)"); err != nil {
		return err
	}
	if _, err = tx.Exec(ctx, `CREATE TABLE IF NOT EXISTS schema_migrations (
		version text PRIMARY KEY, applied_at timestamptz NOT NULL DEFAULT now()
	)`); err != nil {
		return err
	}
	files, err := migrations.ReadDir("migrations")
	if err != nil {
		return err
	}
	sort.Slice(files, func(i, j int) bool { return files[i].Name() < files[j].Name() })
	for _, file := range files {
		version := file.Name()
		if !strings.HasSuffix(version, ".sql") {
			continue
		}
		var exists bool
		if err = tx.QueryRow(ctx, "SELECT EXISTS(SELECT 1 FROM schema_migrations WHERE version=$1)", version).Scan(&exists); err != nil {
			return err
		}
		if exists {
			continue
		}
		sql, readErr := migrations.ReadFile("migrations/" + version)
		if readErr != nil {
			return readErr
		}
		if _, err = tx.Exec(ctx, string(sql)); err != nil {
			return fmt.Errorf("migration %s: %w", version, err)
		}
		if _, err = tx.Exec(ctx, "INSERT INTO schema_migrations(version) VALUES ($1)", version); err != nil {
			return err
		}
	}
	return tx.Commit(ctx)
}

func scan(row pgx.Row) (backup.Job, error) {
	var j backup.Job
	err := row.Scan(&j.ID, &j.Source, &j.Status, &j.Attempts, &j.MaxAttempts,
		&j.ArchiveName, &j.ArchiveSize, &j.LastError, &j.CreatedAt, &j.UpdatedAt,
		&j.CompletedAt, &j.IdempotencyKey)
	return j, err
}

func (s *Store) Create(ctx context.Context, source, key string) (backup.Job, bool, error) {
	id, err := backup.NewID()
	if err != nil {
		return backup.Job{}, false, err
	}
	var nullableKey any
	if key != "" {
		nullableKey = key
	}
	j, err := scan(s.DB.QueryRow(ctx, `INSERT INTO backup_jobs(id, source, idempotency_key)
		VALUES ($1, $2, $3) ON CONFLICT (idempotency_key) DO NOTHING
		RETURNING `+columns, id, source, nullableKey))
	if err == nil {
		return j, true, nil
	}
	if !errors.Is(err, pgx.ErrNoRows) || key == "" {
		return backup.Job{}, false, err
	}
	j, err = scan(s.DB.QueryRow(ctx, `SELECT `+columns+` FROM backup_jobs WHERE idempotency_key=$1`, key))
	if err != nil {
		return backup.Job{}, false, err
	}
	if j.Source != source {
		return backup.Job{}, false, ErrIdempotencyConflict
	}
	return j, false, nil
}

func (s *Store) Get(ctx context.Context, id string) (backup.Job, error) {
	j, err := scan(s.DB.QueryRow(ctx, `SELECT `+columns+` FROM backup_jobs WHERE id=$1`, id))
	if errors.Is(err, pgx.ErrNoRows) {
		return backup.Job{}, ErrNotFound
	}
	return j, err
}

func (s *Store) List(ctx context.Context, limit int) ([]backup.Job, error) {
	rows, err := s.DB.Query(ctx, `SELECT `+columns+` FROM backup_jobs ORDER BY created_at DESC, id DESC LIMIT $1`, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	jobs := make([]backup.Job, 0)
	for rows.Next() {
		j, err := scan(rows)
		if err != nil {
			return nil, err
		}
		jobs = append(jobs, j)
	}
	return jobs, rows.Err()
}

func (s *Store) Cancel(ctx context.Context, id string) (backup.Job, error) {
	j, err := scan(s.DB.QueryRow(ctx, `UPDATE backup_jobs SET status='cancelled',
		lease_until=NULL, lease_token=NULL, updated_at=now()
		WHERE id=$1 AND status IN ('queued','running') RETURNING `+columns, id))
	if err == nil {
		return j, nil
	}
	if errors.Is(err, pgx.ErrNoRows) {
		return s.Get(ctx, id)
	}
	return backup.Job{}, err
}

func (s *Store) Claim(ctx context.Context, token string) (backup.Job, error) {
	_, err := s.DB.Exec(ctx, `UPDATE backup_jobs SET status='failed',
		last_error='worker lease expired after final attempt', lease_until=NULL,
		lease_token=NULL, updated_at=now()
		WHERE status='running' AND lease_until < now() AND attempts >= max_attempts`)
	if err != nil {
		return backup.Job{}, err
	}
	j, err := scan(s.DB.QueryRow(ctx, `WITH candidate AS (
		SELECT id FROM backup_jobs
		WHERE (status='queued' AND next_attempt_at<=now())
		   OR (status='running' AND lease_until<now() AND attempts<max_attempts)
		ORDER BY created_at, id FOR UPDATE SKIP LOCKED LIMIT 1
	)
	UPDATE backup_jobs AS j SET status='running', attempts=j.attempts+1,
		lease_token=$1, lease_until=now()+interval '30 seconds', updated_at=now()
	FROM candidate WHERE j.id=candidate.id RETURNING `+qualifiedColumns, token))
	if err == nil {
		j.LeaseToken = token
	}
	return j, err
}

func (s *Store) Renew(ctx context.Context, id, token string) (bool, error) {
	tag, err := s.DB.Exec(ctx, `UPDATE backup_jobs SET lease_until=now()+interval '30 seconds',
		updated_at=now() WHERE id=$1 AND lease_token=$2 AND status='running'`, id, token)
	return tag.RowsAffected() == 1, err
}

func (s *Store) Complete(ctx context.Context, id, token, name string, size int64) (bool, error) {
	tag, err := s.DB.Exec(ctx, `UPDATE backup_jobs SET status='completed',
		archive_name=$3, archive_size=$4, completed_at=now(), updated_at=now(),
		lease_until=NULL, lease_token=NULL, last_error=NULL
		WHERE id=$1 AND lease_token=$2 AND status='running'`, id, token, name, size)
	return tag.RowsAffected() == 1, err
}

func (s *Store) Retry(ctx context.Context, id, token string, cause error) error {
	msg := cause.Error()
	if len(msg) > 500 {
		msg = msg[:500]
	}
	_, err := s.DB.Exec(ctx, `UPDATE backup_jobs SET
		status=CASE WHEN attempts>=max_attempts THEN 'failed' ELSE 'queued' END,
		next_attempt_at=now()+make_interval(secs=>LEAST(60, power(2, attempts)::integer)),
		last_error=$3, lease_until=NULL, lease_token=NULL, updated_at=now()
		WHERE id=$1 AND lease_token=$2 AND status='running'`, id, token, msg)
	return err
}

func (s *Store) Ping(ctx context.Context) error {
	pingCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	return s.DB.Ping(pingCtx)
}
