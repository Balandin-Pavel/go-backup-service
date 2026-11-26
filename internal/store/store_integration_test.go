package store_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/Balandin-Pavel/go-backup-service/internal/api"
	"github.com/Balandin-Pavel/go-backup-service/internal/backup"
	"github.com/Balandin-Pavel/go-backup-service/internal/store"
	"github.com/Balandin-Pavel/go-backup-service/internal/worker"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

func testStore(t *testing.T) *store.Store {
	t.Helper()
	dsn := os.Getenv("TEST_DATABASE_URL")
	if dsn == "" {
		t.Skip("set TEST_DATABASE_URL to run PostgreSQL integration tests")
	}
	ctx := context.Background()
	admin, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	id, err := backup.NewID()
	if err != nil {
		t.Fatal(err)
	}
	schema := "test_" + strings.ReplaceAll(id, "-", "")
	if _, err := admin.Exec(ctx, "CREATE SCHEMA "+schema); err != nil {
		admin.Close()
		t.Fatal(err)
	}
	cfg, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		t.Fatal(err)
	}
	cfg.ConnConfig.RuntimeParams["search_path"] = schema
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		pool.Close()
		if _, err := admin.Exec(ctx, "DROP SCHEMA "+schema+" CASCADE"); err != nil {
			t.Errorf("drop test schema: %v", err)
		}
		admin.Close()
	})
	repo := &store.Store{DB: pool}
	if err := repo.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	if err := repo.Migrate(ctx); err != nil {
		t.Fatalf("migration is not idempotent: %v", err)
	}
	return repo
}

func TestStoreLifecycle(t *testing.T) {
	repo := testStore(t)
	ctx := context.Background()
	first, created, err := repo.Create(ctx, "reports", "request-1")
	if err != nil || !created {
		t.Fatalf("create: %v, created=%t", err, created)
	}
	again, created, err := repo.Create(ctx, "reports", "request-1")
	if err != nil || created || again.ID != first.ID {
		t.Fatalf("idempotency: %v, created=%t", err, created)
	}
	if _, _, err := repo.Create(ctx, "other", "request-1"); !errors.Is(err, store.ErrIdempotencyConflict) {
		t.Fatalf("expected idempotency conflict, got %v", err)
	}
	token, _ := backup.NewID()
	claimed, err := repo.Claim(ctx, token)
	if err != nil || claimed.ID != first.ID || claimed.Attempts != 1 {
		t.Fatalf("claim: %#v, %v", claimed, err)
	}
	if _, err := repo.Claim(ctx, token); !errors.Is(err, pgx.ErrNoRows) {
		t.Fatalf("double claim: %v", err)
	}
	if ok, err := repo.Renew(ctx, first.ID, token); err != nil || !ok {
		t.Fatalf("renew: %t, %v", ok, err)
	}
	if ok, err := repo.Complete(ctx, first.ID, token, first.ID+".tar.gz", 42); err != nil || !ok {
		t.Fatalf("complete: %t, %v", ok, err)
	}
	done, err := repo.Get(ctx, first.ID)
	if err != nil || done.Status != backup.Completed || done.ArchiveSize == nil || *done.ArchiveSize != 42 {
		t.Fatalf("completed job: %#v, %v", done, err)
	}
	retryJob, _, err := repo.Create(ctx, "retry", "")
	if err != nil {
		t.Fatal(err)
	}
	oldToken, _ := backup.NewID()
	claimed, err = repo.Claim(ctx, oldToken)
	if err != nil || claimed.ID != retryJob.ID {
		t.Fatalf("claim retry job: %#v, %v", claimed, err)
	}
	if err := repo.Retry(ctx, retryJob.ID, oldToken, errors.New("transient")); err != nil {
		t.Fatal(err)
	}
	if _, err := repo.DB.Exec(ctx, "UPDATE backup_jobs SET next_attempt_at=now() WHERE id=$1", retryJob.ID); err != nil {
		t.Fatal(err)
	}
	newToken, _ := backup.NewID()
	claimed, err = repo.Claim(ctx, newToken)
	if err != nil || claimed.ID != retryJob.ID || claimed.Attempts != 2 {
		t.Fatalf("reclaim retry job: %#v, %v", claimed, err)
	}
	if ok, err := repo.Complete(ctx, retryJob.ID, oldToken, "stale.tar.gz", 1); err != nil || ok {
		t.Fatalf("old token completed job: %t, %v", ok, err)
	}
	if _, err := repo.Cancel(ctx, retryJob.ID); err != nil {
		t.Fatal(err)
	}
	queued, _, err := repo.Create(ctx, "other", "")
	if err != nil {
		t.Fatal(err)
	}
	cancelled, err := repo.Cancel(ctx, queued.ID)
	if err != nil || cancelled.Status != backup.Cancelled {
		t.Fatalf("cancel: %#v, %v", cancelled, err)
	}
}

func TestHTTPToArchive(t *testing.T) {
	repo := testStore(t)
	sourceRoot := t.TempDir()
	outputRoot := t.TempDir()
	if err := os.Mkdir(filepath.Join(sourceRoot, "notes"), 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(sourceRoot, "notes", "hello.txt"), []byte("verified"), 0600); err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer((&api.Server{Repo: repo, SourceRoot: sourceRoot, OutputRoot: outputRoot}).Handler())
	defer server.Close()
	request, err := http.NewRequest(http.MethodPost, server.URL+"/v1/backups", bytes.NewBufferString(`{"source":"notes"}`))
	if err != nil {
		t.Fatal(err)
	}
	request.Header.Set("Idempotency-Key", "integration-1")
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusAccepted {
		body, _ := io.ReadAll(response.Body)
		t.Fatalf("POST status %d: %s", response.StatusCode, body)
	}
	var created backup.Job
	if err := json.NewDecoder(response.Body).Decode(&created); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go (&worker.Worker{
		Store: repo, SourceRoot: sourceRoot, OutputRoot: outputRoot,
		Logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
	}).Run(ctx)
	deadline := time.Now().Add(8 * time.Second)
	for {
		job, err := repo.Get(context.Background(), created.ID)
		if err != nil {
			t.Fatal(err)
		}
		if job.Status == backup.Completed {
			break
		}
		if job.Status == backup.Failed || time.Now().After(deadline) {
			t.Fatalf("job did not complete: %#v", job)
		}
		time.Sleep(50 * time.Millisecond)
	}
	download, err := http.Get(fmt.Sprintf("%s/v1/backups/%s/archive", server.URL, created.ID))
	if err != nil {
		t.Fatal(err)
	}
	defer download.Body.Close()
	if download.StatusCode != http.StatusOK || download.Header.Get("Content-Type") != "application/gzip" {
		t.Fatalf("download status: %d, type: %s", download.StatusCode, download.Header.Get("Content-Type"))
	}
	body, err := io.ReadAll(download.Body)
	if err != nil || len(body) == 0 {
		t.Fatalf("empty archive: %v", err)
	}
}
