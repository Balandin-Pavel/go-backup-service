package worker

import (
	"context"
	"errors"
	"log/slog"
	"os"
	"path/filepath"
	"time"

	"github.com/Balandin-Pavel/go-backup-service/internal/backup"
	"github.com/Balandin-Pavel/go-backup-service/internal/store"
	"github.com/jackc/pgx/v5"
)

type Worker struct {
	Store      *store.Store
	SourceRoot string
	OutputRoot string
	Logger     *slog.Logger
}

func (w *Worker) Run(ctx context.Context) {
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for {
		if err := ctx.Err(); err != nil {
			return
		}
		token, err := backup.NewID()
		if err != nil {
			w.Logger.Error("generate lease token", "error", err)
			return
		}
		job, err := w.Store.Claim(ctx, token)
		switch {
		case errors.Is(err, pgx.ErrNoRows):
		case err != nil && ctx.Err() == nil:
			w.Logger.Error("claim backup job", "error", err)
		case err == nil:
			w.process(ctx, job)
			continue
		}
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

func (w *Worker) process(parent context.Context, job backup.Job) {
	ctx, cancel := context.WithCancel(parent)
	defer cancel()
	heartbeatDone := make(chan struct{})
	go func() {
		defer close(heartbeatDone)
		ticker := time.NewTicker(10 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				renewCtx, stop := context.WithTimeout(context.Background(), 3*time.Second)
				ok, err := w.Store.Renew(renewCtx, job.ID, job.LeaseToken)
				stop()
				if err != nil || !ok {
					w.Logger.Warn("lost backup lease", "job_id", job.ID, "error", err)
					cancel()
					return
				}
			}
		}
	}()

	name, size, err := backup.CreateArchive(ctx, w.SourceRoot, w.OutputRoot, job.Source, job.ID)
	cancel()
	<-heartbeatDone
	saveCtx, stop := context.WithTimeout(context.Background(), 5*time.Second)
	defer stop()
	if err != nil {
		if retryErr := w.Store.Retry(saveCtx, job.ID, job.LeaseToken, err); retryErr != nil {
			w.Logger.Error("save backup failure", "job_id", job.ID, "error", retryErr)
		}
		w.Logger.Warn("backup attempt failed", "job_id", job.ID, "attempt", job.Attempts, "error", err)
		return
	}
	ok, err := w.Store.Complete(saveCtx, job.ID, job.LeaseToken, name, size)
	if err != nil || !ok {
		os.Remove(filepath.Join(w.OutputRoot, name))
		w.Logger.Error("complete backup job", "job_id", job.ID, "error", err, "lease_valid", ok)
		return
	}
	w.Logger.Info("backup completed", "job_id", job.ID, "bytes", size)
}
