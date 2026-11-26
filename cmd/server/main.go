package main

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"github.com/Balandin-Pavel/go-backup-service/internal/api"
	"github.com/Balandin-Pavel/go-backup-service/internal/store"
	"github.com/Balandin-Pavel/go-backup-service/internal/worker"
	"github.com/jackc/pgx/v5/pgxpool"
)

func main() {
	log := slog.New(slog.NewJSONHandler(os.Stderr, nil))
	if err := run(log); err != nil {
		log.Error("server stopped", "error", err)
		os.Exit(1)
	}
}

func run(log *slog.Logger) error {
	dsn := os.Getenv("DATABASE_URL")
	sourceRoot := os.Getenv("BACKUP_SOURCE_ROOT")
	outputRoot := os.Getenv("BACKUP_OUTPUT_ROOT")
	if dsn == "" || sourceRoot == "" || outputRoot == "" {
		return errors.New("DATABASE_URL, BACKUP_SOURCE_ROOT and BACKUP_OUTPUT_ROOT are required")
	}
	sourceRoot, err := filepath.EvalSymlinks(sourceRoot)
	if err != nil {
		return err
	}
	if err := os.MkdirAll(outputRoot, 0700); err != nil {
		return err
	}
	outputRoot, err = filepath.EvalSymlinks(outputRoot)
	if err != nil {
		return err
	}
	if relative, err := filepath.Rel(sourceRoot, outputRoot); err != nil ||
		(relative != ".." && !strings.HasPrefix(relative, ".."+string(filepath.Separator))) {
		return errors.New("BACKUP_OUTPUT_ROOT must be outside BACKUP_SOURCE_ROOT")
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	config, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		return err
	}
	config.MaxConns = 10
	db, err := pgxpool.NewWithConfig(ctx, config)
	if err != nil {
		return err
	}
	defer db.Close()
	if err := db.Ping(ctx); err != nil {
		return err
	}
	repo := &store.Store{DB: db}
	if err := repo.Migrate(ctx); err != nil {
		return err
	}

	addr := os.Getenv("HTTP_ADDR")
	if addr == "" {
		addr = "127.0.0.1:8080"
	}
	server := &http.Server{
		Addr:              addr,
		Handler:           (&api.Server{Repo: repo, SourceRoot: sourceRoot, OutputRoot: outputRoot}).Handler(),
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       15 * time.Second,
		WriteTimeout:      5 * time.Minute,
		IdleTimeout:       time.Minute,
	}
	workerDone := make(chan struct{})
	go func() {
		defer close(workerDone)
		(&worker.Worker{
			Store: repo, SourceRoot: sourceRoot, OutputRoot: outputRoot, Logger: log,
		}).Run(ctx)
	}()
	serverErrors := make(chan error, 1)
	go func() { serverErrors <- server.ListenAndServe() }()
	log.Info("server started", "address", addr)
	select {
	case err := <-serverErrors:
		if !errors.Is(err, http.ErrServerClosed) {
			return err
		}
	case <-ctx.Done():
	}
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := server.Shutdown(shutdownCtx); err != nil {
		return err
	}
	select {
	case <-workerDone:
		return nil
	case <-shutdownCtx.Done():
		return shutdownCtx.Err()
	}
}
