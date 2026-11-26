package api

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/Balandin-Pavel/go-backup-service/internal/backup"
	"github.com/Balandin-Pavel/go-backup-service/internal/store"
)

type Repository interface {
	Create(context.Context, string, string) (backup.Job, bool, error)
	Get(context.Context, string) (backup.Job, error)
	List(context.Context, int) ([]backup.Job, error)
	Cancel(context.Context, string) (backup.Job, error)
	Ping(context.Context) error
}

type Server struct {
	Repo       Repository
	SourceRoot string
	OutputRoot string
}

func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /health/live", func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
	})
	mux.HandleFunc("GET /health/ready", func(w http.ResponseWriter, r *http.Request) {
		if err := s.Repo.Ping(r.Context()); err != nil {
			writeError(w, http.StatusServiceUnavailable, "database unavailable")
			return
		}
		writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
	})
	mux.HandleFunc("POST /v1/backups", s.create)
	mux.HandleFunc("GET /v1/backups", s.list)
	mux.HandleFunc("GET /v1/backups/{id}", s.get)
	mux.HandleFunc("POST /v1/backups/{id}/cancel", s.cancel)
	mux.HandleFunc("GET /v1/backups/{id}/archive", s.download)
	return mux
}

func (s *Server) create(w http.ResponseWriter, r *http.Request) {
	var body struct {
		Source string `json:"source"`
	}
	decoder := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<20))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&body); err != nil {
		writeError(w, http.StatusBadRequest, "invalid JSON body")
		return
	}
	if err := decoder.Decode(new(any)); err != io.EOF {
		writeError(w, http.StatusBadRequest, "expected one JSON object")
		return
	}
	source := filepath.Clean(body.Source)
	if _, err := backup.ValidateSource(s.SourceRoot, body.Source); err != nil {
		writeError(w, http.StatusBadRequest, "source must name an existing directory inside BACKUP_SOURCE_ROOT without symbolic links")
		return
	}
	key := r.Header.Get("Idempotency-Key")
	if len(key) > 128 || strings.TrimSpace(key) != key {
		writeError(w, http.StatusBadRequest, "invalid Idempotency-Key")
		return
	}
	j, created, err := s.Repo.Create(r.Context(), source, key)
	if errors.Is(err, store.ErrIdempotencyConflict) {
		writeError(w, http.StatusConflict, err.Error())
		return
	}
	if err != nil {
		writeError(w, http.StatusInternalServerError, "could not create backup job")
		return
	}
	w.Header().Set("Location", "/v1/backups/"+j.ID)
	if created {
		writeJSON(w, http.StatusAccepted, j)
		return
	}
	writeJSON(w, http.StatusOK, j)
}

func (s *Server) get(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	if !validID(id) {
		writeError(w, http.StatusBadRequest, "invalid backup ID")
		return
	}
	j, err := s.Repo.Get(r.Context(), id)
	if errors.Is(err, store.ErrNotFound) {
		writeError(w, http.StatusNotFound, "backup not found")
		return
	}
	if err != nil {
		writeError(w, http.StatusInternalServerError, "could not load backup")
		return
	}
	writeJSON(w, http.StatusOK, j)
}

func (s *Server) list(w http.ResponseWriter, r *http.Request) {
	limit := 20
	if raw := r.URL.Query().Get("limit"); raw != "" {
		n, err := strconv.Atoi(raw)
		if err != nil || n < 1 || n > 100 {
			writeError(w, http.StatusBadRequest, "limit must be between 1 and 100")
			return
		}
		limit = n
	}
	jobs, err := s.Repo.List(r.Context(), limit)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "could not list backups")
		return
	}
	writeJSON(w, http.StatusOK, jobs)
}

func (s *Server) cancel(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	if !validID(id) {
		writeError(w, http.StatusBadRequest, "invalid backup ID")
		return
	}
	j, err := s.Repo.Cancel(r.Context(), id)
	if errors.Is(err, store.ErrNotFound) {
		writeError(w, http.StatusNotFound, "backup not found")
		return
	}
	if err != nil {
		writeError(w, http.StatusInternalServerError, "could not cancel backup")
		return
	}
	if j.Status != backup.Cancelled {
		writeError(w, http.StatusConflict, "backup is already finished")
		return
	}
	writeJSON(w, http.StatusOK, j)
}

func (s *Server) download(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	if !validID(id) {
		writeError(w, http.StatusBadRequest, "invalid backup ID")
		return
	}
	j, err := s.Repo.Get(r.Context(), id)
	if errors.Is(err, store.ErrNotFound) {
		writeError(w, http.StatusNotFound, "backup not found")
		return
	}
	if err != nil {
		writeError(w, http.StatusInternalServerError, "could not load backup")
		return
	}
	if j.Status != backup.Completed || j.ArchiveName == nil {
		writeError(w, http.StatusConflict, "backup is not complete")
		return
	}
	name := id + ".tar.gz"
	if *j.ArchiveName != name {
		writeError(w, http.StatusInternalServerError, "invalid archive name")
		return
	}
	path := filepath.Join(s.OutputRoot, name)
	if _, err := os.Stat(path); err != nil {
		writeError(w, http.StatusNotFound, "archive file is missing")
		return
	}
	w.Header().Set("Content-Type", "application/gzip")
	w.Header().Set("Content-Disposition", "attachment; filename=\""+name+"\"")
	http.ServeFile(w, r, path)
}

func validID(id string) bool {
	if len(id) != 36 {
		return false
	}
	for i, c := range id {
		if i == 8 || i == 13 || i == 18 || i == 23 {
			if c != '-' {
				return false
			}
		} else if !strings.ContainsRune("0123456789abcdef", c) {
			return false
		}
	}
	_, err := hex.DecodeString(strings.ReplaceAll(id, "-", ""))
	return err == nil
}

func writeJSON(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(value)
}

func writeError(w http.ResponseWriter, status int, message string) {
	writeJSON(w, status, map[string]string{"error": message})
}
