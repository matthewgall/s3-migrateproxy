package admin

import (
	"context"
	"crypto/subtle"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/matthewgall/s3-migrateproxy/internal/config"
	"github.com/matthewgall/s3-migrateproxy/internal/metadata"
	"github.com/matthewgall/s3-migrateproxy/internal/providers"
)

// Server is the administrative HTTP API server.
type Server struct {
	cfg    *config.Config
	pool   *pgxpool.Pool
	store  *metadata.Store
	src    providers.Provider
	dst    providers.Provider
	logger *slog.Logger
}

// NewServer constructs an admin server.
func NewServer(cfg *config.Config, pool *pgxpool.Pool, src, dst providers.Provider, logger *slog.Logger) *Server {
	return &Server{
		cfg:    cfg,
		pool:   pool,
		store:  metadata.NewStore(pool),
		src:    src,
		dst:    dst,
		logger: logger,
	}
}

// Serve starts the admin listener and blocks until context cancelled.
func (s *Server) Serve(ctx context.Context) error {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /api/v1/status", s.auth(s.status))
	mux.HandleFunc("GET /api/v1/migration/progress", s.auth(s.progress))
	mux.HandleFunc("POST /api/v1/migration/start", s.auth(s.start))
	mux.HandleFunc("POST /api/v1/migration/pause", s.auth(s.pause))
	mux.HandleFunc("POST /api/v1/migration/resume", s.auth(s.resume))
	mux.HandleFunc("POST /api/v1/migration/reconcile", s.auth(s.reconcile))
	mux.HandleFunc("POST /api/v1/migration/finalise", s.auth(s.finalise))
	mux.HandleFunc("GET /api/v1/migration/errors", s.auth(s.errors))
	mux.HandleFunc("GET /api/v1/migration/report", s.auth(s.report))
	mux.HandleFunc("GET /api/v1/migration/objects/", s.auth(s.objectStatus))
	mux.HandleFunc("POST /api/v1/migration/retry/", s.auth(s.retry))
	mux.HandleFunc("GET /api/v1/providers/health", s.auth(s.health))
	mux.HandleFunc("GET /livez", s.livez)
	mux.HandleFunc("GET /readyz", s.readyz)

	srv := &http.Server{
		Addr:              s.cfg.Admin.ListenAddr,
		Handler:           mux,
		ReadHeaderTimeout: s.cfg.Admin.ReadHeaderTimeout,
	}
	go func() {
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = srv.Shutdown(shutdownCtx)
	}()
	s.logger.Info("admin api listening", "addr", s.cfg.Admin.ListenAddr)
	return srv.ListenAndServe()
}

func (s *Server) auth(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		key := r.Header.Get("Authorization")
		const prefix = "Bearer "
		if !strings.HasPrefix(key, prefix) {
			http.Error(w, "unauthorized", http.StatusUnauthorized)
			return
		}
		token := strings.TrimPrefix(key, prefix)
		// Constant-time compare with configured admin key. In production use a hash.
		expected := s.cfg.Admin.APIKeyHash
		if expected == "" || subtle.ConstantTimeCompare([]byte(token), []byte(expected)) != 1 {
			http.Error(w, "unauthorized", http.StatusUnauthorized)
			return
		}
		next(w, r)
	}
}

func (s *Server) migrationOr404(w http.ResponseWriter, ctx context.Context) *metadata.Migration {
	mig, err := s.store.GetMigrationByName(ctx, s.cfg.MigrationName)
	if err != nil {
		http.Error(w, fmt.Sprintf("migration not found: %v", err), http.StatusNotFound)
		return nil
	}
	return mig
}

func (s *Server) status(w http.ResponseWriter, r *http.Request) {
	mig := s.migrationOr404(w, r.Context())
	if mig == nil {
		return
	}
	writeJSON(w, map[string]any{
		"migration_id": mig.ID,
		"name":         mig.Name,
		"state":        mig.State,
		"updated_at":   mig.UpdatedAt,
	})
}

func (s *Server) progress(w http.ResponseWriter, r *http.Request) {
	mig := s.migrationOr404(w, r.Context())
	if mig == nil {
		return
	}
	stats, err := s.store.Stats(r.Context(), mig.ID)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	writeJSON(w, map[string]any{
		"state": mig.State,
		"stats": stats,
	})
}

func (s *Server) start(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	mig := s.migrationOr404(w, ctx)
	if mig == nil {
		return
	}
	if err := s.store.UpdateMigrationState(ctx, mig.ID, "LIVE_MIGRATING"); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	writeJSON(w, map[string]any{"state": "LIVE_MIGRATING"})
}

func (s *Server) pause(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	mig := s.migrationOr404(w, ctx)
	if mig == nil {
		return
	}
	if err := s.store.UpdateMigrationState(ctx, mig.ID, "PAUSED"); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	writeJSON(w, map[string]any{"state": "PAUSED"})
}

func (s *Server) resume(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	mig := s.migrationOr404(w, ctx)
	if mig == nil {
		return
	}
	if err := s.store.UpdateMigrationState(ctx, mig.ID, "LIVE_MIGRATING"); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	writeJSON(w, map[string]any{"state": "LIVE_MIGRATING"})
}

func (s *Server) reconcile(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	mig := s.migrationOr404(w, ctx)
	if mig == nil {
		return
	}
	// Reset the source discovery checkpoint so the next worker run rescans the bucket.
	if err := s.store.RecordCheckpoint(ctx, mig.ID, "source", "", "", 0, 0); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	if err := s.store.UpdateMigrationState(ctx, mig.ID, "RECONCILING"); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	writeJSON(w, map[string]any{"state": "RECONCILING", "checkpoint_reset": true})
}

func (s *Server) errors(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	mig := s.migrationOr404(w, ctx)
	if mig == nil {
		return
	}
	rows, err := s.store.Pool().Query(ctx, `
		SELECT key, source_version_id, attempt_count, last_error, updated_at
		FROM metadata.migration_tasks
		WHERE migration_id = $1 AND state = 'failed'
		ORDER BY updated_at DESC
		LIMIT 100`, mig.ID)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	defer rows.Close()
	var out []map[string]any
	for rows.Next() {
		var key, ver, lastErr string
		var attempts int
		var updated time.Time
		if err := rows.Scan(&key, &ver, &attempts, &lastErr, &updated); err != nil {
			continue
		}
		out = append(out, map[string]any{"key": key, "source_version_id": ver, "attempts": attempts, "error": lastErr, "updated_at": updated})
	}
	writeJSON(w, out)
}

func (s *Server) report(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	mig := s.migrationOr404(w, ctx)
	if mig == nil {
		return
	}
	rep, err := s.store.GenerateReport(ctx, mig.ID)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	writeJSON(w, map[string]any{
		"state":              mig.State,
		"total_objects":      rep.TotalObjects,
		"migrated_objects":   rep.MigratedObjects,
		"verified_objects":   rep.VerifiedObjects,
		"failed_objects":     rep.FailedObjects,
		"skipped_objects":    rep.SkippedObjects,
		"tombstone_objects":  rep.TombstoneObjects,
		"queue_depth":        rep.QueueDepth,
		"can_finalise":       rep.CanFinalise,
		"discrepancies":      rep.DiscrepancyKeys,
	})
}

func (s *Server) finalise(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	mig := s.migrationOr404(w, ctx)
	if mig == nil {
		return
	}
	rep, err := s.store.GenerateReport(ctx, mig.ID)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	if !rep.CanFinalise {
		writeJSON(w, map[string]any{"ok": false, "can_finalise": false, "discrepancies": rep.DiscrepancyKeys, "queue_depth": rep.QueueDepth})
		return
	}
	if err := s.store.UpdateMigrationState(ctx, mig.ID, "FINALISING"); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	// In a real deployment a background routine would move FINALISING -> COMPLETED.
	if err := s.store.UpdateMigrationState(ctx, mig.ID, "COMPLETED"); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	writeJSON(w, map[string]any{"ok": true, "state": "COMPLETED"})
}

func (s *Server) objectStatus(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	mig := s.migrationOr404(w, ctx)
	if mig == nil {
		return
	}
	key := strings.TrimPrefix(r.URL.Path, "/api/v1/migration/objects/")
	state, err := s.store.GetObjectState(ctx, mig.ID, key)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	writeJSON(w, state)
}

func (s *Server) retry(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	mig := s.migrationOr404(w, ctx)
	if mig == nil {
		return
	}
	key := strings.TrimPrefix(r.URL.Path, "/api/v1/migration/retry/")
	_, err := s.store.Pool().Exec(ctx, `
		UPDATE metadata.migration_tasks
		SET state = 'pending', attempt_count = 0, last_error = ''
		WHERE migration_id = $1 AND key = $2`, mig.ID, key)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	writeJSON(w, map[string]any{"key": key, "state": "pending"})
}

func (s *Server) health(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	res := map[string]any{}
	if _, err := s.src.ListObjectsV2(ctx, providers.ListOptions{MaxKeys: 1}); err != nil {
		res["source"] = map[string]any{"ok": false, "error": err.Error()}
	} else {
		res["source"] = map[string]any{"ok": true}
	}
	if _, err := s.dst.ListObjectsV2(ctx, providers.ListOptions{MaxKeys: 1}); err != nil {
		res["destination"] = map[string]any{"ok": false, "error": err.Error()}
	} else {
		res["destination"] = map[string]any{"ok": true}
	}
	writeJSON(w, res)
}

func (s *Server) livez(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte("ok"))
}

func (s *Server) readyz(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	if err := s.pool.Ping(ctx); err != nil {
		http.Error(w, "database unavailable", http.StatusServiceUnavailable)
		return
	}
	if _, err := s.dst.ListObjectsV2(ctx, providers.ListOptions{MaxKeys: 1}); err != nil {
		http.Error(w, "destination unavailable", http.StatusServiceUnavailable)
		return
	}
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte("ok"))
}

func writeJSON(w http.ResponseWriter, v any) {
	w.Header().Set("Content-Type", "application/json")
	enc := json.NewEncoder(w)
	enc.SetIndent("", "  ")
	_ = enc.Encode(v)
}
