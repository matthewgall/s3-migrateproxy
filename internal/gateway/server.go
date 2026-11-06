package gateway

import (
	"context"
	"log/slog"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/matthewgall/s3-migrateproxy/internal/auth"
	"github.com/matthewgall/s3-migrateproxy/internal/config"
	"github.com/matthewgall/s3-migrateproxy/internal/metadata"
	"github.com/matthewgall/s3-migrateproxy/internal/metrics"
	"github.com/matthewgall/s3-migrateproxy/internal/providers"
)

// Server is the S3-compatible gateway HTTP server.
type Server struct {
	cfg       *config.Config
	listener  string
	validator *auth.Validator
	store     *metadata.Store
	src       providers.Provider
	dst       providers.Provider
	logger    *slog.Logger
}

// NewServer constructs a gateway server.
func NewServer(cfg *config.Config, pool *pgxpool.Pool, src, dst providers.Provider, logger *slog.Logger) *Server {
	credStore := auth.NewStore(pool, cfg.MasterKey)
	return &Server{
		cfg:       cfg,
		listener:  cfg.Gateway.ListenAddr,
		validator: auth.NewValidator(credStore, cfg.Destination.Region),
		store:     metadata.NewStore(pool),
		src:       src,
		dst:       dst,
		logger:    logger,
	}
}

// Serve starts the gateway listener and blocks.
func (s *Server) Serve(ctx context.Context) error {
	mux := http.NewServeMux()
	mux.Handle("/", s)
	srv := &http.Server{
		Addr:              s.listener,
		Handler:           mux,
		ReadHeaderTimeout: s.cfg.Gateway.ReadHeaderTimeout,
		ReadTimeout:       s.cfg.Gateway.ReadTimeout,
		WriteTimeout:      s.cfg.Gateway.WriteTimeout,
		MaxHeaderBytes:    1 << 20,
	}
	go func() {
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 10*time.Second)
		defer cancel()
		_ = srv.Shutdown(shutdownCtx)
	}()
	s.logger.Info("s3 gateway listening", "addr", s.listener)
	return srv.ListenAndServe()
}

// ServeHTTP dispatches S3 API requests and logs each call.
func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	start := time.Now()
	rec := &responseRecorder{ResponseWriter: w, statusCode: http.StatusOK}
	s.serveHTTP(rec, r)

	elapsed := time.Since(start)
	op := operationName(r)
	statusStr := strconv.Itoa(rec.statusCode)
	metrics.RequestCount.WithLabelValues(op, statusStr).Inc()
	metrics.RequestLatency.WithLabelValues(op).Observe(elapsed.Seconds())
	s.logger.Info("s3 request",
		slog.String("method", r.Method),
		slog.String("path", r.URL.Path),
		slog.String("operation", op),
		slog.String("status", statusStr),
		slog.Int64("bytes", rec.bytes),
		slog.Duration("duration", elapsed),
	)
}

func (s *Server) serveHTTP(w http.ResponseWriter, r *http.Request) {
	cred, err := s.validator.Authenticate(r.Context(), s.cfg, r)
	if err != nil {
		s.writeError(w, r, "AccessDenied", err.Error(), http.StatusForbidden)
		return
	}
	_ = cred

	// Load migration record.
	mig, err := s.store.GetMigrationByName(r.Context(), s.cfg.MigrationName)
	if err != nil {
		s.writeError(w, r, "InternalError", "migration not configured", http.StatusInternalServerError)
		return
	}

	// Path is expected to be /<bucket>/<key>; for virtual-hosted style we map to dest bucket.
	bucket, key := splitPath(r.URL.Path)
	_ = bucket

	switch r.Method {
	case http.MethodGet:
		q := r.URL.Query()
		if key == "" && q.Get("location") != "" {
			s.handleGetBucketLocation(w, r)
			return
		}
		if q.Get("list-type") == "2" {
			s.handleListObjectsV2(w, r, mig.ID)
			return
		}
		if q.Get("uploadId") != "" {
			s.handleListParts(w, r, mig.ID, key, q.Get("uploadId"))
			return
		}
		s.handleGetObject(w, r, mig.ID, key)
	case http.MethodHead:
		if key == "" {
			s.handleHeadBucket(w, r)
			return
		}
		s.handleHeadObject(w, r, mig.ID, key)
	case http.MethodPut:
		q := r.URL.Query()
		if r.Header.Get("X-Amz-Copy-Source") != "" {
			s.handleCopyObject(w, r, mig.ID, key)
		} else if q.Get("uploadId") != "" {
			s.handleUploadPart(w, r, mig.ID, key, q.Get("uploadId"))
		} else if key == "" && q.Get("uploads") != "" {
			s.handleCreateMultipartUpload(w, r, mig.ID)
		} else {
			s.handlePutObject(w, r, mig.ID, key)
		}
	case http.MethodDelete:
		if r.URL.Query().Get("uploadId") != "" {
			s.handleAbortMultipartUpload(w, r, mig.ID, key, r.URL.Query().Get("uploadId"))
		} else {
			s.handleDeleteObject(w, r, mig.ID, key)
		}
	case http.MethodPost:
		q := r.URL.Query()
		if q.Get("uploadId") != "" {
			s.handleCompleteMultipartUpload(w, r, mig.ID, key, q.Get("uploadId"))
		} else if key == "" && q.Get("uploads") != "" {
			s.handleListMultipartUploads(w, r, mig.ID)
		} else if key == "" && q.Get("delete") != "" {
			s.handleDeleteObjects(w, r, mig.ID)
		}
	default:
		s.writeError(w, r, "MethodNotAllowed", "method not allowed", http.StatusMethodNotAllowed)
	}
}

type responseRecorder struct {
	http.ResponseWriter
	statusCode int
	bytes      int64
}

func (r *responseRecorder) WriteHeader(status int) {
	r.statusCode = status
	r.ResponseWriter.WriteHeader(status)
}

func (r *responseRecorder) Write(p []byte) (int, error) {
	n, err := r.ResponseWriter.Write(p)
	r.bytes += int64(n)
	return n, err
}

func splitPath(p string) (bucket, key string) {
	p = strings.TrimPrefix(p, "/")
	parts := strings.SplitN(p, "/", 2)
	if len(parts) == 0 {
		return "", ""
	}
	bucket = parts[0]
	if len(parts) == 2 {
		key = parts[1]
	}
	return
}

func operationName(r *http.Request) string {
	if r.URL.Query().Get("uploadId") != "" {
		return "UploadPart"
	}
	if r.URL.Query().Get("acl") != "" {
		return "Unsupported"
	}
	switch r.Method {
	case http.MethodGet:
		return "GetObject"
	case http.MethodHead:
		return "HeadObject"
	case http.MethodPut:
		return "PutObject"
	case http.MethodDelete:
		return "DeleteObject"
	}
	return "Unknown"
}

func (s *Server) writeError(w http.ResponseWriter, r *http.Request, code, message string, status int) {
	setXML(w, status, xmlError(code, message))
}
