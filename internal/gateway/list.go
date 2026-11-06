package gateway

import (
	"net/http"
	"strconv"

	"github.com/matthewgall/s3-migrateproxy/internal/metrics"
)

func (s *Server) handleListObjectsV2(w http.ResponseWriter, r *http.Request, migrationID string) {
	ctx := r.Context()
	q := r.URL.Query()
	prefix := q.Get("prefix")
	delimiter := q.Get("delimiter")
	continuationToken := q.Get("continuation-token")
	maxKeys := int(s.cfg.Gateway.DefaultMaxKeys)
	if raw := q.Get("max-keys"); raw != "" {
		if n, err := strconv.ParseInt(raw, 10, 32); err == nil {
			if nk := int(n); nk > 0 && nk <= 1000 {
				maxKeys = nk
			}
		}
	}
	// strconv.ParseInt guarantees the value fits in int32 and maxKeys is clamped to <= 1000.
	result, err := s.store.ListObjectsV2(ctx, migrationID, prefix, delimiter,
		int32(maxKeys), continuationToken) // #nosec G115 -- maxKeys clamped to <= 1000
	if err != nil {
		s.writeError(w, r, "InternalError", err.Error(), http.StatusInternalServerError)
		return
	}
	metrics.ProviderReads.WithLabelValues("destination").Inc()
	setXML(w, http.StatusOK, xmlListBucketResult(requestID(), result, maxKeys))
}
