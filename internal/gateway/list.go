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
	maxKeys, _ := strconv.Atoi(q.Get("max-keys"))
	if maxKeys <= 0 || maxKeys > 1000 {
		maxKeys = int(s.cfg.Gateway.DefaultMaxKeys)
	}
	result, err := s.store.ListObjectsV2(ctx, migrationID, prefix, delimiter, int32(maxKeys), continuationToken)
	if err != nil {
		s.writeError(w, r, "InternalError", err.Error(), http.StatusInternalServerError)
		return
	}
	metrics.ProviderReads.WithLabelValues("destination").Inc()
	setXML(w, http.StatusOK, xmlListBucketResult(requestID(), result, maxKeys))
}
