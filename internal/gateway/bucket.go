package gateway

import (
	"net/http"

	"github.com/matthewgall/s3-migrateproxy/internal/providers"
)

func (s *Server) handleHeadBucket(w http.ResponseWriter, r *http.Request) {
	// The gateway maps all configured traffic to one migration pair.
	// Clients can probe the destination; if destination reachable, bucket exists.
	if _, err := s.dst.ListObjectsV2(r.Context(), providers.ListOptions{MaxKeys: 1}); err != nil {
		w.WriteHeader(http.StatusServiceUnavailable)
		return
	}
	w.Header().Set("x-amz-bucket-region", s.cfg.Destination.Region)
	w.WriteHeader(http.StatusOK)
}

func (s *Server) handleGetBucketLocation(w http.ResponseWriter, r *http.Request) {
	setXML(w, http.StatusOK, `<?xml version="1.0" encoding="UTF-8"?>`+
		`<LocationConstraint xmlns="http://s3.amazonaws.com/doc/2006-03-01/">`+s.cfg.Destination.Region+`</LocationConstraint>`)
}
