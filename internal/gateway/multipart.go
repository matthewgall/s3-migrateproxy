package gateway

import (
	"database/sql"
	"encoding/xml"
	"fmt"
	"io"
	"math"
	"net/http"
	"strings"

	"github.com/google/uuid"
	metastore "github.com/matthewgall/s3-migrateproxy/internal/metadata"
	"github.com/matthewgall/s3-migrateproxy/internal/metrics"
	"github.com/matthewgall/s3-migrateproxy/internal/providers"
)

func (s *Server) handleCreateMultipartUpload(w http.ResponseWriter, r *http.Request, migrationID string) {
	ctx := r.Context()
	q := r.URL.Query()
	key := q.Get("key")
	if key == "" {
		key = strings.TrimPrefix(r.URL.Path, "/")
	}
	contentType := r.Header.Get("Content-Type")
	if contentType == "" {
		contentType = "application/octet-stream"
	}
	metadata := extractMetadata(r.Header)

	result, err := s.dst.CreateMultipartUpload(ctx, key, providers.PutOptions{
		ContentType: contentType,
		Metadata:    metadata,
	})
	if err != nil {
		s.writeError(w, r, "InternalError", "create multipart upload failed", http.StatusInternalServerError)
		return
	}

	uploadID := uuid.New().String()
	if err := s.store.CreateMultipartRecord(ctx, migrationID, uploadID, key, result.UploadID, metadata); err != nil {
		s.writeError(w, r, "InternalError", "failed to record upload", http.StatusInternalServerError)
		return
	}
	metrics.ProviderWrites.WithLabelValues("destination").Inc()
	setXML(w, http.StatusOK, xmlInitiateMultipartUploadResult(key, uploadID))
}

func (s *Server) handleCompleteMultipartUpload(w http.ResponseWriter, r *http.Request, migrationID, key, uploadID string) {
	ctx := r.Context()
	destUploadID, _, recordedParts, err := s.store.GetMultipartRecord(ctx, migrationID, uploadID)
	if err != nil {
		s.writeError(w, r, "NoSuchUpload", "upload not found", http.StatusNotFound)
		return
	}

	reqParts, err := parseCompleteMultipartUploadBody(r.Body)
	if err != nil {
		s.writeError(w, r, "MalformedXML", err.Error(), http.StatusBadRequest)
		return
	}

	parts := make([]providers.PartInfo, 0, len(reqParts))
	for _, rp := range reqParts {
		if rp.PartNumber < 1 || rp.PartNumber > math.MaxInt32 {
			s.writeError(w, r, "InvalidPart", fmt.Sprintf("invalid part number: %d", rp.PartNumber), http.StatusBadRequest)
			return
		}
		var found *metastore.PartInfo
		rpPartNum := int32(rp.PartNumber) // #nosec G115 -- rp.PartNumber bounded by MaxInt32
		for i := range recordedParts {
			if recordedParts[i].PartNumber == rpPartNum {
				found = &recordedParts[i]
				break
			}
		}
		if found == nil {
			s.writeError(w, r, "InvalidPart", "part not uploaded", http.StatusBadRequest)
			return
		}
		parts = append(parts, providers.PartInfo{PartNumber: found.PartNumber, ETag: found.ETag, Size: found.Size})
	}

	result, err := s.dst.CompleteMultipartUpload(ctx, destUploadID, key, parts)
	if err != nil {
		s.writeError(w, r, "InternalError", "complete multipart upload failed", http.StatusInternalServerError)
		return
	}
	if err := s.store.CompleteMultipartRecord(ctx, migrationID, uploadID); err != nil {
		s.logger.Error("failed to mark multipart complete", "err", err)
	}

	err = s.store.UpsertObjectState(ctx, migrationID, key, 1, func(o *metastore.ObjectState) error {
		o.Deleted = false
		o.DestVersionID = result.VersionID
		o.DestETag = result.ETag
		o.MigrationState = "migrated"
		o.DestModifiedAt = sql.NullTime{Time: timeNow(), Valid: true}
		return nil
	})
	if err != nil {
		s.logger.Error("metadata update after multipart failed", "err", err)
	}

	_ = s.store.RecordEvent(ctx, migrationID, "PUT", key, result.VersionID, 0, map[string]any{"multipart": true, "etag": result.ETag})
	metrics.ProviderWrites.WithLabelValues("destination").Inc()
	setXML(w, http.StatusOK, xmlCompleteMultipartUploadResult(result, key))
}

func (s *Server) handleAbortMultipartUpload(w http.ResponseWriter, r *http.Request, migrationID, key, uploadID string) {
	ctx := r.Context()
	destUploadID, _, _, err := s.store.GetMultipartRecord(ctx, migrationID, uploadID)
	if err != nil {
		w.WriteHeader(http.StatusNoContent)
		return
	}
	_ = s.dst.AbortMultipartUpload(ctx, destUploadID, key)
	_ = s.store.AbortMultipartRecord(ctx, migrationID, uploadID)
	w.WriteHeader(http.StatusNoContent)
}

func (s *Server) handleListParts(w http.ResponseWriter, r *http.Request, migrationID, key, uploadID string) {
	ctx := r.Context()
	destUploadID, _, recordedParts, err := s.store.GetMultipartRecord(ctx, migrationID, uploadID)
	if err != nil {
		s.writeError(w, r, "NoSuchUpload", "upload not found", http.StatusNotFound)
		return
	}
	// Refresh with destination as source of truth.
	parts := make([]providers.PartInfo, 0, len(recordedParts))
	if remote, rerr := s.dst.ListParts(ctx, destUploadID, key, 1000, 0); rerr == nil {
		parts = remote.Parts
	} else {
		for _, p := range recordedParts {
			parts = append(parts, providers.PartInfo{PartNumber: p.PartNumber, ETag: p.ETag, Size: p.Size})
		}
	}
	setXML(w, http.StatusOK, xmlListPartsResult(uploadID, key, parts))
}

func (s *Server) handleListMultipartUploads(w http.ResponseWriter, r *http.Request, migrationID string) {
	ctx := r.Context()
	q := r.URL.Query()
	prefix := q.Get("prefix")
	uploads, err := s.dst.ListMultipartUploads(ctx, providers.ListOptions{Prefix: prefix})
	if err != nil {
		s.writeError(w, r, "InternalError", "list multipart uploads failed", http.StatusInternalServerError)
		return
	}
	setXML(w, http.StatusOK, xmlListMultipartUploadsResult(uploads))
}

type completeMultipartUploadXML struct {
	XMLName xml.Name  `xml:"CompleteMultipartUpload"`
	Parts   []partXML `xml:"Part"`
}

type partXML struct {
	PartNumber int    `xml:"PartNumber"`
	ETag       string `xml:"ETag"`
}

func parseCompleteMultipartUploadBody(r io.Reader) ([]partXML, error) {
	var cu completeMultipartUploadXML
	dec := xml.NewDecoder(r)
	if err := dec.Decode(&cu); err != nil {
		return nil, err
	}
	return cu.Parts, nil
}
