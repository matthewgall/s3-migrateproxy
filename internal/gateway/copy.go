package gateway

import (
	"database/sql"
	"net/http"
	"net/url"
	"strings"

	metastore "github.com/matthewgall/s3-migrateproxy/internal/metadata"
	"github.com/matthewgall/s3-migrateproxy/internal/providers"
)

func (s *Server) handleCopyObject(w http.ResponseWriter, r *http.Request, migrationID, dstKey string) {
	ctx := r.Context()
	// The copy source may be URL-encoded, e.g. /bucket/key or /bucket/key?versionId=...
	srcHdr, _ := url.PathUnescape(r.Header.Get("X-Amz-Copy-Source"))
	if srcHdr == "" {
		s.writeError(w, r, "InvalidArgument", "missing x-amz-copy-source", http.StatusBadRequest)
		return
	}
	srcKey, srcVersionID := parseCopySource(srcHdr)

	metadataDirective := r.Header.Get("X-Amz-Metadata-Directive")
	if metadataDirective == "" {
		metadataDirective = "COPY"
	}
	metadata := extractMetadata(r.Header)
	contentType := r.Header.Get("Content-Type")

	// If the object exists in destination, use server-side copy.
	var result *providers.PutResult
	if _, err := s.dst.HeadObject(ctx, srcKey, srcVersionID); err == nil {
		res, err := s.dst.CopyObject(ctx, dstKey, providers.CopyOptions{
			SourceKey:         srcKey,
			SourceVersionID:   srcVersionID,
			MetadataDirective: metadataDirective,
			ContentType:       contentType,
			Metadata:          metadata,
		})
		if err != nil {
			s.writeError(w, r, "InternalError", "copy failed", http.StatusInternalServerError)
			return
		}
		result = res
	} else {
		// Stream source to destination without buffering the whole object.
		srcObj, err := s.src.GetObject(ctx, srcKey, srcVersionID, "")
		if err != nil {
			if providers.IsNotFound(err) {
				s.writeError(w, r, "NoSuchKey", "source object not found", http.StatusNotFound)
				return
			}
			s.writeError(w, r, "InternalError", "source fetch failed", http.StatusInternalServerError)
			return
		}
		defer srcObj.Body.Close()
		if contentType == "" {
			contentType = srcObj.ContentType
		}
		if metadataDirective == "COPY" {
			metadata = srcObj.Metadata
		}
		res, err := s.dst.PutObject(ctx, dstKey, srcObj.Body, srcObj.Size, providers.PutOptions{
			ContentType: contentType,
			Metadata:    metadata,
		})
		if err != nil {
			s.writeError(w, r, "InternalError", "destination put failed", http.StatusInternalServerError)
			return
		}
		result = res
	}

	err := s.store.UpsertObjectState(ctx, migrationID, dstKey, 1, func(o *metastore.ObjectState) error {
		o.Deleted = false
		o.DestVersionID = result.VersionID
		o.DestETag = result.ETag
		o.MigrationState = "migrated"
		o.DestModifiedAt = sql.NullTime{Time: timeNow(), Valid: true}
		return nil
	})
	if err != nil {
		s.logger.Error("metadata update after copy failed", "err", err)
	}

	_ = s.store.RecordEvent(ctx, migrationID, "COPY", dstKey, result.VersionID, 0, map[string]any{"etag": result.ETag})
	setXML(w, http.StatusOK, xmlCopyObjectResult(result, dstKey))
}

func parseCopySource(src string) (key, versionID string) {
	src = strings.TrimPrefix(src, "/")
	if idx := strings.Index(src, "?versionId="); idx >= 0 {
		versionID = src[idx+11:]
		src = src[:idx]
	}
	parts := strings.SplitN(src, "/", 2)
	if len(parts) == 2 {
		key = parts[1]
	} else {
		key = src
	}
	return
}
