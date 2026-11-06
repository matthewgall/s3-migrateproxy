package gateway

import (
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/xml"
	"fmt"
	"io"
	"math"
	"net/http"
	"strconv"
	"strings"
	"time"

	metastore "github.com/matthewgall/s3-migrateproxy/internal/metadata"
	"github.com/matthewgall/s3-migrateproxy/internal/metrics"
	"github.com/matthewgall/s3-migrateproxy/internal/providers"
)

func (s *Server) handleGetObject(w http.ResponseWriter, r *http.Request, migrationID, key string) {
	rangeHdr := r.Header.Get("Range")
	ctx := r.Context()

	state, dbErr := s.store.GetObjectState(ctx, migrationID, key)
	if dbErr == nil && state != nil && state.Deleted {
		s.writeError(w, r, "NoSuchKey", "object has been deleted", http.StatusNotFound)
		return
	}

	// 1. Try destination.
	obj, err := s.dst.GetObject(ctx, key, "", rangeHdr)
	if err == nil {
		metrics.ProviderReads.WithLabelValues("destination").Inc()
		copyObjectToResponse(w, r, obj)
		return
	}
	if !providers.IsNotFound(err) {
		s.logger.Warn("destination get failed", "key", key, "err", err)
		s.writeError(w, r, "InternalError", "destination unavailable", http.StatusServiceUnavailable)
		return
	}

	// If the metadata store is unavailable we cannot verify tombstones safely.
	if dbErr != nil {
		s.logger.Warn("metadata unavailable; cannot safely fall back to source", "key", key, "err", dbErr)
		s.writeError(w, r, "ServiceUnavailable", "metadata unavailable", http.StatusServiceUnavailable)
		return
	}

	// 2. Authoritative NoSuchKey -> try source.
	obj, err = s.src.GetObject(ctx, key, "", rangeHdr)
	if err != nil {
		if providers.IsNotFound(err) {
			s.writeError(w, r, "NoSuchKey", "object not found", http.StatusNotFound)
			return
		}
		s.writeError(w, r, "InternalError", "source unavailable", http.StatusServiceUnavailable)
		return
	}
	metrics.SourceFallbacks.Inc()
	metrics.ProviderReads.WithLabelValues("source").Inc()
	copyObjectToResponse(w, r, obj)

	// 3. Enqueue background migration, best-effort.
	if err := s.store.TouchObjectAccess(ctx, migrationID, key); err == nil {
		_ = s.store.EnqueueTask(ctx, migrationID, key, "", "access")
	}
}

func copyObjectToResponse(w http.ResponseWriter, r *http.Request, obj *providers.ObjectInfo) {
	if obj.ContentType != "" {
		w.Header().Set("Content-Type", obj.ContentType)
	}
	if obj.ETag != "" {
		w.Header().Set("ETag", obj.ETag)
	}
	if !obj.LastModified.IsZero() {
		w.Header().Set("Last-Modified", obj.LastModified.UTC().Format(http.TimeFormat))
	}
	for k, v := range obj.Metadata {
		w.Header().Set("x-amz-meta-"+k, v)
	}
	if obj.ContentRange != "" {
		w.Header().Set("Content-Range", obj.ContentRange)
		w.WriteHeader(http.StatusPartialContent)
	} else {
		w.WriteHeader(http.StatusOK)
	}
	defer func() { _ = obj.Body.Close() }()
	_, _ = io.Copy(w, obj.Body)
}

func (s *Server) handleHeadObject(w http.ResponseWriter, r *http.Request, migrationID, key string) {
	ctx := r.Context()
	state, dbErr := s.store.GetObjectState(ctx, migrationID, key)
	if dbErr == nil && state != nil && state.Deleted {
		w.WriteHeader(http.StatusNotFound)
		return
	}
	obj, err := s.dst.HeadObject(ctx, key, "")
	if err == nil {
		copyMetadataToResponse(w, obj)
		w.WriteHeader(http.StatusOK)
		return
	}
	if !providers.IsNotFound(err) {
		s.writeError(w, r, "InternalError", "destination unavailable", http.StatusServiceUnavailable)
		return
	}
	if dbErr != nil {
		s.logger.Warn("metadata unavailable; cannot safely head source fallback", "key", key, "err", dbErr)
		s.writeError(w, r, "ServiceUnavailable", "metadata unavailable", http.StatusServiceUnavailable)
		return
	}
	obj, err = s.src.HeadObject(ctx, key, "")
	if err != nil {
		if providers.IsNotFound(err) {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		s.writeError(w, r, "InternalError", "source unavailable", http.StatusServiceUnavailable)
		return
	}
	copyMetadataToResponse(w, obj)
	w.WriteHeader(http.StatusOK)
}

func copyMetadataToResponse(w http.ResponseWriter, obj *providers.ObjectInfo) {
	if obj.ContentType != "" {
		w.Header().Set("Content-Type", obj.ContentType)
	}
	if obj.ETag != "" {
		w.Header().Set("ETag", obj.ETag)
	}
	if !obj.LastModified.IsZero() {
		w.Header().Set("Last-Modified", obj.LastModified.UTC().Format(http.TimeFormat))
	}
	if obj.Size >= 0 {
		w.Header().Set("Content-Length", strconv.FormatInt(obj.Size, 10))
	}
	for k, v := range obj.Metadata {
		w.Header().Set("x-amz-meta-"+k, v)
	}
}

func (s *Server) handlePutObject(w http.ResponseWriter, r *http.Request, migrationID, key string) {
	ctx := r.Context()
	contentType := r.Header.Get("Content-Type")
	if contentType == "" {
		contentType = "application/octet-stream"
	}
	contentLength := r.ContentLength
	if contentLength < 0 {
		contentLength = 0
	}

	// Enforce gateway max body.
	if contentLength > s.cfg.Gateway.MaxRequestBodyBytes {
		s.writeError(w, r, "EntityTooLarge", "object exceeds maximum size", http.StatusBadRequest)
		return
	}

	metadata := extractMetadata(r.Header)

	// Compute content sha256 while streaming.
	var body io.Reader = r.Body
	var shaHex string
	if r.Header.Get("x-amz-content-sha256") != "" && r.Header.Get("x-amz-content-sha256") != "UNSIGNED-PAYLOAD" {
		h := sha256.New()
		body = io.TeeReader(r.Body, h)
		defer func() { shaHex = hex.EncodeToString(h.Sum(nil)) }()
	}

	result, err := s.dst.PutObject(ctx, key, body, contentLength, providers.PutOptions{
		ContentType: contentType,
		Metadata:    metadata,
	})
	if err != nil {
		s.logger.Error("put to destination failed", "key", key, "err", err)
		s.writeError(w, r, "InternalError", "failed to write object", http.StatusInternalServerError)
		return
	}

	// Record metadata after destination ack.
	err = s.store.UpsertObjectState(ctx, migrationID, key, 1, func(o *metastore.ObjectState) error {
		o.Deleted = false
		o.DestVersionID = result.VersionID
		o.DestETag = result.ETag
		o.Size.Int64 = contentLength
		o.Size.Valid = true
		o.ContentType.String = contentType
		o.ContentType.Valid = true
		o.Metadata = metadata
		o.ChecksumSHA256 = shaHex
		o.MigrationState = "migrated"
		o.DestModifiedAt = sql.NullTime{Time: timeNow(), Valid: true}
		return nil
	})
	if err != nil {
		s.logger.Error("metadata update failed after successful put", "key", key, "err", err)
		// Object exists in destination; gateway now has inconsistency. Log and report 500.
		s.writeError(w, r, "InternalError", "metadata error", http.StatusInternalServerError)
		return
	}

	_ = s.store.RecordEvent(ctx, migrationID, "PUT", key, result.VersionID, 0, map[string]any{"size": contentLength, "etag": result.ETag})
	w.Header().Set("ETag", result.ETag)
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte(xmlPutResult(result.ETag)))
}

func (s *Server) handleUploadPart(w http.ResponseWriter, r *http.Request, migrationID, key, uploadID string) {
	ctx := r.Context()
	partNumStr := r.URL.Query().Get("partNumber")
	n, err := strconv.ParseInt(partNumStr, 10, 32)
	if err != nil || n < 1 || n > math.MaxInt32 {
		s.writeError(w, r, "InvalidArgument", "invalid partNumber", http.StatusBadRequest)
		return
	}
	partNum := int(n)

	destUploadID, _, _, err := s.store.GetMultipartRecord(ctx, migrationID, uploadID)
	if err != nil {
		s.writeError(w, r, "NoSuchUpload", "upload not found", http.StatusNotFound)
		return
	}

	etag, err := s.dst.UploadPart(ctx, destUploadID, partNum, r.Body, r.ContentLength)
	if err != nil {
		s.writeError(w, r, "InternalError", "upload part failed", http.StatusInternalServerError)
		return
	}
	if err := s.store.RecordPart(ctx, migrationID, uploadID,
		int32(partNum), etag, r.ContentLength); err != nil { // #nosec G115 -- validated against MaxInt32
		s.writeError(w, r, "InternalError", "failed to record part", http.StatusInternalServerError)
		return
	}
	w.Header().Set("ETag", etag)
	w.WriteHeader(http.StatusOK)
}

func (s *Server) handleDeleteObject(w http.ResponseWriter, r *http.Request, migrationID, key string) {
	ctx := r.Context()
	// Persist tombstone before attempting destination delete.
	if err := s.store.MarkDeleted(ctx, migrationID, key); err != nil {
		s.writeError(w, r, "InternalError", "failed to record deletion", http.StatusInternalServerError)
		return
	}
	// Best-effort delete from destination; source cleanup can be async.
	_ = s.dst.DeleteObject(ctx, key, "")
	_ = s.store.RecordEvent(ctx, migrationID, "DELETE", key, "", 0, map[string]any{})

	w.WriteHeader(http.StatusNoContent)
}

func (s *Server) handleDeleteObjects(w http.ResponseWriter, r *http.Request, migrationID string) {
	ctx := r.Context()
	var req deleteObjectsXML
	if err := xml.NewDecoder(r.Body).Decode(&req); err != nil {
		s.writeError(w, r, "MalformedXML", err.Error(), http.StatusBadRequest)
		return
	}
	var resp deleteObjectsResponseXML
	for _, obj := range req.Objects {
		err := s.store.MarkDeleted(ctx, migrationID, obj.Key)
		_ = s.dst.DeleteObject(ctx, obj.Key, "")
		deleted := err == nil
		resp.Deleted = append(resp.Deleted, deleteObjectsDeletedXML{Key: obj.Key})
		_ = s.store.RecordEvent(ctx, migrationID, "DELETE", obj.Key, "", 0, map[string]any{"batch": true})
		if !deleted {
			resp.Errors = append(resp.Errors, deleteObjectsErrorXML{Key: obj.Key, Code: "InternalError", Message: "failed to record deletion"})
		}
	}
	setXML(w, http.StatusOK, xmlDeleteObjectsResult(&resp))
}

type deleteObjectsXML struct {
	XMLName xml.Name          `xml:"Delete"`
	Objects []deleteObjectXML `xml:"Object"`
	Quiet   bool              `xml:"Quiet"`
}

type deleteObjectXML struct {
	Key       string `xml:"Key"`
	VersionID string `xml:"VersionId"`
}

type deleteObjectsResponseXML struct {
	Deleted []deleteObjectsDeletedXML
	Errors  []deleteObjectsErrorXML
}

type deleteObjectsDeletedXML struct {
	Key       string `xml:"Key"`
	VersionID string `xml:"VersionId,omitempty"`
}

type deleteObjectsErrorXML struct {
	Key     string `xml:"Key"`
	Code    string `xml:"Code"`
	Message string `xml:"Message"`
}

// xmlDeleteObjectsResult serialises the response.
func xmlDeleteObjectsResult(r *deleteObjectsResponseXML) string {
	var b strings.Builder
	b.WriteString(`<?xml version="1.0" encoding="UTF-8"?>`)
	b.WriteString(`<DeleteResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">`)
	for _, d := range r.Deleted {
		b.WriteString("<Deleted>")
		fmt.Fprintf(&b, "<Key>%s</Key>", xmlEscape(d.Key))
		if d.VersionID != "" {
			fmt.Fprintf(&b, "<VersionId>%s</VersionId>", xmlEscape(d.VersionID))
		}
		b.WriteString("</Deleted>")
	}
	for _, e := range r.Errors {
		b.WriteString("<Error>")
		fmt.Fprintf(&b, "<Key>%s</Key><Code>%s</Code><Message>%s</Message>", xmlEscape(e.Key), xmlEscape(e.Code), xmlEscape(e.Message))
		b.WriteString("</Error>")
	}
	b.WriteString("</DeleteResult>")
	return b.String()
}

func extractMetadata(h http.Header) map[string]string {
	m := map[string]string{}
	for k, vs := range h {
		if len(k) > 11 && k[:11] == "X-Amz-Meta-" {
			m[k[11:]] = vs[0]
		}
	}
	return m
}

func timeNow() time.Time { return time.Now() }
