package migration

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"fmt"
	"io"

	"time"

	"github.com/matthewgall/s3-migrateproxy/internal/config"
	"github.com/matthewgall/s3-migrateproxy/internal/metadata"
	"github.com/matthewgall/s3-migrateproxy/internal/metrics"
	"github.com/matthewgall/s3-migrateproxy/internal/providers"
	"github.com/matthewgall/s3-migrateproxy/internal/retry"
	"github.com/matthewgall/s3-migrateproxy/internal/verification"
)

const (
	minPartSize = 5 << 20 // 5 MiB, enforced by most S3 providers
	maxPartSize = 5 << 30 // 5 GiB
	maxParts    = 10000   // S3 limit
)

// Transferer copies a single object between providers.
type Transferer struct {
	cfg   *config.Config
	store *metadata.Store
	src   providers.Provider
	dst   providers.Provider
}

// NewTransferer creates a transferer.
func NewTransferer(cfg *config.Config, store *metadata.Store, src, dst providers.Provider) *Transferer {
	return &Transferer{cfg: cfg, store: store, src: src, dst: dst}
}

// Transfer copies the source object to the destination safely.
func (t *Transferer) Transfer(ctx context.Context, task *metadata.Task) error {
	srcMeta, err := t.src.HeadObject(ctx, task.Key, task.SourceVersionID)
	if err != nil {
		return fmt.Errorf("source head failed: %w", err)
	}

	state, err := t.store.GetObjectState(ctx, task.MigrationID, task.Key)
	if err != nil {
		return fmt.Errorf("metadata read failed: %w", err)
	}
	if state != nil && state.Deleted {
		return t.store.CompleteTask(ctx, task.ID, t.cfg.Worker.WorkerID)
	}
	if state != nil && state.DestVersionID != "" && !state.DestModifiedAt.Time.Before(srcMeta.LastModified) {
		// Destination appears newer or equal.
		return t.store.CompleteTask(ctx, task.ID, t.cfg.Worker.WorkerID)
	}

	partSize := t.cfg.Worker.TransferPartSize
	if partSize < minPartSize {
		partSize = minPartSize
	}
	if partSize > maxPartSize {
		partSize = maxPartSize
	}

	if srcMeta.Size <= partSize {
		return t.copySingle(ctx, task, srcMeta)
	}
	return t.copyMultipart(ctx, task, srcMeta, partSize)
}

func (t *Transferer) copySingle(ctx context.Context, task *metadata.Task, srcMeta *providers.ObjectInfo) error {
	obj, err := retry.Value(ctx, retry.Default(), func() (*providers.ObjectInfo, error) {
		return t.src.GetObject(ctx, task.Key, task.SourceVersionID, "")
	})
	if err != nil {
		return fmt.Errorf("source get failed: %w", err)
	}
	defer obj.Body.Close()

	metadata := obj.Metadata
	contentType := obj.ContentType

	h := sha256.New()
	tee := io.TeeReader(obj.Body, h)

	result, err := retry.Value(ctx, retry.Default(), func() (*providers.PutResult, error) {
		return t.dst.PutObject(ctx, task.Key, tee, srcMeta.Size, providers.PutOptions{
			ContentType: contentType,
			Metadata:    metadata,
		})
	})
	if err != nil {
		return fmt.Errorf("destination put failed: %w", err)
	}

	shaHex := hex.EncodeToString(h.Sum(nil))
	if err := t.verifyAndRecord(ctx, task, srcMeta, result, shaHex, contentType, metadata); err != nil {
		return err
	}
	return t.store.CompleteTask(ctx, task.ID, t.cfg.Worker.WorkerID)
}

func (t *Transferer) copyMultipart(ctx context.Context, task *metadata.Task, srcMeta *providers.ObjectInfo, partSize int64) error {
	totalParts := (srcMeta.Size + partSize - 1) / partSize
	if totalParts > maxParts {
		return fmt.Errorf("object requires %d parts, exceeding limit %d", totalParts, maxParts)
	}

	metadataMap := map[string]string{}
	// Reuse source metadata if we can HEAD it; otherwise leave empty.
	if src, err := retry.Value(ctx, retry.Default(), func() (*providers.ObjectInfo, error) {
		return t.src.HeadObject(ctx, task.Key, task.SourceVersionID)
	}); err == nil {
		metadataMap = src.Metadata
	}

	init, err := retry.Value(ctx, retry.Default(), func() (*providers.MultipartUploadInfo, error) {
		return t.dst.CreateMultipartUpload(ctx, task.Key, providers.PutOptions{
			ContentType: srcMeta.ContentType,
			Metadata:    metadataMap,
		})
	})
	if err != nil {
		return fmt.Errorf("create multipart upload failed: %w", err)
	}
	destUploadID := init.UploadID

	// Incrementally hash all source bytes for end-to-end verification.
	h := sha256.New()

	// Persist a lightweight record so gateway/multipart state remains consistent.
	uploadID := fmt.Sprintf("mig-%s-%d", task.Key, time.Now().UnixNano())
	if err := t.store.CreateMultipartRecord(ctx, task.MigrationID, uploadID, task.Key, destUploadID, metadataMap); err != nil {
		_ = t.dst.AbortMultipartUpload(ctx, destUploadID, task.Key)
		return fmt.Errorf("record multipart upload failed: %w", err)
	}

	partCount := int(totalParts)
	var parts []providers.PartInfo
	for i := 1; i <= partCount; i++ {
		ipart := int64(i)
		start := (ipart - 1) * partSize
		end := start + partSize - 1
		if end >= srcMeta.Size {
			end = srcMeta.Size - 1
		}
		length := end - start + 1

		// Fetch the byte range from source.
		rangeHdr := fmt.Sprintf("bytes=%d-%d", start, end)
		partObj, err := retry.Value(ctx, retry.Default(), func() (*providers.ObjectInfo, error) {
			return t.src.GetObject(ctx, task.Key, task.SourceVersionID, rangeHdr)
		})
		if err != nil {
			_ = t.dst.AbortMultipartUpload(ctx, destUploadID, task.Key)
			_ = t.store.AbortMultipartRecord(ctx, task.MigrationID, uploadID)
			return fmt.Errorf("source get part %d failed: %w", i, err)
		}
		defer func() { _ = partObj.Body.Close() }()

		partNum := int32(i) // #nosec G115 -- part count is bounded by maxParts (10,000)

		tee := io.TeeReader(partObj.Body, h)
		etag, err := retry.Value(ctx, retry.Default(), func() (string, error) {
			return t.dst.UploadPart(ctx, destUploadID, int(partNum), tee, length)
		})
		if err != nil {
			_ = t.dst.AbortMultipartUpload(ctx, destUploadID, task.Key)
			_ = t.store.AbortMultipartRecord(ctx, task.MigrationID, uploadID)
			return fmt.Errorf("destination upload part %d failed: %w", i, err)
		}
		_ = t.store.RecordPart(ctx, task.MigrationID, uploadID, partNum, etag, length)
		parts = append(parts, providers.PartInfo{PartNumber: partNum, ETag: etag, Size: length})
	}

	result, err := retry.Value(ctx, retry.Default(), func() (*providers.PutResult, error) {
		return t.dst.CompleteMultipartUpload(ctx, destUploadID, task.Key, parts)
	})
	if err != nil {
		_ = t.dst.AbortMultipartUpload(ctx, destUploadID, task.Key)
		_ = t.store.AbortMultipartRecord(ctx, task.MigrationID, uploadID)
		return fmt.Errorf("complete multipart upload failed: %w", err)
	}
	_ = t.store.CompleteMultipartRecord(ctx, task.MigrationID, uploadID)

	shaHex := hex.EncodeToString(h.Sum(nil))
	if err := t.verifyAndRecord(ctx, task, srcMeta, result, shaHex, srcMeta.ContentType, metadataMap); err != nil {
		return err
	}
	return t.store.CompleteTask(ctx, task.ID, t.cfg.Worker.WorkerID)
}

func (t *Transferer) verifyAndRecord(ctx context.Context, task *metadata.Task, srcMeta *providers.ObjectInfo, result *providers.PutResult, shaHex, contentType string, meta map[string]string) error {
	dstMeta, err := retry.Value(ctx, retry.Default(), func() (*providers.ObjectInfo, error) {
		return t.dst.HeadObject(ctx, task.Key, "")
	})
	if err != nil {
		return fmt.Errorf("destination head after put failed: %w", err)
	}
	res, err := verification.Compare(ctx, srcMeta.Size, srcMeta.ETag, shaHex, dstMeta.Size, dstMeta.ETag, dstMeta.ChecksumSHA256, nil)
	if err != nil {
		return fmt.Errorf("verification compare failed: %w", err)
	}
	if !res.OK && t.cfg.Worker.VerifyChecksum {
		return fmt.Errorf("verification failed: %s", res.Message)
	}

	err = t.store.UpsertObjectState(ctx, task.MigrationID, task.Key, 0, func(o *metadata.ObjectState) error {
		o.SourceVersionID = task.SourceVersionID
		o.SourceETag = srcMeta.ETag
		o.DestVersionID = result.VersionID
		o.DestETag = result.ETag
		o.Size.Int64 = srcMeta.Size
		o.Size.Valid = true
		o.ContentType.String = contentType
		o.ContentType.Valid = true
		o.Metadata = meta
		o.SourceModifiedAt = sql.NullTime{Time: srcMeta.LastModified, Valid: true}
		o.DestModifiedAt = sql.NullTime{Time: time.Now(), Valid: true}
		o.ChecksumSHA256 = shaHex
		o.MigrationState = "migrated"
		return nil
	})
	if err != nil {
		return fmt.Errorf("metadata update failed: %w", err)
	}
	metrics.ObjectsMigrated.Inc()
	metrics.BytesMigrated.Add(float64(srcMeta.Size))
	_ = t.store.RecordEvent(ctx, task.MigrationID, "MIGRATED", task.Key, task.SourceVersionID, 0, map[string]any{
		"size": srcMeta.Size, "etag": result.ETag,
	})
	return nil
}
