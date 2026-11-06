package metadata

import (
	"context"
	"database/sql"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// Store persists all gateway/worker state in PostgreSQL.
type Store struct {
	pool *pgxpool.Pool
}

// NewStore creates a Store.
func NewStore(pool *pgxpool.Pool) *Store {
	return &Store{pool: pool}
}

// Pool returns the underlying pool.
func (s *Store) Pool() *pgxpool.Pool { return s.pool }

// WithTx runs fn inside a transaction.
func (s *Store) WithTx(ctx context.Context, fn func(pgx.Tx) error) error {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return err
	}
	if err := fn(tx); err != nil {
		_ = tx.Rollback(ctx)
		return err
	}
	return tx.Commit(ctx)
}

// CreateMigration inserts a migration record.
func (s *Store) CreateMigration(ctx context.Context, name, sourceBucketID, destBucketID string, options map[string]any) (*Migration, error) {
	if options == nil {
		options = map[string]any{}
	}
	opts, _ := json.Marshal(options)
	var m Migration
	row := s.pool.QueryRow(ctx, `
		INSERT INTO metadata.migrations (name, source_bucket_id, dest_bucket_id, options)
		VALUES ($1, $2, $3, $4)
		ON CONFLICT (name) DO UPDATE SET updated_at = NOW()
		RETURNING id, name, state, source_bucket_id, dest_bucket_id, options, created_at, updated_at, completed_at`,
		name, sourceBucketID, destBucketID, opts)
	if err := row.Scan(&m.ID, &m.Name, &m.State, &m.SourceBucketID, &m.DestBucketID, &m.Options, &m.CreatedAt, &m.UpdatedAt, &m.CompletedAt); err != nil {
		return nil, fmt.Errorf("create migration: %w", err)
	}
	return &m, nil
}

// GetMigrationByName returns a migration by name.
func (s *Store) GetMigrationByName(ctx context.Context, name string) (*Migration, error) {
	var m Migration
	row := s.pool.QueryRow(ctx, `
		SELECT id, name, state, source_bucket_id, dest_bucket_id, options, created_at, updated_at, completed_at
		FROM metadata.migrations WHERE name = $1`, name)
	if err := row.Scan(&m.ID, &m.Name, &m.State, &m.SourceBucketID, &m.DestBucketID, &m.Options, &m.CreatedAt, &m.UpdatedAt, &m.CompletedAt); err != nil {
		return nil, err
	}
	return &m, nil
}

// UpdateMigrationState transitions the lifecycle.
func (s *Store) UpdateMigrationState(ctx context.Context, id, state string) error {
	completed := sql.NullTime{}
	if state == "COMPLETED" {
		completed = sql.NullTime{Time: timeNow(), Valid: true}
	}
	_, err := s.pool.Exec(ctx, `
		UPDATE metadata.migrations SET state = $2, completed_at = $3, updated_at = NOW() WHERE id = $1`,
		id, state, completed)
	return err
}

// GetObjectState returns the authoritative state for a key.
func (s *Store) GetObjectState(ctx context.Context, migrationID, key string) (*ObjectState, error) {
	var o ObjectState
	var meta json.RawMessage
	err := s.pool.QueryRow(ctx, `
		SELECT migration_id, key, generation, deleted,
		       source_version_id, dest_version_id, source_etag, dest_etag,
		       size, content_type, metadata, checksum_sha256, checksum_algorithm,
		       migration_state, last_accessed_at, source_modified_at, dest_modified_at,
		       created_at, updated_at
		FROM metadata.object_keys
		WHERE migration_id = $1 AND key = $2`, migrationID, key).Scan(
		&o.MigrationID, &o.Key, &o.Generation, &o.Deleted,
		&o.SourceVersionID, &o.DestVersionID, &o.SourceETag, &o.DestETag,
		&o.Size, &o.ContentType, &meta, &o.ChecksumSHA256, &o.ChecksumAlgorithm,
		&o.MigrationState, &o.LastAccessedAt, &o.SourceModifiedAt, &o.DestModifiedAt,
		&o.CreatedAt, &o.UpdatedAt)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, nil
		}
		return nil, err
	}
	if err := json.Unmarshal(meta, &o.Metadata); err != nil {
		o.Metadata = map[string]string{}
	}
	return &o, nil
}

// UpsertObjectState writes the destination-side state, bumping generation.
// It must be called after the destination provider acknowledged the write.
func (s *Store) UpsertObjectState(ctx context.Context, migrationID, key string, generationDelta int64, fn func(*ObjectState) error) error {
	return s.WithTx(ctx, func(tx pgx.Tx) error {
		var o ObjectState
		var meta json.RawMessage
		err := tx.QueryRow(ctx, `
			SELECT migration_id, key, generation, deleted,
			       source_version_id, dest_version_id, source_etag, dest_etag,
			       size, content_type, metadata, checksum_sha256, checksum_algorithm,
			       migration_state, last_accessed_at, source_modified_at, dest_modified_at,
			       created_at, updated_at
			FROM metadata.object_keys
			WHERE migration_id = $1 AND key = $2
			FOR UPDATE`, migrationID, key).Scan(
			&o.MigrationID, &o.Key, &o.Generation, &o.Deleted,
			&o.SourceVersionID, &o.DestVersionID, &o.SourceETag, &o.DestETag,
			&o.Size, &o.ContentType, &meta, &o.ChecksumSHA256, &o.ChecksumAlgorithm,
			&o.MigrationState, &o.LastAccessedAt, &o.SourceModifiedAt, &o.DestModifiedAt,
			&o.CreatedAt, &o.UpdatedAt)
		if err != nil && !errors.Is(err, pgx.ErrNoRows) {
			return err
		}
		if err == nil {
			_ = json.Unmarshal(meta, &o.Metadata)
		} else {
			o.MigrationID = migrationID
			o.Key = key
			o.Metadata = map[string]string{}
		}
		gen := o.Generation + generationDelta
		if generationDelta == 0 {
			gen = o.Generation
		}
		if err := fn(&o); err != nil {
			return err
		}
		o.Generation = gen
		if o.MigrationState == "" {
			o.MigrationState = "migrated"
		}
		meta, _ = json.Marshal(o.Metadata)
		_, err = tx.Exec(ctx, `
			INSERT INTO metadata.object_keys
			(migration_id, key, generation, deleted,
			 source_version_id, dest_version_id, source_etag, dest_etag,
			 size, content_type, metadata, checksum_sha256, checksum_algorithm,
			 migration_state, dest_modified_at, updated_at)
			VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, NOW(), NOW())
			ON CONFLICT (migration_id, key) DO UPDATE SET
			  generation = EXCLUDED.generation,
			  deleted = EXCLUDED.deleted,
			  dest_version_id = EXCLUDED.dest_version_id,
			  dest_etag = EXCLUDED.dest_etag,
			  size = EXCLUDED.size,
			  content_type = EXCLUDED.content_type,
			  metadata = EXCLUDED.metadata,
			  checksum_sha256 = EXCLUDED.checksum_sha256,
			  checksum_algorithm = EXCLUDED.checksum_algorithm,
			  migration_state = EXCLUDED.migration_state,
			  dest_modified_at = EXCLUDED.dest_modified_at,
			  updated_at = NOW()`,
			migrationID, key, o.Generation, o.Deleted,
			o.SourceVersionID, o.DestVersionID, o.SourceETag, o.DestETag,
			o.Size, o.ContentType, meta, o.ChecksumSHA256, o.ChecksumAlgorithm,
			o.MigrationState)
		return err
	})
}

// MarkDeleted creates/updates a tombstone and increments generation.
func (s *Store) MarkDeleted(ctx context.Context, migrationID, key string) error {
	return s.UpsertObjectState(ctx, migrationID, key, 1, func(o *ObjectState) error {
		o.Deleted = true
		o.DestVersionID = ""
		o.DestETag = ""
		o.MigrationState = "migrated"
		return nil
	})
}

// TouchObjectAccess records that the key was read through the gateway.
func (s *Store) TouchObjectAccess(ctx context.Context, migrationID, key string) error {
	_, err := s.pool.Exec(ctx, `
		INSERT INTO metadata.object_keys (migration_id, key, generation, migration_state, last_accessed_at, updated_at)
		VALUES ($1, $2, 1, 'discovered', NOW(), NOW())
		ON CONFLICT (migration_id, key) DO UPDATE SET
		  last_accessed_at = NOW(),
		  updated_at = NOW()`,
		migrationID, key)
	return err
}

// EnqueueTask inserts a migration task if one doesn't already exist.
func (s *Store) EnqueueTask(ctx context.Context, migrationID, key, sourceVersionID, priority string) error {
	_, err := s.pool.Exec(ctx, `
		INSERT INTO metadata.migration_tasks (migration_id, key, source_version_id, priority, state)
		VALUES ($1, $2, $3, $4::metadata.migration_priority, 'pending')
		ON CONFLICT (migration_id, key, source_version_id) DO UPDATE SET
		  priority = CASE
		    WHEN metadata.migration_tasks.priority = 'access' THEN metadata.migration_tasks.priority
		    WHEN EXCLUDED.priority = 'access' THEN EXCLUDED.priority
		    WHEN metadata.migration_tasks.priority = 'high' THEN metadata.migration_tasks.priority
		    ELSE EXCLUDED.priority
		  END,
		  updated_at = NOW()`,
		migrationID, key, sourceVersionID, priority)
	return err
}

// ClaimTask acquires a pending or expired task for the worker.
func (s *Store) ClaimTask(ctx context.Context, migrationID, workerID string, lease time.Duration) (*Task, error) {
	var t Task
	row := s.pool.QueryRow(ctx, `
		WITH next_task AS (
			SELECT id
			FROM metadata.migration_tasks
			WHERE migration_id = $1
			  AND (state = 'pending' OR (state = 'claimed' AND lease_expires_at < NOW()))
			ORDER BY priority, inserted_at, id
			FOR UPDATE SKIP LOCKED
			LIMIT 1
		)
		UPDATE metadata.migration_tasks t
		SET state = 'claimed',
		    owner_id = $2,
		    lease_expires_at = NOW() + $3,
		    attempt_count = attempt_count + 1,
		    updated_at = NOW()
		FROM next_task
		WHERE t.id = next_task.id
		RETURNING t.id, t.migration_id, t.key, t.source_version_id, t.priority::text, t.state, t.owner_id, t.lease_expires_at, t.attempt_count, t.last_error, t.inserted_at, t.updated_at`,
		migrationID, workerID, lease)
	if err := row.Scan(&t.ID, &t.MigrationID, &t.Key, &t.SourceVersionID, &t.Priority, &t.State, &t.OwnerID, &t.LeaseExpiresAt, &t.AttemptCount, &t.LastError, &t.InsertedAt, &t.UpdatedAt); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, nil
		}
		return nil, err
	}
	return &t, nil
}

// RenewLease extends a task lease.
func (s *Store) RenewLease(ctx context.Context, taskID int64, workerID string, lease time.Duration) error {
	_, err := s.pool.Exec(ctx, `
		UPDATE metadata.migration_tasks
		SET lease_expires_at = NOW() + $3, updated_at = NOW()
		WHERE id = $1 AND owner_id = $2`, taskID, workerID, lease)
	return err
}

// CompleteTask marks a task done.
func (s *Store) CompleteTask(ctx context.Context, taskID int64, workerID string) error {
	_, err := s.pool.Exec(ctx, `
		UPDATE metadata.migration_tasks
		SET state = 'completed', lease_expires_at = NULL, last_error = '', updated_at = NOW()
		WHERE id = $1 AND owner_id = $2`, taskID, workerID)
	return err
}

// FailTask records a failure and either re-queues or fails permanently.
func (s *Store) FailTask(ctx context.Context, taskID int64, workerID, reason string) error {
	_, err := s.pool.Exec(ctx, `
		UPDATE metadata.migration_tasks
		SET state = CASE WHEN attempt_count >= 10 THEN 'failed' ELSE 'pending' END,
		    lease_expires_at = NULL,
		    last_error = $3,
		    updated_at = NOW()
		WHERE id = $1 AND owner_id = $2`, taskID, workerID, reason)
	return err
}

// RecordEvent writes an audit event.
func (s *Store) RecordEvent(ctx context.Context, migrationID, eventType, key, versionID string, generation int64, details map[string]any) error {
	d, _ := json.Marshal(details)
	_, err := s.pool.Exec(ctx, `
		INSERT INTO metadata.migration_events (migration_id, event_type, key, version_id, generation, details)
		VALUES ($1, $2, $3, $4, $5, $6)`, migrationID, eventType, key, versionID, generation, d)
	return err
}

// ListObjectsV2 returns a page from the coordinated catalog.
func (s *Store) ListObjectsV2(ctx context.Context, migrationID, prefix, delimiter string, maxKeys int32, continuationToken string) (*ListResult, error) {
	if maxKeys <= 0 || maxKeys > 1000 {
		maxKeys = 1000
	}
	startAfter := parseContinuationToken(continuationToken)
	// Fetch one extra to detect truncation.
	limit := maxKeys + 1
	res := &ListResult{Keys: []ListedObject{}, CommonPrefixes: []string{}}
	prefixSeen := map[string]struct{}{}

	rows, err := s.pool.Query(ctx, `
		SELECT key, size, dest_etag, dest_modified_at, metadata, checksum_sha256
		FROM metadata.object_keys
		WHERE migration_id = $1
		  AND deleted = FALSE
		  AND key > $2
		  AND ($3 = '' OR position($3 IN key) = 1)
		ORDER BY key
		LIMIT $4`, migrationID, startAfter, prefix, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	for rows.Next() {
		var lo ListedObject
		var meta json.RawMessage
		var mod sql.NullTime
		err := rows.Scan(&lo.Key, &lo.Size, &lo.ETag, &mod, &meta, &lo.ChecksumSHA256)
		if err != nil {
			return nil, err
		}
		if mod.Valid {
			lo.LastModified = mod.Time
		}
		res.Keys = append(res.Keys, lo)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	if delimiter != "" && prefix != "" {
		// Determine prefix length to chop.
		prefixLen := len(prefix)
		filtered := res.Keys[:0]
		for _, lo := range res.Keys {
			sub := lo.Key[prefixLen:]
			idx := strings.Index(sub, delimiter)
			if idx >= 0 {
				common := prefix + sub[:idx+len(delimiter)]
				if _, ok := prefixSeen[common]; !ok {
					prefixSeen[common] = struct{}{}
					res.CommonPrefixes = append(res.CommonPrefixes, common)
				}
				continue
			}
			filtered = append(filtered, lo)
		}
		res.Keys = filtered
	}

	if len(res.Keys) > int(maxKeys) {
		res.IsTruncated = true
		last := res.Keys[maxKeys-1].Key
		res.Keys = res.Keys[:maxKeys]
		res.ContinuationToken = formatContinuationToken(last)
	}
	return res, nil
}

// ListedObject is used for listing; defined here to avoid import cycle.
type ListedObject struct {
	Key            string
	Size           int64
	ETag           string
	LastModified   time.Time
	ChecksumSHA256 string
}

// ListResult is the result of a catalog listing.
type ListResult struct {
	Keys              []ListedObject
	CommonPrefixes    []string
	ContinuationToken string
	IsTruncated       bool
}

// ClaimableTasksExist returns true if there is pending work.
func (s *Store) ClaimableTasksExist(ctx context.Context, migrationID string) (bool, error) {
	var n int
	err := s.pool.QueryRow(ctx, `
		SELECT COUNT(*) FROM metadata.migration_tasks
		WHERE migration_id = $1 AND (state = 'pending' OR (state = 'claimed' AND lease_expires_at < NOW()))
		LIMIT 1`, migrationID).Scan(&n)
	if err != nil {
		return false, err
	}
	return n > 0, nil
}

// GenerateReport saves a snapshot report and returns it.
func (s *Store) GenerateReport(ctx context.Context, migrationID string) (*Report, error) {
	stats, err := s.Stats(ctx, migrationID)
	if err != nil {
		return nil, err
	}
	rows, err := s.pool.Query(ctx, `
		SELECT key, migration_state FROM metadata.object_keys
		WHERE migration_id = $1
		  AND deleted = FALSE
		  AND migration_state IN ('failed','undiscovered')
		LIMIT 1000`, migrationID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var discrepancies [][2]string
	for rows.Next() {
		var key, state string
		if err := rows.Scan(&key, &state); err != nil {
			return nil, err
		}
		discrepancies = append(discrepancies, [2]string{key, state})
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	discJSON, _ := json.Marshal(discrepancies)
	rep := &Report{
		MigrationID:      migrationID,
		TotalObjects:     stats.TotalObjects,
		MigratedObjects:  stats.MigratedObjects,
		VerifiedObjects:  stats.VerifiedObjects,
		FailedObjects:    stats.FailedObjects,
		SkippedObjects:   stats.SkippedObjects,
		TombstoneObjects: stats.TombstoneObjects,
		QueueDepth:       stats.QueueDepth,
		DiscrepancyKeys:  discrepancies,
		CanFinalise:      len(discrepancies) == 0 && stats.QueueDepth == 0,
	}
	_, err = s.pool.Exec(ctx, `
		INSERT INTO metadata.migration_reports
		(migration_id, total_objects, migrated_objects, verified_objects, failed_objects, skipped_objects, tombstone_objects, total_bytes, migrated_bytes, discrepancies, report)
		VALUES ($1, $2, $3, $4, $5, $6, $7, 0, 0, $8, $9)`,
		migrationID, rep.TotalObjects, rep.MigratedObjects, rep.VerifiedObjects, rep.FailedObjects, rep.SkippedObjects, rep.TombstoneObjects, discJSON, `{}`)
	return rep, err
}

// Report is a snapshot of migration completeness.
type Report struct {
	MigrationID      string
	TotalObjects     int64
	MigratedObjects  int64
	VerifiedObjects  int64
	FailedObjects    int64
	SkippedObjects   int64
	TombstoneObjects int64
	QueueDepth       int64
	DiscrepancyKeys  [][2]string
	CanFinalise      bool
}

// Stats returns aggregate counts for the migration.
func (s *Store) Stats(ctx context.Context, migrationID string) (*MigrationStats, error) {
	var st MigrationStats
	err := s.pool.QueryRow(ctx, `
		SELECT
		  COUNT(*) FILTER (WHERE deleted = FALSE),
		  COUNT(*) FILTER (WHERE migration_state IN ('migrated','verified')),
		  COUNT(*) FILTER (WHERE migration_state = 'verified'),
		  COUNT(*) FILTER (WHERE migration_state = 'failed'),
		  COUNT(*) FILTER (WHERE migration_state = 'skipped'),
		  COUNT(*) FILTER (WHERE deleted = TRUE)
		FROM metadata.object_keys
		WHERE migration_id = $1`, migrationID).Scan(
		&st.TotalObjects, &st.MigratedObjects, &st.VerifiedObjects,
		&st.FailedObjects, &st.SkippedObjects, &st.TombstoneObjects)
	if err != nil {
		return nil, err
	}
	err = s.pool.QueryRow(ctx, `
		SELECT COUNT(*) FROM metadata.migration_tasks
		WHERE migration_id = $1 AND state IN ('pending','claimed')`, migrationID).Scan(&st.QueueDepth)
	if err != nil {
		return nil, err
	}
	return &st, nil
}

// RecordCheckpoint stores or updates discovery progress.
func (s *Store) RecordCheckpoint(ctx context.Context, migrationID, providerRole, lastKey, lastVersionID string, objectCount, byteCount int64) error {
	_, err := s.pool.Exec(ctx, `
		INSERT INTO metadata.discovery_checkpoints (migration_id, provider_role, last_key, last_version_id, object_count, byte_count, updated_at)
		VALUES ($1, $2, $3, $4, $5, $6, NOW())
		ON CONFLICT (migration_id, provider_role) DO UPDATE SET
		  last_key = EXCLUDED.last_key,
		  last_version_id = EXCLUDED.last_version_id,
		  object_count = EXCLUDED.object_count,
		  byte_count = EXCLUDED.byte_count,
		  updated_at = NOW()`,
		migrationID, providerRole, lastKey, lastVersionID, objectCount, byteCount)
	return err
}

// GetCheckpoint returns current checkpoint.
func (s *Store) GetCheckpoint(ctx context.Context, migrationID, providerRole string) (lastKey string, lastVersionID string, objectCount, byteCount int64, err error) {
	row := s.pool.QueryRow(ctx, `
		SELECT last_key, last_version_id, object_count, byte_count
		FROM metadata.discovery_checkpoints
		WHERE migration_id = $1 AND provider_role = $2`, migrationID, providerRole)
	err = row.Scan(&lastKey, &lastVersionID, &objectCount, &byteCount)
	if errors.Is(err, pgx.ErrNoRows) {
		return "", "", 0, 0, nil
	}
	return
}

// Multipart helpers.
func (s *Store) CreateMultipartRecord(ctx context.Context, migrationID, uploadID, key, destUploadID string, metadata map[string]string) error {
	m, _ := json.Marshal(metadata)
	_, err := s.pool.Exec(ctx, `
		INSERT INTO metadata.multipart_uploads (migration_id, upload_id, key, dest_upload_id, metadata)
		VALUES ($1, $2, $3, $4, $5)`, migrationID, uploadID, key, destUploadID, m)
	return err
}

func (s *Store) GetMultipartRecord(ctx context.Context, migrationID, uploadID string) (key, destUploadID string, parts []PartInfo, err error) {
	var meta json.RawMessage
	err = s.pool.QueryRow(ctx, `
		SELECT key, dest_upload_id, metadata FROM metadata.multipart_uploads
		WHERE migration_id = $1 AND upload_id = $2 AND completed_at IS NULL AND aborted_at IS NULL`,
		migrationID, uploadID).Scan(&key, &destUploadID, &meta)
	if err != nil {
		return
	}
	rows, qerr := s.pool.Query(ctx, `
		SELECT part_number, etag, size FROM metadata.multipart_parts
		WHERE upload_id = (SELECT id FROM metadata.multipart_uploads WHERE migration_id = $1 AND upload_id = $2)
		ORDER BY part_number`, migrationID, uploadID)
	if qerr != nil {
		err = qerr
		return
	}
	defer rows.Close()
	for rows.Next() {
		var p PartInfo
		if err := rows.Scan(&p.PartNumber, &p.ETag, &p.Size); err != nil {
			return "", "", nil, err
		}
		parts = append(parts, p)
	}
	if err := rows.Err(); err != nil {
		return "", "", nil, err
	}
	return
}

func (s *Store) RecordPart(ctx context.Context, migrationID, uploadID string, partNumber int32, etag string, size int64) error {
	_, err := s.pool.Exec(ctx, `
		INSERT INTO metadata.multipart_parts (upload_id, part_number, etag, size)
		VALUES (
		  (SELECT id FROM metadata.multipart_uploads WHERE migration_id = $1 AND upload_id = $2),
		  $3, $4, $5
		)
		ON CONFLICT (upload_id, part_number) DO UPDATE SET etag = EXCLUDED.etag, size = EXCLUDED.size`,
		migrationID, uploadID, partNumber, etag, size)
	return err
}

func (s *Store) CompleteMultipartRecord(ctx context.Context, migrationID, uploadID string) error {
	_, err := s.pool.Exec(ctx, `
		UPDATE metadata.multipart_uploads SET completed_at = NOW(), updated_at = NOW()
		WHERE migration_id = $1 AND upload_id = $2`, migrationID, uploadID)
	return err
}

func (s *Store) AbortMultipartRecord(ctx context.Context, migrationID, uploadID string) error {
	_, err := s.pool.Exec(ctx, `
		UPDATE metadata.multipart_uploads SET aborted_at = NOW(), updated_at = NOW()
		WHERE migration_id = $1 AND upload_id = $2`, migrationID, uploadID)
	return err
}

// parseContinuationToken turns a catalog continuation token into a key.
func parseContinuationToken(t string) string {
	if t == "" {
		return ""
	}
	b, err := base64Decode(t)
	if err != nil {
		return t
	}
	return string(b)
}

// formatContinuationToken encodes a key as the continuation token.
func formatContinuationToken(key string) string {
	return base64Encode([]byte(key))
}

func base64Encode(b []byte) string          { return base64.RawURLEncoding.EncodeToString(b) }
func base64Decode(s string) ([]byte, error) { return base64.RawURLEncoding.DecodeString(s) }

func timeNow() time.Time { return time.Now() }

// PartInfo is a catalog part record.
type PartInfo struct {
	PartNumber int32
	ETag       string
	Size       int64
}
