package metadata

import (
	"database/sql"
	"time"
)

// Migration represents one migration run.
type Migration struct {
	ID               string
	Name             string
	State            string
	SourceBucketID   string
	DestBucketID     string
	Options          map[string]any
	CreatedAt        time.Time
	UpdatedAt        time.Time
	CompletedAt      sql.NullTime
}

// ObjectState is the authoritative metadata for a key within a migration.
type ObjectState struct {
	MigrationID     string
	Key             string
	Generation      int64
	Deleted         bool
	SourceVersionID string
	DestVersionID   string
	SourceETag      string
	DestETag        string
	Size            sql.NullInt64
	ContentType     sql.NullString
	Metadata        map[string]string
	ChecksumSHA256  string
	ChecksumAlgorithm string
	MigrationState  string
	LastAccessedAt  sql.NullTime
	SourceModifiedAt sql.NullTime
	DestModifiedAt  sql.NullTime
	CreatedAt       time.Time
	UpdatedAt       time.Time
}

// HasDest returns true if the object is known to exist in destination.
func (o *ObjectState) HasDest() bool {
	return !o.Deleted && o.DestVersionID != ""
}

// Task is a migration work item.
type Task struct {
	ID            int64
	MigrationID   string
	Key           string
	SourceVersionID string
	Priority      string
	State         string
	OwnerID       string
	LeaseExpiresAt sql.NullTime
	AttemptCount  int
	LastError     string
	InsertedAt    time.Time
	UpdatedAt     time.Time
}

// MigrationStats holds progress counters.
type MigrationStats struct {
	TotalObjects     int64
	MigratedObjects  int64
	VerifiedObjects  int64
	FailedObjects    int64
	SkippedObjects   int64
	TombstoneObjects int64
	QueueDepth       int64
}
