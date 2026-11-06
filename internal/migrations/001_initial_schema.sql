-- S3 Migration Gateway — Initial Schema
-- PostgreSQL 15+

CREATE EXTENSION IF NOT EXISTS "pgcrypto";
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

CREATE SCHEMA IF NOT EXISTS metadata;

-- Top-level migration record and lifecycle state.
CREATE TABLE metadata.migrations (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    name TEXT NOT NULL UNIQUE,
    state TEXT NOT NULL DEFAULT 'PREP',
    source_bucket_id UUID NOT NULL,
    dest_bucket_id UUID NOT NULL,
    options JSONB NOT NULL DEFAULT '{}',
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    completed_at TIMESTAMPTZ
);

-- Bucket configuration. Credentials are stored separately so the gateway can
-- rotate credentials without changing bucket metadata.
CREATE TABLE metadata.buckets (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    role TEXT NOT NULL CHECK (role IN ('source','destination')),
    provider TEXT NOT NULL, -- e.g. 'aws', 'b2', 'r2', 'wasabi', 'generic'
    endpoint TEXT,
    region TEXT NOT NULL,
    bucket TEXT NOT NULL,
    path_style BOOLEAN NOT NULL DEFAULT FALSE,
    tls_skip_verify BOOLEAN NOT NULL DEFAULT FALSE,
    max_concurrency INT NOT NULL DEFAULT 100,
    request_timeout_ms INT NOT NULL DEFAULT 30000,
    retry_max_attempts INT NOT NULL DEFAULT 3,
    capabilities JSONB NOT NULL DEFAULT '{}',
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (role, provider, endpoint, region, bucket)
);

-- Encrypted upstream credentials. In production these should be loaded from a
-- KMS/secrets manager and only the reference stored here. For this reference
-- implementation we store the secret encrypted with a server-managed key
-- derived from the S3_GATEWAY_MASTER_KEY environment variable.
CREATE TABLE metadata.bucket_credentials (
    bucket_id UUID PRIMARY KEY REFERENCES metadata.buckets(id) ON DELETE CASCADE,
    access_key_id TEXT NOT NULL,
    secret_access_key TEXT NOT NULL,
    session_token TEXT,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Client credentials for the S3-compatible gateway listener.
CREATE TABLE metadata.gateway_credentials (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    migration_id UUID REFERENCES metadata.migrations(id) ON DELETE CASCADE,
    access_key TEXT NOT NULL UNIQUE,
    secret_hash TEXT NOT NULL,
    secret_encrypted TEXT NOT NULL,
    secret_prefix TEXT NOT NULL,
    description TEXT,
    active BOOLEAN NOT NULL DEFAULT TRUE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    expires_at TIMESTAMPTZ,
    revoked_at TIMESTAMPTZ
);

-- Authoritative object state. One row per (migration_id, key).
-- This is the source of truth for generations, tombstones, and listing.
CREATE TABLE metadata.object_keys (
    migration_id UUID NOT NULL REFERENCES metadata.migrations(id) ON DELETE CASCADE,
    key TEXT NOT NULL,
    generation BIGINT NOT NULL DEFAULT 1,
    deleted BOOLEAN NOT NULL DEFAULT FALSE,
    -- latest version information from each provider (may be NULL)
    source_version_id TEXT,
    dest_version_id TEXT,
    source_etag TEXT,
    dest_etag TEXT,
    size BIGINT,
    content_type TEXT,
    metadata JSONB NOT NULL DEFAULT '{}',
    checksum_sha256 TEXT,
    checksum_algorithm TEXT,
    migration_state TEXT NOT NULL DEFAULT 'undiscovered'
        CHECK (migration_state IN ('undiscovered','discovered','queued','claimed','migrated','verified','failed','skipped')),
    last_accessed_at TIMESTAMPTZ,
    source_modified_at TIMESTAMPTZ,
    dest_modified_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (migration_id, key)
);

CREATE INDEX idx_object_keys_list
ON metadata.object_keys (migration_id, key)
WHERE deleted = FALSE;

CREATE INDEX idx_object_keys_migration_state
ON metadata.object_keys (migration_id, migration_state, updated_at);

-- Native provider versions when S3 versioning is enabled/working.
CREATE TABLE metadata.object_versions (
    migration_id UUID NOT NULL REFERENCES metadata.migrations(id) ON DELETE CASCADE,
    key TEXT NOT NULL,
    version_id TEXT NOT NULL,
    is_latest BOOLEAN NOT NULL DEFAULT FALSE,
    is_delete_marker BOOLEAN NOT NULL DEFAULT FALSE,
    provider TEXT NOT NULL CHECK (provider IN ('source','destination')),
    etag TEXT,
    size BIGINT,
    modified_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (migration_id, key, version_id, provider)
);

-- Multipart upload state persisted so gateway restarts can recover uploads.
CREATE TABLE metadata.multipart_uploads (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    migration_id UUID NOT NULL REFERENCES metadata.migrations(id) ON DELETE CASCADE,
    upload_id TEXT NOT NULL,
    key TEXT NOT NULL,
    dest_upload_id TEXT NOT NULL,
    metadata JSONB NOT NULL DEFAULT '{}',
    initiated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    completed_at TIMESTAMPTZ,
    aborted_at TIMESTAMPTZ
);

CREATE UNIQUE INDEX idx_multipart_uploads_id
ON metadata.multipart_uploads (migration_id, upload_id);

CREATE TABLE metadata.multipart_parts (
    upload_id UUID NOT NULL REFERENCES metadata.multipart_uploads(id) ON DELETE CASCADE,
    part_number INT NOT NULL,
    etag TEXT NOT NULL,
    size BIGINT,
    PRIMARY KEY (upload_id, part_number)
);

-- Discovery checkpoint for resumable source/destination scans.
CREATE TABLE metadata.discovery_checkpoints (
    migration_id UUID NOT NULL REFERENCES metadata.migrations(id) ON DELETE CASCADE,
    provider_role TEXT NOT NULL CHECK (provider_role IN ('source','destination')),
    last_key TEXT,
    last_version_id TEXT,
    is_complete BOOLEAN NOT NULL DEFAULT FALSE,
    object_count BIGINT NOT NULL DEFAULT 0,
    byte_count BIGINT NOT NULL DEFAULT 0,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (migration_id, provider_role)
);

-- Durable migration task queue.
CREATE TYPE metadata.migration_priority AS ENUM ('access', 'high', 'background');

CREATE TABLE metadata.migration_tasks (
    id BIGSERIAL PRIMARY KEY,
    migration_id UUID NOT NULL REFERENCES metadata.migrations(id) ON DELETE CASCADE,
    key TEXT NOT NULL,
    source_version_id TEXT,
    priority metadata.migration_priority NOT NULL DEFAULT 'background',
    state TEXT NOT NULL DEFAULT 'pending'
        CHECK (state IN ('pending','claimed','completed','failed','cancelled')),
    owner_id TEXT,
    lease_expires_at TIMESTAMPTZ,
    attempt_count INT NOT NULL DEFAULT 0,
    last_error TEXT,
    inserted_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (migration_id, key, source_version_id)
);

-- Index pending and claimed tasks. Use of NOW() in an index predicate is not
-- allowed because it is not immutable; the lease expiry is checked at query time.
CREATE INDEX idx_migration_tasks_claim
ON metadata.migration_tasks (migration_id, state, priority, inserted_at, id)
WHERE state = 'pending' OR state = 'claimed';

CREATE INDEX idx_migration_tasks_migration
ON metadata.migration_tasks (migration_id, state);

-- Mutation and lifecycle events for audit and replay debugging.
CREATE TABLE metadata.migration_events (
    id BIGSERIAL PRIMARY KEY,
    migration_id UUID NOT NULL REFERENCES metadata.migrations(id) ON DELETE CASCADE,
    event_type TEXT NOT NULL,
    key TEXT,
    version_id TEXT,
    generation BIGINT,
    details JSONB NOT NULL DEFAULT '{}',
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_migration_events_migration
ON metadata.migration_events (migration_id, created_at DESC);

-- Final reconciliation report snapshot.
CREATE TABLE metadata.migration_reports (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    migration_id UUID NOT NULL REFERENCES metadata.migrations(id) ON DELETE CASCADE,
    generated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    total_objects BIGINT NOT NULL DEFAULT 0,
    migrated_objects BIGINT NOT NULL DEFAULT 0,
    verified_objects BIGINT NOT NULL DEFAULT 0,
    failed_objects BIGINT NOT NULL DEFAULT 0,
    skipped_objects BIGINT NOT NULL DEFAULT 0,
    tombstone_objects BIGINT NOT NULL DEFAULT 0,
    total_bytes BIGINT NOT NULL DEFAULT 0,
    migrated_bytes BIGINT NOT NULL DEFAULT 0,
    discrepancies JSONB NOT NULL DEFAULT '[]',
    report JSONB NOT NULL DEFAULT '{}'
);

-- Trigger helper for updated_at.
CREATE OR REPLACE FUNCTION metadata.set_updated_at()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trg_migrations_updated_at
BEFORE UPDATE ON metadata.migrations FOR EACH ROW EXECUTE FUNCTION metadata.set_updated_at();

CREATE TRIGGER trg_buckets_updated_at
BEFORE UPDATE ON metadata.buckets FOR EACH ROW EXECUTE FUNCTION metadata.set_updated_at();

CREATE TRIGGER trg_object_keys_updated_at
BEFORE UPDATE ON metadata.object_keys FOR EACH ROW EXECUTE FUNCTION metadata.set_updated_at();

CREATE TRIGGER trg_multipart_uploads_updated_at
BEFORE UPDATE ON metadata.multipart_uploads FOR EACH ROW EXECUTE FUNCTION metadata.set_updated_at();

CREATE TRIGGER trg_migration_tasks_updated_at
BEFORE UPDATE ON metadata.migration_tasks FOR EACH ROW EXECUTE FUNCTION metadata.set_updated_at();
