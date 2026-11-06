package migration

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/matthewgall/s3-migrateproxy/internal/config"
	"github.com/matthewgall/s3-migrateproxy/internal/metadata"
	"github.com/matthewgall/s3-migrateproxy/internal/migrations"
)

func testConfig() *config.Config {
	return &config.Config{
		MigrationName: "test",
		Worker: config.WorkerConfig{
			WorkerID:         "worker-test",
			LeaseDuration:    60 * time.Second,
			TransferPartSize: 5 << 20,
			VerifyChecksum:   false,
		},
	}
}

func testMetadataStore(pool *pgxpool.Pool) *metadata.Store {
	return metadata.NewStore(pool)
}

func testDBPool(t *testing.T) *pgxpool.Pool {
	url := os.Getenv("TEST_DATABASE_URL")
	if url == "" {
		t.Skip("TEST_DATABASE_URL not set")
	}
	ctx := context.Background()
	pool, err := pgxpool.New(ctx, url)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	if _, err := pool.Exec(ctx, `DROP SCHEMA IF EXISTS metadata CASCADE`); err != nil {
		t.Fatalf("drop schema: %v", err)
	}
	if err := migrations.Migrate(ctx, pool); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	return pool
}

func mustMigrationID(ctx context.Context, t *testing.T, pool *pgxpool.Pool) string {
	t.Helper()
	var srcID, destID, migID string
	_ = pool.QueryRow(ctx, `INSERT INTO metadata.buckets (role, provider, region, bucket) VALUES ('source','generic','us-east-1','s') RETURNING id`).Scan(&srcID)
	_ = pool.QueryRow(ctx, `INSERT INTO metadata.buckets (role, provider, region, bucket) VALUES ('destination','generic','us-east-1','d') RETURNING id`).Scan(&destID)
	if err := pool.QueryRow(ctx, `INSERT INTO metadata.migrations (name, source_bucket_id, dest_bucket_id) VALUES ($1,$2,$3) RETURNING id`, t.Name(), srcID, destID).Scan(&migID); err != nil {
		t.Fatalf("migration: %v", err)
	}
	return migID
}
