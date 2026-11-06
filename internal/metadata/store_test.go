package metadata

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/matthewgall/s3-migrateproxy/internal/migrations"
)

const leaseDuration = 60 * time.Second

func testPool(t *testing.T) *pgxpool.Pool {
	url := os.Getenv("TEST_DATABASE_URL")
	if url == "" {
		t.Skip("TEST_DATABASE_URL not set")
	}
	ctx := context.Background()
	pool, err := pgxpool.New(ctx, url)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	migratePool(ctx, t, pool)
	return pool
}

func migratePool(ctx context.Context, t *testing.T, pool *pgxpool.Pool) {
	t.Helper()
	_, err := pool.Exec(ctx, `DROP SCHEMA IF EXISTS metadata CASCADE`)
	if err != nil {
		t.Fatalf("drop schema: %v", err)
	}
	// migrations package will recreate schema.
	if err := migrations.Migrate(ctx, pool); err != nil {
		t.Fatalf("migrate: %v", err)
	}
}

func TestObjectStateUpsertAndGet(t *testing.T) {
	pool := testPool(t)
	defer pool.Close()
	ctx := context.Background()
	store := NewStore(pool)

	migID := mustCreateMigration(ctx, t, pool)
	if err := store.UpsertObjectState(ctx, migID, "obj1", 1, func(o *ObjectState) error {
		o.DestVersionID = "v1"
		o.DestETag = `"etag"`
		o.MigrationState = "migrated"
		return nil
	}); err != nil {
		t.Fatalf("upsert: %v", err)
	}
	state, err := store.GetObjectState(ctx, migID, "obj1")
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if state.DestVersionID != "v1" {
		t.Fatalf("version mismatch")
	}

	if err := store.MarkDeleted(ctx, migID, "obj1"); err != nil {
		t.Fatalf("delete: %v", err)
	}
	state, err = store.GetObjectState(ctx, migID, "obj1")
	if err != nil {
		t.Fatalf("get after delete: %v", err)
	}
	if !state.Deleted {
		t.Fatal("expected tombstone")
	}
}

func TestTaskQueue(t *testing.T) {
	pool := testPool(t)
	defer pool.Close()
	ctx := context.Background()
	store := NewStore(pool)
	migID := mustCreateMigration(ctx, t, pool)

	if err := store.EnqueueTask(ctx, migID, "key1", "", "background"); err != nil {
		t.Fatalf("enqueue: %v", err)
	}
	task, err := store.ClaimTask(ctx, migID, "worker-1", leaseDuration)

	if err != nil {
		t.Fatalf("claim: %v", err)
	}
	if task == nil {
		t.Fatal("expected task")
	}
	if task.Key != "key1" {
		t.Fatalf("key mismatch: %s", task.Key)
	}
	if err := store.CompleteTask(ctx, task.ID, "worker-1"); err != nil {
		t.Fatalf("complete: %v", err)
	}
}

func mustCreateMigration(ctx context.Context, t *testing.T, pool *pgxpool.Pool) string {
	var sourceID, destID string
	_ = pool.QueryRow(ctx, `INSERT INTO metadata.buckets (role, provider, region, bucket) VALUES ('source','generic','us-east-1','s') RETURNING id`).Scan(&sourceID)
	_ = pool.QueryRow(ctx, `INSERT INTO metadata.buckets (role, provider, region, bucket) VALUES ('destination','generic','us-east-1','d') RETURNING id`).Scan(&destID)
	var migID string
	err := pool.QueryRow(ctx, `INSERT INTO metadata.migrations (name, source_bucket_id, dest_bucket_id) VALUES ($1,$2,$3) RETURNING id`, t.Name(), sourceID, destID).Scan(&migID)
	if err != nil {
		t.Fatalf("migration: %v", err)
	}
	return migID
}
