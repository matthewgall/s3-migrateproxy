package migration

import (
	"bytes"
	"context"
	"database/sql"
	"io"
	"testing"
	"time"

	"github.com/matthewgall/s3-migrateproxy/internal/metadata"
	"github.com/matthewgall/s3-migrateproxy/internal/providers"
)

// TestWorkerDoesNotOverwriteNewerDestination proves R4.
func TestWorkerDoesNotOverwriteNewerDestination(t *testing.T) {
	pool := testDBPool(t)
	defer pool.Close()
	ctx := context.Background()
	migID := mustMigrationID(ctx, t, pool)

	src := providers.NewFakeProvider("source")
	dst := providers.NewFakeProvider("destination")

	cfg := testConfig()
	store := testMetadataStore(pool)
	tr := NewTransferer(cfg, store, src, dst)

	v1 := []byte("version-one")
	v2 := []byte("version-two-is-longer")

	if _, err := src.PutObject(ctx, "race.txt", bytes.NewReader(v1), int64(len(v1)), providers.PutOptions{}); err != nil {
		t.Fatal(err)
	}
	// Gateway writes a newer version to destination.
	if _, err := dst.PutObject(ctx, "race.txt", bytes.NewReader(v2), int64(len(v2)), providers.PutOptions{}); err != nil {
		t.Fatal(err)
	}
	// Record that newer destination state.
	if err := store.UpsertObjectState(ctx, migID, "race.txt", 1, func(o *metadata.ObjectState) error {
		o.DestETag = `"new"`
		o.DestVersionID = "v-new"
		o.MigrationState = "migrated"
		o.DestModifiedAt = sql.NullTime{Time: time.Now(), Valid: true}
		return nil
	}); err != nil {
		t.Fatal(err)
	}

	if err := store.EnqueueTask(ctx, migID, "race.txt", "", "background"); err != nil {
		t.Fatal(err)
	}
	task, err := store.ClaimTask(ctx, migID, cfg.Worker.WorkerID, cfg.Worker.LeaseDuration)
	if err != nil {
		t.Fatal(err)
	}

	if err := tr.Transfer(ctx, task); err != nil {
		t.Fatalf("transfer: %v", err)
	}

	// Destination must still contain v2.
	obj, err := dst.GetObject(ctx, "race.txt", "", "")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = obj.Body.Close() }()
	got, _ := io.ReadAll(obj.Body)
	if !bytes.Equal(got, v2) {
		t.Fatalf("destination was overwritten: expected %q, got %q", v2, got)
	}
}

// TestTombstoneBlocksSourceResurrection proves R5.
func TestTombstoneBlocksSourceResurrection(t *testing.T) {
	pool := testDBPool(t)
	defer pool.Close()
	ctx := context.Background()
	migID := mustMigrationID(ctx, t, pool)

	src := providers.NewFakeProvider("source")
	dst := providers.NewFakeProvider("destination")

	cfg := testConfig()
	store := testMetadataStore(pool)
	tr := NewTransferer(cfg, store, src, dst)

	v1 := []byte("still-in-source")
	if _, err := src.PutObject(ctx, "deleted.txt", bytes.NewReader(v1), int64(len(v1)), providers.PutOptions{}); err != nil {
		t.Fatal(err)
	}
	// Gateway records a tombstone.
	if err := store.MarkDeleted(ctx, migID, "deleted.txt"); err != nil {
		t.Fatal(err)
	}

	if err := store.EnqueueTask(ctx, migID, "deleted.txt", "", "background"); err != nil {
		t.Fatal(err)
	}
	task, err := store.ClaimTask(ctx, migID, cfg.Worker.WorkerID, cfg.Worker.LeaseDuration)
	if err != nil {
		t.Fatal(err)
	}

	if err := tr.Transfer(ctx, task); err != nil {
		t.Fatalf("transfer: %v", err)
	}

	if _, err := dst.GetObject(ctx, "deleted.txt", "", ""); !providers.IsNotFound(err) {
		t.Fatal("expected deleted object to remain deleted in destination")
	}
}
