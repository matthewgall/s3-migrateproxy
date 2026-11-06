package migration

import (
	"bytes"
	"context"
	"errors"
	"io"
	"testing"

	"github.com/matthewgall/s3-migrateproxy/internal/providers"
)

func TestTransferMultipart(t *testing.T) {
	pool := testDBPool(t)
	defer pool.Close()
	ctx := context.Background()
	migID := mustMigrationID(ctx, t, pool)

	src := providers.NewFakeProvider("source")
	dst := providers.NewFakeProvider("destination")

	cfg := testConfig()
	store := testMetadataStore(pool)
	transferer := NewTransferer(cfg, store, src, dst)

	data := make([]byte, 13<<20)
	for i := range data {
		data[i] = byte(i % 256)
	}
	if _, err := src.PutObject(ctx, "big.bin", bytes.NewReader(data), int64(len(data)), providers.PutOptions{ContentType: "application/octet-stream"}); err != nil {
		t.Fatalf("source put: %v", err)
	}

	if err := store.EnqueueTask(ctx, migID, "big.bin", "", "background"); err != nil {
		t.Fatalf("enqueue: %v", err)
	}
	task, err := store.ClaimTask(ctx, migID, cfg.Worker.WorkerID, cfg.Worker.LeaseDuration)
	if err != nil {
		t.Fatalf("claim: %v", err)
	}
	if task == nil {
		t.Fatal("expected task")
	}

	if err := transferer.Transfer(ctx, task); err != nil {
		t.Fatalf("transfer: %v", err)
	}

	obj, err := dst.GetObject(ctx, "big.bin", "", "")
	if err != nil {
		t.Fatalf("destination get: %v", err)
	}
	defer obj.Body.Close()
	got := make([]byte, len(data))
	if _, err := obj.Body.Read(got); err != nil && !errors.Is(err, io.EOF) {
		t.Fatalf("read: %v", err)
	}
	if !bytes.Equal(got, data) {
		t.Fatalf("destination data mismatch")
	}

	state, err := store.GetObjectState(ctx, migID, "big.bin")
	if err != nil {
		t.Fatalf("state: %v", err)
	}
	if state.MigrationState != "migrated" {
		t.Fatalf("expected migrated, got %s", state.MigrationState)
	}
}

func TestTransferSingle(t *testing.T) {
	pool := testDBPool(t)
	defer pool.Close()
	ctx := context.Background()
	migID := mustMigrationID(ctx, t, pool)

	src := providers.NewFakeProvider("source")
	dst := providers.NewFakeProvider("destination")

	cfg := testConfig()
	store := testMetadataStore(pool)
	tr := NewTransferer(cfg, store, src, dst)

	data := []byte("hello")
	if _, err := src.PutObject(ctx, "small.txt", bytes.NewReader(data), int64(len(data)), providers.PutOptions{}); err != nil {
		t.Fatal(err)
	}
	if err := store.EnqueueTask(ctx, migID, "small.txt", "", "background"); err != nil {
		t.Fatal(err)
	}
	task, err := store.ClaimTask(ctx, migID, cfg.Worker.WorkerID, cfg.Worker.LeaseDuration)
	if err != nil {
		t.Fatal(err)
	}
	if err := tr.Transfer(ctx, task); err != nil {
		t.Fatalf("transfer: %v", err)
	}
	obj, err := dst.GetObject(ctx, "small.txt", "", "")
	if err != nil {
		t.Fatal(err)
	}
	defer obj.Body.Close()
	got, _ := io.ReadAll(obj.Body)
	if !bytes.Equal(got, data) {
		t.Fatal("data mismatch")
	}
}
