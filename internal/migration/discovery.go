package migration

import (
	"context"
	"fmt"
	"time"

	"github.com/matthewgall/s3-migrateproxy/internal/providers"
)

// Discoverer scans source and destination buckets and enqueues tasks.
type Discoverer struct {
	worker *Worker
}

// NewDiscoverer creates a discoverer for the worker.
func NewDiscoverer(w *Worker) *Discoverer { return &Discoverer{worker: w} }

// RunOnce performs a single source scan up to the configured page size and checkpoint.
func (d *Discoverer) RunOnce(ctx context.Context, migrationID string) error {
	pageSize := d.worker.cfg.Worker.DiscoveryPageSize
	if pageSize <= 0 {
		pageSize = 1000
	}
	lastKey, _, objectCount, byteCount, err := d.worker.store.GetCheckpoint(ctx, migrationID, "source")
	if err != nil {
		return err
	}
	opts := providers.ListOptions{MaxKeys: pageSize, StartAfter: lastKey}
	res, err := d.worker.src.ListObjectsV2(ctx, opts)
	if err != nil {
		return fmt.Errorf("source list failed: %w", err)
	}
	for _, obj := range res.Keys {
		objectCount++
		byteCount += obj.Size
		if err := d.worker.store.EnqueueTask(ctx, migrationID, obj.Key, obj.VersionID, "background"); err != nil {
			return err
		}
		// Also ensure metadata catalog has a row.
		_ = d.worker.store.TouchObjectAccess(ctx, migrationID, obj.Key)
		lastKey = obj.Key
	}
	if err := d.worker.store.RecordCheckpoint(ctx, migrationID, "source", lastKey, "", objectCount, byteCount); err != nil {
		return err
	}
	if !res.IsTruncated {
		// One pass complete; the checkpoint remains at the last key.
		d.worker.logger.Info("source discovery pass complete", "objects", objectCount, "bytes", byteCount)
	}
	return nil
}

// Start runs periodic discovery in the background.
func (d *Discoverer) Start(ctx context.Context, migrationID string) {
	d.worker.wg.Add(1)
	go func() {
		defer d.worker.wg.Done()
		ticker := time.NewTicker(30 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-d.worker.stop:
				return
			case <-ticker.C:
				mig, err := d.worker.store.GetMigrationByName(ctx, d.worker.cfg.MigrationName)
				if err != nil || mig.State != "LIVE_MIGRATING" {
					continue
				}
				if err := d.RunOnce(ctx, mig.ID); err != nil {
					d.worker.logger.Error("discovery failed", "err", err)
				}
			}
		}
	}()
}
