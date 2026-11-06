package migration

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/matthewgall/s3-migrateproxy/internal/config"
	"github.com/matthewgall/s3-migrateproxy/internal/metadata"
	"github.com/matthewgall/s3-migrateproxy/internal/metrics"
	"github.com/matthewgall/s3-migrateproxy/internal/providers"
)

// Worker consumes migration tasks and copies source objects to destination.
type Worker struct {
	cfg        *config.Config
	store      *metadata.Store
	src        providers.Provider
	dst        providers.Provider
	id         string
	logger     *slog.Logger
	stop       chan struct{}
	wg         sync.WaitGroup
	semaphore  chan struct{}
	transferer *Transferer
}

// NewWorker constructs a migration worker.
func NewWorker(cfg *config.Config, store *metadata.Store, src, dst providers.Provider, logger *slog.Logger) *Worker {
	max := cfg.Worker.MaxConcurrentTasks
	if max <= 0 {
		max = 10
	}
	return &Worker{
		cfg:        cfg,
		store:      store,
		src:        src,
		dst:        dst,
		id:         cfg.Worker.WorkerID,
		logger:     logger,
		stop:       make(chan struct{}),
		semaphore:  make(chan struct{}, max),
		transferer: NewTransferer(cfg, store, src, dst),
	}
}

// Start begins the worker loop.
func (w *Worker) Start(ctx context.Context, migrationID string) {
	w.wg.Add(1)
	go func() {
		defer w.wg.Done()
		ticker := time.NewTicker(w.cfg.Worker.PollInterval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-w.stop:
				return
			case <-ticker.C:
				mig, err := w.store.GetMigrationByName(ctx, w.cfg.MigrationName)
				if err != nil {
					continue
				}
				if mig.State == "LIVE_MIGRATING" || mig.State == "RECONCILING" {
					w.processBatch(ctx, mig.ID)
				} else if mig.State == "FINALISING" {
					if err := w.drainAndComplete(ctx, mig.ID); err != nil {
						w.logger.Error("finalisation check failed", "err", err)
					}
				}
			}
		}
	}()
}

// Stop gracefully shuts down the worker.
func (w *Worker) Stop() {
	close(w.stop)
	w.wg.Wait()
}

func (w *Worker) processBatch(ctx context.Context, migrationID string) {
	for {
		task, err := w.store.ClaimTask(ctx, migrationID, w.id, w.cfg.Worker.LeaseDuration)
		if err != nil {
			w.logger.Error("claim task failed", "err", err)
			return
		}
		if task == nil {
			return
		}
		w.semaphore <- struct{}{}
		go func(t *metadata.Task) {
			defer func() { <-w.semaphore }()
			if err := w.runTask(ctx, t); err != nil {
				w.logger.Error("task failed", "key", t.Key, "err", err)
				_ = w.store.FailTask(ctx, t.ID, w.id, err.Error())
				metrics.MigrationFailures.Inc()
			}
		}(task)
	}
}

func (w *Worker) drainAndComplete(ctx context.Context, migrationID string) error {
	claimable, err := w.store.ClaimableTasksExist(ctx, migrationID)
	if err != nil {
		return err
	}
	if claimable {
		// Still work to do; remain in FINALISING.
		return nil
	}
	if _, err := w.store.GenerateReport(ctx, migrationID); err != nil {
		return err
	}
	return w.store.UpdateMigrationState(ctx, migrationID, "COMPLETED")
}

func (w *Worker) runTask(ctx context.Context, task *metadata.Task) error {
	renewCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	// Lease heartbeat.
	done := make(chan struct{})
	defer close(done)
	go func() {
		tick := time.NewTicker(w.cfg.Worker.LeaseRenewInterval)
		defer tick.Stop()
		for {
			select {
			case <-renewCtx.Done():
				return
			case <-done:
				return
			case <-tick.C:
				if err := w.store.RenewLease(renewCtx, task.ID, w.id, w.cfg.Worker.LeaseDuration); err != nil {
					w.logger.Warn("lease renew failed", "task", task.ID, "err", err)
					return
				}
			}
		}
	}()

	if err := w.transferer.Transfer(ctx, task); err != nil {
		return fmt.Errorf("transfer failed: %w", err)
	}
	return nil
}


