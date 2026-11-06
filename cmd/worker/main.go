package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/matthewgall/s3-migrateproxy/internal/config"
	"github.com/matthewgall/s3-migrateproxy/internal/metadata"
	"github.com/matthewgall/s3-migrateproxy/internal/migration"
	"github.com/matthewgall/s3-migrateproxy/internal/migrations"
	"github.com/matthewgall/s3-migrateproxy/internal/providers"
	"github.com/spf13/cobra"
)

var rootCmd = &cobra.Command{
	Use:   "worker",
	Short: "Run the background migration worker",
	RunE:  run,
}

func main() {
	config.BindFlags(rootCmd)
	if err := rootCmd.Execute(); err != nil {
		slog.Error("worker failed", "err", err)
		os.Exit(1)
	}
}

func run(cmd *cobra.Command, _ []string) error {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	slog.SetDefault(logger)

	cfg, err := config.Load(cmd)
	if err != nil {
		return fmt.Errorf("load config: %w", err)
	}
	if err := cfg.Validate(); err != nil {
		return fmt.Errorf("invalid config: %w", err)
	}
	logger.Info("loaded configuration", "config", cfg.Redacted())

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	pool, err := pgxpool.New(ctx, cfg.Database.URL)
	if err != nil {
		return fmt.Errorf("connect database: %w", err)
	}
	defer pool.Close()

	if err := migrations.Migrate(ctx, pool); err != nil {
		return fmt.Errorf("run migrations: %w", err)
	}

	src, err := providers.NewAWS(ctx, cfg.Source)
	if err != nil {
		return fmt.Errorf("source provider: %w", err)
	}
	dst, err := providers.NewAWS(ctx, cfg.Destination)
	if err != nil {
		return fmt.Errorf("destination provider: %w", err)
	}

	var migrationID string
	if err := pool.QueryRow(ctx, `SELECT id FROM metadata.migrations WHERE name = $1`, cfg.MigrationName).Scan(&migrationID); err != nil {
		return fmt.Errorf("migration not found; ensure gateway has created the migration record: %w", err)
	}

	store := metadata.NewStore(pool)
	worker := migration.NewWorker(cfg, store, src, dst, logger)
	discoverer := migration.NewDiscoverer(worker)

	worker.Start(ctx, migrationID)
	discoverer.Start(ctx, migrationID)

	logger.Info("worker started", "worker_id", cfg.Worker.WorkerID, "migration_id", migrationID)
	<-ctx.Done()

	logger.Info("shutting down worker")
	worker.Stop()
	return nil
}
