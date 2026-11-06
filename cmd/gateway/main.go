package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/matthewgall/s3-migrateproxy/internal/admin"
	"github.com/matthewgall/s3-migrateproxy/internal/config"
	"github.com/matthewgall/s3-migrateproxy/internal/gateway"
	"github.com/matthewgall/s3-migrateproxy/internal/migrations"
	"github.com/matthewgall/s3-migrateproxy/internal/providers"
	"github.com/spf13/cobra"
)

var rootCmd = &cobra.Command{
	Use:   "gateway",
	Short: "Run the S3-compatible live migration gateway",
	RunE:  run,
}

func main() {
	config.BindFlags(rootCmd)
	if err := rootCmd.Execute(); err != nil {
		slog.Error("gateway failed", "err", err)
		os.Exit(1)
	}
}

func run(cmd *cobra.Command, _ []string) error {
	cfg, err := config.Load(cmd)
	if err != nil {
		return fmt.Errorf("load config: %w", err)
	}
	if cfg.Admin.APIKeyHash == "" {
		cfg.Admin.APIKeyHash = os.Getenv("S3MIGRATE_ADMIN_API_KEY")
	}

	level := slog.LevelInfo
	_ = level.UnmarshalText([]byte(cfg.LogLevel))
	logger := slog.New(slog.NewJSONHandler(os.Stderr, &slog.HandlerOptions{Level: level}))

	if err := cfg.Validate(); err != nil {
		return fmt.Errorf("invalid config: %w", err)
	}
	logger.Info("starting gateway", "config", cfg.Redacted())

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	if cfg.Database.URL == "" {
		return fmt.Errorf("database URL is required")
	}
	pool, err := pgxpool.New(ctx, cfg.Database.URL)
	if err != nil {
		return fmt.Errorf("connect database: %w", err)
	}
	defer pool.Close()

	if err := migrations.Migrate(ctx, pool); err != nil {
		return fmt.Errorf("migrations failed: %w", err)
	}

	src, err := providers.NewAWS(ctx, cfg.Source)
	if err != nil {
		return fmt.Errorf("source provider: %w", err)
	}
	dst, err := providers.NewAWS(ctx, cfg.Destination)
	if err != nil {
		return fmt.Errorf("destination provider: %w", err)
	}

	gwServer := gateway.NewServer(cfg, pool, src, dst, logger)
	adminServer := admin.NewServer(cfg, pool, src, dst, logger)

	errCh := make(chan error, 2)
	go func() {
		logger.Info("listening for S3 gateway", "addr", cfg.Gateway.ListenAddr)
		errCh <- gwServer.Serve(ctx)
	}()
	go func() {
		logger.Info("listening for admin API", "addr", cfg.Admin.ListenAddr)
		errCh <- adminServer.Serve(ctx)
	}()

	select {
	case <-ctx.Done():
		logger.Info("gateway stopping")
		// Servers already handle their own graceful shutdown; give them a moment.
		time.Sleep(100 * time.Millisecond)
		return nil
	case err := <-errCh:
		return err
	}
}
