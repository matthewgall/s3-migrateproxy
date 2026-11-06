package migrations

import (
	"context"
	"embed"
	"fmt"
	"sort"
	"strings"

	"github.com/jackc/pgx/v5/pgxpool"
)

//go:embed *.sql
var sqlFiles embed.FS

// Migrate runs any migration files not already applied.
func Migrate(ctx context.Context, pool *pgxpool.Pool) error {
	if _, err := pool.Exec(ctx, `
		CREATE SCHEMA IF NOT EXISTS metadata;
		CREATE TABLE IF NOT EXISTS metadata.applied_migrations (
			filename TEXT PRIMARY KEY,
			applied_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
		)`); err != nil {
		return fmt.Errorf("create migration tracking: %w", err)
	}

	files, err := sqlFiles.ReadDir(".")
	if err != nil {
		return err
	}
	sort.Slice(files, func(i, j int) bool { return files[i].Name() < files[j].Name() })

	for _, f := range files {
		if !strings.HasSuffix(f.Name(), ".sql") {
			continue
		}
		var applied bool
		err := pool.QueryRow(ctx, `SELECT TRUE FROM metadata.applied_migrations WHERE filename = $1`, f.Name()).Scan(&applied)
		if err == nil && applied {
			continue
		}
		sql, err := sqlFiles.ReadFile(f.Name())
		if err != nil {
			return fmt.Errorf("read %s: %w", f.Name(), err)
		}
		if err := runInTx(ctx, pool, string(sql)); err != nil {
			return fmt.Errorf("apply %s: %w", f.Name(), err)
		}
		_, err = pool.Exec(ctx, `INSERT INTO metadata.applied_migrations (filename) VALUES ($1) ON CONFLICT (filename) DO NOTHING`, f.Name())
		if err != nil {
			return fmt.Errorf("record %s: %w", f.Name(), err)
		}
	}
	return nil
}

func runInTx(ctx context.Context, pool *pgxpool.Pool, sql string) error {
	tx, err := pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if _, err := tx.Exec(ctx, sql); err != nil {
		return err
	}
	return tx.Commit(ctx)
}
