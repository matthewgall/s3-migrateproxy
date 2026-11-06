package config

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/spf13/cobra"
)

func TestLoadFromEnv(t *testing.T) {
	for k := range envSnapshot(true) {
		t.Setenv(k, "")
	}
	t.Setenv("S3MIGRATE_MIGRATION_NAME", "env-test")
	t.Setenv("S3MIGRATE_DATABASE_URL", "postgres://localhost/db")
	t.Setenv("S3MIGRATE_SOURCE_ENDPOINT", "https://s3.env.example")
	t.Setenv("S3MIGRATE_SOURCE_BUCKET", "env-src")
	t.Setenv("S3MIGRATE_DESTINATION_REGION", "eu-west-1")
	t.Setenv("S3MIGRATE_LOG_LEVEL", "debug")
	t.Setenv("S3MIGRATE_WORKER_MAX_CONCURRENT_TASKS", "4")

	cfg, err := LoadDirect()
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if cfg.MigrationName != "env-test" {
		t.Fatalf("migration name mismatch: %s", cfg.MigrationName)
	}
	if cfg.Database.URL != "postgres://localhost/db" {
		t.Fatalf("database url mismatch")
	}
	if cfg.Source.Endpoint != "https://s3.env.example" {
		t.Fatalf("source endpoint mismatch")
	}
	if cfg.Destination.Region != "eu-west-1" {
		t.Fatalf("destination region mismatch")
	}
	if cfg.LogLevel != "debug" {
		t.Fatalf("log level mismatch")
	}
	if cfg.Worker.MaxConcurrentTasks != 4 {
		t.Fatalf("max concurrent tasks mismatch")
	}
}

func TestLoadFromEnvAliases(t *testing.T) {
	for k := range envSnapshot(true) {
		t.Setenv(k, "")
	}
	t.Setenv("S3MIGRATE_MIGRATION_NAME", "alias-test")
	t.Setenv("S3MIGRATE_SRC_ENDPOINT", "https://src.alias")
	t.Setenv("S3MIGRATE_DEST_ENDPOINT", "https://dest.alias")
	cfg, err := LoadDirect()
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if cfg.Source.Endpoint != "https://src.alias" {
		t.Fatalf("source alias not resolved: %s", cfg.Source.Endpoint)
	}
	if cfg.Destination.Endpoint != "https://dest.alias" {
		t.Fatalf("destination alias not resolved: %s", cfg.Destination.Endpoint)
	}
}

func TestLoadFromFileWithEnvSubstitution(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "config.yaml")
	t.Setenv("EXT_ENDPOINT", "https://from-env.example")
	t.Setenv("EXT_BUCKET", "bucket-from-env")
	content := strings.TrimSpace(`
migration_name: file-test
log_level: warn
database:
  url: postgres://localhost/db
source:
  endpoint: $EXT_ENDPOINT
  bucket: ${EXT_BUCKET}
  region: us-east-1
  access_key_id: AKIA
  secret_access_key: secret
destination:
  endpoint: https://s3.dest.example
  bucket: ${UNSET_VAR:-fallback-bucket}
  region: us-east-1
  access_key_id: AKIA
  secret_access_key: secret
`)
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatal(err)
	}

	root := &cobra.Command{}
	BindFlags(root)
	_ = root.ParseFlags([]string{"--config", path})
	cfg, err := Load(root)
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if cfg.MigrationName != "file-test" {
		t.Fatalf("migration name mismatch: %s", cfg.MigrationName)
	}
	if cfg.LogLevel != "warn" {
		t.Fatalf("log level mismatch")
	}
	if cfg.Source.Endpoint != "https://from-env.example" {
		t.Fatalf("source endpoint not expanded: %s", cfg.Source.Endpoint)
	}
	if cfg.Source.Bucket != "bucket-from-env" {
		t.Fatalf("source bucket not expanded")
	}
	if cfg.Destination.Bucket != "fallback-bucket" {
		t.Fatalf("destination fallback not applied: %s", cfg.Destination.Bucket)
	}
}

func envSnapshot(includePrefix bool) map[string]string {
	out := map[string]string{}
	for _, e := range os.Environ() {
		parts := strings.SplitN(e, "=", 2)
		if includePrefix && strings.HasPrefix(parts[0], EnvPrefix+"_") {
			out[parts[0]] = parts[1]
		}
	}
	return out
}


