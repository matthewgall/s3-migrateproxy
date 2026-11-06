# S3 Live Migration Gateway

Production-grade open-source S3-compatible migration gateway that enables live
migration of object storage between S3-compatible providers without application
downtime.

> **This is a migration tool, not a permanent multi-cloud platform.** Once
> migration is complete the gateway can be removed and clients can talk directly
> to the destination provider.

## Status

This repository implements Milestone 1 (architecture, ADRs, schema,
consistency model) and a production-oriented core implementation covering:

- Stateless S3-compatible gateway with AWS SigV4 authentication.
- Destination-first reads with authoritative source fallback.
- `GetObject`, `HeadObject`, `PutObject`, `DeleteObject`.
- Streaming writes and bounded-memory reads.
- Durable tombstones and generation-based overwrite protection.
- Encrypted credential storage.
- Background migration worker with lease-based task claiming.
- Object discovery, streaming transfers, and post-copy verification.
- Migration lifecycle controls via a separate admin API.
- `migratectl` CLI.
- Prometheus metrics and OpenTelemetry tracing hooks.
- Dockerfile, Docker Compose, and local MinIO integration harness.

Advanced features such as multipart upload, `ListObjectsV2` catalog listing,
`CopyObject`, and presigned URLs have design stubs and are listed in the
compatibility matrix and risk register.

## Quick Start

```bash
# 1. Start Postgres + MinIO source/destination
cd deploy
docker compose up -d

# 2. Create buckets
./init-buckets.sh

# 3. Copy environment and start gateway + worker
cp .env.example .env
# edit .env as needed; defaults point at local MinIO
cd ..
go run ./cmd/gateway
```

In another terminal:

```bash
go run ./cmd/worker
```

The gateway prints a bootstrap S3 credential to stderr on first run.

## Architecture

```
┌─────────┐     ┌──────────┐     ┌───────────┐     ┌──────────────┐
│ Clients │────▶│  Gateway │────▶│ Destination │     │ Source       │
└─────────┘     │  (S3 API)│     │  Provider   │────▶│ fallback     │
                └────┬─────┘     └─────────────┘     └──────────────┘
                     │
                     ▼
              ┌──────────────┐
              │ PostgreSQL   │  metadata, tombstones, queue,
              │ (metadata)   │  discovery checkpoints, lifecycle state
              └──────────────┘
```

## Configuration

Configuration is read from, in order of precedence:

1. Command-line flags (e.g. `--source-endpoint`, `--database-url`).
2. Environment variables prefixed with `S3MIGRATE_` (e.g. `S3MIGRATE_SOURCE_ENDPOINT`).
3. A config file (`--config` / `-c`) supporting YAML, JSON or TOML.
4. Built-in defaults.

Config file values may reference environment variables:

```yaml
database:
  url: ${DATABASE_URL}
source:
  endpoint: ${SOURCE_ENDPOINT:-https://s3.example.com}
```

An annotated example is in [`deploy/config.example.yaml`](deploy/config.example.yaml)
and a dotenv example is in [`deploy/.env.example`](deploy/.env.example).

Required values:

- `master_key` / `S3MIGRATE_MASTER_KEY` — used to encrypt stored credentials.
- `database.url` / `S3MIGRATE_DATABASE_URL`
- `source.*` and `destination.*` provider settings.

## Admin API

The admin API listens on a separate port (default `:8081`) and requires a
bearer token (`S3MIGRATE_ADMIN_API_KEY_HASH`).

```bash
# status
migratectl --key testkey status
migratectl --key testkey progress
migratectl --key testkey start
migratectl --key testkey pause
migratectl --key testkey resume
migratectl --key testkey reconcile
migratectl --key testkey report
migratectl --key testkey health
migratectl --key testkey retry my-object/key
```

## Operation

1. **Preparation**: configure providers and run connectivity checks with the
   admin health endpoint.
2. **Start migration**: `migratectl start` sets state to `LIVE_MIGRATING`.
   - New writes go to destination.
   - Reads fall back to source when an object is not in destination.
   - Background workers copy historical objects.
3. **Reconciliation**: trigger `reconcile` to run additional discovery passes.
4. **Finalise**: after reviewing the report and resolving discrepancies, run
   `migratectl finalise` to move the migration through `FINALISING` to
   `COMPLETED`.
5. **Cutover**: point clients directly at the destination provider and retire
   the gateway.

## Testing

```bash
make test
make vet
```

## License

MIT — see [LICENSE](LICENSE).
