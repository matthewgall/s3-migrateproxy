#!/usr/bin/env bash
# End-to-end smoke test against the Docker Compose stack.
set -euo pipefail

SCRIPT_DIR=$(cd "$(dirname "$0")" && pwd)
DEPLOY_DIR="$SCRIPT_DIR/../../deploy"

cd "$DEPLOY_DIR"

cleanup() {
  docker compose down -v >/dev/null 2>&1 || true
}
trap cleanup EXIT

docker compose up -d --wait
./init-buckets.sh

# Create a source object via MinIO endpoint.
docker run --rm --network host minio/mc:latest \
  cp /etc/hosts source/sourcebucket/smoke-hosts.txt >/dev/null 2>&1 || true

# Start gateway in background, capture bootstrap credential.
cd "$SCRIPT_DIR/../.."
export S3MIGRATE_ADMIN_API_KEY_HASH=testkey
export S3MIGRATE_DATABASE_URL=postgres://migrate:migrate@localhost:5432/migrateproxy?sslmode=disable
export S3MIGRATE_MASTER_KEY=integration-test-master-key
export S3MIGRATE_SOURCE_PROVIDER=generic
export S3MIGRATE_SOURCE_ENDPOINT=http://localhost:9000
export S3MIGRATE_SOURCE_REGION=us-east-1
export S3MIGRATE_SOURCE_BUCKET=sourcebucket
export S3MIGRATE_SOURCE_ACCESS_KEY_ID=minioadmin
export S3MIGRATE_SOURCE_SECRET_ACCESS_KEY=minioadmin
export S3MIGRATE_SOURCE_PATH_STYLE=true
export S3MIGRATE_DEST_PROVIDER=generic
export S3MIGRATE_DEST_ENDPOINT=http://localhost:9010
export S3MIGRATE_DEST_REGION=us-east-1
export S3MIGRATE_DEST_BUCKET=destbucket
export S3MIGRATE_DEST_ACCESS_KEY_ID=minioadmin
export S3MIGRATE_DEST_SECRET_ACCESS_KEY=minioadmin
export S3MIGRATE_DEST_PATH_STYLE=true
export S3MIGRATE_WORKER_ENABLE=true

GW_LOG=$(mktemp)
go run ./cmd/gateway >"$GW_LOG" 2>&1 &
GW_PID=$!

# Wait for gateway to print bootstrap credential.
for i in {1..30}; do
  if grep -q "Secret:" "$GW_LOG"; then
    break
  fi
  sleep 1
done

GW_KEY=$(grep "AccessKey:" "$GW_LOG" | tail -1 | awk '{print $2}')
GW_SECRET=$(grep "Secret:" "$GW_LOG" | tail -1 | awk '{print $2}')

if [[ -z "$GW_KEY" || -z "$GW_SECRET" ]]; then
  echo "failed to obtain gateway credentials"
  cat "$GW_LOG"
  exit 1
fi

# Start worker.
go run ./cmd/worker >/tmp/worker.log 2>&1 &
WORKER_PID=$!

export AWS_ACCESS_KEY_ID="$GW_KEY"
export AWS_SECRET_ACCESS_KEY="$GW_SECRET"
export AWS_DEFAULT_REGION=us-east-1

# Wait for gateway to listen.
for i in {1..30}; do
  if curl -fsS http://localhost:8080/ >/dev/null 2>&1; then
    break
  fi
  sleep 1
done

echo "Gateway key: $GW_KEY"

# Write through gateway.
aws --endpoint-url http://localhost:8080 s3 cp "$GW_LOG" "s3://destbucket/smoke-log.txt"

# Read a source-only object (fallback).
aws --endpoint-url http://localhost:8080 s3 cp "s3://destbucket/smoke-hosts.txt" /tmp/smoke-hosts-read.txt

# List through gateway.
aws --endpoint-url http://localhost:8080 s3 ls "s3://destbucket/"

# Multipart upload (use 10MiB threshold).
aws --endpoint-url http://localhost:8080 s3 cp "$GW_LOG" "s3://destbucket/multipart-log.txt" --multipart-threshold 5MB

# Admin check.
go run ./cmd/migratectl -key testkey -admin http://localhost:8081 status

# Cleanup background processes.
kill $WORKER_PID $GW_PID || true
wait $WORKER_PID $GW_PID || true

echo "Smoke test passed."
