#!/usr/bin/env bash
# Integration test for local Docker Compose stack.
set -euo pipefail

cd "$(dirname "$0")"
cd ../../deploy

docker compose up -d

# Wait for services.
for i in {1..30}; do
  if docker compose ps postgres | grep -q healthy && \
     curl -fsS http://localhost:9000/minio/health/live >/dev/null 2>&1 && \
     curl -fsS http://localhost:9010/minio/health/live >/dev/null 2>&1; then
    break
  fi
  sleep 1
done

./init-buckets.sh

# Seed one object into source.
docker run --rm --network host minio/mc:latest \
  alias set source http://localhost:9000 minioadmin minioadmin --api S3v4 || true
docker run --rm --network host minio/mc:latest \
  cp /etc/hosts source/sourcebucket/test-readme.md || true

echo "Integration harness ready. Start gateway and worker, then run aws CLI tests."
