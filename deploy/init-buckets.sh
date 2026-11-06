#!/bin/sh
set -e
# Create source and destination buckets in local MinIO.
mc alias set source http://localhost:9000 minioadmin minioadmin --api S3v4
mc alias set dest http://localhost:9010 minioadmin minioadmin --api S3v4
mc mb source/sourcebucket || true
mc mb dest/destbucket || true
mc anonymous set download source/sourcebucket || true
mc anonymous set download dest/destbucket || true
