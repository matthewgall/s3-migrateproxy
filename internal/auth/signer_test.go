package auth

import (
	"context"
	"net/http/httptest"
	"os"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	v4 "github.com/aws/aws-sdk-go-v2/aws/signer/v4"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/matthewgall/s3-migrateproxy/internal/migrations"
)

func testAuthDBPool(t *testing.T) *pgxpool.Pool {
	url := os.Getenv("TEST_DATABASE_URL")
	if url == "" {
		t.Skip("TEST_DATABASE_URL not set")
	}
	ctx := context.Background()
	pool, err := pgxpool.New(ctx, url)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	if _, err := pool.Exec(ctx, `DROP SCHEMA IF EXISTS metadata CASCADE`); err != nil {
		t.Fatalf("drop schema: %v", err)
	}
	if err := migrations.Migrate(ctx, pool); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	return pool
}

func mustTestMigrationID(ctx context.Context, t *testing.T, pool *pgxpool.Pool) string {
	var sourceID, destID string
	_ = pool.QueryRow(ctx, `INSERT INTO metadata.buckets (role, provider, region, bucket) VALUES ('source','generic','us-east-1','s') RETURNING id`).Scan(&sourceID)
	_ = pool.QueryRow(ctx, `INSERT INTO metadata.buckets (role, provider, region, bucket) VALUES ('destination','generic','us-east-1','d') RETURNING id`).Scan(&destID)
	var migID string
	if err := pool.QueryRow(ctx, `INSERT INTO metadata.migrations (name, source_bucket_id, dest_bucket_id) VALUES ($1,$2,$3) RETURNING id`, t.Name(), sourceID, destID).Scan(&migID); err != nil {
		t.Fatalf("create migration: %v", err)
	}
	return migID
}

func TestHeaderSignature(t *testing.T) {
	pool := testAuthDBPool(t)
	defer pool.Close()
	ctx := context.Background()
	migID := mustTestMigrationID(ctx, t, pool)
	store := NewStore(pool, "test-master-key")
	cred, secret, err := store.Create(ctx, migID, "test credential")
	if err != nil {
		t.Fatalf("create credential: %v", err)
	}
	if secret == "" {
		t.Fatal("expected secret")
	}

	v := NewValidator(store, "us-east-1")

	// Build a request signed by the AWS SDK with the same secret.
	req := httptest.NewRequest("GET", "http://localhost:8080/destbucket/object", nil)
	req.Host = "localhost:8080"
	creds := aws.Credentials{AccessKeyID: cred.AccessKey, SecretAccessKey: secret}
	signer := v4.NewSigner()

	// The validator expects a real signing time; sign with current UTC.
	now := time.Now().UTC()
	req.Header.Set("X-Amz-Date", now.Format("20060102T150405Z"))
	req.Header.Set("X-Amz-Content-Sha256", "UNSIGNED-PAYLOAD")
	if err := signer.SignHTTP(ctx, creds, req, "UNSIGNED-PAYLOAD", "s3", "us-east-1", now); err != nil {
		t.Fatalf("sign: %v", err)
	}

	validated, err := v.Authenticate(ctx, nil, req)
	if err != nil {
		t.Fatalf("authenticate: %v", err)
	}
	if validated.AccessKey != cred.AccessKey {
		t.Fatalf("access key mismatch")
	}
}

func TestPresignedURL(t *testing.T) {
	pool := testAuthDBPool(t)
	defer pool.Close()
	ctx := context.Background()
	migID := mustTestMigrationID(ctx, t, pool)
	store := NewStore(pool, "test-master-key")
	cred, secret, err := store.Create(ctx, migID, "test credential")
	if err != nil {
		t.Fatalf("create credential: %v", err)
	}

	v := NewValidator(store, "us-east-1")

	// Build the base request with query params for presigned URL.
	now := time.Now().UTC()
	req := httptest.NewRequest("GET", "http://localhost:8080/destbucket/object", nil)
	req.Host = "localhost:8080"
	q := req.URL.Query()
	q.Set("X-Amz-Algorithm", "AWS4-HMAC-SHA256")
	q.Set("X-Amz-Credential", cred.AccessKey+"/"+now.Format("20060102")+"/us-east-1/s3/aws4_request")
	q.Set("X-Amz-Date", now.Format("20060102T150405Z"))
	q.Set("X-Amz-Expires", "900")
	q.Set("X-Amz-SignedHeaders", "host")
	req.URL.RawQuery = q.Encode()

	creds := aws.Credentials{AccessKeyID: cred.AccessKey, SecretAccessKey: secret}
	signer := v4.NewSigner()
	signedURL, _, err := signer.PresignHTTP(ctx, creds, req, "UNSIGNED-PAYLOAD", "s3", "us-east-1", now)
	if err != nil {
		t.Fatalf("presign: %v", err)
	}

	validateReq := httptest.NewRequest("GET", signedURL, nil)
	validateReq.Host = "localhost:8080"
	validated, err := v.Authenticate(ctx, nil, validateReq)
	if err != nil {
		t.Fatalf("authenticate presigned: %v", err)
	}
	if validated.AccessKey != cred.AccessKey {
		t.Fatalf("access key mismatch")
	}
}
