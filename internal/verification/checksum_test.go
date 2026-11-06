package verification

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"strings"
	"testing"
)

func TestCompare(t *testing.T) {
	res, err := Compare(context.Background(), 10, "etag1", "", 10, "etag1", "", nil)
	if err != nil {
		t.Fatalf("compare: %v", err)
	}
	if !res.OK {
		t.Fatalf("expected match for identical size+etag")
	}

	res, err = Compare(context.Background(), 10, "etag1", "", 12, "etag1", "", nil)
	if err != nil {
		t.Fatalf("compare: %v", err)
	}
	if res.OK {
		t.Fatalf("expected mismatch for different sizes")
	}
}

func TestCompareWithReader(t *testing.T) {
	ctx := context.Background()
	h := sha256.Sum256([]byte("hello"))
	srcHash := hex.EncodeToString(h[:])
	res, err := Compare(ctx, 5, "", srcHash, 5, "", "", strings.NewReader("hello"))
	if err != nil {
		t.Fatalf("compare with reader: %v", err)
	}
	if !res.OK {
		t.Fatalf("expected content match; got %s", res.Message)
	}
}
