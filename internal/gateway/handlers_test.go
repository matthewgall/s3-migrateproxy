package gateway

import (
	"strings"
	"testing"
	"time"

	"github.com/matthewgall/s3-migrateproxy/internal/metadata"
)

func TestParseCopySource(t *testing.T) {
	cases := []struct {
		in, key, ver string
	}{
		{"/bucket/key", "key", ""},
		{"/bucket/key?versionId=v1", "key", "v1"},
		{"/key", "key", ""},
	}
	for _, c := range cases {
		k, v := parseCopySource(c.in)
		if k != c.key || v != c.ver {
			t.Fatalf("parseCopySource(%q) = %q,%q, want %q,%q", c.in, k, v, c.key, c.ver)
		}
	}
}

func TestXMLListBucketResult(t *testing.T) {
	res := &metadata.ListResult{
		Keys: []metadata.ListedObject{
			{Key: "a/b.txt", Size: 10, ETag: `"etag"`, LastModified: time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)},
		},
		CommonPrefixes: []string{"a/c/"},
		IsTruncated:    false,
	}
	out := xmlListBucketResult("req1", res, 1000)
	if !strings.Contains(out, `xmlns="http://s3.amazonaws.com/doc/2006-03-01/"`) {
		t.Fatal("missing s3 namespace")
	}
	if !strings.Contains(out, "<Key>a/b.txt</Key>") {
		t.Fatal("missing key")
	}
	if !strings.Contains(out, "<CommonPrefix><Prefix>a/c/</Prefix></CommonPrefix>") {
		t.Fatal("missing common prefix")
	}
}

func TestXMLListBucketResultEscaping(t *testing.T) {
	res := &metadata.ListResult{
		Keys: []metadata.ListedObject{{Key: "a<b&c", Size: 1, ETag: "\"e\""}},
	}
	out := xmlListBucketResult("req", res, 1)
	if strings.Contains(out, "a<b&c") {
		t.Fatal("key not escaped")
	}
	if !strings.Contains(out, "a&lt;b&amp;c") {
		t.Fatal("expected escaped key")
	}
}
