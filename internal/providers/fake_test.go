package providers

import (
	"bytes"
	"context"
	"io"
	"testing"
)

func TestFakeProviderRoundTrip(t *testing.T) {
	ctx := context.Background()
	p := NewFakeProvider("test")
	data := []byte("hello world")
	_, err := p.PutObject(ctx, "k1", bytes.NewReader(data), int64(len(data)), PutOptions{ContentType: "text/plain", Metadata: map[string]string{"a": "b"}})
	if err != nil {
		t.Fatalf("put: %v", err)
	}
	obj, err := p.GetObject(ctx, "k1", "", "")
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	defer obj.Body.Close()
	got, _ := io.ReadAll(obj.Body)
	if !bytes.Equal(got, data) {
		t.Fatalf("data mismatch")
	}
	if obj.ContentType != "text/plain" {
		t.Fatalf("content type mismatch")
	}
	if obj.Metadata["a"] != "b" {
		t.Fatalf("metadata mismatch")
	}
}

func TestFakeProviderNotFound(t *testing.T) {
	ctx := context.Background()
	p := NewFakeProvider("test")
	_, err := p.GetObject(ctx, "missing", "", "")
	if err == nil {
		t.Fatal("expected error")
	}
	if !IsNotFound(err) {
		t.Fatal("expected not found")
	}
}

func TestFakeProviderDelete(t *testing.T) {
	ctx := context.Background()
	p := NewFakeProvider("test")
	_, _ = p.PutObject(ctx, "k", bytes.NewReader([]byte("x")), 1, PutOptions{})
	if err := p.DeleteObject(ctx, "k", ""); err != nil {
		t.Fatal(err)
	}
	if _, err := p.GetObject(ctx, "k", "", ""); !IsNotFound(err) {
		t.Fatal("expected missing after delete")
	}
}

func TestFakeProviderRange(t *testing.T) {
	ctx := context.Background()
	p := NewFakeProvider("test")
	data := []byte("0123456789")
	_, _ = p.PutObject(ctx, "range", bytes.NewReader(data), int64(len(data)), PutOptions{})
	obj, err := p.GetObject(ctx, "range", "", "bytes=2-5")
	if err != nil {
		t.Fatalf("get range: %v", err)
	}
	defer obj.Body.Close()
	got, _ := io.ReadAll(obj.Body)
	if !bytes.Equal(got, []byte("2345")) {
		t.Fatalf("range mismatch: %q", got)
	}
	if obj.ContentRange != "bytes 2-5/10" {
		t.Fatalf("unexpected content range: %s", obj.ContentRange)
	}
}
