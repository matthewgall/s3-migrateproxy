package config

import "testing"

func TestValidateRejectsPrivateEndpoint(t *testing.T) {
	cfg := &Config{
		MigrationName:         "test",
		MasterKey:             "key",
		AllowPrivateEndpoints: false,
		Source: Provider{
			Kind:            "generic",
			Endpoint:        "http://127.0.0.1:9000",
			Region:          "us-east-1",
			Bucket:          "src",
			AccessKeyID:     "a",
			SecretAccessKey: "s",
		},
		Destination: Provider{
			Kind:            "generic",
			Endpoint:        "http://localhost:9010",
			Region:          "us-east-1",
			Bucket:          "dst",
			AccessKeyID:     "a",
			SecretAccessKey: "s",
		},
	}
	if err := cfg.Validate(); err == nil {
		t.Fatal("expected validation error for loopback endpoint")
	}
}

func TestValidateAllowsPublicEndpoint(t *testing.T) {
	cfg := &Config{
		MigrationName: "test",
		MasterKey:     "key",
		Source: Provider{
			Kind:            "generic",
			Endpoint:        "https://s3.example.com",
			Region:          "us-east-1",
			Bucket:          "src",
			AccessKeyID:     "a",
			SecretAccessKey: "s",
		},
		Destination: Provider{
			Kind:            "generic",
			Endpoint:        "https://s3.other.example.com",
			Region:          "us-east-1",
			Bucket:          "dst",
			AccessKeyID:     "a",
			SecretAccessKey: "s",
		},
	}
	if err := cfg.Validate(); err != nil {
		t.Fatalf("unexpected validation error: %v", err)
	}
}

func TestRedactedConfig(t *testing.T) {
	cfg := &Config{
		MasterKey:   "secret",
		Source:      Provider{SecretAccessKey: "src-secret"},
		Destination: Provider{SecretAccessKey: "dst-secret"},
		Admin:       AdminConfig{APIKeyHash: "admin-secret"},
	}
	r := cfg.Redacted()
	if r["MasterKey"] != "[redacted]" {
		t.Fatalf("master key not redacted: %v", r["MasterKey"])
	}
	src := r["Source"].(map[string]any)
	if src["SecretAccessKey"] != "[redacted]" {
		t.Fatalf("source secret not redacted")
	}
}
