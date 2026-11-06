package auth

import (
	"strings"
	"testing"
)

func TestHashAndValidateSecret(t *testing.T) {
	secret, hash, err := GenerateSecret()
	if err != nil {
		t.Fatalf("generate secret: %v", err)
	}
	if secret == "" || hash == "" {
		t.Fatal("expected non-empty secret and hash")
	}
	cred := &GatewayCredential{SecretHash: hash}
	if !ValidateCredentialSecret(cred, secret) {
		t.Fatal("expected secret validation to succeed")
	}
	if ValidateCredentialSecret(cred, "wrong") {
		t.Fatal("expected secret validation to fail")
	}
}

func TestGenerateAccessKey(t *testing.T) {
	key, err := GenerateAccessKey()
	if err != nil {
		t.Fatalf("generate access key: %v", err)
	}
	if key == "" || !strings.HasPrefix(key, credentialPrefix) {
		t.Fatalf("unexpected access key: %s", key)
	}
}
