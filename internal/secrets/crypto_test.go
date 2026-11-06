package secrets

import "testing"

func TestEncryptDecrypt(t *testing.T) {
	key := "test-master-key-that-is-32-bytes-long"
	plain := "super-secret-value"
	enc, err := Encrypt(key, plain)
	if err != nil {
		t.Fatalf("encrypt: %v", err)
	}
	if enc == plain {
		t.Fatal("ciphertext should differ from plaintext")
	}
	dec, err := Decrypt(key, enc)
	if err != nil {
		t.Fatalf("decrypt: %v", err)
	}
	if dec != plain {
		t.Fatalf("expected %q, got %q", plain, dec)
	}
	_, err = Decrypt("wrong-key", enc)
	if err == nil {
		t.Fatal("expected decryption failure with wrong key")
	}
}
