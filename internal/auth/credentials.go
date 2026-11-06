package auth

import (
	"context"
	"crypto/rand"
	"crypto/subtle"
	"database/sql"
	"encoding/hex"
	"fmt"
	"strconv"
	"strings"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/matthewgall/s3-migrateproxy/internal/secrets"
	"golang.org/x/crypto/argon2"
)

const (
	credentialPrefix = "AKIAmigrate"
	keyBytes         = 32
)

// GatewayCredential is a client credential for the S3 listener.
type GatewayCredential struct {
	ID            string
	AccessKey     string
	secretRaw     string
	SecretHash    string
	Active        bool
	Revoked       bool
}

// SecretRaw returns the decrypted raw secret. It must not be logged.
func (c *GatewayCredential) SecretRaw() string { return c.secretRaw }

// Store loads gateway credentials from PostgreSQL.
type Store struct {
	pool      *pgxpool.Pool
	masterKey string
}

// NewStore returns a credential store.
func NewStore(pool *pgxpool.Pool, masterKey string) *Store { return &Store{pool: pool, masterKey: masterKey} }

// Lookup returns the credential and decrypted secret for an access key.
func (s *Store) Lookup(ctx context.Context, accessKey string) (*GatewayCredential, error) {
	var id, secretHash, secretEncrypted string
	var active bool
	var revokedAt sql.NullTime
	err := s.pool.QueryRow(ctx, `
		SELECT id, secret_hash, secret_encrypted, active, revoked_at
		FROM metadata.gateway_credentials
		WHERE access_key = $1`, accessKey).Scan(&id, &secretHash, &secretEncrypted, &active, &revokedAt)
	if err != nil {
		return nil, err
	}
	if !active || revokedAt.Valid {
		return nil, fmt.Errorf("credential inactive")
	}
	secretRaw, err := secrets.Decrypt(s.masterKey, secretEncrypted)
	if err != nil {
		return nil, fmt.Errorf("failed to decrypt credential: %w", err)
	}
	return &GatewayCredential{
		ID:         id,
		AccessKey:  accessKey,
		secretRaw:  secretRaw,
		SecretHash: secretHash,
		Active:     active,
		Revoked:    revokedAt.Valid,
	}, nil
}

// Create inserts a new gateway credential. The returned secret is shown once.
func (s *Store) Create(ctx context.Context, migrationID, description string) (*GatewayCredential, string, error) {
	accessKey, err := GenerateAccessKey()
	if err != nil {
		return nil, "", err
	}
	secretRaw, secretHash, err := GenerateSecret()
	if err != nil {
		return nil, "", err
	}
	secretEncrypted, err := secrets.Encrypt(s.masterKey, secretRaw)
	if err != nil {
		return nil, "", err
	}
	_, err = s.pool.Exec(ctx, `
		INSERT INTO metadata.gateway_credentials (migration_id, access_key, secret_hash, secret_encrypted, secret_prefix, description)
		VALUES ($1, $2, $3, $4, $5, $6)`,
		migrationID, accessKey, secretHash, secretEncrypted, secretRaw[:8], description)
	if err != nil {
		return nil, "", err
	}
	return &GatewayCredential{AccessKey: accessKey}, secretRaw, nil
}

// ValidateSecret compares a candidate secret against the stored hash.
// The stored hash is expected to be encoded as argon2id:$params:$salt:$hash.
func (c *GatewayCredential) ValidateSecret(candidate string) bool {
	return validateSecretHash(c.SecretHash, candidate)
}

// ValidateCredentialSecret validates a candidate secret against a credential hash.
func ValidateCredentialSecret(cred *GatewayCredential, candidate string) bool {
	return validateSecretHash(cred.SecretHash, candidate)
}

func validateSecretHash(hash, candidate string) bool {
	parts := strings.Split(hash, "$")
	if len(parts) != 4 || parts[0] != "argon2id" {
		return false
	}
	params := strings.Split(parts[1], ",")
	if len(params) != 3 {
		return false
	}
	m, err := strconv.ParseUint(strings.TrimPrefix(params[0], "m="), 10, 32)
	if err != nil {
		return false
	}
	t, err := strconv.ParseUint(strings.TrimPrefix(params[1], "t="), 10, 32)
	if err != nil {
		return false
	}
	p, err := strconv.ParseUint(strings.TrimPrefix(params[2], "p="), 10, 8)
	if err != nil {
		return false
	}
	salt, err := hex.DecodeString(parts[2])
	if err != nil {
		return false
	}
	expected, err := hex.DecodeString(parts[3])
	if err != nil {
		return false
	}
	derived := argon2.IDKey([]byte(candidate), salt, uint32(t), uint32(m), uint8(p), uint32(len(expected)))
	return subtle.ConstantTimeCompare(derived, expected) == 1
}

// GenerateAccessKey creates a new random gateway access key.
func GenerateAccessKey() (string, error) {
	b := make([]byte, 8)
	if _, err := rand.Read(b); err != nil {
		return "", err
	}
	return fmt.Sprintf("%s%s", credentialPrefix, hex.EncodeToString(b)), nil
}

// GenerateSecret creates a new random secret and its argon2id hash.
func GenerateSecret() (secret, hash string, err error) {
	b := make([]byte, keyBytes)
	if _, err = rand.Read(b); err != nil {
		return "", "", err
	}
	secret = hex.EncodeToString(b)
	hash, err = HashSecret(secret)
	return secret, hash, err
}

// HashSecret hashes a raw secret using argon2id.
func HashSecret(secret string) (string, error) {
	const (
		time    = 3
		memory  = 64 * 1024
		threads = 4
		keyLen  = 32
	)
	salt := make([]byte, 16)
	if _, err := rand.Read(salt); err != nil {
		return "", err
	}
	h := argon2.IDKey([]byte(secret), salt, time, memory, threads, keyLen)
	return fmt.Sprintf("argon2id$m=%d,t=%d,p=%d$%s$%s", memory, time, threads, hex.EncodeToString(salt), hex.EncodeToString(h)), nil
}
