package auth

import (
	"context"
	"crypto/subtle"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	v4 "github.com/aws/aws-sdk-go-v2/aws/signer/v4"
	"github.com/matthewgall/s3-migrateproxy/internal/config"
)

// Validator verifies AWS Signature Version 4 requests against gateway credentials.
type Validator struct {
	store  *Store
	region string
}

// NewValidator creates a validator for the gateway listener.
func NewValidator(store *Store, region string) *Validator {
	if region == "" {
		region = "us-east-1"
	}
	return &Validator{store: store, region: region}
}

// Authenticate validates an incoming request and returns the matched credential.
func (v *Validator) Authenticate(ctx context.Context, cfg *config.Config, r *http.Request) (*GatewayCredential, error) {
	authHeader := r.Header.Get("Authorization")
	if authHeader != "" {
		return v.authenticateHeader(ctx, cfg, r, authHeader)
	}
	if r.URL.Query().Get("X-Amz-Algorithm") != "" {
		return v.authenticatePresigned(ctx, cfg, r)
	}
	return nil, fmt.Errorf("missing authorization")
}

func (v *Validator) authenticateHeader(ctx context.Context, cfg *config.Config, r *http.Request, authHeader string) (*GatewayCredential, error) {
	accessKey, dateStamp, err := parseAuthHeader(authHeader)
	if err != nil {
		return nil, err
	}
	cred, err := v.store.Lookup(ctx, accessKey)
	if err != nil {
		return nil, fmt.Errorf("invalid access key")
	}

	signTimeStr := r.Header.Get("X-Amz-Date")
	if signTimeStr == "" {
		signTimeStr = dateStamp + "T000000Z"
	}
	signTime, err := time.Parse("20060102T150405Z", signTimeStr)
	if err != nil {
		return nil, fmt.Errorf("invalid amz date")
	}

	payloadHash := payloadHashFor(r)
	creds := aws.Credentials{AccessKeyID: accessKey, SecretAccessKey: cred.SecretRaw()}

	signer := v4.NewSigner()
	clone := r.Clone(ctx)
	clone.URL = clone.URL.ResolveReference(&url.URL{Path: clone.URL.Path, RawQuery: clone.URL.RawQuery})
	// Make sure host header reflects the configured listener.
	if clone.Host == "" {
		clone.Host = r.Host
	}
	if err := signer.SignHTTP(ctx, creds, clone, payloadHash, "s3", v.region, signTime); err != nil {
		return nil, fmt.Errorf("failed to sign: %w", err)
	}
	expectedAuth := clone.Header.Get("Authorization")
	if expectedAuth == "" {
		return nil, fmt.Errorf("failed to compute signature")
	}

	expectedSig := extractSignature(expectedAuth)
	actualSig := extractSignature(authHeader)
	if expectedSig == "" || actualSig == "" {
		return nil, fmt.Errorf("could not extract signature")
	}
	if subtle.ConstantTimeCompare([]byte(expectedSig), []byte(actualSig)) != 1 {
		slog.Default().Debug("signature mismatch", "access_key", accessKey)
		return nil, fmt.Errorf("signature mismatch")
	}
	return cred, nil
}

func (v *Validator) authenticatePresigned(ctx context.Context, cfg *config.Config, r *http.Request) (*GatewayCredential, error) {
	q := r.URL.Query()
	alg := q.Get("X-Amz-Algorithm")
	if alg != "AWS4-HMAC-SHA256" {
		return nil, fmt.Errorf("unsupported algorithm")
	}
	credLine, err := url.QueryUnescape(q.Get("X-Amz-Credential"))
	if err != nil {
		return nil, fmt.Errorf("invalid credential")
	}
	scope := strings.Split(credLine, "/")
	if len(scope) != 5 {
		return nil, fmt.Errorf("invalid credential scope")
	}
	accessKey := scope[0]
	dateStamp := scope[1]
	region := scope[2]
	_ = region
	cred, err := v.store.Lookup(ctx, accessKey)
	if err != nil {
		return nil, fmt.Errorf("invalid access key")
	}

	signTimeStr := q.Get("X-Amz-Date")
	if signTimeStr == "" {
		signTimeStr = dateStamp + "T000000Z"
	}
	signTime, err := time.Parse("20060102T150405Z", signTimeStr)
	if err != nil {
		return nil, fmt.Errorf("invalid amz date")
	}
	expires, err := strconv.Atoi(q.Get("X-Amz-Expires"))
	if err != nil || expires <= 0 {
		expires = 900
	}
	if time.Since(signTime) > time.Duration(expires)*time.Second {
		return nil, fmt.Errorf("presigned url expired")
	}
	_ = expires

	signedHeadersStr := q.Get("X-Amz-SignedHeaders")
	_ = signedHeadersStr

	payloadHash := q.Get("X-Amz-Content-Sha256")
	if payloadHash == "" {
		payloadHash = "UNSIGNED-PAYLOAD"
	}
	if r.Method == "GET" || r.Method == "HEAD" || r.Method == "DELETE" {
		if payloadHash == "UNSIGNED-PAYLOAD" {
			// ok
		}
	}

	creds := aws.Credentials{AccessKeyID: accessKey, SecretAccessKey: cred.SecretRaw()}
	signer := v4.NewSigner()
	clone := r.Clone(ctx)
	clone.URL = clone.URL.ResolveReference(&url.URL{Path: clone.URL.Path, RawQuery: clone.URL.RawQuery})
	if clone.Host == "" {
		clone.Host = r.Host
	}

	// The SDK derives signing time from X-Amz-Date; ensure it is set.
	clone.Header.Set("X-Amz-Date", signTimeStr)

	// Remove the signature so the signer recomputes it.
	cq := clone.URL.Query()
	cq.Del("X-Amz-Signature")
	clone.URL.RawQuery = cq.Encode()

	presignedURL, _, err := signer.PresignHTTP(ctx, creds, clone, payloadHash, "s3", v.region, signTime)
	if err != nil {
		return nil, fmt.Errorf("failed to presign: %w", err)
	}

	expectedQ, err := url.Parse(presignedURL)
	if err != nil {
		return nil, fmt.Errorf("failed to parse presigned url: %w", err)
	}
	expectedSig := expectedQ.Query().Get("X-Amz-Signature")
	actualSig := q.Get("X-Amz-Signature")
	if expectedSig == "" || actualSig == "" {
		return nil, fmt.Errorf("could not extract presigned signature")
	}
	if subtle.ConstantTimeCompare([]byte(expectedSig), []byte(actualSig)) != 1 {
		slog.Default().Debug("presigned signature mismatch", "access_key", accessKey)
		return nil, fmt.Errorf("signature mismatch")
	}
	return cred, nil
}

func parseAuthHeader(h string) (accessKey, dateStamp string, err error) {
	if !strings.HasPrefix(h, "AWS4-HMAC-SHA256 ") {
		return "", "", errors.New("unsupported scheme")
	}
	h = strings.TrimPrefix(h, "AWS4-HMAC-SHA256 ")
	for _, part := range splitCommas(h) {
		part = strings.TrimSpace(part)
		eq := strings.Index(part, "=")
		if eq < 0 {
			continue
		}
		key := part[:eq]
		val := part[eq+1:]
		if key == "Credential" {
			scope := strings.Split(val, "/")
			if len(scope) != 5 {
				return "", "", errors.New("invalid credential scope")
			}
			return scope[0], scope[1], nil
		}
	}
	return "", "", errors.New("missing credential")
}

func splitCommas(s string) []string {
	var out []string
	var buf strings.Builder
	for _, r := range s {
		if r == ',' {
			out = append(out, buf.String())
			buf.Reset()
			continue
		}
		buf.WriteRune(r)
	}
	if buf.Len() > 0 {
		out = append(out, buf.String())
	}
	return out
}

func extractSignature(auth string) string {
	for _, part := range splitCommas(auth) {
		part = strings.TrimSpace(part)
		if strings.HasPrefix(part, "Signature=") {
			return strings.TrimPrefix(part, "Signature=")
		}
	}
	return ""
}

func payloadHashFor(r *http.Request) string {
	sha := r.Header.Get("X-Amz-Content-Sha256")
	if sha != "" {
		return sha
	}
	if r.Method == "GET" || r.Method == "HEAD" || r.Method == "DELETE" {
		return "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
	}
	return "UNSIGNED-PAYLOAD"
}


