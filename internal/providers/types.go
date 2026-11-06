package providers

import (
	"context"
	"io"
	"time"
)

// Capabilities describes what a provider natively supports.
type Capabilities struct {
	NativeConditionalWrites bool
	MultipartUploads        bool
	SupportsVersions        bool
	StrongChecksums         bool // e.g., SHA-256 object attributes
	ETagIsMD5               bool
}

// ObjectInfo represents metadata and a body reader for an object.
type ObjectInfo struct {
	Key            string
	VersionID      string
	Size           int64
	ETag           string
	ContentType    string
	ContentRange   string // populated for ranged requests
	LastModified   time.Time
	Metadata       map[string]string
	Body           io.ReadCloser
	ChecksumSHA256 string // when available
}

// PutOptions for writes.
type PutOptions struct {
	ContentType    string
	ContentLength  int64
	Metadata       map[string]string
	IfMatch        string
	IfNoneMatch    string
	ChecksumSHA256 string // hex
}

// PutResult contains the result of a successful write.
type PutResult struct {
	VersionID string
	ETag      string
}

// ListOptions for paginated listing.
type ListOptions struct {
	Prefix            string
	Delimiter         string
	MaxKeys           int32
	ContinuationToken string
	StartAfter        string
}

// ListResult is a page of keys and/or common prefixes.
type ListResult struct {
	Keys              []ListedObject
	CommonPrefixes    []string
	ContinuationToken string // next continuation token
	IsTruncated       bool
}

// ListedObject is a single item in a listing.
type ListedObject struct {
	Key            string
	Size           int64
	ETag           string
	LastModified   time.Time
	VersionID      string
	IsLatest       bool
	IsDeleteMarker bool
}

// CopyOptions for CopyObject.
type CopyOptions struct {
	SourceKey         string
	SourceVersionID   string
	MetadataDirective string // COPY | REPLACE
	ContentType       string
	Metadata          map[string]string
	IfMatch           string
	IfNoneMatch       string
	IfModifiedSince   time.Time
	IfUnmodifiedSince time.Time
}

// PartInfo for multipart uploads.
type PartInfo struct {
	PartNumber int32
	ETag       string
	Size       int64
}

// MultipartUploadInfo for created uploads.
type MultipartUploadInfo struct {
	UploadID string
	Key      string
}

// Provider abstracts S3 operations over multiple backends.
type Provider interface {
	Name() string
	Bucket() string
	Capabilities() Capabilities

	GetObject(ctx context.Context, key, versionID, rangeHeader string) (*ObjectInfo, error)
	HeadObject(ctx context.Context, key, versionID string) (*ObjectInfo, error)
	PutObject(ctx context.Context, key string, body io.Reader, size int64, opts PutOptions) (*PutResult, error)
	DeleteObject(ctx context.Context, key, versionID string) error
	ListObjectsV2(ctx context.Context, opts ListOptions) (*ListResult, error)
	ListObjectVersions(ctx context.Context, opts ListOptions) (*ListResult, error)

	CreateMultipartUpload(ctx context.Context, key string, opts PutOptions) (*MultipartUploadInfo, error)
	UploadPart(ctx context.Context, uploadID string, partNumber int, body io.Reader, size int64) (string, error)
	CompleteMultipartUpload(ctx context.Context, uploadID string, key string, parts []PartInfo) (*PutResult, error)
	AbortMultipartUpload(ctx context.Context, uploadID, key string) error
	ListParts(ctx context.Context, uploadID, key string, maxParts int32, partNumberMarker int32) (*ListPartsResult, error)
	ListMultipartUploads(ctx context.Context, opts ListOptions) ([]MultipartUploadInfo, error)

	CopyObject(ctx context.Context, dstKey string, opts CopyOptions) (*PutResult, error)
}

// ListPartsResult is the result of ListParts.
type ListPartsResult struct {
	Parts                []PartInfo
	IsTruncated          bool
	NextPartNumberMarker int32
}
