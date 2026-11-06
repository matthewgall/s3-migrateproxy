package providers

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"strconv"
	"strings"
	"sync"
	"time"
)

// FakeProvider is an in-memory provider for unit tests.
type FakeProvider struct {
	mu      sync.RWMutex
	name    string
	objects map[string]*fakeObject
	uploads map[string]*fakeUpload
}

type fakeObject struct {
	data        []byte
	contentType string
	metadata    map[string]string
	versionID   string
	modified    time.Time
}

type fakeUpload struct {
	parts map[int][]byte
	key   string
}

// NewFakeProvider creates an empty fake provider.
func NewFakeProvider(name string) *FakeProvider {
	return &FakeProvider{
		name:    name,
		objects: map[string]*fakeObject{},
		uploads: map[string]*fakeUpload{},
	}
}

func (f *FakeProvider) Name() string   { return f.name }
func (f *FakeProvider) Bucket() string { return "bucket" }
func (f *FakeProvider) Capabilities() Capabilities {
	return Capabilities{MultipartUploads: true}
}

func (f *FakeProvider) PutObject(ctx context.Context, key string, body io.Reader, size int64, opts PutOptions) (*PutResult, error) {
	data, err := io.ReadAll(body)
	if err != nil {
		return nil, err
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	f.objects[key] = &fakeObject{
		data:        data,
		contentType: opts.ContentType,
		metadata:    opts.Metadata,
		versionID:   "v1",
		modified:    time.Now(),
	}
	return &PutResult{VersionID: "v1", ETag: "\"etag\""}, nil
}

func (f *FakeProvider) GetObject(ctx context.Context, key, versionID, rangeHeader string) (*ObjectInfo, error) {
	f.mu.RLock()
	obj, ok := f.objects[key]
	f.mu.RUnlock()
	if !ok {
		return nil, &S3Error{Code: "NoSuchKey", Message: "not found", StatusCode: 404}
	}
	data := obj.data
	contentRange := ""
	if rangeHeader != "" {
		start, end, ok := parseByteRange(rangeHeader, int64(len(data)))
		if !ok {
			return nil, &S3Error{Code: "InvalidRange", Message: "invalid range", StatusCode: 416}
		}
		data = data[start : end+1]
		contentRange = fmt.Sprintf("bytes %d-%d/%d", start, end, len(obj.data))
	}
	return &ObjectInfo{
		Key:          key,
		Size:         int64(len(data)),
		ETag:         "\"etag\"",
		ContentType:  obj.contentType,
		ContentRange: contentRange,
		LastModified: obj.modified,
		Metadata:     obj.metadata,
		Body:         io.NopCloser(bytes.NewReader(data)),
	}, nil
}

func parseByteRange(s string, total int64) (start, end int64, ok bool) {
	prefix := "bytes="
	if !strings.HasPrefix(s, prefix) {
		return 0, 0, false
	}
	s = s[len(prefix):]
	parts := strings.Split(s, "-")
	if len(parts) != 2 {
		return 0, 0, false
	}
	if parts[0] == "" {
		// suffix range not supported in fake
		return 0, 0, false
	}
	start, err1 := strconv.ParseInt(parts[0], 10, 64)
	end, err2 := strconv.ParseInt(parts[1], 10, 64)
	if err1 != nil || err2 != nil || start > end || start >= total || end >= total {
		return 0, 0, false
	}
	return start, end, true
}

func (f *FakeProvider) HeadObject(ctx context.Context, key, versionID string) (*ObjectInfo, error) {
	obj, err := f.GetObject(ctx, key, versionID, "")
	if err != nil {
		return nil, err
	}
	_ = obj.Body.Close()
	return obj, nil
}

func (f *FakeProvider) DeleteObject(ctx context.Context, key, versionID string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	delete(f.objects, key)
	return nil
}

func (f *FakeProvider) ListObjectsV2(ctx context.Context, opts ListOptions) (*ListResult, error) {
	f.mu.RLock()
	defer f.mu.RUnlock()
	res := &ListResult{}
	for k, obj := range f.objects {
		if opts.Prefix != "" && !strings.HasPrefix(k, opts.Prefix) {
			continue
		}
		res.Keys = append(res.Keys, ListedObject{Key: k, Size: int64(len(obj.data)), ETag: "\"etag\"", LastModified: obj.modified})
	}
	return res, nil
}

func (f *FakeProvider) ListObjectVersions(ctx context.Context, opts ListOptions) (*ListResult, error) {
	return f.ListObjectsV2(ctx, opts)
}

func (f *FakeProvider) CreateMultipartUpload(ctx context.Context, key string, opts PutOptions) (*MultipartUploadInfo, error) {
	uploadID := "upload-" + key
	f.mu.Lock()
	defer f.mu.Unlock()
	f.uploads[uploadID] = &fakeUpload{key: key, parts: map[int][]byte{}}
	return &MultipartUploadInfo{UploadID: uploadID, Key: key}, nil
}

func (f *FakeProvider) UploadPart(ctx context.Context, uploadID string, partNumber int, body io.Reader, size int64) (string, error) {
	data, err := io.ReadAll(body)
	if err != nil {
		return "", err
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.uploads[uploadID] == nil {
		return "", fmt.Errorf("no such upload")
	}
	f.uploads[uploadID].parts[partNumber] = data
	return fmt.Sprintf("\"%s-%d\"", uploadID, partNumber), nil
}

func (f *FakeProvider) CompleteMultipartUpload(ctx context.Context, uploadID string, key string, parts []PartInfo) (*PutResult, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	u, ok := f.uploads[uploadID]
	if !ok {
		return nil, fmt.Errorf("no such upload")
	}
	var data []byte
	for _, p := range parts {
		data = append(data, u.parts[int(p.PartNumber)]...)
	}
	f.objects[key] = &fakeObject{data: data, versionID: "v1", modified: time.Now()}
	delete(f.uploads, uploadID)
	return &PutResult{VersionID: "v1", ETag: "\"multipart\""}, nil
}

func (f *FakeProvider) AbortMultipartUpload(ctx context.Context, uploadID, key string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	delete(f.uploads, uploadID)
	return nil
}

func (f *FakeProvider) ListParts(ctx context.Context, uploadID, key string, maxParts int32, partNumberMarker int32) (*ListPartsResult, error) {
	return &ListPartsResult{}, nil
}

func (f *FakeProvider) ListMultipartUploads(ctx context.Context, opts ListOptions) ([]MultipartUploadInfo, error) {
	return nil, nil
}

func (f *FakeProvider) CopyObject(ctx context.Context, dstKey string, opts CopyOptions) (*PutResult, error) {
	src, err := f.GetObject(ctx, opts.SourceKey, opts.SourceVersionID, "")
	if err != nil {
		return nil, err
	}
	defer func() { _ = src.Body.Close() }()
	data, _ := io.ReadAll(src.Body)
	f.mu.Lock()
	defer f.mu.Unlock()
	f.objects[dstKey] = &fakeObject{data: data, versionID: "v1", modified: time.Now()}
	return &PutResult{VersionID: "v1", ETag: "\"etag\""}, nil
}
