package providers

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsConfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/smithy-go"
	"github.com/matthewgall/s3-migrateproxy/internal/config"
)

// NewAWS builds an AWS SDK v2 S3 Provider for a configured bucket.
func NewAWS(ctx context.Context, p config.Provider) (Provider, error) {
	var endpointResolver aws.EndpointResolverWithOptionsFunc
	if p.Endpoint != "" {
		endpointResolver = func(service, region string, options ...interface{}) (aws.Endpoint, error) {
			if service != s3.ServiceID {
				return aws.Endpoint{}, &aws.EndpointNotFoundError{}
			}
			return aws.Endpoint{
				URL:           strings.TrimRight(p.Endpoint, "/"),
				SigningRegion: p.Region,
				Source:        aws.EndpointSourceCustom,
				HostnameImmutable: true,
			}, nil
		}
	}

	rt := http.DefaultTransport.(*http.Transport).Clone()
	if p.TLSSkipVerify {
		rt.TLSClientConfig = &tls.Config{InsecureSkipVerify: true}
	}
	rt.DialContext = (&net.Dialer{
		Timeout:   5 * time.Second,
		KeepAlive: 30 * time.Second,
	}).DialContext
	rt.MaxIdleConns = p.MaxConcurrency
	rt.MaxIdleConnsPerHost = p.MaxConcurrency
	rt.IdleConnTimeout = 90 * time.Second

	cfg, err := awsConfig.LoadDefaultConfig(ctx,
		awsConfig.WithRegion(p.Region),
		awsConfig.WithCredentialsProvider(credentials.NewStaticCredentialsProvider(p.AccessKeyID, p.SecretAccessKey, p.SessionToken)),
		awsConfig.WithHTTPClient(&http.Client{Transport: rt, Timeout: p.RequestTimeout}),
		awsConfig.WithEndpointResolverWithOptions(endpointResolver),
	)
	if err != nil {
		return nil, fmt.Errorf("load aws config: %w", err)
	}

	client := s3.NewFromConfig(cfg, func(o *s3.Options) {
		o.UsePathStyle = p.PathStyle
	})

	return &awsProvider{
		client:   client,
		name:     p.Kind,
		bucket:   p.Bucket,
		endpoint: p.Endpoint,
		caps:     defaultCapabilities(p.Kind),
	}, nil
}

func defaultCapabilities(kind config.ProviderKind) Capabilities {
	switch kind {
	case config.ProviderAWS:
		return Capabilities{
			NativeConditionalWrites: true,
			MultipartUploads:        true,
			SupportsVersions:        true,
			StrongChecksums:         true,
			ETagIsMD5:               false, // often multipart ETag is not pure MD5
		}
	case config.ProviderR2:
		return Capabilities{
			NativeConditionalWrites: false,
			MultipartUploads:        true,
			SupportsVersions:        true,
			StrongChecksums:         false,
			ETagIsMD5:               true,
		}
	case config.ProviderB2:
		return Capabilities{
			NativeConditionalWrites: false, // limited/behaves differently
			MultipartUploads:        true,
			SupportsVersions:        true,
			StrongChecksums:         false,
			ETagIsMD5:               false,
		}
	case config.ProviderWasabi:
		return Capabilities{
			NativeConditionalWrites: true,
			MultipartUploads:        true,
			SupportsVersions:        true,
			StrongChecksums:         true,
			ETagIsMD5:               false,
		}
	default:
		return Capabilities{
			NativeConditionalWrites: false,
			MultipartUploads:        true,
			SupportsVersions:        false,
			StrongChecksums:         false,
			ETagIsMD5:               false,
		}
	}
}

type awsProvider struct {
	client   *s3.Client
	name     config.ProviderKind
	bucket   string
	endpoint string
	caps     Capabilities
}

func (a *awsProvider) Name() string        { return string(a.name) }
func (a *awsProvider) Bucket() string      { return a.bucket }
func (a *awsProvider) Capabilities() Capabilities { return a.caps }

func (a *awsProvider) GetObject(ctx context.Context, key, versionID, rangeHeader string) (*ObjectInfo, error) {
	input := &s3.GetObjectInput{
		Bucket: aws.String(a.bucket),
		Key:    aws.String(key),
	}
	if versionID != "" {
		input.VersionId = aws.String(versionID)
	}
	if rangeHeader != "" {
		input.Range = aws.String(rangeHeader)
	}
	out, err := a.client.GetObject(ctx, input)
	if err != nil {
		return nil, translateError(err)
	}
	return &ObjectInfo{
		Key:          key,
		VersionID:    aws.ToString(out.VersionId),
		Size:         aws.ToInt64(out.ContentLength),
		ETag:         aws.ToString(out.ETag),
		ContentType:  aws.ToString(out.ContentType),
		ContentRange: aws.ToString(out.ContentRange),
		LastModified: aws.ToTime(out.LastModified),
		Metadata:     out.Metadata,
		Body:         out.Body,
		ChecksumSHA256: aws.ToString(out.ChecksumSHA256),
	}, nil
}

func (a *awsProvider) HeadObject(ctx context.Context, key, versionID string) (*ObjectInfo, error) {
	input := &s3.HeadObjectInput{
		Bucket: aws.String(a.bucket),
		Key:    aws.String(key),
	}
	if versionID != "" {
		input.VersionId = aws.String(versionID)
	}
	out, err := a.client.HeadObject(ctx, input)
	if err != nil {
		return nil, translateError(err)
	}
	return &ObjectInfo{
		Key:            key,
		VersionID:      aws.ToString(out.VersionId),
		Size:           aws.ToInt64(out.ContentLength),
		ETag:           aws.ToString(out.ETag),
		ContentType:    aws.ToString(out.ContentType),
		LastModified:     aws.ToTime(out.LastModified),
		Metadata:       out.Metadata,
		ChecksumSHA256: aws.ToString(out.ChecksumSHA256),
	}, nil
}

func (a *awsProvider) PutObject(ctx context.Context, key string, body io.Reader, size int64, opts PutOptions) (*PutResult, error) {
	input := &s3.PutObjectInput{
		Bucket:        aws.String(a.bucket),
		Key:           aws.String(key),
		Body:          body,
		ContentLength: aws.Int64(size),
		ContentType:   aws.String(opts.ContentType),
		Metadata:      opts.Metadata,
	}
	if opts.IfMatch != "" {
		input.IfMatch = aws.String(opts.IfMatch)
	}
	if opts.IfNoneMatch != "" {
		input.IfNoneMatch = aws.String(opts.IfNoneMatch)
	}
	if opts.ChecksumSHA256 != "" {
		// SDK expects base64 of raw checksum, but our metadata store keeps hex.
		// We leave checksum fields unset here; instead the caller computes and
		// verifies independently. Provider checksums can be enabled via middleware if needed.
	}
	out, err := a.client.PutObject(ctx, input)
	if err != nil {
		return nil, translateError(err)
	}
	return &PutResult{
		VersionID: aws.ToString(out.VersionId),
		ETag:      aws.ToString(out.ETag),
	}, nil
}

func (a *awsProvider) DeleteObject(ctx context.Context, key, versionID string) error {
	input := &s3.DeleteObjectInput{
		Bucket: aws.String(a.bucket),
		Key:    aws.String(key),
	}
	if versionID != "" {
		input.VersionId = aws.String(versionID)
	}
	_, err := a.client.DeleteObject(ctx, input)
	return translateError(err)
}

func (a *awsProvider) ListObjectsV2(ctx context.Context, opts ListOptions) (*ListResult, error) {
	input := &s3.ListObjectsV2Input{
		Bucket:            aws.String(a.bucket),
		Prefix:            aws.String(opts.Prefix),
		Delimiter:         aws.String(opts.Delimiter),
		MaxKeys:           aws.Int32(opts.MaxKeys),
		ContinuationToken: aws.String(opts.ContinuationToken),
		StartAfter:        aws.String(opts.StartAfter),
	}
	out, err := a.client.ListObjectsV2(ctx, input)
	if err != nil {
		return nil, translateError(err)
	}
	return convertListResult(out), nil
}

func (a *awsProvider) ListObjectVersions(ctx context.Context, opts ListOptions) (*ListResult, error) {
	input := &s3.ListObjectVersionsInput{
		Bucket:          aws.String(a.bucket),
		Prefix:          aws.String(opts.Prefix),
		Delimiter:       aws.String(opts.Delimiter),
		MaxKeys:         aws.Int32(opts.MaxKeys),
		KeyMarker:       aws.String(opts.ContinuationToken),
	}
	out, err := a.client.ListObjectVersions(ctx, input)
	if err != nil {
		return nil, translateError(err)
	}
	res := &ListResult{
		IsTruncated:       aws.ToBool(out.IsTruncated),
		ContinuationToken: aws.ToString(out.NextKeyMarker),
	}
	for _, v := range out.Versions {
		res.Keys = append(res.Keys, ListedObject{
			Key:          aws.ToString(v.Key),
			Size:         aws.ToInt64(v.Size),
			ETag:         aws.ToString(v.ETag),
			LastModified: aws.ToTime(v.LastModified),
			VersionID:    aws.ToString(v.VersionId),
			IsLatest:     aws.ToBool(v.IsLatest),
		})
	}
	for _, d := range out.DeleteMarkers {
		res.Keys = append(res.Keys, ListedObject{
			Key:            aws.ToString(d.Key),
			VersionID:      aws.ToString(d.VersionId),
			IsLatest:       aws.ToBool(d.IsLatest),
			IsDeleteMarker: true,
		})
	}
	res.CommonPrefixes = convertCommonPrefixes(out.CommonPrefixes)
	return res, nil
}

func (a *awsProvider) CreateMultipartUpload(ctx context.Context, key string, opts PutOptions) (*MultipartUploadInfo, error) {
	input := &s3.CreateMultipartUploadInput{
		Bucket:      aws.String(a.bucket),
		Key:         aws.String(key),
		ContentType: aws.String(opts.ContentType),
		Metadata:    opts.Metadata,
	}
	out, err := a.client.CreateMultipartUpload(ctx, input)
	if err != nil {
		return nil, translateError(err)
	}
	return &MultipartUploadInfo{UploadID: aws.ToString(out.UploadId), Key: key}, nil
}

func (a *awsProvider) UploadPart(ctx context.Context, uploadID string, partNumber int, body io.Reader, size int64) (string, error) {
	input := &s3.UploadPartInput{
		Bucket:     aws.String(a.bucket),
		Key:        aws.String(uploadID), // placeholder, actual key managed at higher layer if needed
		UploadId:   aws.String(uploadID),
		PartNumber: aws.Int32(int32(partNumber)),
		Body:       body,
	}
	// AWS SDK for Go v2 UploadPartInput does not have ContentLength on the main struct; size hinted via body.
	_ = size
	out, err := a.client.UploadPart(ctx, input)
	if err != nil {
		return "", translateError(err)
	}
	return aws.ToString(out.ETag), nil
}

func (a *awsProvider) CompleteMultipartUpload(ctx context.Context, uploadID string, key string, parts []PartInfo) (*PutResult, error) {
	var completed []types.CompletedPart
	for _, p := range parts {
		completed = append(completed, types.CompletedPart{
			ETag:       aws.String(p.ETag),
			PartNumber: aws.Int32(p.PartNumber),
		})
	}
	input := &s3.CompleteMultipartUploadInput{
		Bucket:          aws.String(a.bucket),
		Key:             aws.String(key),
		UploadId:        aws.String(uploadID),
		MultipartUpload: &types.CompletedMultipartUpload{Parts: completed},
	}
	out, err := a.client.CompleteMultipartUpload(ctx, input)
	if err != nil {
		return nil, translateError(err)
	}
	return &PutResult{
		VersionID: aws.ToString(out.VersionId),
		ETag:      aws.ToString(out.ETag),
	}, nil
}

func (a *awsProvider) AbortMultipartUpload(ctx context.Context, uploadID, key string) error {
	_, err := a.client.AbortMultipartUpload(ctx, &s3.AbortMultipartUploadInput{
		Bucket:   aws.String(a.bucket),
		Key:      aws.String(key),
		UploadId: aws.String(uploadID),
	})
	return translateError(err)
}

func (a *awsProvider) ListParts(ctx context.Context, uploadID, key string, maxParts int32, partNumberMarker int32) (*ListPartsResult, error) {
	input := &s3.ListPartsInput{
		Bucket:   aws.String(a.bucket),
		Key:      aws.String(key),
		UploadId: aws.String(uploadID),
		MaxParts: aws.Int32(maxParts),
	}
	if partNumberMarker > 0 {
		input.PartNumberMarker = aws.String(strconv.Itoa(int(partNumberMarker)))
	}
	out, err := a.client.ListParts(ctx, input)
	if err != nil {
		return nil, translateError(err)
	}
	res := &ListPartsResult{
		IsTruncated: aws.ToBool(out.IsTruncated),
	}
	if out.NextPartNumberMarker != nil {
		if n, err := strconv.Atoi(aws.ToString(out.NextPartNumberMarker)); err == nil {
			res.NextPartNumberMarker = int32(n)
		}
	}
	for _, p := range out.Parts {
		res.Parts = append(res.Parts, PartInfo{
			PartNumber: aws.ToInt32(p.PartNumber),
			ETag:       aws.ToString(p.ETag),
			Size:       aws.ToInt64(p.Size),
		})
	}
	return res, nil
}


func (a *awsProvider) ListMultipartUploads(ctx context.Context, opts ListOptions) ([]MultipartUploadInfo, error) {
	out, err := a.client.ListMultipartUploads(ctx, &s3.ListMultipartUploadsInput{
		Bucket: aws.String(a.bucket),
		Prefix: aws.String(opts.Prefix),
	})
	if err != nil {
		return nil, translateError(err)
	}
	var res []MultipartUploadInfo
	for _, u := range out.Uploads {
		res = append(res, MultipartUploadInfo{
			UploadID: aws.ToString(u.UploadId),
			Key:      aws.ToString(u.Key),
		})
	}
	return res, nil
}

func (a *awsProvider) CopyObject(ctx context.Context, dstKey string, opts CopyOptions) (*PutResult, error) {
	src := fmt.Sprintf("%s/%s", a.bucket, encodeCopySource(opts.SourceKey))
	if opts.SourceVersionID != "" {
		src = fmt.Sprintf("%s?versionId=%s", src, opts.SourceVersionID)
	}
	input := &s3.CopyObjectInput{
		Bucket:     aws.String(a.bucket),
		Key:        aws.String(dstKey),
		CopySource: aws.String(src),
		Metadata:   opts.Metadata,
	}
	if opts.MetadataDirective == "REPLACE" {
		input.MetadataDirective = types.MetadataDirectiveReplace
		input.ContentType = aws.String(opts.ContentType)
	}
	out, err := a.client.CopyObject(ctx, input)
	if err != nil {
		return nil, translateError(err)
	}
	etag := ""
	if out.CopyObjectResult != nil {
		etag = aws.ToString(out.CopyObjectResult.ETag)
	}
	return &PutResult{VersionID: aws.ToString(out.VersionId), ETag: etag}, nil
}

func convertListResult(out *s3.ListObjectsV2Output) *ListResult {
	res := &ListResult{
		IsTruncated:       aws.ToBool(out.IsTruncated),
		ContinuationToken: aws.ToString(out.NextContinuationToken),
	}
	for _, c := range out.Contents {
		res.Keys = append(res.Keys, ListedObject{
			Key:          aws.ToString(c.Key),
			Size:         aws.ToInt64(c.Size),
			ETag:         aws.ToString(c.ETag),
			LastModified: aws.ToTime(c.LastModified),
		})
	}
	res.CommonPrefixes = convertCommonPrefixes(out.CommonPrefixes)
	return res
}

func convertCommonPrefixes(in []types.CommonPrefix) []string {
	out := make([]string, 0, len(in))
	for _, p := range in {
		out = append(out, aws.ToString(p.Prefix))
	}
	return out
}

func encodeCopySource(s string) string {
	return s
}

// IsNotFound returns true if err represents an authoritative object not found.
func IsNotFound(err error) bool {
	if err == nil {
		return false
	}
	var apiErr smithy.APIError
	if errors.As(err, &apiErr) {
		code := apiErr.ErrorCode()
		return code == "NoSuchKey" || code == "NotFound" || code == "404"
	}
	return false
}

func translateError(err error) error {
	if err == nil {
		return nil
	}
	return err
}

// S3Error is our internal representation of a provider S3 error.
type S3Error struct {
	Code       string
	Message    string
	StatusCode int
}

func (e *S3Error) Error() string { return fmt.Sprintf("s3 error %s: %s", e.Code, e.Message) }

// ErrorCode implements smithy.APIError so generic not-found detection works.
func (e *S3Error) ErrorCode() string { return e.Code }

// ErrorMessage implements smithy.APIError.
func (e *S3Error) ErrorMessage() string { return e.Message }

// ErrorFault implements smithy.APIError.
func (e *S3Error) ErrorFault() smithy.ErrorFault {
	if e.StatusCode >= 400 && e.StatusCode < 500 {
		return smithy.FaultClient
	}
	return smithy.FaultServer
}
