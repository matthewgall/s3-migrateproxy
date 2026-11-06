package verification

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"hash"
	"io"
)

// Result holds verification outcome.
type Result struct {
	OK       bool
	MatchSize bool
	MatchETag bool
	MatchSHA256 bool
	SourceSHA256 string
	DestSHA256   string
	Message  string
}

// Compare compares source and destination object attributes.
func Compare(ctx context.Context, srcSize int64, srcETag, srcSHA string, destSize int64, destETag, destSHA string, content io.Reader) (*Result, error) {
	res := &Result{
		MatchSize: srcSize == destSize,
		MatchETag: srcSize == destSize && srcETag != "" && srcETag == destETag,
	}
	if content != nil && (srcSHA == "" || destSHA == "") {
		h := sha256.New()
		if _, err := io.Copy(h, content); err != nil {
			return nil, err
		}
		hash := hex.EncodeToString(h.Sum(nil))
		if srcSHA == "" {
			srcSHA = hash
		} else {
			destSHA = hash
		}
	}
	res.SourceSHA256 = srcSHA
	res.DestSHA256 = destSHA
	res.MatchSHA256 = srcSHA != "" && srcSHA == destSHA
	res.OK = res.MatchSize && (res.MatchSHA256 || res.MatchETag)
	if !res.OK {
		res.Message = fmt.Sprintf("size=%v etag=%v sha256=%v", res.MatchSize, res.MatchETag, res.MatchSHA256)
	}
	return res, nil
}

// Reader wraps an io.Reader to compute a checksum while reading.
func Reader(r io.Reader, h hash.Hash) io.Reader {
	return io.TeeReader(r, h)
}
