package gateway

import (
	"encoding/xml"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/matthewgall/s3-migrateproxy/internal/metadata"
	"github.com/matthewgall/s3-migrateproxy/internal/providers"
)

func requestID() string { return "req-" + strconv.FormatInt(timeNow().UnixNano(), 10) }

func xmlError(code, message string) string {
	return `<?xml version="1.0" encoding="UTF-8"?>` +
		`<Error>` +
		`<Code>` + xmlEscape(code) + `</Code>` +
		`<Message>` + xmlEscape(message) + `</Message>` +
		`<RequestId>` + xmlEscape(requestID()) + `</RequestId>` +
		`</Error>`
}

func xmlEscape(s string) string {
	var b strings.Builder
	_ = xml.EscapeText(&b, []byte(s))
	return b.String()
}

func xmlListBucketResult(reqID string, result *metadata.ListResult, maxKeys int) string {
	var b strings.Builder
	b.WriteString(`<?xml version="1.0" encoding="UTF-8"?>`)
	b.WriteString(`<ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">`)
	fmt.Fprintf(&b, "<Name>migration</Name><Prefix></Prefix><StartAfter></StartAfter><MaxKeys>%d</MaxKeys>", maxKeys)
	fmt.Fprintf(&b, "<IsTruncated>%t</IsTruncated>", result.IsTruncated)
	if result.IsTruncated {
		fmt.Fprintf(&b, "<NextContinuationToken>%s</NextContinuationToken>", xmlEscape(result.ContinuationToken))
	}
	for _, p := range result.CommonPrefixes {
		b.WriteString("<CommonPrefix><Prefix>")
		b.WriteString(xmlEscape(p))
		b.WriteString("</Prefix></CommonPrefix>")
	}
	for _, o := range result.Keys {
		b.WriteString("<Contents>")
		fmt.Fprintf(&b, "<Key>%s</Key>", xmlEscape(o.Key))
		fmt.Fprintf(&b, "<Size>%d</Size>", o.Size)
		fmt.Fprintf(&b, "<ETag>%s</ETag>", xmlEscape(o.ETag))
		if !o.LastModified.IsZero() {
			fmt.Fprintf(&b, "<LastModified>%s</LastModified>", o.LastModified.UTC().Format(time.RFC3339))
		}
		b.WriteString("</Contents>")
	}
	b.WriteString("</ListBucketResult>")
	return b.String()
}

func xmlInitiateMultipartUploadResult(key, uploadID string) string {
	return "<?xml version=\"1.0\" encoding=\"UTF-8\"?>" +
		"<InitiateMultipartUploadResult xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">" +
		"<Bucket>migration</Bucket>" +
		"<Key>" + xmlEscape(key) + "</Key>" +
		"<UploadId>" + xmlEscape(uploadID) + "</UploadId>" +
		"</InitiateMultipartUploadResult>"
}

func xmlCompleteMultipartUploadResult(result *providers.PutResult, key string) string {
	etag := result.ETag
	if etag == "" {
		etag = "\"etag\""
	}
	return "<?xml version=\"1.0\" encoding=\"UTF-8\"?>" +
		"<CompleteMultipartUploadResult xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">" +
		"<Location></Location>" +
		"<Bucket>migration</Bucket>" +
		"<Key>" + xmlEscape(key) + "</Key>" +
		"<ETag>" + xmlEscape(etag) + "</ETag>" +
		"</CompleteMultipartUploadResult>"
}

func xmlListPartsResult(uploadID, key string, parts []providers.PartInfo) string {
	var b strings.Builder
	b.WriteString(`<?xml version="1.0" encoding="UTF-8"?>`)
	b.WriteString(`<ListPartsResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">`)
	fmt.Fprintf(&b, "<Bucket>migration</Bucket><Key>%s</Key><UploadId>%s</UploadId>", xmlEscape(key), xmlEscape(uploadID))
	fmt.Fprintf(&b, "<IsTruncated>false</IsTruncated>")
	for _, p := range parts {
		fmt.Fprintf(&b, "<Part><PartNumber>%d</PartNumber><ETag>%s</ETag><Size>%d</Size></Part>", p.PartNumber, xmlEscape(p.ETag), p.Size)
	}
	b.WriteString("</ListPartsResult>")
	return b.String()
}

func xmlListMultipartUploadsResult(uploads []providers.MultipartUploadInfo) string {
	var b strings.Builder
	b.WriteString(`<?xml version="1.0" encoding="UTF-8"?>`)
	b.WriteString(`<ListMultipartUploadsResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">`)
	fmt.Fprintf(&b, "<Bucket>migration</Bucket><KeyMarker></KeyMarker><UploadIdMarker></UploadIdMarker>")
	for _, u := range uploads {
		fmt.Fprintf(&b, "<Upload><Key>%s</Key><UploadId>%s</UploadId><Initiated>%s</Initiated></Upload>", xmlEscape(u.Key), xmlEscape(u.UploadID), timeNow().UTC().Format(time.RFC3339))
	}
	b.WriteString("</ListMultipartUploadsResult>")
	return b.String()
}

func xmlCopyObjectResult(result *providers.PutResult, key string) string {
	etag := result.ETag
	if etag == "" {
		etag = "\"etag\""
	}
	return "<?xml version=\"1.0\" encoding=\"UTF-8\"?>" +
		"<CopyObjectResult>" +
		"<ETag>" + xmlEscape(etag) + "</ETag>" +
		"</CopyObjectResult>"
}

func xmlPutResult(etag string) string {
	return `<?xml version="1.0" encoding="UTF-8"?>` +
		`<PutObjectResult><ETag>` + xmlEscape(etag) + `</ETag></PutObjectResult>`
}

func xmlDeleteResult() string { return "" }

func setXML(w http.ResponseWriter, status int, body string) {
	w.Header().Set("Content-Type", "application/xml")
	w.WriteHeader(status)
	_, _ = w.Write([]byte(body))
}
