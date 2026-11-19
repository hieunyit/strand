// Package objectstore provides a small S3-compatible client for durable backup artifacts.
// It is separate from the bounded-object spool backend: backup objects use streaming APIs.
package objectstore

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/xml"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
	"unicode/utf8"

	mrand "math/rand/v2"
)

const (
	defaultPartSize int64 = 64 << 20
	minPartSize     int64 = 5 << 20
	maxPartSize     int64 = 1 << 30
	singlePutLimit  int64 = 8 << 20
	maxParts              = 10000
	maxErrorBody          = 64 << 10
)

// Config configures path-style S3 access. Insecure HTTP is accepted only for loopback
// endpoints when AllowInsecureLocal is explicitly enabled.
type Config struct {
	Endpoint           string
	Bucket             string
	Region             string
	AccessKey          string
	SecretKey          string
	SessionToken       string
	HTTPClient         *http.Client
	AllowInsecureLocal bool
	PartSize           int64  // Multipart part size; default 64 MiB, maximum 1 GiB.
	TempDir            string // Temporary disk staging directory; empty uses the OS default.
	// Concurrency is the number of parts uploaded in parallel (default 4, maximum 32). Temporary disk use
	// is at most Concurrency × PartSize.
	Concurrency int
	// MaxRetries is how many times a request is retried after a network error or a retryable status
	// (408, 429, 5xx); default 4. RetryBaseDelay is the first backoff (default 500 ms, doubling).
	MaxRetries     int
	RetryBaseDelay time.Duration
}

// ObjectInfo describes an object and its optional Strand SHA-256 metadata.
type ObjectInfo struct {
	Key          string
	Size         int64
	ETag         string
	LastModified time.Time
	SHA256       string
}

// Store is the S3-compatible backup artifact interface.
type Store interface {
	Upload(context.Context, string, io.Reader, int64, string) (ObjectInfo, error)
	Download(context.Context, string) (io.ReadCloser, ObjectInfo, error)
	Head(context.Context, string) (ObjectInfo, error)
	Delete(context.Context, string) error
	List(context.Context, string) ([]ObjectInfo, error)
}

// S3 implements Store using S3 Signature Version 4 and multipart transfers.
type S3 struct {
	cfg      Config
	endpoint *url.URL
	client   *http.Client
}

// New validates configuration and creates a client. It never creates buckets implicitly.
func New(cfg Config) (*S3, error) {
	if cfg.Bucket == "" || cfg.AccessKey == "" || cfg.SecretKey == "" {
		return nil, errors.New("objectstore: bucket, access key, and secret key are required")
	}
	if cfg.Region == "" {
		cfg.Region = "us-east-1"
	}
	if cfg.PartSize == 0 {
		cfg.PartSize = defaultPartSize
	}
	if cfg.PartSize < minPartSize || cfg.PartSize > maxPartSize {
		return nil, fmt.Errorf("objectstore: part size must be between %d and %d bytes", minPartSize, maxPartSize)
	}
	if cfg.Concurrency == 0 {
		cfg.Concurrency = 4
	}
	if cfg.Concurrency < 1 || cfg.Concurrency > 32 {
		return nil, errors.New("objectstore: concurrency must be between 1 and 32")
	}
	if cfg.MaxRetries == 0 {
		cfg.MaxRetries = 4
	}
	if cfg.MaxRetries < 0 {
		cfg.MaxRetries = 0
	}
	if cfg.RetryBaseDelay <= 0 {
		cfg.RetryBaseDelay = 500 * time.Millisecond
	}
	u, err := url.Parse(strings.TrimRight(cfg.Endpoint, "/"))
	if err != nil || u.Host == "" || (u.Scheme != "https" && u.Scheme != "http") {
		// Do not echo the endpoint: a malformed value may still carry user info.
		return nil, errors.New("objectstore: invalid endpoint (want http(s)://host[:port][/path])")
	}
	if u.User != nil || u.RawQuery != "" || u.Fragment != "" {
		return nil, errors.New("objectstore: endpoint must not contain user info, query, or fragment")
	}
	if u.Scheme != "https" && !(cfg.AllowInsecureLocal && isLoopbackHost(u.Hostname())) {
		return nil, errors.New("objectstore: HTTPS is required (HTTP is allowed only for explicitly enabled loopback endpoints)")
	}
	client := cfg.HTTPClient
	if client == nil {
		client = &http.Client{Transport: &http.Transport{
			Proxy:             http.ProxyFromEnvironment,
			DialContext:       (&net.Dialer{Timeout: 10 * time.Second, KeepAlive: 30 * time.Second}).DialContext,
			ForceAttemptHTTP2: true, MaxIdleConns: 100, MaxIdleConnsPerHost: 16,
			IdleConnTimeout: 90 * time.Second, TLSHandshakeTimeout: 10 * time.Second,
			ResponseHeaderTimeout: 30 * time.Second,
		}, CheckRedirect: func(_ *http.Request, _ []*http.Request) error {
			// A redirect would invalidate the SigV4 host/path signature and could
			// forward a signed request to an unintended endpoint.
			return http.ErrUseLastResponse
		}}
	}
	return &S3{cfg: cfg, endpoint: u, client: client}, nil
}

// CreateBucket creates the configured bucket (explicit operator/test tooling; uploads never create buckets).
func (s *S3) CreateBucket(ctx context.Context) error {
	resp, err := s.request(ctx, http.MethodPut, "", nil, nil, nil, 0)
	if err != nil {
		return err
	}
	if !isSuccess(resp.StatusCode) {
		return s.responseError("create bucket", resp)
	}
	closeResponse(resp)
	return nil
}

func isLoopbackHost(host string) bool {
	if strings.EqualFold(host, "localhost") {
		return true
	}
	ip := net.ParseIP(host)
	return ip != nil && ip.IsLoopback()
}

// Upload streams an artifact using bounded memory and multipart uploads for large objects.
// Set size to -1 when unknown. A supplied SHA-256 is verified before finalization and stored
// as object metadata; when omitted, Upload still calculates and returns the SHA-256.
func (s *S3) Upload(ctx context.Context, key string, src io.Reader, size int64, sha256Hex string) (ObjectInfo, error) {
	if ctx == nil || src == nil {
		return ObjectInfo{}, errors.New("objectstore: context and upload reader are required")
	}
	if size < -1 {
		return ObjectInfo{}, errors.New("objectstore: size must be non-negative or -1 when unknown")
	}
	if err := validateKey(key); err != nil {
		return ObjectInfo{}, err
	}
	var want []byte
	if sha256Hex != "" {
		var err error
		want, err = hex.DecodeString(sha256Hex)
		if err != nil || len(want) != sha256.Size {
			return ObjectInfo{}, errors.New("objectstore: SHA-256 must be a 64-character hex digest")
		}
	}
	// Probe a bounded prefix for unknown-length streams. Small streams use a
	// regular PUT, avoiding implementations that reject a sub-5-MiB final part.
	if size < 0 {
		probe, err := io.ReadAll(io.LimitReader(&contextReader{ctx: ctx, r: src}, singlePutLimit+1))
		if err != nil {
			return ObjectInfo{}, fmt.Errorf("objectstore: probe upload stream: %w", err)
		}
		if int64(len(probe)) <= singlePutLimit {
			return s.uploadSingle(ctx, key, probe, want)
		}
		src = io.MultiReader(bytes.NewReader(probe), src)
	}
	if size >= 0 && size <= singlePutLimit {
		data, err := readExactBounded(ctx, src, size)
		if err != nil {
			return ObjectInfo{}, err
		}
		return s.uploadSingle(ctx, key, data, want)
	}
	partSize := s.cfg.PartSize
	if size > 0 {
		minimum := 1 + (size-1)/maxParts
		if minimum > partSize {
			// Round upward to a MiB boundary so the temporary-disk requirement is predictable.
			partSize = ((minimum + (1 << 20) - 1) / (1 << 20)) * (1 << 20)
		}
		if partSize > maxPartSize {
			return ObjectInfo{}, fmt.Errorf("objectstore: size %d exceeds the multipart limit (%d parts × %d-byte maximum part)", size, maxParts, maxPartSize)
		}
	}
	return s.uploadMultipart(ctx, key, src, size, strings.ToLower(sha256Hex), partSize)
}

func (s *S3) uploadSingle(ctx context.Context, key string, data []byte, want []byte) (ObjectInfo, error) {
	sum := sha256.Sum256(data)
	if len(want) > 0 && !bytes.Equal(sum[:], want) {
		return ObjectInfo{}, errors.New("objectstore: upload SHA-256 does not match supplied digest")
	}
	// The digest is known before a single PUT, so always store it: Download can then verify the body even
	// without catalog metadata.
	headers := http.Header{"Content-Type": {"application/octet-stream"}, "X-Amz-Meta-Strand-Sha256": {hex.EncodeToString(sum[:])}}
	resp, err := s.request(ctx, http.MethodPut, key, nil, headers, data, int64(len(data)))
	if err != nil {
		return ObjectInfo{}, err
	}
	if !isSuccess(resp.StatusCode) {
		return ObjectInfo{}, s.responseError("PUT object", resp)
	}
	info := ObjectInfo{Key: key, Size: int64(len(data)), ETag: cleanETag(resp.Header.Get("ETag")), SHA256: hex.EncodeToString(sum[:])}
	closeResponse(resp)
	return info, nil
}

func readExactBounded(ctx context.Context, r io.Reader, size int64) ([]byte, error) {
	if size > maxPartSize {
		return nil, errors.New("objectstore: internal single-part size exceeds memory bound")
	}
	buf := make([]byte, size)
	if _, err := io.ReadFull(&contextReader{ctx: ctx, r: r}, buf); err != nil {
		return nil, fmt.Errorf("objectstore: read upload body: %w", err)
	}
	var extra [1]byte
	n, err := io.ReadFull(&contextReader{ctx: ctx, r: r}, extra[:])
	if n != 0 || (err != nil && !errors.Is(err, io.EOF)) {
		if n != 0 {
			return nil, errors.New("objectstore: upload reader contains more bytes than declared size")
		}
		return nil, fmt.Errorf("objectstore: check upload size: %w", err)
	}
	return buf, nil
}

func (s *S3) initiateMultipart(ctx context.Context, key, digest string) (string, error) {
	headers := http.Header{"Content-Type": {"application/octet-stream"}}
	if digest != "" {
		headers.Set("X-Amz-Meta-Strand-Sha256", digest)
	}
	resp, err := s.request(ctx, http.MethodPost, key, url.Values{"uploads": {""}}, headers, nil, 0)
	if err != nil {
		return "", err
	}
	if !isSuccess(resp.StatusCode) {
		return "", s.responseError("initiate multipart upload", resp)
	}
	var init struct {
		UploadID string `xml:"UploadId"`
	}
	err = xml.NewDecoder(io.LimitReader(resp.Body, maxErrorBody)).Decode(&init)
	closeResponse(resp)
	if err != nil {
		return "", fmt.Errorf("objectstore: decode multipart initiation response: %w", err)
	}
	if init.UploadID == "" {
		return "", errors.New("objectstore: multipart initiation response has no upload ID")
	}
	return init.UploadID, nil
}

// unknownSizePartSize grows the part size for streams of unknown length so the 10,000-part limit still
// admits multi-TiB archives while small archives keep small parts: every 1,000 parts the size doubles,
// capped at the 1 GiB maximum (64 MiB base: 64 GiB, 128 GiB, ... then 1 GiB parts, ~6.9 TiB in total).
func unknownSizePartSize(base int64, partNo int) int64 {
	n := base
	for tier := (partNo - 1) / 1000; tier > 0 && n < maxPartSize; tier-- {
		n *= 2
	}
	if n > maxPartSize {
		n = maxPartSize
	}
	return n
}

// uploadMultipart stages each part on disk (bounded memory), hashes the whole stream sequentially and
// uploads up to Concurrency parts in parallel. At most Concurrency staged parts exist at any time, so the
// temporary-disk requirement is Concurrency × part size. Any failure (including cancellation) aborts the
// multipart upload so no orphaned parts remain billed in the bucket.
func (s *S3) uploadMultipart(ctx context.Context, key string, src io.Reader, size int64, digest string, partSize int64) (_ ObjectInfo, retErr error) {
	uploadID, err := s.initiateMultipart(ctx, key, digest)
	if err != nil {
		return ObjectInfo{}, err
	}
	completed := false
	defer func() {
		if !completed {
			if abortErr := s.abortMultipart(ctx, key, uploadID); abortErr != nil {
				retErr = errors.Join(retErr, abortErr)
			}
		}
	}()

	upCtx, cancelUp := context.WithCancel(ctx)
	defer cancelUp()
	var (
		errMu    sync.Mutex
		firstErr error
		partsMu  sync.Mutex
		parts    = make([]completedPart, 0)
		wg       sync.WaitGroup
	)
	setErr := func(err error) {
		errMu.Lock()
		if firstErr == nil {
			firstErr = err
			cancelUp()
		}
		errMu.Unlock()
	}
	getErr := func() error {
		errMu.Lock()
		defer errMu.Unlock()
		return firstErr
	}
	sem := make(chan struct{}, s.cfg.Concurrency)

	h := sha256.New()
	left, partNo := size, 1
	var total int64
	reader := &contextReader{ctx: upCtx, r: src}
read:
	for size < 0 || left > 0 {
		if partNo > maxParts {
			if size >= 0 {
				setErr(fmt.Errorf("objectstore: object exceeds maximum of %d multipart parts", maxParts))
				break
			}
			// Unknown-length input may end exactly at the 10,000th part; probe EOF
			// before rejecting so that the maximum valid object remains uploadable.
			var extra [1]byte
			n, probeErr := io.ReadFull(reader, extra[:])
			if n > 0 {
				setErr(fmt.Errorf("objectstore: object exceeds maximum of %d multipart parts", maxParts))
			} else if !errors.Is(probeErr, io.EOF) {
				setErr(fmt.Errorf("objectstore: check end of upload after part limit: %w", probeErr))
			}
			break
		}
		want := partSize
		if size < 0 {
			want = unknownSizePartSize(partSize, partNo)
		} else if left < want {
			want = left
		}
		select {
		case sem <- struct{}{}:
		case <-upCtx.Done():
			break read
		}
		release := func() { <-sem }
		partFile, err := os.CreateTemp(s.cfg.TempDir, "strand-s3-part-*")
		if err != nil {
			release()
			setErr(fmt.Errorf("objectstore: create temporary upload part: %w", err))
			break
		}
		partName := partFile.Name()
		discard := func() {
			_ = partFile.Close()
			_ = os.Remove(partName)
			release()
		}
		partLen, readErr := io.CopyN(io.MultiWriter(partFile, h), reader, want)
		if readErr != nil && (size >= 0 || !errors.Is(readErr, io.EOF)) {
			discard()
			if upErr := getErr(); upErr != nil {
				break // the reader stopped because a part upload failed; report that cause
			}
			setErr(fmt.Errorf("objectstore: read upload part %d: %w", partNo, readErr))
			break
		}
		if partLen == 0 {
			discard()
			if size < 0 {
				break
			}
			setErr(fmt.Errorf("objectstore: no bytes read for upload part %d", partNo))
			break
		}
		wg.Add(1)
		go func(n int, f *os.File, length int64) {
			defer wg.Done()
			defer discard()
			// The SigV4 payload hash of the part is computed here, off the sequential read path, so
			// hashing overlaps across parts on multi-core hosts. S3 verifies it against the received body.
			ph := sha256.New()
			if _, err := io.Copy(ph, io.NewSectionReader(f, 0, length)); err != nil {
				setErr(fmt.Errorf("objectstore: hash staged part %d: %w", n, err))
				return
			}
			eTag, err := s.uploadPart(upCtx, key, uploadID, n, f, length, hex.EncodeToString(ph.Sum(nil)))
			if err != nil {
				setErr(err)
				return
			}
			partsMu.Lock()
			parts = append(parts, completedPart{Number: n, ETag: eTag})
			partsMu.Unlock()
		}(partNo, partFile, partLen)
		total += partLen
		partNo++
		if size >= 0 {
			left -= partLen
		} else if errors.Is(readErr, io.EOF) || partLen < want {
			break
		}
	}
	wg.Wait()
	if err := getErr(); err != nil {
		if ctx.Err() != nil {
			return ObjectInfo{}, fmt.Errorf("objectstore: upload cancelled: %w", ctx.Err())
		}
		return ObjectInfo{}, err
	}
	if err := ctx.Err(); err != nil {
		return ObjectInfo{}, fmt.Errorf("objectstore: upload cancelled: %w", err)
	}
	if size >= 0 {
		var extra [1]byte
		n, readErr := io.ReadFull(reader, extra[:])
		if n != 0 || (readErr != nil && !errors.Is(readErr, io.EOF)) {
			if n != 0 {
				return ObjectInfo{}, errors.New("objectstore: upload reader contains more bytes than declared size")
			}
			return ObjectInfo{}, fmt.Errorf("objectstore: check upload size: %w", readErr)
		}
	}
	if len(parts) == 0 {
		return ObjectInfo{}, errors.New("objectstore: multipart upload has no parts")
	}
	actualDigest := hex.EncodeToString(h.Sum(nil))
	if digest != "" && !strings.EqualFold(actualDigest, digest) {
		return ObjectInfo{}, errors.New("objectstore: upload SHA-256 does not match supplied digest")
	}
	sort.Slice(parts, func(i, j int) bool { return parts[i].Number < parts[j].Number })
	eTag, err := s.completeMultipart(ctx, key, uploadID, parts, total)
	if err != nil {
		return ObjectInfo{}, err
	}
	completed = true
	return ObjectInfo{Key: key, Size: total, ETag: eTag, SHA256: actualDigest}, nil
}

func (s *S3) uploadPart(ctx context.Context, key, uploadID string, n int, f *os.File, length int64, sum string) (string, error) {
	q := url.Values{"partNumber": {strconv.Itoa(n)}, "uploadId": {uploadID}}
	body := func() io.Reader { return io.NewSectionReader(f, 0, length) }
	resp, err := s.do(ctx, http.MethodPut, key, q, nil, body, length, sum)
	if err != nil {
		return "", err
	}
	if !isSuccess(resp.StatusCode) {
		return "", s.responseError(fmt.Sprintf("upload part %d", n), resp)
	}
	eTag := cleanETag(resp.Header.Get("ETag"))
	closeResponse(resp)
	if eTag == "" {
		return "", fmt.Errorf("objectstore: multipart part %d response did not include ETag", n)
	}
	return eTag, nil
}

// completeMultipart retries transient failures, including S3's documented "200 OK with an error body".
// If a retry finds the upload gone (the first completion succeeded but its response was lost), the
// object is accepted only when HEAD shows the expected size and a multipart ETag with the same part count.
func (s *S3) completeMultipart(ctx context.Context, key, uploadID string, parts []completedPart, total int64) (string, error) {
	body, err := xml.Marshal(completeMultipart{Parts: parts})
	if err != nil {
		return "", err
	}
	q := url.Values{"uploadId": {uploadID}}
	headers := http.Header{"Content-Type": {"application/xml"}}
	var lastErr error
	for attempt := 0; attempt <= s.cfg.MaxRetries; attempt++ {
		if attempt > 0 {
			if err := s.backoff(ctx, attempt); err != nil {
				return "", err
			}
		}
		cr, err := s.request(ctx, http.MethodPost, key, q, headers, body, int64(len(body)))
		if err != nil {
			return "", err
		}
		if !isSuccess(cr.StatusCode) {
			rerr := s.responseError("complete multipart upload", cr)
			if attempt > 0 && strings.Contains(rerr.Error(), "NoSuchUpload") {
				if head, herr := s.Head(ctx, key); herr == nil && head.Size == total && strings.HasSuffix(head.ETag, "-"+strconv.Itoa(len(parts))) {
					return head.ETag, nil
				}
			}
			return "", rerr
		}
		var result struct {
			ETag    string `xml:"ETag"`
			Code    string `xml:"Code"`
			Message string `xml:"Message"`
		}
		err = xml.NewDecoder(io.LimitReader(cr.Body, maxErrorBody)).Decode(&result)
		headerETag := cr.Header.Get("ETag")
		closeResponse(cr)
		if err != nil && !errors.Is(err, io.EOF) {
			return "", fmt.Errorf("objectstore: decode multipart completion response: %w", err)
		}
		if result.Code != "" {
			lastErr = fmt.Errorf("objectstore: complete multipart upload failed (%s): %s", result.Code, s.redact(result.Message))
			if result.Code == "InternalError" || result.Code == "SlowDown" || result.Code == "ServiceUnavailable" {
				continue
			}
			return "", lastErr
		}
		if result.ETag == "" {
			result.ETag = headerETag
		}
		return cleanETag(result.ETag), nil
	}
	return "", lastErr
}

// AbortStaleUploads aborts every unfinished multipart upload whose key is exactly key. A crashed or killed
// uploader (SIGKILL, host loss) cannot run its deferred abort, so callers that own key exclusively (the
// per-backup catalog lock) call this before uploading to reclaim the orphaned parts. It returns the number
// of uploads aborted.
func (s *S3) AbortStaleUploads(ctx context.Context, key string) (int, error) {
	if err := validateKey(key); err != nil {
		return 0, err
	}
	aborted := 0
	keyMarker, idMarker := "", ""
	for page := 0; page < 1000; page++ {
		q := url.Values{"uploads": {""}, "prefix": {key}}
		if keyMarker != "" {
			q.Set("key-marker", keyMarker)
			q.Set("upload-id-marker", idMarker)
		}
		resp, err := s.request(ctx, http.MethodGet, "", q, nil, nil, 0)
		if err != nil {
			return aborted, err
		}
		if resp.StatusCode != http.StatusOK {
			return aborted, s.responseError("list multipart uploads", resp)
		}
		var res struct {
			Uploads []struct {
				Key      string `xml:"Key"`
				UploadID string `xml:"UploadId"`
			} `xml:"Upload"`
			IsTruncated        bool   `xml:"IsTruncated"`
			NextKeyMarker      string `xml:"NextKeyMarker"`
			NextUploadIDMarker string `xml:"NextUploadIdMarker"`
		}
		err = xml.NewDecoder(io.LimitReader(resp.Body, 16<<20)).Decode(&res)
		closeResponse(resp)
		if err != nil {
			return aborted, fmt.Errorf("objectstore: decode multipart upload list: %w", err)
		}
		for _, u := range res.Uploads {
			if u.Key != key || u.UploadID == "" {
				continue
			}
			if err := s.abortMultipart(ctx, key, u.UploadID); err != nil {
				return aborted, err
			}
			aborted++
		}
		if !res.IsTruncated {
			return aborted, nil
		}
		if res.NextKeyMarker == keyMarker && res.NextUploadIDMarker == idMarker {
			return aborted, errors.New("objectstore: truncated multipart upload list has a repeated marker")
		}
		keyMarker, idMarker = res.NextKeyMarker, res.NextUploadIDMarker
	}
	return aborted, errors.New("objectstore: too many multipart upload list pages")
}

type completedPart struct {
	Number int
	ETag   string
}

type completeMultipart struct {
	XMLName xml.Name        `xml:"CompleteMultipartUpload"`
	Parts   []completedPart `xml:"Part"`
}

func (p completedPart) MarshalXML(e *xml.Encoder, start xml.StartElement) error {
	start.Name.Local = "Part"
	if err := e.EncodeToken(start); err != nil {
		return err
	}
	if err := e.EncodeElement(p.Number, xml.StartElement{Name: xml.Name{Local: "PartNumber"}}); err != nil {
		return err
	}
	if err := e.EncodeElement(p.ETag, xml.StartElement{Name: xml.Name{Local: "ETag"}}); err != nil {
		return err
	}
	return e.EncodeToken(start.End())
}

func (s *S3) abortMultipart(ctx context.Context, key, uploadID string) error {
	abortCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 10*time.Second)
	defer cancel()
	resp, err := s.request(abortCtx, http.MethodDelete, key, url.Values{"uploadId": {uploadID}}, nil, nil, 0)
	if err != nil {
		return fmt.Errorf("objectstore: abort multipart upload: %w", err)
	}
	defer closeResponse(resp)
	if resp.StatusCode != http.StatusNoContent && resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusNotFound {
		return s.responseError("abort multipart upload", resp)
	}
	return nil
}

// Download opens an object body for streaming; callers must close the returned body.
func (s *S3) Download(ctx context.Context, key string) (io.ReadCloser, ObjectInfo, error) {
	if err := validateKey(key); err != nil {
		return nil, ObjectInfo{}, err
	}
	resp, err := s.request(ctx, http.MethodGet, key, nil, nil, nil, 0)
	if err != nil {
		return nil, ObjectInfo{}, err
	}
	if resp.StatusCode != http.StatusOK {
		return nil, ObjectInfo{}, s.responseError("GET object", resp)
	}
	info := infoFromHeaders(key, resp.Header)
	return &verifiedBody{body: resp.Body, wantSize: info.Size, wantSHA: info.SHA256, hash: sha256.New()}, info, nil
}

// DownloadVerified opens an object and verifies, while it is read, that it has exactly wantSize bytes and
// the SHA-256 wantSHA (from the backup catalog). The final Read returns an error instead of io.EOF on a
// mismatch, so a consumer that copies to EOF never mistakes a corrupted or substituted object for a good
// one. A Strand SHA-256 object metadata value that disagrees with wantSHA is rejected before any byte is
// returned.
func (s *S3) DownloadVerified(ctx context.Context, key string, wantSize int64, wantSHA string) (io.ReadCloser, ObjectInfo, error) {
	if wantSize < 0 || !isHexSHA256(wantSHA) {
		return nil, ObjectInfo{}, errors.New("objectstore: verified download requires the expected size and SHA-256")
	}
	body, info, err := s.Download(ctx, key)
	if err != nil {
		return nil, ObjectInfo{}, err
	}
	wantSHA = strings.ToLower(wantSHA)
	if info.SHA256 != "" && info.SHA256 != wantSHA {
		_ = body.Close()
		return nil, ObjectInfo{}, errors.New("objectstore: object SHA-256 metadata does not match the expected digest")
	}
	if info.Size >= 0 && info.Size != wantSize {
		_ = body.Close()
		return nil, ObjectInfo{}, fmt.Errorf("objectstore: object size %d does not match the expected %d bytes", info.Size, wantSize)
	}
	vb := body.(*verifiedBody)
	vb.wantSize, vb.wantSHA = wantSize, wantSHA
	info.Size, info.SHA256 = wantSize, wantSHA
	return vb, info, nil
}

func isHexSHA256(s string) bool {
	b, err := hex.DecodeString(s)
	return err == nil && len(b) == sha256.Size
}

type verifiedBody struct {
	body     io.ReadCloser
	wantSize int64
	wantSHA  string
	read     int64
	hash     io.Writer
	checked  bool
}

func (v *verifiedBody) Read(p []byte) (int, error) {
	n, err := v.body.Read(p)
	if n > 0 {
		_, _ = v.hash.Write(p[:n])
		v.read += int64(n)
	}
	if errors.Is(err, io.EOF) && !v.checked {
		v.checked = true
		if v.wantSize >= 0 && v.read != v.wantSize {
			return n, fmt.Errorf("objectstore: downloaded size mismatch: got %d, expected %d", v.read, v.wantSize)
		}
		if v.wantSHA != "" {
			if h, ok := v.hash.(interface{ Sum([]byte) []byte }); ok && !strings.EqualFold(hex.EncodeToString(h.Sum(nil)), v.wantSHA) {
				return n, errors.New("objectstore: downloaded SHA-256 mismatch")
			}
		}
	}
	return n, err
}
func (v *verifiedBody) Close() error { return v.body.Close() }

// Head returns object size and the stored SHA-256 metadata when present.
func (s *S3) Head(ctx context.Context, key string) (ObjectInfo, error) {
	if err := validateKey(key); err != nil {
		return ObjectInfo{}, err
	}
	resp, err := s.request(ctx, http.MethodHead, key, nil, nil, nil, 0)
	if err != nil {
		return ObjectInfo{}, err
	}
	defer closeResponse(resp)
	if resp.StatusCode != http.StatusOK {
		return ObjectInfo{}, s.responseError("HEAD object", resp)
	}
	return infoFromHeaders(key, resp.Header), nil
}

// Delete removes an object. Deleting an absent key is treated as success.
func (s *S3) Delete(ctx context.Context, key string) error {
	if err := validateKey(key); err != nil {
		return err
	}
	resp, err := s.request(ctx, http.MethodDelete, key, nil, nil, nil, 0)
	if err != nil {
		return err
	}
	defer closeResponse(resp)
	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusNoContent && resp.StatusCode != http.StatusNotFound {
		return s.responseError("DELETE object", resp)
	}
	return nil
}

// List returns every object under prefix, following ListObjectsV2 continuation tokens.
func (s *S3) List(ctx context.Context, prefix string) ([]ObjectInfo, error) {
	var out []ObjectInfo
	token := ""
	for {
		q := url.Values{"list-type": {"2"}, "prefix": {prefix}}
		if token != "" {
			q.Set("continuation-token", token)
		}
		resp, err := s.request(ctx, http.MethodGet, "", q, nil, nil, 0)
		if err != nil {
			return nil, err
		}
		if resp.StatusCode != http.StatusOK {
			return nil, s.responseError("LIST objects", resp)
		}
		var page listV2Result
		err = xml.NewDecoder(io.LimitReader(resp.Body, 16<<20)).Decode(&page)
		closeResponse(resp)
		if err != nil {
			return nil, fmt.Errorf("objectstore: decode list response: %w", err)
		}
		for _, item := range page.Contents {
			info := ObjectInfo{Key: item.Key, Size: item.Size, ETag: cleanETag(item.ETag)}
			if item.LastModified != "" {
				info.LastModified, _ = time.Parse(time.RFC3339, item.LastModified)
			}
			out = append(out, info)
		}
		if !page.IsTruncated {
			return out, nil
		}
		if page.NextContinuationToken == "" || page.NextContinuationToken == token {
			return nil, errors.New("objectstore: truncated list page has missing/repeated continuation token")
		}
		token = page.NextContinuationToken
	}
}

type listV2Result struct {
	Contents []struct {
		Key          string `xml:"Key"`
		Size         int64  `xml:"Size"`
		ETag         string `xml:"ETag"`
		LastModified string `xml:"LastModified"`
	} `xml:"Contents"`
	IsTruncated           bool   `xml:"IsTruncated"`
	NextContinuationToken string `xml:"NextContinuationToken"`
}

func (s *S3) request(ctx context.Context, method, key string, query url.Values, headers http.Header, body []byte, contentLength int64) (*http.Response, error) {
	payloadHash := sha256.Sum256(body)
	return s.do(ctx, method, key, query, headers, func() io.Reader { return bytes.NewReader(body) }, contentLength, hex.EncodeToString(payloadHash[:]))
}

// retryableStatus reports S3 responses that the AWS SDKs retry: throttling and transient server errors.
func retryableStatus(code int) bool {
	switch code {
	case http.StatusRequestTimeout, http.StatusTooManyRequests, http.StatusInternalServerError,
		http.StatusBadGateway, http.StatusServiceUnavailable, http.StatusGatewayTimeout:
		return true
	}
	return false
}

func (s *S3) backoff(ctx context.Context, attempt int) error {
	d := s.cfg.RetryBaseDelay << (attempt - 1)
	if d > 20*time.Second || d <= 0 {
		d = 20 * time.Second
	}
	d = d/2 + time.Duration(mrand.Int64N(int64(d/2)+1)) // equal jitter
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-t.C:
		return nil
	}
}

// attemptTimeout bounds one request attempt that sends a body, so a stalled connection (server stops
// reading, middlebox drops packets) fails and is retried instead of hanging forever: 2 minutes plus
// the time to send the body at 256 KiB/s. GET/HEAD responses are bounded by the transport's
// ResponseHeaderTimeout and by the caller's context while streaming.
func attemptTimeout(length int64) time.Duration {
	return 2*time.Minute + time.Duration(length/(256<<10))*time.Second
}

// do sends one signed request, retrying network errors and retryable statuses with exponential backoff.
// body is called once per attempt and must return a fresh reader positioned at the start of the payload.
func (s *S3) do(ctx context.Context, method, key string, query url.Values, headers http.Header, body func() io.Reader, contentLength int64, payloadHash string) (*http.Response, error) {
	if ctx == nil {
		return nil, errors.New("objectstore: request context is required")
	}
	for attempt := 0; ; attempt++ {
		if attempt > 0 {
			if err := s.backoff(ctx, attempt); err != nil {
				return nil, fmt.Errorf("objectstore: %s request: %w", method, err)
			}
		}
		attemptCtx, cancel := ctx, context.CancelFunc(func() {})
		if method != http.MethodGet && method != http.MethodHead {
			attemptCtx, cancel = context.WithTimeout(ctx, attemptTimeout(contentLength))
		}
		resp, err := s.requestReader(attemptCtx, method, key, query, headers, body(), contentLength, payloadHash)
		last := attempt >= s.cfg.MaxRetries || ctx.Err() != nil
		if err == nil && (!retryableStatus(resp.StatusCode) || last) {
			resp.Body = &cancelOnClose{ReadCloser: resp.Body, cancel: cancel}
			return resp, nil
		}
		if resp != nil {
			closeResponse(resp)
		}
		cancel()
		if err != nil && last {
			return nil, err
		}
	}
}

type cancelOnClose struct {
	io.ReadCloser
	cancel context.CancelFunc
}

func (c *cancelOnClose) Close() error {
	err := c.ReadCloser.Close()
	c.cancel()
	return err
}

func (s *S3) requestReader(ctx context.Context, method, key string, query url.Values, headers http.Header, body io.Reader, contentLength int64, payloadHash string) (*http.Response, error) {
	if ctx == nil {
		return nil, errors.New("objectstore: request context is required")
	}
	u := *s.endpoint
	basePath := strings.TrimRight(u.EscapedPath(), "/")
	objectPath := awsEscape(s.cfg.Bucket)
	if key != "" {
		objectPath += "/" + escapeKey(key)
	}
	escapedPath := basePath + "/" + objectPath
	decoded, err := url.PathUnescape(escapedPath)
	if err != nil {
		return nil, fmt.Errorf("objectstore: invalid object path: %w", err)
	}
	u.Path, u.RawPath = decoded, escapedPath
	u.RawQuery = canonicalQuery(query)
	req, err := http.NewRequestWithContext(ctx, method, u.String(), body)
	if err != nil {
		return nil, err
	}
	if headers != nil {
		req.Header = headers.Clone()
	}
	if contentLength >= 0 {
		req.ContentLength = contentLength
		if contentLength == 0 {
			req.Body = http.NoBody
		}
	}
	s.sign(req, payloadHash)
	resp, err := s.client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("objectstore: %s request: %s", method, s.redact(err.Error()))
	}
	return resp, nil
}

// redact removes configured credentials from text that may come from a server or the network stack
// (a misbehaving endpoint or proxy may echo request headers back in an error message).
func (s *S3) redact(text string) string {
	for _, secret := range []string{s.cfg.SecretKey, s.cfg.SessionToken, s.cfg.AccessKey} {
		if len(secret) >= 4 {
			text = strings.ReplaceAll(text, secret, "[redacted]")
		}
	}
	return text
}

func (s *S3) sign(req *http.Request, payloadHex string) {
	now := time.Now().UTC()
	dt, amzDate := now.Format("20060102"), now.Format("20060102T150405Z")
	req.Header.Set("X-Amz-Date", amzDate)
	req.Header.Set("X-Amz-Content-Sha256", payloadHex)
	if s.cfg.SessionToken != "" {
		req.Header.Set("X-Amz-Security-Token", s.cfg.SessionToken)
	}
	canonicalHeaders, signedHeaders := canonicalHeaders(req)
	canonicalRequest := strings.Join([]string{req.Method, req.URL.EscapedPath(), canonicalQueryFromRaw(req.URL.RawQuery), canonicalHeaders, signedHeaders, payloadHex}, "\n")
	h := sha256.Sum256([]byte(canonicalRequest))
	scope := dt + "/" + s.cfg.Region + "/s3/aws4_request"
	stringToSign := "AWS4-HMAC-SHA256\n" + amzDate + "\n" + scope + "\n" + hex.EncodeToString(h[:])
	kDate := hmacSHA256([]byte("AWS4"+s.cfg.SecretKey), []byte(dt))
	kRegion := hmacSHA256(kDate, []byte(s.cfg.Region))
	kService := hmacSHA256(kRegion, []byte("s3"))
	kSigning := hmacSHA256(kService, []byte("aws4_request"))
	sig := hex.EncodeToString(hmacSHA256(kSigning, []byte(stringToSign)))
	req.Header.Set("Authorization", "AWS4-HMAC-SHA256 Credential="+s.cfg.AccessKey+"/"+scope+", SignedHeaders="+signedHeaders+", Signature="+sig)
}

func canonicalHeaders(req *http.Request) (string, string) {
	values := map[string][]string{"host": {req.URL.Host}}
	for k, vs := range req.Header {
		lk := strings.ToLower(k)
		if lk == "authorization" || lk == "user-agent" || lk == "content-length" {
			continue
		}
		values[lk] = vs
	}
	keys := make([]string, 0, len(values))
	for k := range values {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	var b strings.Builder
	for _, k := range keys {
		clean := make([]string, len(values[k]))
		for i, v := range values[k] {
			clean[i] = strings.Join(strings.Fields(v), " ")
		}
		b.WriteString(k)
		b.WriteByte(':')
		b.WriteString(strings.Join(clean, ","))
		b.WriteByte('\n')
	}
	return b.String(), strings.Join(keys, ";")
}

func canonicalQuery(v url.Values) string {
	if len(v) == 0 {
		return ""
	}
	keys := make([]string, 0, len(v))
	for k := range v {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	var pairs []string
	for _, k := range keys {
		vals := append([]string(nil), v[k]...)
		sort.Strings(vals)
		for _, x := range vals {
			pairs = append(pairs, awsEscape(k)+"="+awsEscape(x))
		}
	}
	return strings.Join(pairs, "&")
}
func canonicalQueryFromRaw(raw string) string {
	v, _ := url.ParseQuery(raw)
	return canonicalQuery(v)
}
func awsEscape(s string) string {
	const hexChars = "0123456789ABCDEF"
	var b strings.Builder
	for i := 0; i < len(s); i++ {
		c := s[i]
		if c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' || c >= '0' && c <= '9' || strings.ContainsRune("-_.~", rune(c)) {
			b.WriteByte(c)
		} else {
			b.WriteByte('%')
			b.WriteByte(hexChars[c>>4])
			b.WriteByte(hexChars[c&15])
		}
	}
	return b.String()
}
func escapeKey(key string) string {
	parts := strings.Split(key, "/")
	for i := range parts {
		parts[i] = awsEscape(parts[i])
	}
	return strings.Join(parts, "/")
}
func hmacSHA256(key, data []byte) []byte {
	h := hmac.New(sha256.New, key)
	_, _ = h.Write(data)
	return h.Sum(nil)
}
func validateKey(key string) error {
	if key == "" {
		return errors.New("objectstore: object key is required")
	}
	if len(key) > 1024 {
		return errors.New("objectstore: object key exceeds the S3 1,024-byte limit")
	}
	if !utf8.ValidString(key) {
		return errors.New("objectstore: object key must be valid UTF-8")
	}
	return nil
}
func cleanETag(s string) string { return strings.Trim(s, "\"") }
func isSuccess(status int) bool { return status >= 200 && status < 300 }
func closeResponse(r *http.Response) {
	if r != nil && r.Body != nil {
		_, _ = io.Copy(io.Discard, io.LimitReader(r.Body, maxErrorBody))
		_ = r.Body.Close()
	}
}

func (s *S3) responseError(op string, resp *http.Response) error {
	body, _ := io.ReadAll(io.LimitReader(resp.Body, maxErrorBody))
	closeResponse(resp)
	var e struct {
		Code    string `xml:"Code"`
		Message string `xml:"Message"`
	}
	_ = xml.Unmarshal(body, &e)
	if e.Code != "" {
		return fmt.Errorf("objectstore: %s failed (%d %s): %s", op, resp.StatusCode, s.redact(e.Code), s.redact(e.Message))
	}
	return fmt.Errorf("objectstore: %s failed with HTTP %d", op, resp.StatusCode)
}

func infoFromHeaders(key string, h http.Header) ObjectInfo {
	info := ObjectInfo{Key: key, Size: -1, ETag: cleanETag(h.Get("ETag")), SHA256: strings.ToLower(h.Get("X-Amz-Meta-Strand-Sha256"))}
	if n, err := strconv.ParseInt(h.Get("Content-Length"), 10, 64); err == nil {
		info.Size = n
	}
	if t, err := http.ParseTime(h.Get("Last-Modified")); err == nil {
		info.LastModified = t
	}
	return info
}

type contextReader struct {
	ctx context.Context
	r   io.Reader
}

func (r *contextReader) Read(p []byte) (int, error) {
	select {
	case <-r.ctx.Done():
		return 0, r.ctx.Err()
	default:
		return r.r.Read(p)
	}
}
