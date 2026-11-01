package spool

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
	"net/http"
	"net/url"
	"sort"
	"strings"
	"sync"
	"time"
)

// BucketBackend là interface lưu trữ đối tượng cho tầng bucket staging của Spool.
// spec: 02 §3, §9, §10
type BucketBackend interface {
	Put(ctx context.Context, key string, data []byte) error
	Get(ctx context.Context, key string) ([]byte, error)
	Delete(ctx context.Context, key string) error
	List(ctx context.Context, prefix string) ([]string, error)
	Close() error
}

// MinIOConfig cấu hình kết nối tới MinIO / S3.
type MinIOConfig struct {
	Endpoint  string // e.g. "http://localhost:9000"
	Bucket    string // e.g. "strand-staging"
	AccessKey string // e.g. "minioadmin"
	SecretKey string // e.g. "minioadmin"
	Region    string // default "us-east-1"
}

// MinIOBackend cài đặt BucketBackend cho MinIO qua HTTP REST và AWS SigV4.
type MinIOBackend struct {
	cfg        MinIOConfig
	httpClient *http.Client
	mu         sync.Mutex
	ensured    bool
}

// NewMinIOBackend tạo một MinIOBackend mới.
func NewMinIOBackend(cfg MinIOConfig) (*MinIOBackend, error) {
	if cfg.Endpoint == "" {
		cfg.Endpoint = "http://localhost:9000"
	}
	if cfg.Bucket == "" {
		cfg.Bucket = "strand-staging"
	}
	if cfg.Region == "" {
		cfg.Region = "us-east-1"
	}
	// Xóa trailing slash nếu có
	cfg.Endpoint = strings.TrimRight(cfg.Endpoint, "/")

	return &MinIOBackend{
		cfg: cfg,
		httpClient: &http.Client{
			Timeout: 30 * time.Second,
		},
	}, nil
}

// EnsureBucket đảm bảo bucket đã tồn tại trên MinIO.
func (b *MinIOBackend) EnsureBucket(ctx context.Context) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.ensured {
		return nil
	}

	targetURL := fmt.Sprintf("%s/%s", b.cfg.Endpoint, b.cfg.Bucket)
	req, err := http.NewRequestWithContext(ctx, http.MethodHead, targetURL, nil)
	if err != nil {
		return fmt.Errorf("create HEAD request failed: %w", err)
	}
	signSigV4(req, nil, b.cfg.AccessKey, b.cfg.SecretKey, b.cfg.Region, "s3", time.Now().UTC())

	resp, err := b.httpClient.Do(req)
	if err == nil {
		_ = resp.Body.Close()
		if resp.StatusCode == http.StatusOK {
			b.ensured = true
			return nil
		}
	}

	// Nếu bucket chưa có (404), tạo bucket bằng PUT
	putReq, err := http.NewRequestWithContext(ctx, http.MethodPut, targetURL, nil)
	if err != nil {
		return fmt.Errorf("create PUT bucket request failed: %w", err)
	}
	signSigV4(putReq, nil, b.cfg.AccessKey, b.cfg.SecretKey, b.cfg.Region, "s3", time.Now().UTC())

	putResp, err := b.httpClient.Do(putReq)
	if err != nil {
		return fmt.Errorf("ensure bucket PUT failed: %w", err)
	}
	defer putResp.Body.Close()

	if putResp.StatusCode != http.StatusOK && putResp.StatusCode != http.StatusConflict {
		body, _ := io.ReadAll(putResp.Body)
		return fmt.Errorf("failed to create bucket %s, status=%d, body=%s", b.cfg.Bucket, putResp.StatusCode, string(body))
	}

	b.ensured = true
	return nil
}

// Put ghi đối tượng lên MinIO.
func (b *MinIOBackend) Put(ctx context.Context, key string, data []byte) error {
	if err := b.EnsureBucket(ctx); err != nil {
		return err
	}

	cleanKey := strings.TrimPrefix(key, "/")
	targetURL := fmt.Sprintf("%s/%s/%s", b.cfg.Endpoint, b.cfg.Bucket, cleanKey)

	req, err := http.NewRequestWithContext(ctx, http.MethodPut, targetURL, bytes.NewReader(data))
	if err != nil {
		return fmt.Errorf("create PUT request failed: %w", err)
	}
	req.Header.Set("Content-Type", "application/octet-stream")
	signSigV4(req, data, b.cfg.AccessKey, b.cfg.SecretKey, b.cfg.Region, "s3", time.Now().UTC())

	resp, err := b.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("MinIO PUT failed: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusCreated && resp.StatusCode != http.StatusNoContent {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("MinIO PUT %s failed with status %d: %s", key, resp.StatusCode, string(body))
	}
	return nil
}

// Get đọc đối tượng từ MinIO.
func (b *MinIOBackend) Get(ctx context.Context, key string) ([]byte, error) {
	cleanKey := strings.TrimPrefix(key, "/")
	targetURL := fmt.Sprintf("%s/%s/%s", b.cfg.Endpoint, b.cfg.Bucket, cleanKey)

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, targetURL, nil)
	if err != nil {
		return nil, fmt.Errorf("create GET request failed: %w", err)
	}
	signSigV4(req, nil, b.cfg.AccessKey, b.cfg.SecretKey, b.cfg.Region, "s3", time.Now().UTC())

	resp, err := b.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("MinIO GET failed: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusNotFound {
		return nil, errors.New("object not found")
	}
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("MinIO GET %s failed with status %d: %s", key, resp.StatusCode, string(body))
	}

	return io.ReadAll(resp.Body)
}

// Delete xóa đối tượng khỏi MinIO.
func (b *MinIOBackend) Delete(ctx context.Context, key string) error {
	cleanKey := strings.TrimPrefix(key, "/")
	targetURL := fmt.Sprintf("%s/%s/%s", b.cfg.Endpoint, b.cfg.Bucket, cleanKey)

	req, err := http.NewRequestWithContext(ctx, http.MethodDelete, targetURL, nil)
	if err != nil {
		return fmt.Errorf("create DELETE request failed: %w", err)
	}
	signSigV4(req, nil, b.cfg.AccessKey, b.cfg.SecretKey, b.cfg.Region, "s3", time.Now().UTC())

	resp, err := b.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("MinIO DELETE failed: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusNoContent && resp.StatusCode != http.StatusNotFound {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("MinIO DELETE %s failed with status %d: %s", key, resp.StatusCode, string(body))
	}
	return nil
}

type listBucketResult struct {
	Contents []struct {
		Key string `xml:"Key"`
	} `xml:"Contents"`
}

// List liệt kê các đối tượng theo tiền tố prefix.
func (b *MinIOBackend) List(ctx context.Context, prefix string) ([]string, error) {
	targetURL := fmt.Sprintf("%s/%s?prefix=%s", b.cfg.Endpoint, b.cfg.Bucket, url.QueryEscape(prefix))

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, targetURL, nil)
	if err != nil {
		return nil, fmt.Errorf("create LIST request failed: %w", err)
	}
	signSigV4(req, nil, b.cfg.AccessKey, b.cfg.SecretKey, b.cfg.Region, "s3", time.Now().UTC())

	resp, err := b.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("MinIO LIST failed: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("MinIO LIST failed with status %d: %s", resp.StatusCode, string(body))
	}

	data, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}

	var res listBucketResult
	if err := xml.Unmarshal(data, &res); err != nil {
		return nil, fmt.Errorf("parse XML LIST response failed: %w", err)
	}

	keys := make([]string, 0, len(res.Contents))
	for _, c := range res.Contents {
		keys = append(keys, c.Key)
	}
	sort.Strings(keys)
	return keys, nil
}

// Close đóng backend.
func (b *MinIOBackend) Close() error {
	b.httpClient.CloseIdleConnections()
	return nil
}

// MemoryBucketBackend là in-memory backend cho kiểm thử đơn vị.
type MemoryBucketBackend struct {
	mu      sync.RWMutex
	objects map[string][]byte
}

func NewMemoryBucketBackend() *MemoryBucketBackend {
	return &MemoryBucketBackend{
		objects: make(map[string][]byte),
	}
}

func (m *MemoryBucketBackend) Put(ctx context.Context, key string, data []byte) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	cp := make([]byte, len(data))
	copy(cp, data)
	m.objects[key] = cp
	return nil
}

func (m *MemoryBucketBackend) Get(ctx context.Context, key string) ([]byte, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	data, ok := m.objects[key]
	if !ok {
		return nil, errors.New("object not found")
	}
	cp := make([]byte, len(data))
	copy(cp, data)
	return cp, nil
}

func (m *MemoryBucketBackend) Delete(ctx context.Context, key string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.objects, key)
	return nil
}

func (m *MemoryBucketBackend) List(ctx context.Context, prefix string) ([]string, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	var keys []string
	for k := range m.objects {
		if strings.HasPrefix(k, prefix) {
			keys = append(keys, k)
		}
	}
	sort.Strings(keys)
	return keys, nil
}

func (m *MemoryBucketBackend) Close() error {
	return nil
}

// signSigV4 ký yêu cầu HTTP theo AWS Signature Version 4.
func signSigV4(req *http.Request, payload []byte, accessKey, secretKey, region, service string, now time.Time) {
	if accessKey == "" || secretKey == "" {
		return
	}

	dateStr := now.Format("20060102")
	amzDate := now.Format("20060102T150405Z")

	payloadHash := sha256.Sum256(payload)
	payloadHashHex := hex.EncodeToString(payloadHash[:])

	req.Header.Set("x-amz-date", amzDate)
	req.Header.Set("x-amz-content-sha256", payloadHashHex)
	req.Header.Set("Host", req.URL.Host)

	// Canonical headers
	signedHeaders := "host;x-amz-content-sha256;x-amz-date"
	canonicalHeaders := fmt.Sprintf("host:%s\nx-amz-content-sha256:%s\nx-amz-date:%s\n",
		req.URL.Host, payloadHashHex, amzDate)

	canonicalURI := req.URL.Path
	if canonicalURI == "" {
		canonicalURI = "/"
	}

	canonicalQuery := req.URL.RawQuery

	canonicalRequest := fmt.Sprintf("%s\n%s\n%s\n%s\n%s\n%s",
		req.Method,
		canonicalURI,
		canonicalQuery,
		canonicalHeaders,
		signedHeaders,
		payloadHashHex,
	)

	reqHash := sha256.Sum256([]byte(canonicalRequest))
	reqHashHex := hex.EncodeToString(reqHash[:])

	credentialScope := fmt.Sprintf("%s/%s/%s/aws4_request", dateStr, region, service)
	stringToSign := fmt.Sprintf("AWS4-HMAC-SHA256\n%s\n%s\n%s",
		amzDate,
		credentialScope,
		reqHashHex,
	)

	// Tính signing key
	kDate := hmacSHA256([]byte("AWS4"+secretKey), []byte(dateStr))
	kRegion := hmacSHA256(kDate, []byte(region))
	kService := hmacSHA256(kRegion, []byte(service))
	kSigning := hmacSHA256(kService, []byte("aws4_request"))

	signature := hex.EncodeToString(hmacSHA256(kSigning, []byte(stringToSign)))

	authHeader := fmt.Sprintf("AWS4-HMAC-SHA256 Credential=%s/%s, SignedHeaders=%s, Signature=%s",
		accessKey, credentialScope, signedHeaders, signature)
	req.Header.Set("Authorization", authHeader)
}

func hmacSHA256(key, data []byte) []byte {
	h := hmac.New(sha256.New, key)
	h.Write(data)
	return h.Sum(nil)
}
