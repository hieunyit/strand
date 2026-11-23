package testutil_test

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/xml"
	"fmt"
	"io"
	"net"
	"net/http"
	"testing"
	"time"

	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"

	"strand/internal/testutil"
)

type s3ErrorResponse struct {
	XMLName   xml.Name `xml:"Error"`
	Code      string   `xml:"Code"`
	Message   string   `xml:"Message"`
	Resource  string   `xml:"Resource"`
	RequestID string   `xml:"RequestId"`
}

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

	signedHeaders := "host;x-amz-content-sha256;x-amz-date"
	canonicalHeaders := fmt.Sprintf("host:%s\nx-amz-content-sha256:%s\nx-amz-date:%s\n",
		req.URL.Host, payloadHashHex, amzDate)

	canonicalURI := req.URL.Path
	if canonicalURI == "" {
		canonicalURI = "/"
	}
	canonicalQuery := req.URL.RawQuery

	canonicalRequest := fmt.Sprintf("%s\n%s\n%s\n%s\n%s\n%s",
		req.Method, canonicalURI, canonicalQuery, canonicalHeaders, signedHeaders, payloadHashHex)

	canonicalRequestHash := sha256.Sum256([]byte(canonicalRequest))
	canonicalRequestHashHex := hex.EncodeToString(canonicalRequestHash[:])

	credentialScope := fmt.Sprintf("%s/%s/%s/aws4_request", dateStr, region, service)
	stringToSign := fmt.Sprintf("AWS4-HMAC-SHA256\n%s\n%s\n%s",
		amzDate, credentialScope, canonicalRequestHashHex)

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

// TestMinIO_PostReadyProbe kiểm tra và ghi nhận hành vi S3 của MinIO ngay sau khi /minio/health/ready trả 200.
// Kiểm tra hiện tượng MinIO 503 sau ready: capture status, S3 code, request-id, và timing.
func TestMinIO_PostReadyProbe(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()

	t.Logf("pinned MinIO image: %s", testutil.PinnedMinIOImage)

	req := testcontainers.GenericContainerRequest{
		ContainerRequest: testcontainers.ContainerRequest{
			Image:        testutil.PinnedMinIOImage,
			Entrypoint:   []string{testutil.MinIOEntrypoint},
			ExposedPorts: []string{"9000/tcp"},
			Env: map[string]string{
				"MINIO_ROOT_USER":     "minioadmin",
				"MINIO_ROOT_PASSWORD": "minioadmin",
			},
			Cmd: []string{"server", "/tmp/minio-data"},
			WaitingFor: wait.ForHTTP("/minio/health/ready").
				WithPort("9000/tcp").
				WithStartupTimeout(90 * time.Second),
		},
		Started: true,
	}

	startT := time.Now()
	c, err := testcontainers.GenericContainer(ctx, req)
	if err != nil {
		t.Fatalf("failed to start container: %v", err)
	}
	defer func() {
		termCtx, termCancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer termCancel()
		if termErr := c.Terminate(termCtx); termErr != nil {
			t.Logf("terminate error: %v", termErr)
		}
	}()

	readyT := time.Now()
	t.Logf("container ready after %v from start", readyT.Sub(startT))

	host, err := c.Host(ctx)
	if err != nil {
		t.Fatalf("get host: %v", err)
	}
	port, err := c.MappedPort(ctx, "9000/tcp")
	if err != nil {
		t.Fatalf("get mapped port: %v", err)
	}
	endpoint := "http://" + net.JoinHostPort(host, port.Port())
	t.Logf("endpoint: %s", endpoint)

	client := &http.Client{Timeout: 5 * time.Second}

	// Chạy vòng probe lặp lại: create bucket -> put -> get -> delete object -> delete bucket
	type probeResult struct {
		op        string
		iteration int
		elapsed   time.Duration
		status    int
		s3Code    string
		requestID string
		err       error
	}

	var results []probeResult
	var status503Count int

	for i := 1; i <= 25; i++ {
		bucket := fmt.Sprintf("probe-bucket-%d", i)
		bucketURL := fmt.Sprintf("%s/%s", endpoint, bucket)

		// 1. Create bucket (PUT /bucket)
		opStart := time.Now()
		reqCreate, _ := http.NewRequestWithContext(ctx, http.MethodPut, bucketURL, nil)
		signSigV4(reqCreate, nil, "minioadmin", "minioadmin", "us-east-1", "s3", time.Now().UTC())
		respCreate, err := client.Do(reqCreate)
		elapsed := time.Since(readyT)

		res := probeResult{op: "CreateBucket", iteration: i, elapsed: elapsed, err: err}
		if respCreate != nil {
			res.status = respCreate.StatusCode
			res.requestID = respCreate.Header.Get("x-amz-request-id")
			if respCreate.StatusCode >= 300 {
				body, _ := io.ReadAll(respCreate.Body)
				var s3Err s3ErrorResponse
				_ = xml.Unmarshal(body, &s3Err)
				res.s3Code = s3Err.Code
				if res.s3Code == "" {
					res.s3Code = string(body)
				}
			}
			respCreate.Body.Close()
		}
		if res.status == http.StatusServiceUnavailable {
			status503Count++
		}
		results = append(results, res)

		if res.status != http.StatusOK && res.status != http.StatusConflict {
			t.Logf("[ITER %d] CreateBucket non-200: status=%d s3Code=%s reqID=%s err=%v (elapsed=%v)",
				i, res.status, res.s3Code, res.requestID, res.err, elapsed)
			continue
		}

		// 2. PutObject (PUT /bucket/obj)
		data := []byte(fmt.Sprintf("probe payload %d", i))
		objURL := fmt.Sprintf("%s/%s/obj-%d", endpoint, bucket, i)
		reqPut, _ := http.NewRequestWithContext(ctx, http.MethodPut, objURL, bytes.NewReader(data))
		reqPut.Header.Set("Content-Type", "application/octet-stream")
		signSigV4(reqPut, data, "minioadmin", "minioadmin", "us-east-1", "s3", time.Now().UTC())
		respPut, err := client.Do(reqPut)
		elapsedPut := time.Since(readyT)

		resPut := probeResult{op: "PutObject", iteration: i, elapsed: elapsedPut, err: err}
		if respPut != nil {
			resPut.status = respPut.StatusCode
			resPut.requestID = respPut.Header.Get("x-amz-request-id")
			if respPut.StatusCode >= 300 {
				body, _ := io.ReadAll(respPut.Body)
				var s3Err s3ErrorResponse
				_ = xml.Unmarshal(body, &s3Err)
				resPut.s3Code = s3Err.Code
			}
			respPut.Body.Close()
		}
		if resPut.status == http.StatusServiceUnavailable {
			status503Count++
		}
		results = append(results, resPut)

		// 3. GetObject (GET /bucket/obj)
		reqGet, _ := http.NewRequestWithContext(ctx, http.MethodGet, objURL, nil)
		signSigV4(reqGet, nil, "minioadmin", "minioadmin", "us-east-1", "s3", time.Now().UTC())
		respGet, err := client.Do(reqGet)
		elapsedGet := time.Since(readyT)

		resGet := probeResult{op: "GetObject", iteration: i, elapsed: elapsedGet, err: err}
		if respGet != nil {
			resGet.status = respGet.StatusCode
			resGet.requestID = respGet.Header.Get("x-amz-request-id")
			if respGet.StatusCode >= 300 {
				body, _ := io.ReadAll(respGet.Body)
				var s3Err s3ErrorResponse
				_ = xml.Unmarshal(body, &s3Err)
				resGet.s3Code = s3Err.Code
			}
			respGet.Body.Close()
		}
		if resGet.status == http.StatusServiceUnavailable {
			status503Count++
		}
		results = append(results, resGet)

		// 4. DeleteObject (DELETE /bucket/obj)
		reqDel, _ := http.NewRequestWithContext(ctx, http.MethodDelete, objURL, nil)
		signSigV4(reqDel, nil, "minioadmin", "minioadmin", "us-east-1", "s3", time.Now().UTC())
		respDel, err := client.Do(reqDel)
		elapsedDel := time.Since(readyT)

		resDel := probeResult{op: "DeleteObject", iteration: i, elapsed: elapsedDel, err: err}
		if respDel != nil {
			resDel.status = respDel.StatusCode
			resDel.requestID = respDel.Header.Get("x-amz-request-id")
			respDel.Body.Close()
		}
		if resDel.status == http.StatusServiceUnavailable {
			status503Count++
		}
		results = append(results, resDel)

		// 5. DeleteBucket (DELETE /bucket)
		reqDelB, _ := http.NewRequestWithContext(ctx, http.MethodDelete, bucketURL, nil)
		signSigV4(reqDelB, nil, "minioadmin", "minioadmin", "us-east-1", "s3", time.Now().UTC())
		respDelB, err := client.Do(reqDelB)
		elapsedDelB := time.Since(readyT)

		resDelB := probeResult{op: "DeleteBucket", iteration: i, elapsed: elapsedDelB, err: err}
		if respDelB != nil {
			resDelB.status = respDelB.StatusCode
			resDelB.requestID = respDelB.Header.Get("x-amz-request-id")
			respDelB.Body.Close()
		}
		if resDelB.status == http.StatusServiceUnavailable {
			status503Count++
		}
		results = append(results, resDelB)

		_ = opStart
	}

	t.Logf("Probe completed: total operations = %d, 503 status count = %d", len(results), status503Count)
	for idx, r := range results {
		if r.status != http.StatusOK && r.status != http.StatusNoContent {
			t.Logf("probe[%d]: %s status=%d s3Code=%s reqID=%s err=%v elapsed=%v",
				idx, r.op, r.status, r.s3Code, r.requestID, r.err, r.elapsed)
		}
	}
}

// TestMinIO_PreAndPostReadyCapture ghi nhận status, S3 code, request-id, và body khi MinIO đang khởi động
// để kiểm tra nguồn gốc của lỗi 503 Service Unavailable.
func TestMinIO_PreAndPostReadyCapture(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	// Khởi động container KHÔNG dùng ForHTTP để có thể gửi request ngay từ millisecond đầu tiên
	req := testcontainers.GenericContainerRequest{
		ContainerRequest: testcontainers.ContainerRequest{
			Image:        testutil.PinnedMinIOImage,
			Entrypoint:   []string{testutil.MinIOEntrypoint},
			ExposedPorts: []string{"9000/tcp"},
			Env: map[string]string{
				"MINIO_ROOT_USER":     "minioadmin",
				"MINIO_ROOT_PASSWORD": "minioadmin",
			},
			Cmd: []string{"server", "/tmp/minio-data"},
		},
		Started: true,
	}

	startT := time.Now()
	c, err := testcontainers.GenericContainer(ctx, req)
	if err != nil {
		t.Fatalf("start container: %v", err)
	}
	defer func() {
		termCtx, termCancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer termCancel()
		_ = c.Terminate(termCtx)
	}()

	host, err := c.Host(ctx)
	if err != nil {
		t.Fatalf("get host: %v", err)
	}
	port, err := c.MappedPort(ctx, "9000/tcp")
	if err != nil {
		t.Fatalf("get port: %v", err)
	}
	endpoint := "http://" + net.JoinHostPort(host, port.Port())
	t.Logf("endpoint: %s", endpoint)

	client := &http.Client{Timeout: 500 * time.Millisecond}

	// Bắn request liên tục vào cả /minio/health/ready và S3 API (/probe-bucket)
	type sample struct {
		target    string
		elapsed   time.Duration
		status    int
		s3Code    string
		requestID string
		body      string
		err       error
	}

	var samples []sample
	readyReached := false
	var readyElapsed time.Duration

	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		elapsed := time.Since(startT)

		// 1. Check health/ready
		respHealth, errHealth := client.Get(endpoint + "/minio/health/ready")
		sHealth := sample{target: "/minio/health/ready", elapsed: elapsed, err: errHealth}
		if respHealth != nil {
			sHealth.status = respHealth.StatusCode
			sHealth.requestID = respHealth.Header.Get("x-amz-request-id")
			b, _ := io.ReadAll(respHealth.Body)
			sHealth.body = string(b)
			respHealth.Body.Close()
			if sHealth.status == http.StatusOK && !readyReached {
				readyReached = true
				readyElapsed = elapsed
				t.Logf("health/ready FIRST returned 200 at elapsed %v", readyElapsed)
			}
		}
		samples = append(samples, sHealth)

		// 2. Check S3 API directly (HEAD /probe-bucket)
		reqS3, _ := http.NewRequestWithContext(ctx, http.MethodHead, endpoint+"/probe-bucket", nil)
		signSigV4(reqS3, nil, "minioadmin", "minioadmin", "us-east-1", "s3", time.Now().UTC())
		respS3, errS3 := client.Do(reqS3)
		sS3 := sample{target: "HEAD /probe-bucket", elapsed: elapsed, err: errS3}
		if respS3 != nil {
			sS3.status = respS3.StatusCode
			sS3.requestID = respS3.Header.Get("x-amz-request-id")
			respS3.Body.Close()
		}
		samples = append(samples, sS3)

		if readyReached && time.Since(startT) > readyElapsed+500*time.Millisecond {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}

	t.Logf("Captured %d samples. readyReached=%v at %v", len(samples), readyReached, readyElapsed)
	for _, s := range samples {
		if s.status == http.StatusServiceUnavailable {
			t.Logf("[503 OBSERVED] target=%s elapsed=%v status=%d reqID=%s body=%q err=%v",
				s.target, s.elapsed, s.status, s.requestID, s.body, s.err)
		} else if s.status != 0 && s.status != http.StatusOK && s.status != http.StatusNotFound {
			t.Logf("[NON-200] target=%s elapsed=%v status=%d reqID=%s body=%q err=%v",
				s.target, s.elapsed, s.status, s.requestID, s.body, s.err)
		}
	}
}
