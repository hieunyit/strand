package objectstore

import (
	"bytes"
	"crypto/md5"
	"encoding/hex"
	"encoding/xml"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// fakeS3 is a minimal in-memory path-style S3 used to test client behaviour that a real server cannot
// be made to exhibit on demand (transient 5xx, lost responses, "200 with an error body").
type fakeS3 struct {
	t       *testing.T
	mu      sync.Mutex
	objects map[string][]byte
	meta    map[string]http.Header
	uploads map[string]map[int][]byte // uploadID -> part -> data
	upKey   map[string]string
	nextID  int
	aborted []string

	// fail hooks: return a status (0 = proceed) for the n-th request matching op.
	failHook func(op string, n int) int
	counts   map[string]int
	inflight atomic.Int32
	maxInfl  atomic.Int32
	partWait time.Duration
	authSeen []string
}

func newFakeS3(t *testing.T) (*fakeS3, *httptest.Server) {
	f := &fakeS3{t: t, objects: map[string][]byte{}, meta: map[string]http.Header{}, uploads: map[string]map[int][]byte{}, upKey: map[string]string{}, counts: map[string]int{}}
	srv := httptest.NewServer(f)
	t.Cleanup(srv.Close)
	return f, srv
}

func (f *fakeS3) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	parts := strings.SplitN(strings.TrimPrefix(r.URL.Path, "/"), "/", 2)
	key := ""
	if len(parts) == 2 {
		key = parts[1]
	}
	op := r.Method
	switch {
	case r.Method == http.MethodPost && q.Has("uploads"):
		op = "initiate"
	case r.Method == http.MethodPut && q.Has("partNumber"):
		op = "part"
	case r.Method == http.MethodPost && q.Has("uploadId"):
		op = "complete"
	case r.Method == http.MethodDelete && q.Has("uploadId"):
		op = "abort"
	case r.Method == http.MethodGet && q.Has("uploads"):
		op = "listuploads"
	}
	body, _ := io.ReadAll(r.Body)
	f.mu.Lock()
	f.counts[op]++
	n := f.counts[op]
	f.authSeen = append(f.authSeen, r.Header.Get("Authorization"))
	hook := f.failHook
	f.mu.Unlock()
	if hook != nil {
		if code := hook(op, n); code != 0 {
			w.WriteHeader(code)
			fmt.Fprintf(w, "<Error><Code>InternalError</Code><Message>try again</Message></Error>")
			return
		}
	}
	if op == "part" {
		c := f.inflight.Add(1)
		for {
			m := f.maxInfl.Load()
			if c <= m || f.maxInfl.CompareAndSwap(m, c) {
				break
			}
		}
		time.Sleep(f.partWait)
		defer f.inflight.Add(-1)
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	switch op {
	case "initiate":
		f.nextID++
		id := "up" + strconv.Itoa(f.nextID)
		f.uploads[id] = map[int][]byte{}
		f.upKey[id] = key
		fmt.Fprintf(w, "<InitiateMultipartUploadResult><UploadId>%s</UploadId></InitiateMultipartUploadResult>", id)
	case "part":
		id := q.Get("uploadId")
		pn, _ := strconv.Atoi(q.Get("partNumber"))
		if _, ok := f.uploads[id]; !ok {
			w.WriteHeader(404)
			fmt.Fprint(w, "<Error><Code>NoSuchUpload</Code></Error>")
			return
		}
		f.uploads[id][pn] = body
		sum := md5.Sum(body)
		w.Header().Set("ETag", `"`+hex.EncodeToString(sum[:])+`"`)
	case "complete":
		id := q.Get("uploadId")
		up, ok := f.uploads[id]
		if !ok {
			w.WriteHeader(404)
			fmt.Fprint(w, "<Error><Code>NoSuchUpload</Code><Message>gone</Message></Error>")
			return
		}
		var req struct {
			Parts []struct {
				N int `xml:"PartNumber"`
			} `xml:"Part"`
		}
		if err := xml.Unmarshal(body, &req); err != nil {
			w.WriteHeader(400)
			return
		}
		var all []byte
		prev := 0
		for _, p := range req.Parts {
			if p.N <= prev {
				w.WriteHeader(400)
				fmt.Fprint(w, "<Error><Code>InvalidPartOrder</Code></Error>")
				return
			}
			prev = p.N
			all = append(all, up[p.N]...)
		}
		f.objects[key] = all
		f.meta[key] = http.Header{}
		delete(f.uploads, id)
		etag := fmt.Sprintf("abc-%d", len(req.Parts))
		fmt.Fprintf(w, "<CompleteMultipartUploadResult><ETag>&quot;%s&quot;</ETag></CompleteMultipartUploadResult>", etag)
	case "abort":
		id := q.Get("uploadId")
		delete(f.uploads, id)
		f.aborted = append(f.aborted, id)
		w.WriteHeader(204)
	case "listuploads":
		fmt.Fprint(w, "<ListMultipartUploadsResult>")
		ids := make([]string, 0)
		for id := range f.uploads {
			ids = append(ids, id)
		}
		sort.Strings(ids)
		for _, id := range ids {
			if strings.HasPrefix(f.upKey[id], q.Get("prefix")) {
				fmt.Fprintf(w, "<Upload><Key>%s</Key><UploadId>%s</UploadId></Upload>", f.upKey[id], id)
			}
		}
		fmt.Fprint(w, "<IsTruncated>false</IsTruncated></ListMultipartUploadsResult>")
	case http.MethodPut:
		f.objects[key] = body
		h := http.Header{}
		if v := r.Header.Get("X-Amz-Meta-Strand-Sha256"); v != "" {
			h.Set("X-Amz-Meta-Strand-Sha256", v)
		}
		f.meta[key] = h
		w.Header().Set("ETag", `"e"`)
	case http.MethodHead, http.MethodGet:
		data, ok := f.objects[key]
		if !ok {
			w.WriteHeader(404)
			return
		}
		for k, v := range f.meta[key] {
			w.Header()[k] = v
		}
		w.Header().Set("Content-Length", strconv.Itoa(len(data)))
		w.Header().Set("ETag", fmt.Sprintf(`"abc-%d"`, 1))
		if r.Method == http.MethodGet {
			_, _ = io.Copy(w, bytes.NewReader(data))
		}
	default:
		w.WriteHeader(405)
	}
}

func (f *fakeS3) openUploads() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.uploads)
}

func (f *fakeS3) count(op string) int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.counts[op]
}
