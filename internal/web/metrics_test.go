package web

import (
	"net"
	"net/http"
	"testing"
)

// newMetricsServer chạy một /metrics giả, trả địa chỉ host:port.
func newMetricsServer(t *testing.T, body func() string) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	mux := http.NewServeMux()
	mux.HandleFunc("/metrics", func(w http.ResponseWriter, r *http.Request) { _, _ = w.Write([]byte(body())) })
	srv := &http.Server{Handler: mux}
	go srv.Serve(ln)
	t.Cleanup(func() { srv.Close() })
	return ln.Addr().String()
}
