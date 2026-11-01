package metrics

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"sync"
	"time"
)

// Server phục vụ HTTP endpoint /metrics và /healthz theo spec/02 §11.
type Server struct {
	addr    string
	metrics *Metrics
	health  *HealthChecker
	logger  *slog.Logger
	srv     *http.Server
	ln      net.Listener
	mu      sync.Mutex
	closed  bool
}

// NewServer khởi tạo HTTP metrics & healthz server.
func NewServer(addr string, m *Metrics, h *HealthChecker, logger *slog.Logger) *Server {
	if addr == "" {
		addr = ":9464" // Mặc định spec/02 §11
	}
	if logger == nil {
		logger = slog.Default()
	}
	return &Server{
		addr:    addr,
		metrics: m,
		health:  h,
		logger:  logger,
	}
}

// Start khởi chạy HTTP server.
func (s *Server) Start() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	ln, err := net.Listen("tcp", s.addr)
	if err != nil {
		return fmt.Errorf("listen on %s: %w", s.addr, err)
	}
	s.ln = ln

	mux := http.NewServeMux()
	mux.Handle("/metrics", s.metrics.Handler())
	mux.HandleFunc("/healthz", s.health.Handler())

	s.srv = &http.Server{
		Handler:      mux,
		ReadTimeout:  10 * time.Second,
		WriteTimeout: 10 * time.Second,
	}

	go func() {
		if err := s.srv.Serve(ln); err != nil && !errors.Is(err, http.ErrServerClosed) {
			s.logger.Error("metrics http server error", "error", err)
		}
	}()

	s.logger.Info("metrics and healthz server listening", "addr", ln.Addr().String())
	return nil
}

// Addr trả về địa chỉ thực tế của listener (hữu ích khi dùng :0 cho tests).
func (s *Server) Addr() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.ln != nil {
		return s.ln.Addr().String()
	}
	return s.addr
}

// Close đóng server một cách an toàn.
func (s *Server) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return nil
	}
	s.closed = true
	if s.srv != nil {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		return s.srv.Shutdown(ctx)
	}
	return nil
}
