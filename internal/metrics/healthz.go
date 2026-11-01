package metrics

import (
	"encoding/json"
	"fmt"
	"net/http"
	"sync"
	"time"
)

// HealthChecker theo dõi tiến độ vòng lặp chính của pipeline theo spec/02 §11.
type HealthChecker struct {
	mu              sync.RWMutex
	lastProgress    time.Time
	maxStallTimeout time.Duration
	failed          bool
	failReason      string
}

// NewHealthChecker khởi tạo HealthChecker với thời gian timeout tối đa cho phép kẹt tiến độ.
func NewHealthChecker(maxStallTimeout time.Duration) *HealthChecker {
	if maxStallTimeout <= 0 {
		maxStallTimeout = 15 * time.Second
	}
	return &HealthChecker{
		lastProgress:    time.Now(),
		maxStallTimeout: maxStallTimeout,
	}
}

// RecordProgress ghi nhận vòng lặp chính vừa hoàn thành một bước xử lý (còn tiến).
func (h *HealthChecker) RecordProgress() {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.lastProgress = time.Now()
	h.failed = false
	h.failReason = ""
}

// SetFailed đánh dấu pipeline gặp lỗi nghiêm trọng không thể tiếp tục.
func (h *HealthChecker) SetFailed(reason string) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.failed = true
	h.failReason = reason
}

// IsHealthy kiểm tra xem vòng lặp chính còn tiến hay không.
func (h *HealthChecker) IsHealthy() (bool, string) {
	h.mu.RLock()
	defer h.mu.RUnlock()

	if h.failed {
		return false, fmt.Sprintf("pipeline failed: %s", h.failReason)
	}

	since := time.Since(h.lastProgress)
	if since > h.maxStallTimeout {
		return false, fmt.Sprintf("main loop stalled: no progress for %v (threshold: %v)", since.Round(time.Millisecond), h.maxStallTimeout)
	}

	return true, "ok"
}

// Handler trả về http.HandlerFunc xử lý endpoint /healthz.
func (h *HealthChecker) Handler() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		healthy, reason := h.IsHealthy()
		w.Header().Set("Content-Type", "application/json")
		if healthy {
			w.WriteHeader(http.StatusOK)
			_ = json.NewEncoder(w).Encode(map[string]interface{}{
				"status": "ok",
				"time":   time.Now().Format(time.RFC3339),
			})
			return
		}

		w.WriteHeader(http.StatusServiceUnavailable)
		_ = json.NewEncoder(w).Encode(map[string]interface{}{
			"status": "unhealthy",
			"reason": reason,
			"time":   time.Now().Format(time.RFC3339),
		})
	}
}
