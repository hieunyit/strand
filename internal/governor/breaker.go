package governor

import (
	"fmt"
	"sync"
	"time"
)

// BreakerConfig cấu hình circuit breaker theo spec/02 §7, §10.
type BreakerConfig struct {
	MaxRatio      float64       // Tỉ lệ p99 query latency so với baseline (mặc định 3.0)
	MaxReplicaLag time.Duration // Độ trễ replica tối đa cho phép trước khi ngắt (ví dụ 10s)
	CooldownTicks int           // Số tick liên tục dưới ngưỡng để đóng lại breaker (mặc định 60s)
}

// Breaker cài đặt cơ chế Circuit Breaker bảo vệ cơ sở dữ liệu.
// spec: 02 §7:
// "Kích hoạt khi tín hiệu đọc trực tiếp từ DB vượt ngưỡng: p99 latency query của chính Strand so với baseline,
// độ trễ commit, hoặc replica lag... Khi đó R giảm ngay về 50% và background dừng."
type Breaker struct {
	mu                     sync.RWMutex
	cfg                    BreakerConfig
	isOpen                 bool
	consecutiveNormalTicks int
	lastReason             string
}

// NewBreaker khởi tạo Breaker mới.
func NewBreaker(cfg BreakerConfig) *Breaker {
	if cfg.MaxRatio <= 0 {
		cfg.MaxRatio = 3.0
	}
	if cfg.CooldownTicks <= 0 {
		cfg.CooldownTicks = 60
	}
	return &Breaker{
		cfg: cfg,
	}
}

// Evaluate đánh giá các tín hiệu DB trực tiếp và cập nhật trạng thái breaker.
func (b *Breaker) Evaluate(sig DBSignals) (isOpen bool, reason string) {
	b.mu.Lock()
	defer b.mu.Unlock()

	triggered := false
	var triggerReason string

	// 1. Kiểm tra p99 latency query của Strand so với baseline
	if sig.QueryBaseline > 0 && sig.QueryP99 > 0 {
		ratio := float64(sig.QueryP99) / float64(sig.QueryBaseline)
		if ratio > b.cfg.MaxRatio {
			triggered = true
			triggerReason = fmt.Sprintf("strand query p99 latency ratio (%.2f) exceeds max_ratio (%.2f)", ratio, b.cfg.MaxRatio)
		}
	}

	// 2. Kiểm tra replica lag
	if b.cfg.MaxReplicaLag > 0 && sig.ReplicaLagSeconds > b.cfg.MaxReplicaLag.Seconds() {
		triggered = true
		triggerReason = fmt.Sprintf("replica lag (%.2fs) exceeds max threshold (%.2fs)", sig.ReplicaLagSeconds, b.cfg.MaxReplicaLag.Seconds())
	}

	// 3. Tín hiệu cứng (Hard signal)
	if sig.HardSignal {
		triggered = true
		triggerReason = "hard critical DB signal triggered"
	}

	if triggered {
		b.isOpen = true
		b.consecutiveNormalTicks = 0
		b.lastReason = triggerReason
		return true, triggerReason
	}

	// Nếu đang mở nhưng tín hiệu hiện tại đã bình thường: đếm số tick cooldown
	if b.isOpen {
		b.consecutiveNormalTicks++
		if b.consecutiveNormalTicks >= b.cfg.CooldownTicks {
			// Đã qua thời gian cooldown liên tục dưới ngưỡng -> đóng lại breaker
			b.isOpen = false
			b.consecutiveNormalTicks = 0
			b.lastReason = ""
			return false, ""
		}
		return true, b.lastReason
	}

	return false, ""
}

// Reason: lý do lần mở gần nhất khi đang mở ("" khi đóng).
func (b *Breaker) Reason() string {
	b.mu.RLock()
	defer b.mu.RUnlock()
	if !b.isOpen {
		return ""
	}
	return b.lastReason
}

// IsOpen trả về trạng thái hiện tại của breaker.
func (b *Breaker) IsOpen() bool {
	b.mu.RLock()
	defer b.mu.RUnlock()
	return b.isOpen
}
