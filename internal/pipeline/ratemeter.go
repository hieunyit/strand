package pipeline

import (
	"sync"
	"time"
)

// rateMeter đổi bộ đếm cộng dồn (appliedRows) thành tốc độ trong cửa sổ gần nhất cho `strand top`
// (review R18: trước đây trả thẳng tổng row từ lúc khởi động dưới nhãn rows/s). sample được gọi định
// kỳ bởi monitor; rate trả tốc độ giữa hai mẫu gần nhất (0 khi chưa đủ hai mẫu).
type rateMeter struct {
	mu         sync.Mutex
	prevN, n   int64
	prevT, t   time.Time
	haveTwo    bool
	haveSample bool
}

func (m *rateMeter) sample(total int64, now time.Time) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.haveSample {
		m.prevN, m.prevT, m.haveTwo = m.n, m.t, true
	}
	m.n, m.t, m.haveSample = total, now, true
}

func (m *rateMeter) rate() float64 {
	m.mu.Lock()
	defer m.mu.Unlock()
	if !m.haveTwo {
		return 0
	}
	dt := m.t.Sub(m.prevT).Seconds()
	if dt <= 0 || m.n < m.prevN {
		return 0
	}
	return float64(m.n-m.prevN) / dt
}
