package governor

import (
	"context"
	"time"
)

// Zone định nghĩa vùng áp lực của Governor theo spec/02 §7.
type Zone int

const (
	ZoneGreen  Zone = 0 // Vùng an toàn
	ZoneYellow Zone = 1 // Vùng cảnh báo, tiệm cận ngưỡng
	ZoneRed    Zone = 2 // Vùng quá tải, áp suất P >= 1.0 hoặc breaker mở
)

func (z Zone) String() string {
	switch z {
	case ZoneGreen:
		return "Green"
	case ZoneYellow:
		return "Yellow"
	case ZoneRed:
		return "Red"
	default:
		return "Unknown"
	}
}

// DBSignals chứa các chỉ số sức khỏe đọc trực tiếp từ cơ sở dữ liệu.
// spec: 02 §7: không phụ thuộc metrics_source bên ngoài.
type DBSignals struct {
	ReplicaLagSeconds float64       // Độ trễ replica (ví dụ Seconds_Behind_Source trên MySQL)
	CommitLagSeconds  float64       // Độ trễ commit
	QueryP99          time.Duration // Latency p99 của truy vấn Strand
	QueryBaseline     time.Duration // Latency baseline ban đầu
	HardSignal        bool          // Tín hiệu khẩn cấp tác dụng ngay (ví dụ disk critical / out of space)
}

// DBSignalReader là interface thu thập tín hiệu trực tiếp từ DB.
type DBSignalReader interface {
	ReadSignals(ctx context.Context) (DBSignals, error)
}

// Governor là interface chung cho các bộ điều tiết GRACE v2, onoff và static theo spec/02 §3, §7.
type Governor interface {
	// Tick chu kỳ điều tiết (governor.tick, mặc định 1s)
	Tick(ctx context.Context) error

	// CurrentRate trả về tốc độ R (tokens/s) cho foreground và background
	CurrentRate() (foregroundRate float64, backgroundRate float64)

	// Zone trả về vùng áp lực hiện tại
	Zone() Zone

	// Pressure trả về giá trị áp suất P hiện tại (0.0 -> >= 1.0)
	Pressure() float64

	// BreakerOpen trả về trạng thái của circuit breaker
	BreakerOpen() bool

	// RecordQueryLatency ghi nhận thời gian thực thi truy vấn của Strand
	RecordQueryLatency(d time.Duration)
}
