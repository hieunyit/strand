package verify

import (
	"context"
	"time"
)

// Mode phân loại chế độ đối chiếu dữ liệu theo spec/design.md §7.
type Mode string

const (
	ModeDelta  Mode = "delta"  // Chỉ kiểm tra các range PK vừa xuất hiện trong luồng CDC
	ModeSample Mode = "sample" // Chọn ngẫu nhiên một tỉ lệ range PK để đối chiếu
	ModeFull   Mode = "full"   // Đối chiếu toàn bộ bảng theo vòng
)

// TableConfig cấu hình kiểm tra đối chiếu cho một bảng.
type TableConfig struct {
	Schema       string
	Table        string
	PKColumns    []string
	ValueColumns []string
	SampleRate   float64  // 0.0 -> 1.0 (ví dụ 0.1 là 10% số range)
	ChunkSize    int      // Số dòng trong mỗi chunk/range để tính hash
	DeltaRanges  [][2]any // Khoảng PK [min, max] được chỉ định khi chạy ModeDelta
}

// Mismatch ghi nhận một sai lệch dữ liệu giữa source và destination.
// spec: 02 §12
type Mismatch struct {
	Schema      string `json:"schema"`
	Table       string `json:"table"`
	PK          any    `json:"pk"`
	SourceValue any    `json:"source_value"`
	TargetValue any    `json:"target_value"`
	Reason      string `json:"reason"`
}

// HotRowReport ghi nhận một row đang nhận cập nhật quá dày không thể chốt hash tĩnh.
// spec: 02 §12 (unverifiable_hot)
type HotRowReport struct {
	Schema     string `json:"schema"`
	Table      string `json:"table"`
	PK         any    `json:"pk"`
	EventCount int    `json:"event_count"`
	Reason     string `json:"reason"`
}

// VerifyResult chứa kết quả tổng hợp của một phiên verify.
type VerifyResult struct {
	Mode            Mode           `json:"mode"`
	Table           string         `json:"table"`
	RangesChecked   int            `json:"ranges_checked"`
	RowsChecked     int            `json:"rows_checked"`
	Mismatches      []Mismatch     `json:"mismatches"`
	UnverifiableHot []HotRowReport `json:"unverifiable_hot"`
	Duration        time.Duration  `json:"duration"`
}

// RowFetcher là interface truy vấn dữ liệu theo range hoặc theo PK từ cơ sở dữ liệu.
type RowFetcher interface {
	FetchRange(ctx context.Context, schema, table string, pkCol string, low, high any) ([]map[string]any, error)
	FetchRowByPK(ctx context.Context, schema, table string, pkCol string, pk any) (map[string]any, error)
	GetPKRange(ctx context.Context, schema, table string, pkCol string) (min, max any, count int64, err error)
}
