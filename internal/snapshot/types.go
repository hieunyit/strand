package snapshot

import (
	"database/sql"
	"strconv"
	"strings"
	"time"

	"strand/internal/checkpoint"
	"strand/internal/model"
)

// spec: 02 §5, design §6.1

// RangeState biểu diễn 5 trạng thái theo một chiều của một range PK khi snapshot chạy song song với stream.
type RangeState int

const (
	// StatePending ("Chưa đọc"): Event từ stream thuộc range này -> Bỏ.
	StatePending RangeState = iota
	// StateReading ("Đang đọc"): Đọc low watermark -> SELECT chunk. Event có GTID in low -> Bỏ. Ngược lại -> buffer gộp theo PK.
	StateReading
	// StateRendezvous ("Chờ điểm hẹn"): Đã SELECT xong, đọc high watermark. Reader chờ đến khi stream xử lý hết high.
	StateRendezvous
	// StateFlushing ("Flush"): Trong một transaction: merge có thứ tự row snapshot với buffer, commit kèm checkpoint.
	StateFlushing
	// StateDone ("Xong"): Range hoàn thành, stream apply bình thường.
	StateDone
)

func (s RangeState) String() string {
	switch s {
	case StatePending:
		return "Pending"
	case StateReading:
		return "Reading"
	case StateRendezvous:
		return "Rendezvous"
	case StateFlushing:
		return "Flushing"
	case StateDone:
		return "Done"
	default:
		return "Unknown"
	}
}

// Action biểu diễn hành động cần thực hiện đối với một stream event.
type Action int

const (
	ActionDiscard Action = iota
	ActionBuffer
	ActionApply
)

func (a Action) String() string {
	switch a {
	case ActionDiscard:
		return "Discard"
	case ActionBuffer:
		return "Buffer"
	case ActionApply:
		return "Apply"
	default:
		return "Unknown"
	}
}

// ChunkOptions cấu hình cho việc chia chunk và timeout theo spec: 02 §5.
type ChunkOptions struct {
	Timeout            time.Duration // Mặc định 5s: quá timeout thì hủy, chia đôi chunk, chạy lại.
	MaxChunkBytes      int64         // Mặc định 64MB (64 * 1024 * 1024).
	MinChunkRows       int64         // Mặc định 1: chặn vòng lặp chia chunk.
	DefaultChunkRows   int64         // Mặc định 5000: số hàng mặc định mỗi chunk.
	SleepBetweenChunks time.Duration // Throttling giữa các chunk.
}

// DefaultChunkOptions trả về cấu hình mặc định theo đặc tả spec: 02 §5 và design §6.1.
func DefaultChunkOptions() ChunkOptions {
	return ChunkOptions{
		Timeout:          5 * time.Second,
		MaxChunkBytes:    64 * 1024 * 1024,
		MinChunkRows:     1,
		DefaultChunkRows: 5000,
	}
}

// Chunk biểu diễn một đoạn khoảng khóa chính [StartPK, EndPK] cần đọc.
type Chunk struct {
	ID            int64
	Table         model.TableID
	PKCol         string
	StartPK       int64
	EndPK         int64
	IsTail        bool // Chunk đuôi [b_last, +inf)
	EstimatedRows int64

	State         RangeState
	LowWatermark  checkpoint.AppliedSet
	HighWatermark checkpoint.AppliedSet

	SnapshotRows    []*model.RowChange
	BufferedChanges map[int64]*model.RowChange // PK -> last image / tombstone
}

// RowCount trả về số lượng hàng ước tính trong chunk.
func (c *Chunk) RowCount() int64 {
	if c.IsTail {
		return 1
	}
	if c.EndPK < c.StartPK {
		return 0
	}
	return c.EndPK - c.StartPK + 1
}

// ParseMySQLGTIDSet chuyển chuỗi GTID executed (có thể gồm nhiều uuid cách nhau bởi dấu phẩy) thành MySQLAppliedSet.
func ParseMySQLGTIDSet(gtidStr string) *checkpoint.MySQLAppliedSet {
	set := checkpoint.NewMySQLAppliedSet()
	gtidStr = strings.ReplaceAll(gtidStr, "\n", "")
	for _, part := range strings.Split(gtidStr, ",") {
		part = strings.TrimSpace(part)
		if part != "" {
			set.ParseAndAddRange(part)
		}
	}
	return set
}

// GetGlobalGTIDExecuted truy vấn @@GLOBAL.gtid_executed từ MySQL DB.
func GetGlobalGTIDExecuted(db *sql.DB) (string, error) {
	var gtid string
	err := db.QueryRow("SELECT @@GLOBAL.gtid_executed").Scan(&gtid)
	if err != nil {
		return "", err
	}
	return gtid, nil
}

// LatestPos trích xuất vị trí Pos cao nhất từ chuỗi GTID executed.
func LatestPos(gtidStr string) model.Pos {
	set := ParseMySQLGTIDSet(gtidStr)
	return set.High()
}

// ParseGNO trích xuất số thứ tự GNO từ một model.Pos của MySQL.
func ParseGNO(p model.Pos) uint64 {
	if p.Engine != model.EngineMySQL || p.GTID == "" {
		return 0
	}
	parts := strings.Split(p.GTID, ":")
	if len(parts) == 0 {
		return 0
	}
	gno, err := strconv.ParseUint(parts[len(parts)-1], 10, 64)
	if err != nil {
		return 0
	}
	return gno
}

// StreamHasReachedHigh kiểm tra xem vị trí stream hiện tại đã đạt hoặc vượt qua mốc high watermark hay chưa (Rendezvous).
func StreamHasReachedHigh(streamPos model.Pos, high checkpoint.AppliedSet) bool {
	if high == nil {
		return true
	}
	highPos := high.High()
	highGNO := ParseGNO(highPos)
	streamGNO := ParseGNO(streamPos)
	return streamGNO >= highGNO
}

