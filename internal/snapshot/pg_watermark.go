package snapshot

// Watermark của snapshot logic từ source PostgreSQL (02 §5, design §6.1).
//
// Ở MySQL, low watermark là gtid_executed đọc trước SELECT. Ở PG, "LSN đọc trước SELECT" KHÔNG dùng
// được làm low: commit record được ghi vào WAL (và có thể đã được logical decoding gửi đi) TRƯỚC khi
// backend gọi ProcArrayEndTransaction, nên một Txn có commit LSN ≤ pg_current_wal_lsn() đọc ngay trước
// SELECT vẫn có thể CHƯA visible với snapshot của SELECT (ngược lại, với synchronous_commit=off, Txn
// có thể visible trước khi commit record được flush/stream). Bỏ một event "vì LSN ≤ low" khi Txn chưa
// visible sẽ làm mất thay đổi đó (vi phạm RS-1/0 lệch).
//
// Vì vậy low của PG là chính snapshot MVCC: S0 = pg_current_snapshot() (xmin, xmax, xip) lấy SAU khi
// chunk chuyển sang "Đang đọc" và TRƯỚC mọi SELECT của chunk. Một Txn đã commit (mọi Txn stream gửi
// đều đã commit) có xid top-level X visible với S0 khi và chỉ khi X < xmin, hoặc X < xmax và X ∉ xip
// — đúng định nghĩa visibility của PG (subtransaction commit cùng top-level, Txn PREPARE vẫn nằm trong
// xip tới COMMIT PREPARED). pgoutput gửi X trong BEGIN / Stream Commit / Commit Prepared.
//
// Quy tắc (chứng minh trong pg_snapshot.go): event của Txn visible với S0 → bỏ (kèm ảnh cũ hơn cùng PK
// trong buffer); ngược lại → gộp theo PK. Mọi SELECT của chunk chạy sau S0 nên thấy ít nhất những gì
// S0 thấy (visibility chỉ tăng theo thời gian).

import (
	"fmt"
	"sort"
	"strconv"
	"strings"
	"sync"
)

// PGSnapshot là (xmin, xmax, xip) của pg_current_snapshot() (xid8, 64 bit có epoch).
type PGSnapshot struct {
	Xmin, Xmax uint64
	Xip        map[uint64]struct{}
}

// ParsePGSnapshot đọc dạng text "xmin:xmax:xip1,xip2,..." của pg_snapshot.
func ParsePGSnapshot(s string) (*PGSnapshot, error) {
	parts := strings.Split(strings.TrimSpace(s), ":")
	if len(parts) != 3 {
		return nil, fmt.Errorf("invalid pg_snapshot %q", s)
	}
	xmin, err := strconv.ParseUint(parts[0], 10, 64)
	if err != nil {
		return nil, fmt.Errorf("invalid pg_snapshot xmin %q: %w", s, err)
	}
	xmax, err := strconv.ParseUint(parts[1], 10, 64)
	if err != nil {
		return nil, fmt.Errorf("invalid pg_snapshot xmax %q: %w", s, err)
	}
	out := &PGSnapshot{Xmin: xmin, Xmax: xmax, Xip: map[uint64]struct{}{}}
	if parts[2] != "" {
		for _, x := range strings.Split(parts[2], ",") {
			v, err := strconv.ParseUint(x, 10, 64)
			if err != nil {
				return nil, fmt.Errorf("invalid pg_snapshot xip %q: %w", s, err)
			}
			out.Xip[v] = struct{}{}
		}
	}
	return out, nil
}

func (s *PGSnapshot) String() string {
	xs := make([]uint64, 0, len(s.Xip))
	for x := range s.Xip {
		xs = append(xs, x)
	}
	sort.Slice(xs, func(i, j int) bool { return xs[i] < xs[j] })
	strs := make([]string, len(xs))
	for i, x := range xs {
		strs[i] = strconv.FormatUint(x, 10)
	}
	return fmt.Sprintf("%d:%d:%s", s.Xmin, s.Xmax, strings.Join(strs, ","))
}

// full đổi xid 32 bit (pgoutput) sang xid8 theo epoch của snapshot: chọn giá trị có 32 bit thấp = xid
// gần xmax nhất (trong cửa sổ [xmax−2^31, xmax+2^31)). PG bảo đảm mọi xid còn có nghĩa nằm trong
// 2^31 quanh nextXid (chống wraparound), nên phép đổi này là duy nhất.
func (s *PGSnapshot) full(xid uint64) uint64 {
	if xid > 0xFFFFFFFF {
		return xid // đã là xid8
	}
	base := s.Xmax &^ 0xFFFFFFFF
	c := base | xid
	switch {
	case c >= s.Xmax+(1<<31):
		if c >= 1<<32 {
			c -= 1 << 32
		}
	case c+(1<<31) < s.Xmax:
		c += 1 << 32
	}
	return c
}

// Visible: Txn ĐÃ COMMIT có xid top-level xid có visible với snapshot không. xid = 0 (không biết) →
// false: event được gộp vào buffer, luôn an toàn (bỏ event chỉ an toàn khi chắc chắn visible).
func (s *PGSnapshot) Visible(xid uint64) bool {
	if s == nil || xid == 0 {
		return false
	}
	return s.visibleFull(s.full(xid))
}

func (s *PGSnapshot) visibleFull(x uint64) bool {
	// Cùng thứ tự với XidInMVCCSnapshot: ≥ xmax trước (snapshot export của slot có thể có xmin > xmax).
	if x >= s.Xmax {
		return false
	}
	if x < s.Xmin {
		return true
	}
	_, running := s.Xip[x]
	return !running
}

// Covers: mọi Txn đã commit visible với e cũng visible với s. Txn visible với e có xid < e.Xmax; nếu
// s.Xmax ≥ e.Xmax thì nó cũng < s.Xmax, nên chỉ còn phải chắc nó không nằm trong s.Xip.
func (s *PGSnapshot) Covers(e *PGSnapshot) bool {
	if e == nil {
		return true
	}
	if s.Xmax < e.Xmax {
		return false
	}
	for x := range s.Xip {
		if e.visibleFull(x) {
			return false
		}
	}
	return true
}

// XIDTracker giữ xid top-level của mọi Txn router đã xử lý mà CHƯA được xác nhận visible với một
// snapshot nào ở source. Txn có thể được stream trước khi visible (xem đầu file), nên trước khi đọc
// biên / SELECT chunk, bên đọc phải chờ một snapshot thấy hết các xid này: event của chúng đã bị bỏ
// (range "Chưa đọc") hoặc đã apply rồi bị ClearTarget xóa khi đăng ký bảng, nên chỉ còn lần đọc
// chunk sau đó mang chúng tới target. Visible một lần thì visible mãi, nên xid được gỡ ngay.
type XIDTracker struct {
	mu sync.Mutex
	m  map[uint32]struct{}
}

// NewXIDTracker tạo XIDTracker rỗng.
func NewXIDTracker() *XIDTracker { return &XIDTracker{m: map[uint32]struct{}{}} }

// Add ghi nhận xid của Txn vừa được router xử lý (0 = bỏ qua).
func (t *XIDTracker) Add(xid uint64) {
	if xid == 0 {
		return
	}
	t.mu.Lock()
	t.m[uint32(xid)] = struct{}{}
	t.mu.Unlock()
}

// Pending trả bản sao các xid chưa xác nhận visible.
func (t *XIDTracker) Pending() []uint32 {
	t.mu.Lock()
	defer t.mu.Unlock()
	out := make([]uint32, 0, len(t.m))
	for x := range t.m {
		out = append(out, x)
	}
	return out
}

// Prune gỡ các xid visible với s; trả số xid còn lại.
func (t *XIDTracker) Prune(s *PGSnapshot) int {
	t.mu.Lock()
	defer t.mu.Unlock()
	for x := range t.m {
		if s.Visible(uint64(x)) {
			delete(t.m, x)
		}
	}
	return len(t.m)
}

// Len trả số xid đang theo dõi.
func (t *XIDTracker) Len() int {
	t.mu.Lock()
	defer t.mu.Unlock()
	return len(t.m)
}

// AllVisible: mọi xid trong xids visible với s.
func AllVisible(s *PGSnapshot, xids []uint32) bool {
	for _, x := range xids {
		if !s.Visible(uint64(x)) {
			return false
		}
	}
	return true
}
