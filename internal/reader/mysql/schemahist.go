package mysql

import (
	"fmt"
	"strings"

	gomysql "github.com/go-mysql-org/go-mysql/mysql"
)

// Lịch sử cấu trúc bảng nguồn cho TABLE_MAP không có metadata (MySQL 5.5–5.7).
//
// Vấn đề: information_schema chỉ cho cấu trúc HIỆN TẠI. Nếu Strand dừng trong lúc DDL chạy ở nguồn, khi
// khởi động lại binlog TRƯỚC DDL vẫn phải giải mã theo cấu trúc lúc đó. Vì vậy mỗi info đã đọc được lưu bền
// (state của pipeline) kèm khoảng hiệu lực tính theo GTID:
//
//	From : GTID set mà từ đó info đúng. Là GTID của DDL gần nhất reader đã thấy trước khi đọc info,
//	       hoặc (chưa thấy DDL nào) tập `from` lúc mở luồng — vì TABLE_MAP khớp info và không có DDL nào
//	       ở giữa nên cấu trúc không đổi trong đoạn [from, TABLE_MAP].
//	Until: GTID của DDL đầu tiên sau đó (mọi QUERY không phải BEGIN/COMMIT đều coi là DDL, thận trọng);
//	       rỗng = chưa thấy DDL nào sau From.
//
// Khi khởi động lại với tập đã áp `from'`, version dùng được khi From ⊆ from' (DDL/điểm mở đầu đã nằm
// trong phần đã áp, nên không có DDL nào của bảng chen giữa) và Until ∉ from' (DDL kết thúc chưa áp:
// binlog từ from' đến Until còn theo cấu trúc này). Until ⊆ from' nghĩa là DDL đã qua: version cũ.
//
// Thứ tự an toàn: reader ghi Until (supersede) ĐỒNG BỘ trước khi phát QUERY của DDL ra pipeline, nên
// checkpoint không thể vượt qua DDL mà state chưa ghi nhận DDL đó. Info đọc từ event vượt checkpoint
// không làm hỏng replay vì được gắn From/Until: lúc restart chỉ version có From ⊆ checkpoint mới được dùng.

// SchemaVersion là một info đã đọc cùng khoảng hiệu lực (xem trên). JSON, lưu trong state pipeline.
type SchemaVersion struct {
	Info  *TableInfo `json:"info"`
	From  string     `json:"from,omitempty"`
	Until string     `json:"until,omitempty"`
}

// SchemaHistoryStore là nơi lưu bền lịch sử cấu trúc (key "schema.table"). Save nhận bản đầy đủ và phải
// bền trước khi trả về; implementation tự sao chép nếu giữ lại.
type SchemaHistoryStore interface {
	LoadSchemaHistory() map[string][]SchemaVersion
	SaveSchemaHistory(map[string][]SchemaVersion) error
}

// maxSchemaVersions: số version tối đa giữ cho mỗi bảng (phòng state phình khi nhiều DDL giữa hai lần
// khởi động; bỏ version cũ nhất chỉ làm rơi về information_schema như trước, vẫn có kiểm tra khớp).
const maxSchemaVersions = 4

// gtidIn: gtid (một GTID hoặc một tập dạng chuỗi) ⊆ set. Chuỗi rỗng luôn thuộc.
func gtidIn(set gomysql.GTIDSet, s string) (bool, error) {
	if strings.TrimSpace(s) == "" {
		return true, nil
	}
	o, err := gomysql.ParseMysqlGTIDSet(s)
	if err != nil {
		return false, fmt.Errorf("parse gtid set %q: %w", s, err)
	}
	return set.Contain(o), nil
}

// seedSchemaVersions (thuần) chọn version dùng được cho luồng bắt đầu ở `from` và lọc bỏ version đã cũ
// (Until ⊆ from). cur=nil nếu không có version nào dùng được (rơi về information_schema).
func seedSchemaVersions(vs []SchemaVersion, from gomysql.GTIDSet) (cur *TableInfo, kept []SchemaVersion) {
	for _, v := range vs {
		if v.Info == nil {
			continue
		}
		if ok, err := gtidIn(from, v.Until); err != nil || (v.Until != "" && ok) {
			continue // DDL kết thúc đã áp (hoặc state hỏng): version cũ
		}
		kept = append(kept, v)
		if ok, err := gtidIn(from, v.From); err == nil && ok {
			cur = v.Info
		}
	}
	return cur, kept
}

// schemaHistory là bản trong bộ nhớ của lịch sử, thuộc về goroutine đọc của một luồng.
type schemaHistory struct {
	store   SchemaHistoryStore
	hist    map[string][]SchemaVersion
	start   string // `from` lúc mở luồng
	lastDDL string // GTID của DDL gần nhất đã thấy trong luồng này
}

// open nạp lịch sử, trả cache khởi tạo (key → info) cho luồng bắt đầu ở from.
func openSchemaHistory(store SchemaHistoryStore, from gomysql.GTIDSet) (*schemaHistory, map[string]*TableInfo) {
	h := &schemaHistory{store: store, hist: map[string][]SchemaVersion{}, start: from.String()}
	cache := map[string]*TableInfo{}
	if store == nil {
		return h, cache
	}
	for key, vs := range store.LoadSchemaHistory() {
		cur, kept := seedSchemaVersions(vs, from)
		if len(kept) > 0 {
			h.hist[key] = kept
		}
		if cur != nil {
			cache[key] = cur
		}
	}
	return h, cache
}

// validFrom: From cho info vừa đọc ở luồng này.
func (h *schemaHistory) validFrom() string {
	if h.lastDDL != "" {
		return h.lastDDL
	}
	return h.start
}

// record ghi info mới đọc cho key. Version còn mở (Until rỗng) của key là version đã bị binlog bác bỏ
// (không có DDL nào kết thúc nó) nên bị thay thế.
func (h *schemaHistory) record(key string, info *TableInfo) error {
	if h.store == nil {
		return nil
	}
	var vs []SchemaVersion
	for _, v := range h.hist[key] {
		if v.Until != "" {
			vs = append(vs, v)
		}
	}
	vs = append(vs, SchemaVersion{Info: info, From: h.validFrom()})
	if len(vs) > maxSchemaVersions {
		vs = vs[len(vs)-maxSchemaVersions:]
	}
	h.hist[key] = vs
	return h.store.SaveSchemaHistory(h.hist)
}

// supersede được gọi khi đọc DDL ở gtid: mọi version đang mở kết thúc ở đây. Ghi bền xong mới trả về.
func (h *schemaHistory) supersede(gtid string) error {
	h.lastDDL = gtid
	if h.store == nil || gtid == "" {
		return nil
	}
	changed := false
	for _, vs := range h.hist {
		for i := range vs {
			if vs[i].Until == "" {
				vs[i].Until = gtid
				changed = true
			}
		}
	}
	if !changed {
		return nil
	}
	return h.store.SaveSchemaHistory(h.hist)
}
