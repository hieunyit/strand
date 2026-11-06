package snapshot

// Snapshot logic khi source và target sắp PK KHÁC thứ tự (Gate 2 "collation _ci (MySQL) → C (PG)",
// CrossEngineKeySet). Ví dụ: VARCHAR utf8mb4_0900_ai_ci / utf8mb4_general_ci ở MySQL → text COLLATE "C"
// ở PG: 'a' < 'B' ở source nhưng 'B' < 'a' ở target; source coi 'a' = 'A' (= 'a ' với PAD SPACE), target
// thì không.
//
// Range-state, keyset walk và SELECT chunk ở source đều theo thứ tự SOURCE (codec source: WEIGHT_STRING
// của chính MySQL) — không đổi. Chỉ các thao tác ở TARGET từng dựa trên "cùng thứ tự" mới phải thay:
//
//  1. Flush chunk (FlushChunk): đường thường thay RANGE [lo, hi] ở target (DELETE khóa trong range vắng
//     trong rows, rồi upsert). Target không tính được range theo thứ tự source, nên ở đây flush theo
//     TẬP KHÓA CHÍNH XÁC (FlushExact): upsert các row đã merge và DELETE đúng byte khóa của tombstone
//     trong buffer. Không xóa theo range.
//  2. Dọn phần "Chưa đọc" của plan dở khi chạy tiếp (ClearUnreadRange của pipeline): quét khóa target,
//     tính khóa source trong Go (EncodeMany, weight do MySQL source tính), xóa đúng các row có khóa
//     source thuộc phần chưa đọc (InUnread).
//
// Vì sao flush theo tập khóa là đủ — bất biến T: "mọi row ở target của bảng có khóa source thuộc một
// range Xong" (range Xong gồm cả phần sau b_last). Chứng minh T đúng tại mọi thời điểm từ lúc đăng ký:
//   - Đăng ký bảng mới: ClearTarget xóa sạch bảng đích trong barrier (router dừng) — T đúng (rỗng).
//   - Đăng ký plan dở: trước đó, event của phần chưa đọc bị bỏ (keepBeforeRegister: PendingKey theo thứ
//     tự source); nhưng flush của lần chạy trước có thể đã commit mà plan chưa kịp fsync, nên target còn
//     row có khóa source trong phần chưa đọc. Bước 2 xóa ĐÚNG mọi row đó (quét toàn bộ target, phép
//     thuộc tính bằng khóa source) trong barrier — T đúng.
//   - Sau đăng ký, chỉ có ba đường ghi target của bảng: (a) stream apply event khi range của khóa (theo
//     khóa SOURCE của event, FindChunk) là Xong; (b) flush chunk C — mọi row đã merge có khóa source
//     trong C (SELECT theo range source, event buffer được gán vào C bằng chính khóa source), và C thành
//     Xong trong cùng bước (MarkDoneLocked, giữ m.mu); (c) DELETE tombstone của C (khóa source trong C).
//     Event của range Chưa đọc bị bỏ, của range Đang đọc/Chờ điểm hẹn được buffer — không ghi target.
//     Mỗi row target có đúng một khóa source (CrossEngineKeySet điều kiện 1: bằng ở target ⇒ bằng ở
//     source), nên "khóa source của row" xác định và T được giữ.
//
// Từ T: ngay trước flush của chunk C (chưa Xong), target KHÔNG có row nào có khóa source thuộc C. Kết
// quả đúng của C ở target (= trạng thái source tại vị trí router, 02 §5) là đúng tập row đã merge
// (MergeLocked: row snapshot, buffer đè, tombstone bỏ) — y như đường range. Upsert các row đó là đủ:
//   - row bị xóa ở source TRƯỚC khi chunk đọc: không có trong SnapshotRows, chưa từng được ghi (T) ⇒ vắng ✓;
//   - row bị xóa SAU low: tombstone trong buffer bỏ nó khỏi tập merge ⇒ không được ghi ✓; DELETE theo
//     byte khóa của tombstone chỉ là phòng thủ (theo T nó không khớp row nào);
//   - đổi khóa trong cùng lớp bằng của source ('a' → 'A', 'a' → 'a '): event UPDATE có OldKey (assembler
//     so giá trị chính xác, KeysEqual), tách thành tombstone 'a' + upsert 'A' cùng khóa source K; buffer
//     giữ ảnh cuối (upsert 'A') ⇒ target nhận 'A', không có 'a' (T) ✓. Ở range Xong, writer apply theo byte
//     (keysDiffer so giá trị chính xác) như mọi stream.
//   - Tombstone không bao giờ trùng byte với một row đã merge: trùng byte ⇒ cùng khóa source ⇒ cùng ô
//     buffer, mà ô đó là tombstone thì khóa đã bị bỏ khỏi tập merge. Nên DELETE không cần "AND NOT IN
//     (rows)" và không cascade sang bảng con của row còn lại.
//
// Byte khóa: upsert và DELETE ở target bind giá trị khóa đúng như source gửi (binlog / SELECT), không chuẩn
// hóa theo collation (bindValue giữ chuỗi), nên target lưu đúng byte của source.
//
// Checkpoint (CP-*): không đổi — flush và MarkDone chạy như đường range (cùng barrier, cùng savePlan sau
// flush); plan vẫn lưu biên theo khóa SOURCE. Crash giữa flush và savePlan: row đã flush nằm trong phần
// mà plan coi là chưa đọc ⇒ bước 2 xóa khi chạy tiếp (như đường range dùng DELETE range).

import (
	"context"
	"database/sql"
	"fmt"
	"sort"
	"strings"

	"strand/internal/model"
)

// exactDeletes: cận (giá trị chính xác) của các tombstone trong buffer của chunk — phần DELETE của flush
// theo tập khóa — theo thứ tự khóa (xác định, dễ kiểm).
func exactDeletes(chunk *Chunk, codec *PKCodec) ([]Bound, error) {
	var out []Bound
	for k, rc := range chunk.BufferedChanges {
		if rc.Op != model.OpDelete {
			continue
		}
		b, err := exactBound(codec, k, rc.Key)
		if err != nil {
			return nil, err
		}
		out = append(out, b)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].K < out[j].K })
	return out, nil
}

// exactBound dựng Bound của khóa đã mã hóa k với giá trị vals giữ dạng bind (không tính lại weight).
func exactBound(codec *PKCodec, k Key, vals []model.Value) (Bound, error) {
	norm := make([]model.Value, len(vals))
	for i, col := range codec.Cols {
		v, err := bindValue(col, vals[i])
		if err != nil {
			return Bound{}, err
		}
		norm[i] = v
	}
	return Bound{K: k, V: norm}, nil
}

// InUnread: khóa source k thuộc phần "Chưa đọc" của plan dở — range [lo, hi] (khi hasRange; lo có thể
// loại trừ) hoặc một khóa chờ đọc lại (reread, theo khóa source). Dùng khi dọn target theo tập khóa
// (CrossEngineKeySet): row target nào có khóa source thỏa InUnread thì bị xóa.
func InUnread(k Key, lo, hi Bound, hasRange bool, reread map[Key]bool) bool {
	if hasRange && geLo(k, lo) && k <= hi.K {
		return true
	}
	return reread[k]
}

// ScanPGKeys quét MỌI row của bảng PG q.Table theo trang page row (keyset theo thứ tự PK của target,
// q.Codec là codec TARGET), gọi fn với khóa từng row dạng text (cột::text — giá trị bind lại được ở cả
// codec target lẫn codec source, CrossEngineKeySet điều kiện 3) và các biểu thức extra (text, không NULL)
// cùng row. fn có thể xóa row đã thấy: trang sau bắt đầu SAU khóa cuối của trang trước nên không bỏ sót
// row nào còn lại. Mỗi trang là một truy vấn riêng (không giữ snapshot dài trên target).
func ScanPGKeys(ctx context.Context, db *sql.DB, q PGKeySQL, extra []string, page int,
	fn func(keys [][]model.Value, extra [][]string) error) error {
	if page <= 0 {
		page = 1000
	}
	cols := q.Cols()
	sel := make([]string, 0, len(cols)+len(extra))
	for _, c := range cols {
		sel = append(sel, c+"::text")
	}
	sel = append(sel, extra...)
	from := quotePGIdent(q.Table.Schema) + "." + quotePGIdent(q.Table.Table)
	if q.Alias != "" {
		from += " " + q.Alias
	}
	var last *Bound
	for {
		where, args := "TRUE", []any(nil)
		if last != nil {
			var err error
			if where, args, _, err = q.Cmp(">", *last, 1); err != nil {
				return err
			}
		}
		keys, ex, err := pgPage(ctx, db, fmt.Sprintf("SELECT %s FROM %s WHERE %s ORDER BY %s LIMIT %d",
			strings.Join(sel, ", "), from, where, q.OrderBy(false), page), args, len(cols), len(extra))
		if err != nil {
			return fmt.Errorf("scan keys of %s: %w", q.Table, err)
		}
		if len(keys) == 0 {
			return nil
		}
		b, err := q.Codec.BoundOf(keys[len(keys)-1])
		if err != nil {
			return err
		}
		if err := fn(keys, ex); err != nil {
			return err
		}
		if len(keys) < page {
			return nil
		}
		last = &b
	}
}

func pgPage(ctx context.Context, db *sql.DB, query string, args []any, nk, ne int) ([][]model.Value, [][]string, error) {
	rows, err := db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, nil, err
	}
	defer rows.Close()
	var keys [][]model.Value
	var ex [][]string
	for rows.Next() {
		vs := make([]sql.NullString, nk+ne)
		ptrs := make([]any, len(vs))
		for i := range vs {
			ptrs[i] = &vs[i]
		}
		if err := rows.Scan(ptrs...); err != nil {
			return nil, nil, err
		}
		k := make([]model.Value, nk)
		e := make([]string, ne)
		for i, v := range vs {
			if !v.Valid {
				return nil, nil, fmt.Errorf("unexpected NULL in column %d of a key scan", i+1)
			}
			if i < nk {
				k[i] = v.String
			} else {
				e[i-nk] = v.String
			}
		}
		keys, ex = append(keys, k), append(ex, e)
	}
	return keys, ex, rows.Err()
}
