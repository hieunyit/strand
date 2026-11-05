package snapshot

import (
	"context"
	"database/sql"
	"fmt"

	"strand/internal/model"
)

// FindChunkForTest: chunk mà range-state dùng cho khóa pk của bảng (nil = "Xong" / không đăng ký).
func (m *RangeStateManager) FindChunkForTest(t model.TableID, pk int64) *Chunk {
	m.mu.RLock()
	defer m.mu.RUnlock()
	tr, ok := m.tables[t.String()]
	if !ok {
		return nil
	}
	return tr.FindChunk(pk)
}

// Các đường ghi target MySQL của snapshot, cho test fence (SG-1, review R07).
var (
	FlushRowsToTargetForTest = flushRowsToTarget
	ClearMySQLTableForTest   = clearMySQLTable
)

// ClearUnreadForTest gọi clearUnread của snapshotter (không hook) trên dstDB.
func (s *MySQLSnapshotter) ClearUnreadForTest(ctx context.Context, dstDB *sql.DB, t model.TableID, pk string, lo, hi int64) error {
	return s.clearUnread(ctx, dstDB, t, pk, lo, hi, true, nil)
}

// KeyRangeDebugForTest: biên bảng và số row trong [min, max] theo điều kiện tuple (chẩn đoán việc C).
func KeyRangeDebugForTest(ctx context.Context, db *sql.DB, t model.TableID, codec *PKCodec) (string, int, error) {
	kq := newKeySQL(t, codec)
	lo, hi, ok, err := kq.mysqlKeyBounds(ctx, db)
	if err != nil || !ok {
		return "", 0, err
	}
	cond, args, err := kq.rangeCond(lo, hi)
	if err != nil {
		return "", 0, err
	}
	var n int
	err = db.QueryRowContext(ctx, "SELECT COUNT(*) FROM "+quoteTable(t)+" WHERE "+cond, args...).Scan(&n)
	return fmt.Sprintf("%s %#v %#v", cond, lo.V, hi.V), n, err
}
