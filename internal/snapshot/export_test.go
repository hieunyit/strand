package snapshot

import (
	"context"
	"database/sql"

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
