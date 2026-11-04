package snapshot

import "strand/internal/model"

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
