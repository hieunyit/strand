package snapshot_test

import (
	"testing"

	"strand/internal/checkpoint"
	"strand/internal/model"
	"strand/internal/snapshot"
)

// Chi phí quyết định range cho một event trên router (đường PK số nguyên, việc C phải không chậm đi đáng kể).
func BenchmarkHandleStreamEventIntPK(b *testing.B) {
	m := snapshot.NewRangeStateManager()
	tbl := model.TableID{Schema: "app", Table: "t"}
	var chunks []*snapshot.Chunk
	for i := int64(0); i < 200; i++ {
		st := snapshot.StatePending
		if i%3 == 0 {
			st = snapshot.StateDone
		}
		chunks = append(chunks, &snapshot.Chunk{ID: i, Table: tbl, PKCol: "id", StartPK: i * 1000, EndPK: i*1000 + 999, State: st})
	}
	m.RegisterTable(tbl, "id", chunks, 199999)
	pos := model.Pos{Engine: model.EngineMySQL, GTID: "3E11FA47-71CA-11E1-9E33-C80AA9429562:5"}
	_ = checkpoint.NewMySQLAppliedSet
	ev := &model.RowChange{Table: tbl, Op: model.OpUpdate, Key: []model.Value{int64(0)}}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		ev.Key[0] = int64(i % 250000)
		m.HandleStreamEvent(ev, pos)
	}
}
