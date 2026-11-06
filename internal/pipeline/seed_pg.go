package pipeline

import (
	"strand/internal/model"
	"strand/internal/seed"
)

// spec: 02 §9 (strand sync --start-at seed, PG)
//
// Các quyết định thuần (không cần DB) của startFromSeedPG, tách ra để kiểm bằng unit test.

// pgSeedTablesDone: bảng nào của pipeline đã có dữ liệu trong seed (Done=true) và bảng nào phải
// snapshot logic (Done=false). Dùng seed.SeedCovers — cùng cách hiểu "tables" với planner/apply;
// seed vật lý mặc định ghi ["*"] = mọi bảng (review N06).
func pgSeedTablesDone(m *seed.SeedManifest, ids []model.TableID) map[model.TableID]bool {
	out := make(map[model.TableID]bool, len(ids))
	for _, id := range ids {
		out[id] = seed.SeedCovers(m.Tables, id)
	}
	return out
}
