package pipeline

import (
	"testing"

	"strand/internal/model"
	"strand/internal/seed"
)

// review N06: startFromSeedPG — seed vật lý ghi tables=["*"] → không bảng nào bị snapshot logic lại;
// danh sách cụ thể / glob schema chỉ đánh dấu đúng các bảng khớp.
func TestN06_PGSeedTablesDone(t *testing.T) {
	ids := []model.TableID{{Schema: "public", Table: "orders"}, {Schema: "app", Table: "Users"}, {Schema: "app", Table: "items"}}
	all := pgSeedTablesDone(&seed.SeedManifest{Kind: "pgbackrest", Tables: []string{"*"}}, ids)
	for _, id := range ids {
		if !all[id] {
			t.Fatalf("physical seed tables=[*]: %v marked not done (would be re-snapshotted)", id)
		}
	}
	some := pgSeedTablesDone(&seed.SeedManifest{Kind: "strand-export", Tables: []string{"orders", `app."Users"`}}, ids)
	want := map[model.TableID]bool{ids[0]: true, ids[1]: true, ids[2]: false}
	for id, w := range want {
		if some[id] != w {
			t.Fatalf("export tables: %v done=%v, want %v", id, some[id], w)
		}
	}
	glob := pgSeedTablesDone(&seed.SeedManifest{Kind: "strand-export", Tables: []string{"app.*"}}, ids)
	if glob[ids[0]] || !glob[ids[1]] || !glob[ids[2]] {
		t.Fatalf("schema glob: %v", glob)
	}
}
