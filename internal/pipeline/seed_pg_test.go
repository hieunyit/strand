package pipeline

import (
	"errors"
	"testing"

	"strand/internal/exitcode"
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

// review N09: startFromSeedPG phải đối chiếu source_identity của seed.json với system_identifier của
// source, không chỉ ghi log.
func TestN09_CheckPGSeedSourceIdentity(t *testing.T) {
	const id = "7123456789012345678"
	if err := checkPGSeedSourceIdentity(id, id); err != nil {
		t.Fatalf("same cluster rejected: %v", err)
	}
	for name, c := range map[string][2]string{
		"other-cluster":     {id, "7123456789012345679"},
		"manifest-empty":    {"", id},
		"source-unknown":    {id, ""},
		"both-empty":        {"", ""},
		"whitespace-only":   {"  ", id},
		"not-a-prefix-pass": {"712345678901234567", id},
	} {
		err := checkPGSeedSourceIdentity(c[0], c[1])
		var se *exitcode.Error
		if !errors.As(err, &se) || se.Code != exitcode.ErrSeedInvariantFailed || exitcode.GetExitCode(err) != exitcode.Rejected {
			t.Fatalf("%s: want seed_invariant_failed/Rejected, got %v", name, err)
		}
	}
}
