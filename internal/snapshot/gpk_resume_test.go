package snapshot_test

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"
	"testing"

	"strand/internal/checkpoint"
	"strand/internal/model"
	"strand/internal/snapshot"
)

func dumpRows(t *testing.T, db *sql.DB, q string) string {
	t.Helper()
	rows, err := db.Query(q)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	var out []string
	for rows.Next() {
		var a, b, v sql.NullString
		if err := rows.Scan(&a, &b, &v); err != nil {
			t.Fatal(err)
		}
		out = append(out, fmt.Sprintf("%q|%q|%q", a.String, b.String, v.String))
	}
	sort.Strings(out)
	return strings.Join(out, "\n")
}

// invariant: RS-1, CP-5 — việc C: snapshot khóa tổng quát (VARCHAR _bin + INT) dừng giữa chừng, target có
// row cũ ở phần chưa đọc (như kill -9 giữa flush và lưu plan); chạy tiếp từ plan lưu qua JSON: phần chưa
// đọc được xóa theo tuple rồi đọc lại, target cuối giống hệt source.
func TestGPK_ResumeFromSavedPlanWithGenericKeys(t *testing.T) {
	srcDSN, dstDSN := getMySQLDSNs(t)
	src, _ := sql.Open("mysql", srcDSN)
	dst, _ := sql.Open("mysql", dstDSN)
	defer src.Close()
	defer dst.Close()
	ctx := context.Background()
	ddl := "(k VARCHAR(16) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin NOT NULL, n INT NOT NULL, v VARCHAR(20), PRIMARY KEY (k, n))"
	for _, db := range []*sql.DB{src, dst} {
		db.Exec("DROP TABLE IF EXISTS gpk_resume")
		if _, err := db.Exec("CREATE TABLE gpk_resume " + ddl); err != nil {
			t.Fatal(err)
		}
	}
	for i := 0; i < 300; i++ {
		if _, err := src.Exec("INSERT INTO gpk_resume VALUES (?, ?, ?)", fmt.Sprintf("k%03d", i/3), i%3, fmt.Sprintf("v%d", i)); err != nil {
			t.Fatal(err)
		}
	}
	var schema string
	_ = src.QueryRow("SELECT DATABASE()").Scan(&schema)
	var dschema string
	_ = dst.QueryRow("SELECT DATABASE()").Scan(&dschema)
	table := model.TableID{Schema: schema, Table: "gpk_resume"}
	codec := mysqlCodec(t, src, "gpk_resume", []string{"k", "n"})

	var saved []byte // plan lưu qua JSON như localState
	newSnap := func() *snapshot.MySQLSnapshotter {
		s := snapshot.NewMySQLSnapshotter(snapshot.ChunkOptions{DefaultChunkRows: 25, MinChunkRows: 1, MaxChunkBytes: 1 << 30})
		s.TargetName = func(model.TableID) model.TableID { return model.TableID{Schema: dschema, Table: "gpk_resume"} }
		s.LoadPlan = func(model.TableID) (snapshot.SnapshotPlan, bool) {
			if saved == nil {
				return snapshot.SnapshotPlan{}, false
			}
			var p snapshot.SnapshotPlan
			if err := json.Unmarshal(saved, &p); err != nil {
				t.Fatal(err)
			}
			return p, true
		}
		s.SavePlan = func(_ model.TableID, p snapshot.SnapshotPlan) error {
			b, err := json.Marshal(p)
			saved = b
			return err
		}
		return s
	}
	noWait := func(context.Context, checkpoint.AppliedSet) error { return nil }
	cols := []string{"k", "n", "v"}

	// Lượt 1: dừng sau 4 chunk.
	s1 := newSnap()
	cctx, cancel := context.WithCancel(ctx)
	chunks := 0
	s1.Pace = func(context.Context) error {
		if chunks++; chunks == 4 {
			cancel()
			return context.Canceled
		}
		return nil
	}
	if err := s1.SnapshotAddKeys(cctx, src, dst, table, codec, cols, noWait); !errors.Is(err, context.Canceled) {
		t.Fatalf("first run: %v", err)
	}
	var p snapshot.SnapshotPlan
	_ = json.Unmarshal(saved, &p)
	if !p.GenericKey || len(p.LastKey) != 2 || p.Finished {
		t.Fatalf("saved plan after 4 chunks: %s", saved)
	}
	// Row cũ ở phần chưa đọc (khóa sau LastKey, ≤ max), rồi source xóa một nhóm khóa ở phần đó.
	if _, err := dst.Exec("INSERT INTO gpk_resume VALUES ('k090', 1, 'STALE'), ('k095', 0, 'GONE')"); err != nil {
		t.Fatal(err)
	}
	if _, err := src.Exec("DELETE FROM gpk_resume WHERE k = 'k095'"); err != nil {
		t.Fatal(err)
	}

	// Lượt 2: chạy tiếp từ plan đã lưu.
	s2 := newSnap()
	if err := s2.SnapshotAddKeys(ctx, src, dst, table, codec, cols, noWait); err != nil {
		t.Fatalf("resume: %v", err)
	}
	if a, b := dumpRows(t, src, "SELECT k, n, v FROM gpk_resume"), dumpRows(t, dst, "SELECT k, n, v FROM gpk_resume"); a != b {
		as, bs := strings.Split(a, "\n"), strings.Split(b, "\n")
		in := func(xs []string) map[string]bool {
			m := map[string]bool{}
			for _, x := range xs {
				m[x] = true
			}
			return m
		}
		ma, mb := in(as), in(bs)
		var onlySrc, onlyDst []string
		for _, x := range as {
			if !mb[x] {
				onlySrc = append(onlySrc, x)
			}
		}
		for _, x := range bs {
			if !ma[x] {
				onlyDst = append(onlyDst, x)
			}
		}
		t.Fatalf("target differs from source after resume: only in source %v, only in target %v", onlySrc, onlyDst)
	}
	_ = json.Unmarshal(saved, &p)
	if !p.Finished {
		t.Fatalf("plan not finished after resume: %s", saved)
	}
}
