package verify_test

// verify --mode sample cho PK tổng quát ở PG→PG và MySQL→PG (cần container MySQL + PG):
// SampleKeyRangesEngine chọn range theo tuple PK của source với số row đọc chặn theo rate, và
// MerkleVerifier.VerifyKeyRanges so từng row: dữ liệu giống hệt → 0 lệch; rate 1 → mọi row được so và
// đúng khóa lệch được báo; bảng source rỗng → range All bắt row thừa ở target.

import (
	"context"
	"database/sql"
	"fmt"
	"math/rand"
	"reflect"
	"testing"

	"strand/internal/model"
	"strand/internal/snapshot"
	"strand/internal/verify"
)

func sampleAndVerify(t *testing.T, mv *verify.MerkleVerifier, db verifyDB, engine model.Engine, tbl model.TableID, sc, dc *snapshot.PKCodec, rate float64) *verify.FullVerifyReport {
	t.Helper()
	ctx := context.Background()
	ranges, err := verify.SampleKeyRangesEngine(ctx, db, engine, tbl, sc, rate, 100, rand.New(rand.NewSource(7)))
	if err != nil {
		t.Fatalf("sample %s: %v", tbl, err)
	}
	if len(ranges) == 0 {
		t.Fatalf("sample %s: no ranges", tbl)
	}
	rep, err := mv.VerifyKeyRanges(ctx, tbl, sc, dc, ranges)
	if err != nil {
		t.Fatalf("verify %s: %v", tbl, err)
	}
	return rep
}

func TestSampleKeysDB_PGToPGCompositeAndString(t *testing.T) {
	pg := getPostgres(t)
	ctx := context.Background()
	mustExec(t, pg, "CREATE SCHEMA IF NOT EXISTS sk1; CREATE SCHEMA IF NOT EXISTS sk2")
	for _, c := range []struct{ name, ddl, fill string }{
		{"comp", `(a int NOT NULL, b text COLLATE "C" NOT NULL, v int, PRIMARY KEY (a, b))`,
			`SELECT g % 50, 'k' || g, g FROM generate_series(1, 20000) g`},
		{"str", `(k text COLLATE "C" PRIMARY KEY, v int)`, `SELECT md5(g::text), g FROM generate_series(1, 20000) g`},
	} {
		for _, s := range []string{"sk1", "sk2"} {
			mustExec(t, pg, fmt.Sprintf("DROP TABLE IF EXISTS %s.%s", s, c.name))
			mustExec(t, pg, fmt.Sprintf("CREATE TABLE %s.%s %s", s, c.name, c.ddl))
			mustExec(t, pg, fmt.Sprintf("INSERT INTO %s.%s %s", s, c.name, c.fill))
			mustExec(t, pg, fmt.Sprintf("ANALYZE %s.%s", s, c.name))
		}
		tbl := model.TableID{Schema: "sk1", Table: c.name}
		dstT := model.TableID{Schema: "sk2", Table: c.name}
		pkCols := []string{"a", "b"}
		if c.name == "str" {
			pkCols = []string{"k"}
		}
		sc, err := snapshot.PGPKCodec(ctx, pg, tbl, pkCols)
		if err != nil {
			t.Fatal(err)
		}
		dc, err := snapshot.PGPKCodec(ctx, pg, dstT, pkCols)
		if err != nil {
			t.Fatal(err)
		}
		mv := verify.NewMerkleVerifier(verify.FullVerifyConfig{Source: pg, Target: pg, SourceEngine: model.EnginePG,
			TargetEngine: model.EnginePG, TargetName: func(model.TableID) model.TableID { return dstT }})
		// rate 1%: 20000 × 0.01 / 100 + 1 = 3 range × 100 row — đọc chặn theo rate.
		rep := sampleAndVerify(t, mv, pg, model.EnginePG, tbl, sc, dc, 0.01)
		if len(rep.Mismatches) != 0 || rep.RowsChecked == 0 || rep.RowsChecked > 3*100 {
			t.Fatalf("%s rate 0.01: %+v", c.name, *rep)
		}
		// Lệch một row: rate 1 so mọi row, báo đúng khóa.
		if c.name == "comp" {
			mustExec(t, pg, "UPDATE sk2.comp SET v = -1 WHERE a = 7 AND b = 'k57'")
		} else {
			mustExec(t, pg, "UPDATE sk2.str SET v = -1 WHERE k = md5('57')")
		}
		rep = sampleAndVerify(t, mv, pg, model.EnginePG, tbl, sc, dc, 1)
		if rep.RowsChecked != 20000 || len(rep.Mismatches) != 1 {
			t.Fatalf("%s rate 1: %+v", c.name, *rep)
		}
	}
	// Bảng source rỗng, target có row thừa: range All.
	mustExec(t, pg, "DROP TABLE IF EXISTS sk1.empty; DROP TABLE IF EXISTS sk2.empty")
	mustExec(t, pg, `CREATE TABLE sk1.empty (k text COLLATE "C" PRIMARY KEY); CREATE TABLE sk2.empty (k text COLLATE "C" PRIMARY KEY)`)
	mustExec(t, pg, "INSERT INTO sk2.empty VALUES ('extra')")
	tbl, dstT := model.TableID{Schema: "sk1", Table: "empty"}, model.TableID{Schema: "sk2", Table: "empty"}
	sc, _ := snapshot.PGPKCodec(ctx, pg, tbl, []string{"k"})
	dc, _ := snapshot.PGPKCodec(ctx, pg, dstT, []string{"k"})
	mv := verify.NewMerkleVerifier(verify.FullVerifyConfig{Source: pg, Target: pg, SourceEngine: model.EnginePG,
		TargetEngine: model.EnginePG, TargetName: func(model.TableID) model.TableID { return dstT }})
	if rep := sampleAndVerify(t, mv, pg, model.EnginePG, tbl, sc, dc, 0.01); !reflect.DeepEqual(rep.Mismatches, []string{"5:extra"}) {
		t.Fatalf("empty source: %+v", *rep)
	}
}

func TestSampleKeysDB_MySQLToPGCompositeInt(t *testing.T) {
	src, _ := getMySQL(t)
	pg := getPostgres(t)
	ctx := context.Background()
	mustExec(t, src, "DROP TABLE IF EXISTS verify_src.skc")
	mustExec(t, src, "CREATE TABLE verify_src.skc (a INT NOT NULL, b BIGINT NOT NULL, v VARCHAR(20), PRIMARY KEY (a, b))")
	mustExec(t, pg, "DROP TABLE IF EXISTS public.skc")
	mustExec(t, pg, "CREATE TABLE public.skc (a int NOT NULL, b bigint NOT NULL, v varchar(20), PRIMARY KEY (a, b))")
	mustExec(t, src, `INSERT INTO verify_src.skc
		WITH RECURSIVE g(n) AS (SELECT 1 UNION ALL SELECT n + 1 FROM g WHERE n < 5000) SELECT n % 40, n, CONCAT('v', n) FROM g`)
	mustExec(t, pg, "INSERT INTO public.skc SELECT g % 40, g, 'v' || g FROM generate_series(1, 5000) g")
	mustExec(t, src, "ANALYZE TABLE verify_src.skc")
	tbl, dstT := model.TableID{Schema: "verify_src", Table: "skc"}, model.TableID{Schema: "public", Table: "skc"}
	sc, err := snapshot.MySQLPKCodec(ctx, src, tbl, []string{"a", "b"})
	if err != nil {
		t.Fatal(err)
	}
	dc, err := snapshot.PGPKCodec(ctx, pg, dstT, []string{"a", "b"})
	if err != nil {
		t.Fatal(err)
	}
	if err := snapshot.CrossEngineSameOrder(sc, dc); err != nil {
		t.Fatal(err)
	}
	mv := verify.NewMerkleVerifier(verify.FullVerifyConfig{Source: src, Target: pg, SourceEngine: model.EngineMySQL,
		TargetEngine: model.EnginePG, TargetName: func(model.TableID) model.TableID { return dstT }})
	rep := sampleAndVerify(t, mv, src, model.EngineMySQL, tbl, sc, dc, 0.05)
	if len(rep.Mismatches) != 0 || rep.RowsChecked == 0 || rep.RowsChecked > 6*100 { // TABLE_ROWS là ước lượng
		t.Fatalf("rate 0.05: %+v", *rep)
	}
	mustExec(t, pg, "UPDATE public.skc SET v = 'x' WHERE a = 2 AND b = 42")
	rep = sampleAndVerify(t, mv, src, model.EngineMySQL, tbl, sc, dc, 1)
	if rep.RowsChecked != 5000 || !reflect.DeepEqual(rep.Mismatches, []string{"1:2,2:42"}) {
		t.Fatalf("rate 1: %+v", *rep)
	}
}

// verifyDB: *sql.DB (đặt tên để chữ ký helper ngắn).
type verifyDB = *sql.DB
