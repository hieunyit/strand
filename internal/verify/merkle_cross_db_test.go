package verify_test

// Gate 2 "_ci → C" (cần container MySQL + PG): verify full khi target sắp PK khác thứ tự source
// (VerifyTableFullKeysExact). Dữ liệu giống hệt (khóa sắp khác nhau giữa _ci và "C") → 0 lệch ở mọi cỡ
// chunk; rồi đổi khóa ở target sang biến thể cùng lớp của source, thêm row ở hai đầu không gian khóa, xóa
// một row, đổi giá trị — verify phải liệt kê đúng các khóa đó.

import (
	"context"
	"fmt"
	"reflect"
	"testing"

	"strand/internal/model"
	"strand/internal/snapshot"
	"strand/internal/verify"
)

func TestG2_FullVerifyExactKeyMySQLCIToPGC(t *testing.T) {
	src, _ := getMySQL(t)
	pg := getPostgres(t)
	ctx := context.Background()
	for _, c := range []struct {
		name, coll, pk, pgPK string
		composite            bool
	}{
		{"g2_ai", "utf8mb4_0900_ai_ci", "PRIMARY KEY (k)", "PRIMARY KEY (k)", false},
		{"g2_gen", "utf8mb4_general_ci", "PRIMARY KEY (t, k)", "PRIMARY KEY (t, k)", true},
	} {
		mustExec(t, src, "DROP TABLE IF EXISTS verify_src."+c.name)
		mustExec(t, src, fmt.Sprintf("CREATE TABLE verify_src.%s (t INT NOT NULL, k VARCHAR(20) COLLATE %s NOT NULL, v INT, %s) DEFAULT CHARSET utf8mb4",
			c.name, c.coll, c.pk))
		mustExec(t, pg, "DROP TABLE IF EXISTS public."+c.name)
		mustExec(t, pg, fmt.Sprintf(`CREATE TABLE public.%s (t int NOT NULL, k varchar(20) COLLATE "C" NOT NULL, v int, %s)`, c.name, c.pgPK))
		keys := []string{"a", "B", "c", "_x", "Zed", "é", "ébc", "-1", "ab ", "Ab1", "zz", "ß"}
		for i := 0; i < 120; i++ {
			keys = append(keys, fmt.Sprintf("Key-%03d", i), fmt.Sprintf("key_%03d", i))
		}
		n := 0
		for i, k := range keys {
			tv := 0
			if c.composite {
				tv = i % 3
			}
			res, err := src.Exec(fmt.Sprintf("INSERT IGNORE INTO verify_src.%s VALUES (?, ?, ?)", c.name), tv, k, i)
			if err != nil {
				t.Fatal(err)
			}
			if a, _ := res.RowsAffected(); a == 1 {
				mustExec(t, pg, fmt.Sprintf("INSERT INTO public.%s VALUES ($1, $2, $3)", c.name), tv, k, i)
				n++
			}
		}
		tbl := model.TableID{Schema: "verify_src", Table: c.name}
		dstT := model.TableID{Schema: "public", Table: c.name}
		pkCols := []string{"k"}
		if c.composite {
			pkCols = []string{"t", "k"}
		}
		sc, err := snapshot.MySQLPKCodec(ctx, src, tbl, pkCols)
		if err != nil {
			t.Fatal(err)
		}
		dc, err := snapshot.PGPKCodec(ctx, pg, dstT, pkCols)
		if err != nil {
			t.Fatal(err)
		}
		if snapshot.CrossEngineSameOrder(sc, dc) == nil {
			t.Fatalf("%s: _ci → C accepted as same order", c.name)
		}
		if err := snapshot.CrossEngineKeySet(sc, dc); err != nil {
			t.Fatalf("%s: %v", c.name, err)
		}
		run := func(chunk int) *verify.FullVerifyReport {
			t.Helper()
			mv := verify.NewMerkleVerifier(verify.FullVerifyConfig{Source: src, Target: pg, SourceEngine: model.EngineMySQL,
				TargetEngine: model.EnginePG, ChunkRows: chunk, TargetName: func(model.TableID) model.TableID { return dstT }})
			rep, err := mv.VerifyTableFullKeysExact(ctx, tbl, sc, dc)
			if err != nil {
				t.Fatalf("%s: %v", c.name, err)
			}
			return rep
		}
		for _, chunk := range []int{1, 7, 1000} {
			if rep := run(chunk); len(rep.Mismatches) != 0 || rep.MismatchedChunks != 0 {
				t.Fatalf("%s chunk %d: identical data reported %+v", c.name, chunk, *rep)
			}
		}
		// Lệch: biến thể cùng lớp ('a' → 'A' ở target), row thừa trước min và sau max (theo thứ tự source),
		// row thiếu, giá trị khác.
		mustExec(t, pg, fmt.Sprintf("UPDATE public.%s SET k = 'A' WHERE k = 'a'", c.name))
		mustExec(t, pg, fmt.Sprintf("INSERT INTO public.%s VALUES (0, '!', 0), (0, 'ÿÿÿ', 0)", c.name))
		mustExec(t, pg, fmt.Sprintf("DELETE FROM public.%s WHERE k = 'zz'", c.name))
		mustExec(t, pg, fmt.Sprintf("UPDATE public.%s SET v = -1 WHERE k = 'Key-005'", c.name))
		pre := ""
		if c.composite {
			pre = "1:0,"
		}
		want := sorted([]string{pre + "1:a", pre + "1:A", "1:0,1:!", "1:0,6:ÿÿÿ"})
		if !c.composite {
			want = sorted([]string{"1:a", "1:A", "1:!", "6:ÿÿÿ"})
		}
		// 'zz' và 'Key-005': cột t của chúng tùy chỉ số; tìm lại từ source.
		for _, k := range []string{"zz", "Key-005"} {
			var tv int
			if err := src.QueryRow(fmt.Sprintf("SELECT t FROM verify_src.%s WHERE BINARY k = ?", c.name), k).Scan(&tv); err != nil {
				t.Fatal(err)
			}
			if c.composite {
				want = append(want, fmt.Sprintf("%d:%d,%d:%s", len(fmt.Sprint(tv)), tv, len(k), k))
			} else {
				want = append(want, fmt.Sprintf("%d:%s", len(k), k))
			}
		}
		want = sorted(want)
		for _, chunk := range []int{1, 7, 1000} {
			rep := run(chunk)
			if got := sorted(rep.Mismatches); !reflect.DeepEqual(got, want) {
				t.Fatalf("%s chunk %d: mismatches %q, want %q", c.name, chunk, got, want)
			}
			if rep.RowsChecked == 0 || rep.MismatchedChunks == 0 {
				t.Fatalf("%s chunk %d: report %+v", c.name, chunk, *rep)
			}
		}
		_ = n
	}
}
