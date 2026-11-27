package verify_test

import (
	"context"
	"reflect"
	"testing"

	"strand/internal/model"
	"strand/internal/verify"
)

// spec: 02 §8 (zero date). Với tables.zero_date = set_null | epoch, writer ghi NULL / 1970-01-01 thay
// zero date của MySQL; verify phải so với đúng giá trị thay đó (không báo lệch), và vẫn bắt lệch thật.
func TestVerifyZeroDatePolicy(t *testing.T) {
	src, _ := getMySQL(t)
	pg := getPostgres(t)
	ctx := context.Background()

	mustExec(t, src, "DROP TABLE IF EXISTS verify_src.zd")
	mustExec(t, src, `CREATE TABLE verify_src.zd (id INT PRIMARY KEY, day DATE NULL, dt DATETIME(6) NULL, ts TIMESTAMP(6) NULL, v INT)`)
	conn, err := src.Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	for _, q := range []string{
		"SET SESSION sql_mode = ''",
		"SET SESSION time_zone = '+00:00'",
		"INSERT INTO verify_src.zd VALUES (1, '0000-00-00', '0000-00-00 00:00:00', '0000-00-00 00:00:00', 1)",
		"INSERT INTO verify_src.zd VALUES (2, '2024-02-29', '2024-02-29 10:11:12.5', '2024-02-29 10:11:12.5', 2)",
		"INSERT INTO verify_src.zd VALUES (3, '2024-05-00', '2024-00-10 00:00:00', NULL, 3)",
	} {
		if _, err := conn.ExecContext(ctx, q); err != nil {
			t.Fatalf("%s: %v", q, err)
		}
	}
	_ = conn.Close()

	tbl := model.TableID{Schema: "verify_src", Table: "zd"}
	dst := model.TableID{Schema: "public", Table: "zd"}
	newMV := func(policy string) *verify.MerkleVerifier {
		cfg := verify.FullVerifyConfig{Source: src, Target: pg, PipelineID: "pipe_zd_" + policy,
			SourceEngine: model.EngineMySQL, TargetEngine: model.EnginePG, ChunkRows: 2,
			TargetName: func(model.TableID) model.TableID { return dst }}
		if policy != "" {
			cfg.ZeroDate = func(id model.TableID) string {
				if id != tbl {
					t.Errorf("ZeroDate asked for %v", id)
				}
				return policy
			}
		}
		return verify.NewMerkleVerifier(cfg)
	}
	run := func(policy string) []string {
		t.Helper()
		rep, err := newMV(policy).VerifyTableFull(ctx, tbl, "id")
		if err != nil {
			t.Fatalf("policy %q: %v", policy, err)
		}
		return sorted(rep.Mismatches)
	}

	for _, c := range []struct {
		policy string
		rows   []string // dữ liệu target giống hệt những gì writer ghi theo policy
	}{
		{"epoch", []string{
			"(1, '1970-01-01', '1970-01-01 00:00:00', '1970-01-01 00:00:00+00', 1)",
			"(3, '1970-01-01', '1970-01-01 00:00:00', NULL, 3)",
		}},
		{"null", []string{
			"(1, NULL, NULL, NULL, 1)",
			"(3, NULL, NULL, NULL, 3)",
		}},
	} {
		mustExec(t, pg, "DROP TABLE IF EXISTS public.zd")
		mustExec(t, pg, `CREATE TABLE public.zd (id int PRIMARY KEY, day date, dt timestamp(6), ts timestamptz(6), v int)`)
		mustExec(t, pg, "INSERT INTO public.zd VALUES "+c.rows[0]+", (2, '2024-02-29', '2024-02-29 10:11:12.5', '2024-02-29 10:11:12.5+00', 2), "+c.rows[1])

		if got := run(c.policy); len(got) != 0 {
			t.Fatalf("policy %s: replaced zero dates reported as mismatches %v", c.policy, got)
		}
		// Không có chính sách (reject): zero date của source không khớp giá trị thay ở target.
		if got, want := run(""), []string{"1", "3"}; !reflect.DeepEqual(got, want) {
			t.Fatalf("policy %s, verified as reject: mismatches = %v, want %v", c.policy, got, want)
		}
		// Lệch thật ở hàng thường và ở hàng có giá trị thay vẫn bị bắt.
		mustExec(t, pg, "UPDATE public.zd SET dt = dt + interval '1 second' WHERE id = 2")
		mustExec(t, pg, "UPDATE public.zd SET v = 99 WHERE id = 1")
		if got, want := run(c.policy), []string{"1", "2"}; !reflect.DeepEqual(got, want) {
			t.Fatalf("policy %s, modified: mismatches = %v, want %v", c.policy, got, want)
		}
	}
}
