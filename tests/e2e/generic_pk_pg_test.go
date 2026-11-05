package e2e

import (
	"fmt"
	"math/rand"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// Việc C, binary thật, PG→PG: `snapshot add` một bảng PK (int4, text COLLATE "C") có cột TOAST lưu ngoài
// dòng, dưới tải ghi đồng thời gồm UPDATE đổi PK mà doc không đổi (đường đọc lại khóa); cuối: `verify
// --mode full` 0 lệch và dữ liệu hai phía giống hệt.
func TestC_E2E_PGGenericPKSnapshotAdd(t *testing.T) {
	seed := seedFromEnv(t)
	slot, pub := "strand_e2e_gpk", "strand_e2e_gpk_pub"
	src, dst, lsn := setupPGPair(t, slot, pub)
	ddl := `CREATE TABLE app.gk (a int4 NOT NULL, b text COLLATE "C" NOT NULL, v int4, doc text, PRIMARY KEY (a, b))`
	src.exec(t, ddl, "ALTER TABLE app.gk ALTER COLUMN doc SET STORAGE EXTERNAL", "ALTER PUBLICATION "+pub+" ADD TABLE app.gk")
	dst.exec(t, ddl)
	words := []string{"a", "A", "b", "ab", "é", "中", "😀", "0", "10", "9", "z"}
	key := func(r *rand.Rand) (int, string) {
		return r.Intn(200) - 100, words[r.Intn(len(words))] + fmt.Sprint(r.Intn(30))
	}
	r := rand.New(rand.NewSource(seed))
	doc := strings.Repeat("d", 3000)
	for i := 0; i < 4000; i++ {
		a, b := key(r)
		if _, err := src.DB.Exec("INSERT INTO app.gk VALUES ($1, $2, $3, $4) ON CONFLICT DO NOTHING", a, b, i, doc); err != nil {
			t.Fatal(err)
		}
	}
	cfg := strandCfg{Name: "e2e-pg-gpk", SourceKind: "postgres", TargetKind: "postgres",
		SourceDSN: src.DSN, TargetDSN: dst.DSN, SourceExtra: fmt.Sprintf(", slot: %s, publication: %s", slot, pub),
		Include: []string{"app.items", "app.accounts", "app.marker", "app.gk"}, Dir: testDir(t), Bootstrap: "start_at",
		SourceIOPS: 20000, ChunkRows: "50"}
	p := startStrand(t, cfg.write(t), "--start-at", lsn)
	waitSynced(t, &cfg, p, time.Minute)

	stop := make(chan struct{})
	var wg sync.WaitGroup
	var ops, moves atomic.Int64
	for w := 0; w < 3; w++ {
		wg.Add(1)
		go func(s int64) {
			defer wg.Done()
			rr := rand.New(rand.NewSource(s))
			for {
				select {
				case <-stop:
					return
				default:
				}
				a, b := key(rr)
				var err error
				switch rr.Intn(4) {
				case 0:
					_, err = src.DB.Exec("INSERT INTO app.gk VALUES ($1, $2, 1, $3) ON CONFLICT DO NOTHING", a, b, doc)
				case 1:
					_, err = src.DB.Exec("DELETE FROM app.gk WHERE a = $1 AND b = $2", a, b)
				default: // đổi PK, doc (TOAST ngoài dòng) không đổi
					a2, b2 := key(rr)
					var res interface{ RowsAffected() (int64, error) }
					res, err = src.DB.Exec(`UPDATE app.gk SET a = $1, b = $2, v = v + 1 WHERE a = $3 AND b = $4
						AND NOT EXISTS (SELECT 1 FROM app.gk WHERE a = $1 AND b = $2)`, a2, b2, a, b)
					if err == nil {
						if n, _ := res.RowsAffected(); n > 0 {
							moves.Add(1)
						}
					}
				}
				if err == nil {
					ops.Add(1)
				}
				time.Sleep(time.Millisecond)
			}
		}(seed + int64(w))
	}
	time.Sleep(time.Second)
	if out, code := runStrandCLI(t, 30*time.Second, "snapshot", "add", "--table", "app.gk", "--socket", cfg.Socket); code != 0 {
		t.Fatalf("snapshot add: exit %d: %s", code, out)
	}
	time.Sleep(time.Second)
	waitSynced(t, &cfg, p, 5*time.Minute)
	time.Sleep(2 * time.Second)
	close(stop)
	wg.Wait()
	waitPGCaughtUp(t, src, dst, &cfg, p, 5*time.Minute)
	if code := p.stop(t); code != 0 {
		t.Fatalf("exit %d\n%s", code, p.tail())
	}
	if !strings.Contains(p.output.String(), `"snapshot done"`) {
		t.Fatalf("snapshot add did not finish\n%s", p.tail())
	}
	out, code := runStrandCLI(t, 5*time.Minute, "verify", "-c", cfg.write(t), "--table", "app.gk", "--mode", "full")
	if code != 0 {
		t.Fatalf("verify --mode full app.gk: exit %d\n%s", code, out)
	}
	dumpAll := func(n *pgNode) string {
		rows, err := n.DB.Query("SELECT t::text FROM app.gk t ORDER BY 1")
		if err != nil {
			t.Fatal(err)
		}
		defer rows.Close()
		var out []string
		for rows.Next() {
			var x string
			_ = rows.Scan(&x)
			out = append(out, x)
		}
		return strings.Join(out, "\n")
	}
	if a, b := dumpAll(src), dumpAll(dst); a != b {
		t.Fatalf("app.gk differs (%d vs %d rows)", strings.Count(a, "\n")+1, strings.Count(b, "\n")+1)
	}
	t.Logf("%d ops, %d PK moves; %s", ops.Load(), moves.Load(), strings.TrimSpace(out))
}
