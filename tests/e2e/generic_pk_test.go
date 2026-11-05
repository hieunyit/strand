package e2e

import (
	"fmt"
	"math/rand"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// Việc C (mức 1), binary thật: snapshot logic MySQL→MySQL của bảng PK ghép (INT, BIGINT) và VARCHAR
// utf8mb4_bin + INT dưới tải ghi đồng thời (INSERT, UPDATE, DELETE, UPDATE đổi PK xuyên chunk; chunk 50
// row), bảng đích do schema-init tạo; cuối: `verify --mode full` 0 lệch và dump hai phía giống hệt.
func TestC_E2E_GenericPKSnapshotUnderLoad(t *testing.T) {
	seed := seedFromEnv(t)
	src, dst := newPair(t)
	tables := []struct {
		name, ddl string
		pk        []string
		key       func(r *rand.Rand) []any
	}{
		{"gk_comp", "a INT NOT NULL, b BIGINT NOT NULL, v INT, PRIMARY KEY (a, b)", []string{"a", "b"},
			func(r *rand.Rand) []any { return []any{r.Intn(60) - 30, r.Int63n(80) - 40} }},
		{"gk_str", "k VARCHAR(24) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin NOT NULL, n INT NOT NULL, v INT, PRIMARY KEY (k, n)", []string{"k", "n"},
			func(r *rand.Rand) []any {
				w := []string{"a", "A", "ab", "ab ", "é", "e", "中", "😀", "10", "9", "z", "Z"}
				return []any{fmt.Sprintf("%s%d", w[r.Intn(len(w))], r.Intn(60)), r.Intn(4)}
			}},
	}
	r := rand.New(rand.NewSource(seed))
	var include []string
	for _, tb := range tables {
		src.exec(t, fmt.Sprintf("CREATE TABLE app.%s (%s)", tb.name, tb.ddl))
		for i := 0; i < 2000; i++ {
			k := tb.key(r)
			ph := strings.TrimSuffix(strings.Repeat("?,", len(k)+1), ",")
			if _, err := src.DB.Exec(fmt.Sprintf("INSERT IGNORE INTO app.%s (%s, v) VALUES (%s)", tb.name, strings.Join(tb.pk, ","), ph), append(k, i)...); err != nil {
				t.Fatal(err)
			}
		}
		include = append(include, "app."+tb.name)
	}
	where := func(pk []string) string {
		w := make([]string, len(pk))
		for i, c := range pk {
			w[i] = c + " = ?"
		}
		return strings.Join(w, " AND ")
	}
	stop := make(chan struct{})
	var wg sync.WaitGroup
	var ops atomic.Int64
	for i, tb := range tables {
		tb := tb
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
					k := tb.key(rr)
					ph := strings.TrimSuffix(strings.Repeat("?,", len(k)+1), ",")
					var err error
					switch rr.Intn(4) {
					case 0:
						_, err = src.DB.Exec(fmt.Sprintf("INSERT IGNORE INTO app.%s (%s, v) VALUES (%s)", tb.name, strings.Join(tb.pk, ","), ph), append(k, 1)...)
					case 1:
						_, err = src.DB.Exec(fmt.Sprintf("UPDATE app.%s SET v = v + 1 WHERE %s", tb.name, where(tb.pk)), k...)
					case 2:
						_, err = src.DB.Exec(fmt.Sprintf("DELETE FROM app.%s WHERE %s", tb.name, where(tb.pk)), k...)
					default:
						nk := tb.key(rr)
						set := make([]string, len(tb.pk))
						for j, c := range tb.pk {
							set[j] = c + " = ?"
						}
						_, err = src.DB.Exec(fmt.Sprintf("UPDATE IGNORE app.%s SET %s WHERE %s", tb.name, strings.Join(set, ", "), where(tb.pk)), append(nk, k...)...)
					}
					if err == nil {
						ops.Add(1)
					}
					time.Sleep(2 * time.Millisecond)
				}
			}(seed + int64(i*10+w))
		}
	}

	cfg := strandCfg{Name: "e2e-generic-pk", SourceDSN: src.RootDSN, TargetDSN: dst.RootDSN + "strand",
		Include: include, Dir: testDir(t), CreateSchema: "auto", ChunkRows: "50", VerifyDelta: "on"}
	path := cfg.write(t)
	p := startStrand(t, path)
	waitSynced(t, &cfg, p, 5*time.Minute)
	time.Sleep(2 * time.Second)
	// Verify trực tiếp theo tuple PK khi pipeline đang chạy dưới tải: không báo lệch giả.
	for _, tb := range tables {
		out, code := runStrandCLI(t, 10*time.Minute, "verify", "-c", path, "--table", "app."+tb.name, "--mode", "sample", "--rate", "1.0")
		if code != 0 {
			t.Fatalf("verify sample app.%s under load: exit %d\n%s", tb.name, code, out)
		}
		t.Logf("%s", strings.TrimSpace(out))
	}
	out, code := runStrandCLI(t, 10*time.Minute, "verify", "-c", path, "--mode", "delta", "--socket", cfg.Socket)
	if code != 0 || !strings.Contains(out, "changed keys") {
		t.Fatalf("verify delta under load: exit %d\n%s", code, out)
	}
	t.Logf("%s", strings.TrimSpace(out))
	close(stop)
	wg.Wait()
	waitCaughtUp(t, src, dst, &cfg, p, 3*time.Minute)
	if code := p.stop(t); code != 0 {
		t.Fatalf("exit %d\n%s", code, p.tail())
	}
	for _, tb := range tables {
		out, code := runStrandCLI(t, 5*time.Minute, "verify", "-c", path, "--table", "app."+tb.name, "--mode", "full")
		if code != 0 {
			t.Fatalf("verify --mode full app.%s: exit %d\n%s", tb.name, code, out)
		}
		t.Logf("%s", strings.TrimSpace(out))
		a := dumpE2E(t, src, "app."+tb.name)
		b := dumpE2E(t, dst, "app."+tb.name)
		if a != b {
			t.Fatalf("app.%s differs between source and target", tb.name)
		}
	}
	t.Logf("workload: %d ops", ops.Load())
}

func dumpE2E(t *testing.T, n *mysqlNode, table string) string {
	t.Helper()
	rs, err := n.DB.Query("SELECT * FROM " + table)
	if err != nil {
		t.Fatal(err)
	}
	defer rs.Close()
	cols, _ := rs.Columns()
	var out []string
	for rs.Next() {
		raw := make([][]byte, len(cols))
		ptrs := make([]any, len(raw))
		for i := range raw {
			ptrs[i] = &raw[i]
		}
		if err := rs.Scan(ptrs...); err != nil {
			t.Fatal(err)
		}
		parts := make([]string, len(raw))
		for i, b := range raw {
			parts[i] = fmt.Sprintf("%q", b)
		}
		out = append(out, strings.Join(parts, ","))
	}
	sort.Strings(out)
	return strings.Join(out, "\n")
}
