package pipeline

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"math/rand"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"strand/internal/config"
)

type gpkShape struct {
	name, ddl string
	pk        []string
	key       func(r *rand.Rand) []any // giá trị các cột PK
}

func gpkShapes() []gpkShape {
	strs := []string{"a", "A", "b", "ab", "ab ", "abc", "é", "e", "zz", "z", "ña", "中", "😀", "0", "10", "9", " x", "x  ", "Z", "aa", "aaa"}
	return []gpkShape{
		{"comp_int", "a INT NOT NULL, b BIGINT NOT NULL, v INT, PRIMARY KEY (a, b)", []string{"a", "b"},
			func(r *rand.Rand) []any { return []any{r.Intn(30) - 15, r.Int63n(40) - 20} }},
		{"str_bin", "k VARCHAR(24) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin NOT NULL, v INT, PRIMARY KEY (k)", []string{"k"},
			func(r *rand.Rand) []any {
				return []any{fmt.Sprintf("%s%d%s", strs[r.Intn(len(strs))], r.Intn(40), strs[r.Intn(len(strs))])}
			}},
		{"str_ci", "k VARCHAR(24) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NOT NULL, n INT NOT NULL, v INT, PRIMARY KEY (k, n)", []string{"k", "n"},
			func(r *rand.Rand) []any {
				ci := []string{"a", "A", "á", "Á", "à", "ab", "AB", "Ab ", "e", "é", "É", "ß", "ss", "SS", "z", "Z", "中", "😀", ""}
				return []any{fmt.Sprintf("%s%d%s", ci[r.Intn(len(ci))], r.Intn(15), ci[r.Intn(len(ci))]), r.Intn(3)}
			}},
		{"dt_int", "d DATETIME(3) NOT NULL, n INT NOT NULL, v INT, PRIMARY KEY (d, n)", []string{"d", "n"},
			func(r *rand.Rand) []any {
				return []any{time.Unix(1700000000+r.Int63n(60)*3600, r.Int63n(3)*500e6).UTC().Format("2006-01-02 15:04:05.000"), r.Intn(20)}
			}},
		{"bin_dec", "b VARBINARY(6) NOT NULL, x DECIMAL(12,4) NOT NULL, v INT, PRIMARY KEY (b, x)", []string{"b", "x"},
			func(r *rand.Rand) []any {
				bs := make([]byte, r.Intn(3))
				for i := range bs {
					bs[i] = []byte{0, 1, 0x7f, 0xff}[r.Intn(4)]
				}
				return []any{bs, fmt.Sprintf("%d.%04d", r.Intn(20)-10, r.Intn(3)*2500)}
			}},
	}
}

func gpkWhere(pk []string) string {
	w := make([]string, len(pk))
	for i, c := range pk {
		w[i] = c + " = ?"
	}
	return strings.Join(w, " AND ")
}

// dumpTable: mọi row dạng text, đã sắp — để so source/target.
func dumpTable(ctx context.Context, db *sql.DB, table string) ([]string, error) {
	rows, err := db.QueryContext(ctx, "SELECT * FROM "+table)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	cols, _ := rows.Columns()
	var out []string
	for rows.Next() {
		raw := make([]sql.RawBytes, len(cols))
		ptrs := make([]any, len(raw))
		for i := range raw {
			ptrs[i] = &raw[i]
		}
		if err := rows.Scan(ptrs...); err != nil {
			return nil, err
		}
		parts := make([]string, len(raw))
		for i, b := range raw {
			parts[i] = fmt.Sprintf("%q", b)
		}
		out = append(out, strings.Join(parts, ","))
	}
	sort.Strings(out)
	return out, rows.Err()
}

// invariant: RS-1/RS-2, CP-2 — việc C mức 1, MySQL→MySQL. Snapshot logic các bảng có PK ghép / chuỗi
// _bin PAD SPACE / DATETIME+int / VARBINARY+DECIMAL dưới tải ghi đồng thời (INSERT, UPDATE, DELETE, UPDATE
// đổi PK xuyên chunk; chunk 40 row): target cuối giống hệt source.
func TestGPK_MySQLSnapshotUnderLoadWithGenericKeys(t *testing.T) {
	db, dsn := startTestDB(t, "mysql")
	ctx := context.Background()
	mustExec := func(q string, args ...any) {
		t.Helper()
		if _, err := db.ExecContext(ctx, q, args...); err != nil {
			t.Fatalf("%s: %v", q, err)
		}
	}
	mustExec("CREATE DATABASE dst")
	shapes := gpkShapes()
	if only := os.Getenv("GPK_ONLY"); only != "" {
		var keep []gpkShape
		for _, sh := range shapes {
			if sh.name == only {
				keep = append(keep, sh)
			}
		}
		shapes = keep
	}
	noLoad := os.Getenv("GPK_NOLOAD") != ""
	r := rand.New(rand.NewSource(11))
	include := []string{}
	rename := map[string]string{}
	for _, sh := range shapes {
		mustExec(fmt.Sprintf("CREATE TABLE app.%s (%s)", sh.name, sh.ddl))
		mustExec(fmt.Sprintf("CREATE TABLE dst.%s LIKE app.%s", sh.name, sh.name))
		for i := 0; i < 600; i++ {
			k := sh.key(r)
			ph := strings.TrimSuffix(strings.Repeat("?,", len(k)+1), ",")
			mustExec(fmt.Sprintf("INSERT IGNORE INTO app.%s (%s, v) VALUES (%s)", sh.name, strings.Join(sh.pk, ","), ph), append(k, i)...)
		}
		include = append(include, "app."+sh.name)
		rename["app."+sh.name] = "dst." + sh.name
	}

	// Tải ghi đồng thời suốt snapshot.
	stop := make(chan struct{})
	var wg sync.WaitGroup
	var ops atomic.Int64
	for _, sh := range shapes {
		sh := sh
		wg.Add(1)
		go func() {
			defer wg.Done()
			rr := rand.New(rand.NewSource(int64(len(sh.name)) * 977))
			for {
				select {
				case <-stop:
					return
				default:
				}
				if noLoad {
					time.Sleep(10 * time.Millisecond)
					continue
				}
				k := sh.key(rr)
				ph := strings.TrimSuffix(strings.Repeat("?,", len(k)+1), ",")
				var err error
				switch rr.Intn(4) {
				case 0:
					_, err = db.ExecContext(ctx, fmt.Sprintf("INSERT IGNORE INTO app.%s (%s, v) VALUES (%s)", sh.name, strings.Join(sh.pk, ","), ph), append(k, rr.Intn(1000))...)
				case 1:
					_, err = db.ExecContext(ctx, fmt.Sprintf("UPDATE app.%s SET v = v + 1 WHERE %s", sh.name, gpkWhere(sh.pk)), k...)
				case 2:
					_, err = db.ExecContext(ctx, fmt.Sprintf("DELETE FROM app.%s WHERE %s", sh.name, gpkWhere(sh.pk)), k...)
				case 3: // đổi PK (row chuyển sang chunk khác)
					nk := sh.key(rr)
					set := make([]string, len(sh.pk))
					for i, c := range sh.pk {
						set[i] = c + " = ?"
					}
					_, err = db.ExecContext(ctx, fmt.Sprintf("UPDATE IGNORE app.%s SET %s WHERE %s", sh.name, strings.Join(set, ", "), gpkWhere(sh.pk)), append(nk, k...)...)
				}
				if err == nil {
					ops.Add(1)
				}
				time.Sleep(time.Millisecond)
			}
		}()
	}

	dir := t.TempDir()
	cfg := &config.Config{Name: "gpk",
		Source: config.SourceConfig{Kind: "mysql", DSN: dsn, ServerID: 4343},
		Target: config.TargetConfig{Kind: "mysql", DSN: strings.Replace(dsn, "/app?", "/dst?", 1)},
		Tables: config.TablesConfig{Include: include, Rename: rename}}
	cfg.Resources.Spool.Path = dir
	cfg.Resources.Workers.Max = 4
	cfg.Governor.Algorithm = "static"
	cfg.Resources.Source.IOPS.Soft = 100000
	cfg.Bootstrap.Logical.ChunkRows = "40"
	rctx, cancel := context.WithCancel(ctx)
	done := make(chan error, 1)
	go func() {
		done <- Run(rctx, Options{Config: cfg, ShutdownTimeout: 5 * time.Second, Logger: slog.New(slog.NewTextHandler(io.Discard, nil))})
	}()
	defer func() {
		cancel()
		<-done
	}()
	// Chờ mọi bảng snapshot xong.
	deadline := time.Now().Add(3 * time.Minute)
	for {
		raw, _ := os.ReadFile(filepath.Join(dir, "pipeline-state.json"))
		var st stateFile
		_ = json.Unmarshal(raw, &st)
		all := true
		for _, n := range include {
			all = all && st.Done[n]
		}
		if all {
			break
		}
		select {
		case err := <-done:
			t.Fatalf("pipeline exited during snapshot: %v", err)
		default:
		}
		if time.Now().After(deadline) {
			t.Fatalf("snapshot did not finish: %+v", st.Done)
		}
		time.Sleep(100 * time.Millisecond)
	}
	time.Sleep(time.Second) // tải tiếp sau snapshot
	close(stop)
	wg.Wait()
	// Target bắt kịp rồi so.
	for _, sh := range shapes {
		var srcRows, dstRows []string
		var err error
		ok := false
		for i := 0; i < 120 && !ok; i++ {
			if srcRows, err = dumpTable(ctx, db, "app."+sh.name); err != nil {
				t.Fatal(err)
			}
			if dstRows, err = dumpTable(ctx, db, "dst."+sh.name); err != nil {
				t.Fatal(err)
			}
			ok = strings.Join(srcRows, "\n") == strings.Join(dstRows, "\n")
			if !ok {
				time.Sleep(500 * time.Millisecond)
			}
		}
		if !ok {
			t.Fatalf("%s: target differs from source (%d vs %d rows)", sh.name, len(dstRows), len(srcRows))
		}
		t.Logf("%s: %d rows identical", sh.name, len(srcRows))
	}
	t.Logf("workload: %d ops during snapshot", ops.Load())
}
