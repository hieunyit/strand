package pipeline

import (
	"context"
	"encoding/json"
	"errors"
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
	"strand/internal/exitcode"
)

// invariant: RS-1/RS-2 — việc C mức 1, MySQL→PG: PK (INT, BIGINT) / (DATETIME(3), INT) / (VARBINARY,
// DECIMAL) / BIGINT UNSIGNED (> MaxInt64 sang numeric) snapshot dưới tải; PK chuỗi có collation bị từ chối
// rõ ràng (thứ tự MySQL ≠ "C" của PG).
func TestGPK_MySQLToPGSnapshotWithGenericKeys(t *testing.T) {
	my, myDSN := startTestDB(t, "mysql")
	pg, pgDSN := startTestDB(t, "pg")
	ctx := context.Background()
	myExec := func(q string, args ...any) {
		t.Helper()
		if _, err := my.ExecContext(ctx, q, args...); err != nil {
			t.Fatalf("%s: %v", q, err)
		}
	}
	pgExec := func(q string) {
		t.Helper()
		if _, err := pg.ExecContext(ctx, q); err != nil {
			t.Fatalf("%s: %v", q, err)
		}
	}
	shapes := gpkShapes()
	keep := map[string]string{ // bảng đích PG tương ứng
		"comp_int": "a int4 NOT NULL, b int8 NOT NULL, v int4, PRIMARY KEY (a, b)",
		"dt_int":   "d timestamp(3) NOT NULL, n int4 NOT NULL, v int4, PRIMARY KEY (d, n)",
		"bin_dec":  "b bytea NOT NULL, x numeric(12,4) NOT NULL, v int4, PRIMARY KEY (b, x)",
	}
	r := rand.New(rand.NewSource(3))
	var use []gpkShape
	for _, sh := range shapes {
		if keep[sh.name] == "" {
			continue
		}
		use = append(use, sh)
		myExec(fmt.Sprintf("CREATE TABLE app.%s (%s)", sh.name, sh.ddl))
		pgExec(fmt.Sprintf("CREATE TABLE public.%s (%s)", sh.name, keep[sh.name]))
		for i := 0; i < 400; i++ {
			k := sh.key(r)
			ph := strings.TrimSuffix(strings.Repeat("?,", len(k)+1), ",")
			myExec(fmt.Sprintf("INSERT IGNORE INTO app.%s (%s, v) VALUES (%s)", sh.name, strings.Join(sh.pk, ","), ph), append(k, i)...)
		}
	}
	myExec("CREATE TABLE app.uns (u BIGINT UNSIGNED NOT NULL PRIMARY KEY, v INT)")
	pgExec("CREATE TABLE public.uns (u numeric(20,0) NOT NULL PRIMARY KEY, v int4)")
	for i := 0; i < 300; i++ {
		myExec("INSERT IGNORE INTO app.uns VALUES (?, ?)", uint64(1<<63)-150+uint64(i)*3, i)
	}
	include := []string{"app.comp_int", "app.dt_int", "app.bin_dec", "app.uns"}
	rename := map[string]string{}
	for _, n := range include {
		rename[n] = "public." + strings.TrimPrefix(n, "app.")
	}

	stop := make(chan struct{})
	var wg sync.WaitGroup
	var ops atomic.Int64
	for _, sh := range use {
		sh := sh
		wg.Add(1)
		go func() {
			defer wg.Done()
			rr := rand.New(rand.NewSource(int64(len(sh.name)) * 7))
			for {
				select {
				case <-stop:
					return
				default:
				}
				k := sh.key(rr)
				ph := strings.TrimSuffix(strings.Repeat("?,", len(k)+1), ",")
				var err error
				switch rr.Intn(3) {
				case 0:
					_, err = my.ExecContext(ctx, fmt.Sprintf("INSERT IGNORE INTO app.%s (%s, v) VALUES (%s)", sh.name, strings.Join(sh.pk, ","), ph), append(k, 7)...)
				case 1:
					_, err = my.ExecContext(ctx, fmt.Sprintf("UPDATE app.%s SET v = v + 1 WHERE %s", sh.name, gpkWhere(sh.pk)), k...)
				default:
					_, err = my.ExecContext(ctx, fmt.Sprintf("DELETE FROM app.%s WHERE %s", sh.name, gpkWhere(sh.pk)), k...)
				}
				if err == nil {
					ops.Add(1)
				}
				time.Sleep(time.Millisecond)
			}
		}()
	}
	mkcfg := func(name string, inc []string) (*config.Config, string) {
		dir := t.TempDir()
		cfg := &config.Config{Name: name,
			Source: config.SourceConfig{Kind: "mysql", DSN: myDSN, ServerID: 5151},
			Target: config.TargetConfig{Kind: "postgres", DSN: pgDSN},
			Tables: config.TablesConfig{Include: inc, Rename: rename}}
		cfg.Resources.Spool.Path = dir
		cfg.Resources.Workers.Max = 4
		cfg.Governor.Algorithm = "static"
		cfg.Resources.Source.IOPS.Soft = 100000
		cfg.Bootstrap.Logical.ChunkRows = "30"
		return cfg, dir
	}
	cfg, dir := mkcfg("gpkmypg", include)
	rctx, cancel := context.WithCancel(ctx)
	done := make(chan error, 1)
	go func() {
		done <- Run(rctx, Options{Config: cfg, ShutdownTimeout: 5 * time.Second, Logger: slog.New(slog.NewTextHandler(io.Discard, nil))})
	}()
	deadline := time.Now().Add(4 * time.Minute)
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
	time.Sleep(time.Second)
	close(stop)
	wg.Wait()
	// So theo số row + giá trị v theo khóa (dạng text của hai engine khác nhau).
	count := func(q string, db interface {
		QueryRowContext(context.Context, string, ...any) interface{ Scan(...any) error }
	}) {
	}
	_ = count
	for _, tb := range []string{"comp_int", "dt_int", "bin_dec", "uns"} {
		ok := false
		var sm, pm string
		for i := 0; i < 120 && !ok; i++ {
			var a, b []string
			rows, err := my.QueryContext(ctx, fmt.Sprintf("SELECT CONCAT_WS('|', COUNT(*), COALESCE(SUM(v), 0)) FROM app.%s", tb))
			if err != nil {
				t.Fatal(err)
			}
			for rows.Next() {
				var s string
				_ = rows.Scan(&s)
				a = append(a, s)
			}
			rows.Close()
			prow, err := pg.QueryContext(ctx, fmt.Sprintf("SELECT concat_ws('|', count(*), COALESCE(sum(v), 0)) FROM public.%s", tb))
			if err != nil {
				t.Fatal(err)
			}
			for prow.Next() {
				var s string
				_ = prow.Scan(&s)
				b = append(b, s)
			}
			prow.Close()
			sort.Strings(a)
			sort.Strings(b)
			sm, pm = strings.Join(a, ","), strings.Join(b, ",")
			if ok = sm == pm; !ok {
				time.Sleep(500 * time.Millisecond)
			}
		}
		if !ok {
			t.Fatalf("%s: MySQL count|sum(v) %s, PG %s", tb, sm, pm)
		}
		t.Logf("%s: count|sum(v) %s on both", tb, sm)
	}
	cancel()
	<-done
	t.Logf("workload: %d ops", ops.Load())

	// PK chuỗi mà snapshot logic chưa chứng minh được (latin1: khóa bị chuyển mã sang PG nên không còn là
	// giá trị source để tính WEIGHT_STRING): từ chối rõ ràng. utf8mb4 → "C" nay được hỗ trợ (flush theo
	// tập khóa, gate2/collation). ctx có hạn: nếu bảng bị nhận nhầm, pipeline không chạy mãi.
	myExec("CREATE TABLE app.strk (k VARCHAR(10) CHARACTER SET latin1 COLLATE latin1_swedish_ci NOT NULL PRIMARY KEY, v INT)")
	myExec("INSERT INTO app.strk VALUES ('a', 1)")
	pgExec(`CREATE TABLE public.strk (k varchar(10) COLLATE "C" NOT NULL PRIMARY KEY, v int4)`)
	rename["app.strk"] = "public.strk"
	cfg2, _ := mkcfg("gpkmypg_str", []string{"app.strk"})
	rctx, rcancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer rcancel()
	err := Run(rctx, Options{Config: cfg2, ShutdownTimeout: 5 * time.Second, Logger: slog.New(slog.NewTextHandler(io.Discard, nil))})
	var x *exitcode.Error
	if !errors.As(err, &x) || x.Code != exitcode.ErrTypeUnmappable || !strings.Contains(err.Error(), "seed it from a backup") {
		t.Fatalf("string PK MySQL→PG: Run = %v, want a clear rejection", err)
	}
}
