package e2e

import (
	"database/sql"
	"fmt"
	"math/rand"
	"regexp"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

var (
	fkParentStartRe = regexp.MustCompile(`"msg":"snapshot start"[^\n]*"table":"app.items"`)
	fkParentDoneRe  = regexp.MustCompile(`"msg":"snapshot done"[^\n]*"table":"app.items"`)
)

// review N10: MySQL → PG, schema đích do Strand tạo (có FK children.pid → items.id). Bảng cha items
// được snapshot chậm (chunk nhỏ, IOPS thấp); trong lúc đó tải INSERT row con trỏ tới row cha ở phần
// items CHƯA đọc. Trước bản sửa, event của children (chờ snapshot, range chưa đăng ký) được apply thẳng
// vào PG → vi phạm FK → pipeline dừng. Sau bản sửa: bootstrap xong, hai bảng giống hệt source, verify full qua.
func TestN10_E2E_MySQLToPGFKChildBeforeParentSnapshot(t *testing.T) {
	seed := seedFromEnv(t)
	src := startMySQL(t, mysqlImage("source"), "src", 1, nil)
	dst := startPG(t, "pgdst")
	run := func(db *sql.DB, q string, args ...any) {
		t.Helper()
		if _, err := db.Exec(q, args...); err != nil {
			t.Fatalf("%s: %v", q, err)
		}
	}
	run(src.DB, "CREATE DATABASE app")
	run(src.DB, "CREATE TABLE app.items (id bigint PRIMARY KEY, v varchar(100) NOT NULL, ver bigint NOT NULL)")
	run(src.DB, "CREATE TABLE app.children (id bigint PRIMARY KEY, v varchar(100) NOT NULL, ver bigint NOT NULL, pid bigint NOT NULL, FOREIGN KEY (pid) REFERENCES app.items(id))")
	const parents = 20000
	for batch := 0; batch < parents/500; batch++ {
		var values []string
		for j := 1; j <= 500; j++ {
			values = append(values, fmt.Sprintf("(%d,'p',1)", batch*500+j))
		}
		run(src.DB, "INSERT INTO app.items VALUES "+strings.Join(values, ","))
	}
	run(src.DB, "INSERT INTO app.children VALUES (1,'c',1,1),(2,'c',1,2)")

	cfg := strandCfg{Name: "n10-fk", TargetKind: "postgres", SourceDSN: src.RootDSN, TargetDSN: dst.DSN,
		Include: []string{"app.*"}, Dir: testDir(t), SourceIOPS: 20, ChunkRows: "500", LagSLO: "10m"}
	path := cfg.write(t)
	p := startStrand(t, path)
	t.Cleanup(func() {
		if !p.exited() {
			p.kill9()
		}
	})

	// Tải chỉ bắt đầu khi items đang snapshot: con trỏ tới cha ở nửa sau (chưa đọc).
	deadline := time.Now().Add(2 * time.Minute)
	for !fkParentStartRe.MatchString(p.output.String()) {
		if p.exited() || time.Now().After(deadline) {
			t.Fatalf("parent snapshot did not start (exited=%v)\n%s", p.exited(), p.tail())
		}
		time.Sleep(50 * time.Millisecond)
	}
	var stopped atomic.Bool
	var inserted atomic.Int64
	loadErr := make(chan error, 1)
	done := make(chan struct{})
	go func() {
		defer close(done)
		r := rand.New(rand.NewSource(seed))
		next := int64(100)
		for !stopped.Load() {
			pid := parents/2 + r.Int63n(parents/2) // phần items chưa được snapshot
			var err error
			if r.Intn(4) == 0 {
				_, err = src.DB.Exec("UPDATE app.children SET ver=ver+1, pid=? WHERE id=1", pid)
			} else {
				_, err = src.DB.Exec("INSERT INTO app.children VALUES (?, 'load', 1, ?)", next, pid)
				next++
			}
			if err != nil {
				loadErr <- err
				return
			}
			inserted.Add(1)
			time.Sleep(20 * time.Millisecond)
		}
	}()
	t.Cleanup(func() { stopped.Store(true); <-done })

	// Tiền đề: có commit của bảng con trong lúc items còn đang snapshot.
	deadline = time.Now().Add(2 * time.Minute)
	for inserted.Load() < 50 {
		if p.exited() || time.Now().After(deadline) {
			t.Fatalf("no child load during the parent snapshot (ops=%d, exited=%v)\n%s", inserted.Load(), p.exited(), p.tail())
		}
		time.Sleep(50 * time.Millisecond)
	}
	if fkParentDoneRe.MatchString(p.output.String()) {
		t.Fatalf("parent snapshot finished before the child load overlapped it; slow the snapshot down")
	}
	waitSynced(t, &cfg, p, 6*time.Minute)
	stopped.Store(true)
	<-done
	select {
	case err := <-loadErr:
		t.Fatal(err)
	default:
	}
	waitMySQLPGCaughtUp(t, src, dst, &cfg, p, 3*time.Minute)
	t.Logf("child ops during bootstrap: %d", inserted.Load())

	for _, q := range []string{"SELECT id, v, ver FROM app.items ORDER BY id", "SELECT id, v, ver, pid FROM app.children ORDER BY id"} {
		a := dumpCanon(t, src.DB, q, -1)
		b := dumpCanon(t, dst.DB, q, -1)
		if len(a) != len(b) {
			t.Fatalf("%s: source=%d target=%d rows", q, len(a), len(b))
		}
		for k, v := range a {
			if strings.Join(b[k], "|") != strings.Join(v, "|") {
				t.Fatalf("%s: row %s source=%v target=%v", q, k, v, b[k])
			}
		}
	}
	if code := p.stop(t); code != 0 {
		t.Fatalf("strand exit code %d\n%s", code, p.tail())
	}
	for _, table := range []string{"app.items", "app.children"} {
		out, code := runStrandCLI(t, 3*time.Minute, "verify", "-c", path, "--mode", "full", "--table", table, "--rate", "1000")
		if code != 0 {
			t.Fatalf("verify %s exit %d:\n%s", table, code, out)
		}
	}
}
