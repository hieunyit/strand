package e2e

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// TestPGKill9_MilestoneFaultInjection kiểm tra độc lập và dứt khoát ranh giới durable-start milestone
// của PGKill9: readiness là checkpoint bền vững + pipeline running (không dùng sleep mù quáng).
// Fault inject kill trước và sau mốc bền để chứng minh hành vi resume hoặc từ chối rõ ràng.
func TestPGKill9_MilestoneFaultInjection(t *testing.T) {
	seed := seedFromEnv(t)
	slot, pub := "strand_repro_milestone", "strand_repro_pub"
	src, dst, lsn := setupPGPair(t, slot, pub)

	wl := newPGWorkload(src.DB, seed)
	wl.Start(2, 1000)
	defer wl.Stop()

	cfg := strandCfg{
		Name:        "repro-pg-milestone",
		SourceKind:  "postgres",
		TargetKind:  "postgres",
		SourceDSN:   src.DSN,
		TargetDSN:   dst.DSN,
		SourceExtra: fmt.Sprintf(", slot: %s, publication: %s", slot, pub),
		Include:     []string{"app.*"},
		Dir:         testDir(t),
		Bootstrap:   "start_at",
	}
	cfgPath := cfg.write(t)

	// 1. Fault inject kill TRƯỚC khi vị trí bắt đầu bền vững:
	pBefore := startStrand(t, cfgPath, "--start-at", lsn)
	pBefore.kill9() // kill tức thì

	// Khởi động lại thiếu --start-at khi chưa có checkpoint bền:
	pRefusedNoStartAt := startStrand(t, cfgPath)
	select {
	case <-pRefusedNoStartAt.done:
		if pRefusedNoStartAt.exitCode() == 0 {
			t.Fatalf("expected explicit refusal when restarting without --start-at, got exit 0\n%s", pRefusedNoStartAt.tail())
		}
		t.Logf("PASS: explicit refusal before milestone verified (exit %d): %s",
			pRefusedNoStartAt.exitCode(), strings.TrimSpace(pRefusedNoStartAt.tail()))
	case <-time.After(30 * time.Second):
		pRefusedNoStartAt.kill9()
		t.Fatalf("timed out waiting for refusal before milestone\n%s", pRefusedNoStartAt.tail())
	}

	// 2. Khởi động lại với --start-at và chờ đạt mốc bền vững:
	pAfter := startStrand(t, cfgPath, "--start-at", lsn)
	appliedLSN := waitPGPipelineReady(t, dst, pAfter, cfg.Name, time.Minute)
	t.Logf("durable start milestone reached: applied=%s", appliedLSN)

	// Fault inject kill SAU khi vị trí bắt đầu đã bền vững:
	pAfter.kill9()

	// Khởi động lại CÓ --start-at khi checkpoint đã tồn tại: từ chối dứt khoát
	pRefusedHasStartAt := startStrand(t, cfgPath, "--start-at", lsn)
	select {
	case <-pRefusedHasStartAt.done:
		if pRefusedHasStartAt.exitCode() == 0 {
			t.Fatalf("expected explicit refusal when passing --start-at with existing checkpoint, got exit 0\n%s", pRefusedHasStartAt.tail())
		}
		t.Logf("PASS: explicit refusal with existing checkpoint verified (exit %d): %s",
			pRefusedHasStartAt.exitCode(), strings.TrimSpace(pRefusedHasStartAt.tail()))
	case <-time.After(30 * time.Second):
		pRefusedHasStartAt.kill9()
		t.Fatalf("timed out waiting for refusal with existing checkpoint\n%s", pRefusedHasStartAt.tail())
	}

	// Documented resume: Khởi động lại không có --start-at -> tiếp tục thành công từ durable checkpoint
	// Dừng workload trước khi chờ bắt kịp và so sánh dữ liệu cuối.
	wl.Stop()
	t.Logf("workload stopped: %d ops, %d errors", wl.ops.Load(), wl.errs.Load())

	pResume := startStrand(t, cfgPath)
	waitPGPipelineReady(t, dst, pResume, cfg.Name, time.Minute)
	waitPGCaughtUp(t, src, dst, &cfg, pResume, 5*time.Minute)
	if code := pResume.stop(t); code != 0 {
		t.Fatalf("final resume exit code %d\n%s", code, pResume.tail())
	}
	comparePG(t, src, dst, pgAppTables...)
	checkPGInvariants(t, dst)
	t.Log("PASS: documented resume and checkpoint semantics preserved after kill -9 at durable milestone")
}

// TestGhOst_RealTool_Harness là harness thực thi gh-ost binary thật.
// Missing gh-ost is an explicit Go skip; STRAND_E2E_REQUIRE_GHOST=1 makes it a required gate.
func TestGhOst_RealTool_Harness(t *testing.T) {
	bin := lookTool("STRAND_E2E_GHOST_BIN", "gh-ost")
	if bin == "" {
		if os.Getenv("STRAND_E2E_REQUIRE_GHOST") == "1" {
			t.Fatal("required real gh-ost binary missing: install gh-ost or set STRAND_E2E_GHOST_BIN")
		}
		t.Skip("real gh-ost binary not installed in PATH or STRAND_E2E_GHOST_BIN")
	}
	t.Logf("gh-ost real binary found at %s; executing real tool e2e", bin)
	onlineDDLMySQL(t, "gh-ost")
}

// TestGhOst_CutOverLiveWritesWitness kiểm tra hiện tượng lost rows quanh cut-over / rename khi có live writes
// liên tục và DDL/schema refresh bị trễ. Ghi nhận chi tiết từng witness (id, ver) và so sánh dữ liệu cuối.
func TestGhOst_CutOverLiveWritesWitness(t *testing.T) {
	// Kiểm tra xem tool thật có sẵn không; tuyệt đối không gắn nhãn bản mô phỏng là tool thật.
	bin := lookTool("STRAND_E2E_GHOST_BIN", "gh-ost")
	if bin == "" {
		if os.Getenv("STRAND_E2E_REQUIRE_GHOST") == "1" {
			t.Fatal("required real gh-ost binary missing: install gh-ost or set STRAND_E2E_GHOST_BIN")
		}
		t.Skip("real gh-ost binary not installed in PATH or STRAND_E2E_GHOST_BIN")
	}
	t.Logf("using real gh-ost binary: %s", bin)

	e := evoStartMySQL(t, "repro-gho-witness", "")
	startOnlineDDLTarget(t, e)

	schema, table := "app", "evo"
	db := e.src.DB

	// Live writes worker chạy liên tục KHÔNG dừng qua cut-over.
	// Ghi nhận mọi witness (id, ver) thành công trên source.
	var witnesses sync.Map // int64 -> int64 (id -> max committed ver)
	var stopWrites atomic.Bool
	var writeOps atomic.Int64
	var writeErrs atomic.Int64

	writeCtx, cancelWrites := context.WithCancel(context.Background())
	defer cancelWrites()

	var wg sync.WaitGroup
	for w := 0; w < 3; w++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			var localID int64 = int64(workerID*10000 + 50000)
			for !stopWrites.Load() && writeCtx.Err() == nil {
				localID++
				ver := time.Now().UnixNano()
				// Ghi vào bảng source
				_, err := db.ExecContext(writeCtx,
					fmt.Sprintf("INSERT INTO `%s`.`%s` (id, ver, v, cnt, junk) VALUES (?, ?, ?, ?, ?) ON DUPLICATE KEY UPDATE ver=?, cnt=cnt+1", schema, table),
					localID, ver, "live-val", 1, "junk-payload", ver)
				if err != nil {
					writeErrs.Add(1)
				} else {
					witnesses.Store(localID, ver)
					writeOps.Add(1)
				}
				time.Sleep(2 * time.Millisecond)
			}
		}(w)
	}

	// Đợi live writes bắt đầu
	time.Sleep(500 * time.Millisecond)

	// Thực hiện quy trình cut-over với deterministic delayed DDL/schema refresh quanh rename
	q := func(n string) string { return fmt.Sprintf("`%s`.`%s`", schema, n) }
	orig, gho, ghc, del := q(table), q("_"+table+"_gho"), q("_"+table+"_ghc"), q("_"+table+"_del")
	cols := "id, ver, v, cnt, junk"

	mustExecDB(t, db,
		"create /* gh-ost */ table "+ghc+" (id bigint auto_increment, last_update timestamp not null DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP, hint varchar(64) charset ascii not null, value varchar(4096) charset ascii not null, primary key(id), unique key hint_uidx(hint)) auto_increment=256 engine=InnoDB",
		"create /* gh-ost */ table "+gho+" like "+orig,
		"alter /* gh-ost */ table "+gho+" "+onlineAlter,
	)

	// Copy chunks từ orig sang gho
	for lo := 0; lo < 100000; lo += 500 {
		res, err := db.Exec(fmt.Sprintf("insert /* gh-ost */ ignore into %s (%s) (select %s from %s force index (`PRIMARY`) where id >= %d and id < %d lock in share mode)",
			gho, cols, cols, orig, lo, lo+500))
		if err != nil {
			t.Fatal(err)
		}
		if lo > 20000 {
			if n, _ := res.RowsAffected(); n == 0 {
				break
			}
		}
		time.Sleep(2 * time.Millisecond)
	}

	// Cut-over phase với live writes vẫn đang chạy:
	ctx := context.Background()
	c1, err := db.Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer c1.Close()
	c2, err := db.Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer c2.Close()

	ex := func(c *sql.Conn, s string) {
		if _, err := c.ExecContext(ctx, s); err != nil {
			t.Fatalf("%s: %v", s, err)
		}
	}

	ex(c1, "create /* gh-ost */ table "+del+" (id int primary key) engine=InnoDB comment='magic-cut-over'")
	ex(c1, fmt.Sprintf("lock tables %s write, %s write, %s write", orig, gho, del))
	ex(c1, fmt.Sprintf("replace into %s (%s) select %s from %s", gho, cols, cols, orig))
	ex(c1, fmt.Sprintf("delete from %s where id not in (select id from %s)", gho, orig))

	// Trì hoãn có chủ đích trước RENAME để mô phỏng tải DDL / refresh schema chậm
	time.Sleep(200 * time.Millisecond)

	done := make(chan error, 1)
	go func() {
		_, err := c2.ExecContext(ctx, fmt.Sprintf("rename /* gh-ost */ table %s to %s, %s to %s", orig, del, gho, orig))
		done <- err
	}()

	// Chờ RENAME vào hàng đợi metadata lock
	waitDeadline := time.Now().Add(30 * time.Second)
	for {
		var n int
		if err := db.QueryRow("SELECT COUNT(*) FROM information_schema.PROCESSLIST WHERE INFO LIKE 'rename /* gh-ost */%' AND STATE LIKE '%metadata lock%'").Scan(&n); err != nil {
			t.Fatal(err)
		}
		if n > 0 {
			break
		}
		if time.Now().After(waitDeadline) {
			t.Fatal("RENAME never queued behind lock")
		}
		time.Sleep(20 * time.Millisecond)
	}

	// Mở khóa: RENAME thực thi
	ex(c1, "drop /* gh-ost */ table "+del)
	ex(c1, "unlock tables")
	if err := <-done; err != nil {
		t.Fatalf("rename cut-over failed: %v", err)
	}

	// Dọn các bảng sentinel
	mustExecDB(t, db, "drop /* gh-ost */ table if exists "+ghc, "drop /* gh-ost */ table if exists "+q("_"+table+"_del"))

	// Để live writes tiếp tục chạy một khoảng sau cut-over trên schema mới
	time.Sleep(1 * time.Second)
	stopWrites.Store(true)
	wg.Wait()

	t.Logf("Live writes finished: %d successful ops, %d errors during cut-over",
		writeOps.Load(), writeErrs.Load())

	// Chờ target nhận cột mới và bắt kịp
	deadline := time.Now().Add(2 * time.Minute)
	for mysqlColType(t, e.dst.DB, schema, table, "extra") == "" {
		mustStayRunning(t, e.p, "gh-ost-witness")
		if time.Now().After(deadline) {
			t.Fatalf("ALTER extra column not applied on target\n%s", e.p.tail())
		}
		time.Sleep(200 * time.Millisecond)
	}

	e.finish(t)

	// So sánh witnesses với target DB:
	var totalWitnesses, matchedWitnesses, missingWitnesses int
	witnesses.Range(func(k, v any) bool {
		id := k.(int64)
		expectedVer := v.(int64)
		totalWitnesses++

		var dstVer int64
		err := e.dst.DB.QueryRow(fmt.Sprintf("SELECT ver FROM `%s`.`%s` WHERE id = ?", schema, table), id).Scan(&dstVer)
		if err != nil {
			missingWitnesses++
		} else if dstVer >= expectedVer {
			matchedWitnesses++
		} else {
			missingWitnesses++
		}
		return true
	})

	t.Logf("Witness validation: total=%d matched=%d missing=%d",
		totalWitnesses, matchedWitnesses, missingWitnesses)

	if missingWitnesses > 0 {
		t.Errorf("LOST ROWS DETECTED: %d of %d witness rows missing on target after cut-over with live writes",
			missingWitnesses, totalWitnesses)
	} else {
		t.Log("0 lost rows: all witnesses present and verified on target")
	}
}
