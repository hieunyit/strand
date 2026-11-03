package pg_test

import (
	"context"
	"errors"
	"fmt"
	"math/big"
	"strings"
	"testing"
	"time"

	"strand/internal/checkpoint"
	"strand/internal/exitcode"
	"strand/internal/model"
	"strand/internal/reader/pg"
)

const readTimeout = 30 * time.Second

func exitOf(err error) int { return exitcode.GetExitCode(err) }

// AC1: ghi 1.000 Txn, đọc 400, Close, Start lại với AppliedSet chứa 400 Txn đầu → nhận đúng 600 Txn
// còn lại, không trùng, không thiếu. Slot được xác nhận (flush) mới tới Txn 200 nên server gửi lại
// 201..400 và reader phải tự bỏ qua (CP-2). Kèm kiểm: lwd thấp hơn confirmed_flush → log_gap (exit 5).
func TestAC1_ResumeFromSlot(t *testing.T) {
	inst := sharedPG(t)
	db := inst.db
	pub := setupTables(t, db, "ac1", "CREATE TABLE ac1 (id int PRIMARY KEY, val text)", "ac1")
	slot := "ac1_slot"
	dropSlotLater(t, db, slot)
	cfg := pg.Config{DSN: inst.dsn, Slot: slot, Publication: pub, CreateSlot: true, StatusInterval: 200 * time.Millisecond}

	r0 := newReader(t, cfg)
	if r0.ConsistentPoint().LSN == 0 {
		t.Fatal("tạo slot phải trả consistent_point")
	}
	_ = r0.Close()

	for i := 0; i < 1000; i++ {
		mustExec(t, db, "INSERT INTO ac1 (id, val) VALUES ($1, $2)", i, fmt.Sprintf("v%d", i))
	}

	cfg.CreateSlot = false
	r1 := newReader(t, cfg)
	ch, err := r1.Start(context.Background(), nil)
	if err != nil {
		t.Fatal(err)
	}
	if v, s, tp := r1.Protocol(); v != 4 || !s || !tp {
		t.Fatalf("PG18 phải nhận proto 4 + streaming + two_phase, được %d %v %v", v, s, tp)
	}
	tr := newTxnReader(t, ch)
	lsns := make([]uint64, 400)
	for i := 0; i < 400; i++ {
		txn := tr.next(readTimeout)
		rows := txnRows(t, txn)
		if len(rows) != 1 || col(t, rows[0], "id") != int64(i) {
			t.Fatalf("Txn %d sai nội dung: %+v", i, rows)
		}
		lsns[i] = txn.SourcePos.LSN
	}
	if got := r1.Position(); got.LSN < lsns[399] {
		t.Fatalf("Position %v phải ≥ commit LSN Txn 400 (%d)", got, lsns[399])
	}
	// Pipeline xác nhận đã apply tới Txn 200: slot chỉ được tiến tới đúng đó.
	r1.SetConfirmed(lsns[199])
	waitFor(t, 10*time.Second, "confirmed_flush_lsn = lwd", func() bool { return confirmedFlush(t, db, slot) == lsns[199] })
	if err := r1.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if cf := confirmedFlush(t, db, slot); cf != lsns[199] {
		t.Fatalf("flush phải là lwd đã apply (%d), không phải LSN đã đọc; slot = %d", lsns[199], cf)
	}

	// lwd < confirmed_flush: slot đã tiến quá phần đã apply → log_gap.
	rg := newReader(t, cfg)
	if _, err := rg.Start(context.Background(), checkpoint.NewPGAppliedSet(lsns[99])); exitOf(err) != exitcode.DataError {
		t.Fatalf("lwd < confirmed_flush phải là log_gap exit 5, được %v", err)
	}
	_ = rg.Close()

	applied := checkpoint.NewPGAppliedSet(0)
	applied.Vouch(lsns[399], nil) // 400 Txn đầu đã apply
	r2 := newReader(t, cfg)
	ch2, err := r2.Start(context.Background(), applied)
	if err != nil {
		t.Fatal(err)
	}
	insertMarker(t, db, "ac1", "end")
	tr2 := newTxnReader(t, ch2)
	got, _ := tr2.untilMarker("end", readTimeout)
	if len(got) != 600 {
		t.Fatalf("phải nhận đúng 600 Txn, nhận %d", len(got))
	}
	for i, txn := range got {
		rows := txnRows(t, txn)
		if len(rows) != 1 || col(t, rows[0], "id") != int64(400+i) || col(t, rows[0], "val") != fmt.Sprintf("v%d", 400+i) {
			t.Fatalf("Txn thứ %d sau resume sai: %+v", i, rows)
		}
		if applied.Contains(txn.SourcePos) {
			t.Fatalf("Txn %v đã apply mà vẫn được phát (vi phạm CP-2)", txn.SourcePos)
		}
	}
}

// AC2: cột TOAST > 2 KB không đổi khi UPDATE cột khác → ColValue{Unchanged: true} (không phải NULL),
// đi qua Assembler vẫn giữ nguyên cờ.
func TestAC2_TOASTUnchanged(t *testing.T) {
	inst := sharedPG(t)
	db := inst.db
	pub := setupTables(t, db, "ac2", `CREATE TABLE ac2 (id int PRIMARY KEY, big text, other int);
		ALTER TABLE ac2 ALTER COLUMN big SET STORAGE EXTERNAL`, "ac2")
	slot := "ac2_slot"
	dropSlotLater(t, db, slot)
	r := newReader(t, pg.Config{DSN: inst.dsn, Slot: slot, Publication: pub, CreateSlot: true})
	ch, err := r.Start(context.Background(), nil)
	if err != nil {
		t.Fatal(err)
	}
	tr := newTxnReader(t, ch)

	bigVal := strings.Repeat("0123456789abcdef", 512) // 8 KB, lưu ngoài dòng (STORAGE EXTERNAL)
	mustExec(t, db, "INSERT INTO ac2 VALUES (1, $1, 1)", bigVal)
	mustExec(t, db, "UPDATE ac2 SET other = 2 WHERE id = 1")
	mustExec(t, db, "UPDATE ac2 SET big = 'nho', other = 3 WHERE id = 1")
	insertMarker(t, db, "ac2", "end")
	got, _ := tr.untilMarker("end", readTimeout)
	if len(got) != 3 {
		t.Fatalf("phải có 3 Txn, được %d", len(got))
	}

	ins := txnRows(t, got[0])[0]
	if cv := colValue(t, ins, "big"); cv.Unchanged || cv.Value != bigVal {
		t.Fatalf("INSERT phải có đủ giá trị big, được unchanged=%v len=%d", cv.Unchanged, len(fmt.Sprint(cv.Value)))
	}

	upd := txnRows(t, got[1])[0]
	if upd.Op != model.OpUpdate || len(upd.Key) != 1 || upd.Key[0] != int64(1) || upd.OldKey != nil {
		t.Fatalf("UPDATE sai khóa: %+v", upd)
	}
	cv := colValue(t, upd, "big")
	if !cv.Unchanged || cv.Value != nil {
		t.Fatalf("cột TOAST không đổi phải là ColValue{Unchanged:true}, được %+v", cv)
	}
	if col(t, upd, "other") != int64(2) || colValue(t, upd, "other").Unchanged {
		t.Fatalf("cột other phải = 2: %+v", upd.Cols)
	}

	upd2 := txnRows(t, got[2])[0]
	if cv := colValue(t, upd2, "big"); cv.Unchanged || cv.Value != "nho" {
		t.Fatalf("cột TOAST bị đổi phải có giá trị mới: %+v", cv)
	}
}

// Mapping giá trị pgoutput (text) → model.Value qua một server thật.
func TestValueMapping(t *testing.T) {
	inst := sharedPG(t)
	db := inst.db
	pub := setupTables(t, db, "vm", `CREATE TABLE vm (
		id bigint PRIMARY KEY, b bool, i2 smallint, i4 int, f8 float8, f4 real, n numeric(40,20), by bytea,
		d date, ts timestamp, tz timestamptz, tx text, j jsonb, u uuid, nul text, inf timestamptz, arr int[])`, "vm")
	slot := "vm_slot"
	dropSlotLater(t, db, slot)
	r := newReader(t, pg.Config{DSN: inst.dsn, Slot: slot, Publication: pub, CreateSlot: true})
	ch, err := r.Start(context.Background(), nil)
	if err != nil {
		t.Fatal(err)
	}
	mustExec(t, db, `SET TIME ZONE 'Asia/Ho_Chi_Minh'`) // phiên ghi khác UTC: reader vẫn phải ra UTC
	mustExec(t, db, `INSERT INTO vm VALUES (9223372036854775807, true, -5, 42, 0.1, 1.5,
		'12345678901234567890.12345678901234567890', '\xdeadbeef', '2026-11-03', '2026-11-03 13:37:49.123456',
		'2026-11-03 20:37:49.5+07', 'xin chào', '{"k": [1, 2]}', 'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11', NULL,
		'infinity', '{1,2,3}')`)
	insertMarker(t, db, "vm", "end")
	got, _ := newTxnReader(t, ch).untilMarker("end", readTimeout)
	if len(got) != 1 {
		t.Fatalf("phải có 1 Txn, được %d", len(got))
	}
	rc := txnRows(t, got[0])[0]
	want := map[string]any{
		"id":  int64(9223372036854775807),
		"b":   true,
		"i2":  int64(-5),
		"i4":  int64(42),
		"f8":  0.1,
		"f4":  1.5,
		"n":   "12345678901234567890.12345678901234567890",
		"d":   time.Date(2026, 11, 3, 0, 0, 0, 0, time.UTC),
		"ts":  time.Date(2026, 11, 3, 13, 37, 49, 123456000, time.UTC),
		"tz":  time.Date(2026, 11, 3, 13, 37, 49, 500000000, time.UTC),
		"tx":  "xin chào",
		"j":   `{"k": [1, 2]}`,
		"u":   "a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11",
		"nul": nil,
		"inf": "infinity",
		"arr": "{1,2,3}",
	}
	for name, w := range want {
		g := col(t, rc, name)
		if wt, ok := w.(time.Time); ok {
			gt, ok := g.(time.Time)
			if !ok || !gt.Equal(wt) || gt.Location() != time.UTC {
				t.Errorf("cột %s: muốn %v (UTC), được %#v", name, wt, g)
			}
			continue
		}
		if g != w {
			t.Errorf("cột %s: muốn %#v, được %#v", name, w, g)
		}
	}
	if b, ok := col(t, rc, "by").([]byte); !ok || fmt.Sprintf("%x", b) != "deadbeef" {
		t.Errorf("bytea sai: %#v", col(t, rc, "by"))
	}
	// numeric giữ chính xác tuyệt đối.
	if _, ok := new(big.Float).SetString(col(t, rc, "n").(string)); !ok {
		t.Errorf("numeric không parse được")
	}
}

// AC3: Txn lớn (> logical_decoding_work_mem, bị stream) và two-phase: chỉ phát khi commit thật, đúng
// một lần; abort / ROLLBACK PREPARED / ROLLBACK TO SAVEPOINT không phát. Kèm restart khi đang có Txn
// PREPARE treo: slot không được tiến quá PREPARE, COMMIT PREPARED sau restart vẫn phát đủ một lần.
func TestAC3_StreamingAndTwoPhase(t *testing.T) {
	inst := sharedPG(t)
	db := inst.db
	pub := setupTables(t, db, "ac3", "CREATE TABLE ac3 (id serial PRIMARY KEY, tag text, pad text)", "ac3")
	slot := "ac3_slot"
	dropSlotLater(t, db, slot)
	cfg := pg.Config{DSN: inst.dsn, Slot: slot, Publication: pub, CreateSlot: true, StatusInterval: 200 * time.Millisecond}
	r := newReader(t, cfg)
	ch, err := r.Start(context.Background(), nil)
	if err != nil {
		t.Fatal(err)
	}
	tr := newTxnReader(t, ch)

	fill := func(tag string, n int) string {
		return fmt.Sprintf("INSERT INTO ac3 (tag, pad) SELECT '%s', repeat('p', 100) FROM generate_series(1, %d);", tag, n)
	}

	// (a) Txn lớn commit; (b) Txn lớn rollback; (c) Txn lớn có ROLLBACK TO SAVEPOINT.
	mustExec(t, db, "BEGIN;"+fill("big_commit", 5000)+"COMMIT;")
	mustExec(t, db, "BEGIN;"+fill("big_abort", 5000)+"ROLLBACK;")
	mustExec(t, db, "BEGIN;"+fill("sub_keep", 3000)+"SAVEPOINT s1;"+fill("sub_drop", 3000)+
		"ROLLBACK TO SAVEPOINT s1;"+fill("sub_keep2", 10)+"COMMIT;")
	// (d) PREPARE rồi ROLLBACK PREPARED; (e) PREPARE rồi COMMIT PREPARED; (f) Txn lớn PREPARE (stream prepare).
	mustExec(t, db, "BEGIN;"+fill("p_rb", 3)+"PREPARE TRANSACTION 'g_rb';")
	mustExec(t, db, "ROLLBACK PREPARED 'g_rb'")
	mustExec(t, db, "BEGIN;"+fill("p_cm", 3)+"PREPARE TRANSACTION 'g_cm';")
	mustExec(t, db, "BEGIN;"+fill("p_big", 4000)+"PREPARE TRANSACTION 'g_big';")
	mustExec(t, db, "BEGIN;"+fill("p_big_rb", 4000)+"PREPARE TRANSACTION 'g_big_rb';")
	mustExec(t, db, "COMMIT PREPARED 'g_cm'")
	mustExec(t, db, "ROLLBACK PREPARED 'g_big_rb'")
	mustExec(t, db, "COMMIT PREPARED 'g_big'")
	insertMarker(t, db, "ac3", "m0")

	got, m0 := tr.untilMarker("m0", 60*time.Second)
	count := func(txn *model.Txn) map[string]int {
		c := map[string]int{}
		for _, rc := range txnRows(t, txn) {
			c[col(t, rc, "tag").(string)]++
		}
		return c
	}
	if len(got) != 4 {
		for _, g := range got {
			t.Logf("Txn %v: %v", g.SourcePos, count(g))
		}
		t.Fatalf("phải phát đúng 4 Txn (big_commit, sub, g_cm, g_big), được %d", len(got))
	}
	wantTxns := []map[string]int{
		{"big_commit": 5000},
		{"sub_keep": 3000, "sub_keep2": 10},
		{"p_cm": 3},
		{"p_big": 4000},
	}
	for i, w := range wantTxns {
		if c := count(got[i]); fmt.Sprint(c) != fmt.Sprint(w) {
			t.Fatalf("Txn %d: muốn %v, được %v", i, w, c)
		}
	}
	var streamTxns int64
	waitFor(t, 10*time.Second, "pg_stat_replication_slots.stream_txns > 0", func() bool {
		_ = db.QueryRow(`SELECT coalesce(stream_txns, 0) FROM pg_stat_replication_slots WHERE slot_name = $1`, slot).Scan(&streamTxns)
		return streamTxns > 0
	})
	var twoPhase bool
	if err := db.QueryRow(`SELECT two_phase FROM pg_replication_slots WHERE slot_name = $1`, slot).Scan(&twoPhase); err != nil || !twoPhase {
		t.Fatalf("slot phải bật two_phase sau START_REPLICATION (two_phase 'on'): %v %v", twoPhase, err)
	}
	t.Logf("stream_txns=%d", streamTxns)

	// (g) PREPARE treo qua một lần restart.
	mustExec(t, db, "BEGIN;"+fill("p_restart", 5)+"PREPARE TRANSACTION 'g_restart';")
	insertMarker(t, db, "ac3", "m1")
	got, m1 := tr.untilMarker("m1", readTimeout)
	if len(got) != 0 {
		t.Fatalf("Txn PREPARE chưa commit không được phát: %d Txn", len(got))
	}
	before := confirmedFlush(t, db, slot)
	r.SetConfirmed(m1.SourcePos.LSN)
	waitFor(t, 10*time.Second, "slot tiến tới PREPARE treo", func() bool { return confirmedFlush(t, db, slot) > before })
	time.Sleep(time.Second) // vài chu kỳ status
	if cf := confirmedFlush(t, db, slot); cf >= m1.SourcePos.LSN || cf <= m0.SourcePos.LSN {
		t.Fatalf("confirmed_flush phải bị giữ ở PREPARE LSN (giữa m0=%d và m1=%d), được %d", m0.SourcePos.LSN, m1.SourcePos.LSN, cf)
	}
	if err := r.Close(); err != nil {
		t.Fatal(err)
	}

	applied := checkpoint.NewPGAppliedSet(0)
	applied.Vouch(m1.SourcePos.LSN, nil)
	cfg.CreateSlot = false
	r2 := newReader(t, cfg)
	ch2, err := r2.Start(context.Background(), applied)
	if err != nil {
		t.Fatal(err)
	}
	mustExec(t, db, "COMMIT PREPARED 'g_restart'")
	insertMarker(t, db, "ac3", "m2")
	got, _ = newTxnReader(t, ch2).untilMarker("m2", readTimeout)
	if len(got) != 1 || fmt.Sprint(count(got[0])) != fmt.Sprint(map[string]int{"p_restart": 5}) {
		t.Fatalf("sau restart phải phát đúng Txn g_restart một lần, được %d Txn", len(got))
	}
}

// AC4: consumer không đọc kênh trong 30s (backpressure), wal_sender_timeout=10s → kết nối vẫn sống
// nhờ goroutine status; sau đó đọc tiếp đủ dữ liệu. Đối chứng: không gửi status thì server cắt kết nối.
func TestAC4_StatusUnderBackpressure(t *testing.T) {
	inst := sharedPG(t)
	db := inst.db
	var timeout string
	_ = db.QueryRow("SHOW wal_sender_timeout").Scan(&timeout)
	if timeout != "10s" {
		t.Fatalf("container phải đặt wal_sender_timeout=10s, đang là %s", timeout)
	}
	pub := setupTables(t, db, "ac4", "CREATE TABLE ac4 (id int PRIMARY KEY, pad text)", "ac4")

	run := func(t *testing.T, slot string, interval time.Duration, wait time.Duration) (pidBefore, pidAfter int, tr *txnReader, r *pg.Reader) {
		dropSlotLater(t, db, slot)
		r = newReader(t, pg.Config{DSN: inst.dsn, Slot: slot, Publication: pub, CreateSlot: true, StatusInterval: interval, BufferSize: 1})
		ch, err := r.Start(context.Background(), nil)
		if err != nil {
			t.Fatal(err)
		}
		// ~4 MB dữ liệu: vượt cả kênh lẫn bộ đệm TCP, walsender bị chặn khi gửi.
		mustExec(t, db, "DELETE FROM ac4")
		mustExec(t, db, `DO $$ BEGIN FOR i IN 1..2000 LOOP INSERT INTO ac4 VALUES (i, repeat(md5(i::text), 64)); COMMIT; END LOOP; END $$`)
		q := `SELECT coalesce(active_pid, 0) FROM pg_replication_slots WHERE slot_name = $1`
		waitFor(t, 10*time.Second, "slot active", func() bool { _ = db.QueryRow(q, slot).Scan(&pidBefore); return pidBefore != 0 })
		time.Sleep(wait)
		_ = db.QueryRow(q, slot).Scan(&pidAfter)
		return pidBefore, pidAfter, newTxnReader(t, ch), r
	}

	t.Run("status_goroutine", func(t *testing.T) {
		pid1, pid2, tr, r := run(t, "ac4_slot", 2*time.Second, 30*time.Second)
		if pid1 != pid2 {
			t.Fatalf("walsender đổi/mất trong lúc backpressure: pid %d → %d", pid1, pid2)
		}
		var last *model.Txn
		for i := 1; i <= 2000; i++ {
			last = tr.next(readTimeout)
			if rows := txnRows(t, last); col(t, rows[0], "id") != int64(i) {
				t.Fatalf("Txn %d sai: %+v", i, rows)
			}
		}
		// flush = lwd do pipeline xác nhận, không phải LSN đã đọc.
		if cf := confirmedFlush(t, db, "ac4_slot"); cf >= last.SourcePos.LSN {
			t.Fatalf("chưa SetConfirmed mà slot đã tiến tới %d (≥ LSN đã đọc %d)", cf, last.SourcePos.LSN)
		}
		r.SetConfirmed(last.SourcePos.LSN)
		waitFor(t, 10*time.Second, "confirmed_flush = lwd", func() bool { return confirmedFlush(t, db, "ac4_slot") == last.SourcePos.LSN })
	})

	t.Run("control_without_status", func(t *testing.T) {
		// Chu kỳ status 1 giờ ≈ không gửi: server phải cắt kết nối sau wal_sender_timeout.
		pid1, pid2, tr, _ := run(t, "ac4_ctl", time.Hour, 25*time.Second)
		if pid1 == pid2 {
			t.Fatalf("đối chứng: không gửi status mà walsender %d vẫn sống — test AC4 không đủ nhạy", pid1)
		}
		// Reader phải báo lỗi (không treo, không im lặng).
		deadline := time.After(60 * time.Second)
		for {
			select {
			case ev, ok := <-tr.ch:
				if !ok {
					t.Fatal("kênh đóng mà không có EventError")
				}
				if ev.Err != nil {
					t.Logf("lỗi như mong đợi: %v", ev.Err)
					return
				}
			case <-deadline:
				t.Fatal("không nhận được EventError sau khi server cắt kết nối")
			}
		}
	})
}

// AC5: tạo slot với Failover=true trên PG18 → pg_replication_slots.failover = true; two_phase vẫn bật.
func TestAC5_FailoverSlot(t *testing.T) {
	inst := sharedPG(t)
	db := inst.db
	pub := setupTables(t, db, "ac5", "CREATE TABLE ac5 (id int PRIMARY KEY)", "ac5")
	slot := "ac5_slot"
	dropSlotLater(t, db, slot)
	r := newReader(t, pg.Config{DSN: inst.dsn, Slot: slot, Publication: pub, CreateSlot: true, Failover: true})
	var failover bool
	if err := db.QueryRow(`SELECT failover FROM pg_replication_slots WHERE slot_name = $1`, slot).Scan(&failover); err != nil {
		t.Fatal(err)
	}
	if !failover {
		t.Fatal("slot tạo với Failover=true trên PG18 phải có failover = true")
	}
	ch, err := r.Start(context.Background(), nil)
	if err != nil {
		t.Fatal(err)
	}
	if v, s, tp := r.Protocol(); v != 4 || !s || !tp {
		t.Fatalf("PG18 failover slot: proto %d streaming %v two_phase %v", v, s, tp)
	}
	mustExec(t, db, "INSERT INTO ac5 VALUES (1)")
	insertMarker(t, db, "ac5", "end")
	if got, _ := newTxnReader(t, ch).untilMarker("end", readTimeout); len(got) != 1 {
		t.Fatalf("phải đọc được 1 Txn, được %d", len(got))
	}
}

// AC5 trên PG17 (bản đầu tiên có failover slot): FAILOVER được nhận, Start thành công (nếu server từ
// chối two-phase trên failover slot thì reader lùi về two_phase off), và Txn PREPARE/COMMIT PREPARED
// vẫn phát đúng một lần, ROLLBACK PREPARED không phát.
func TestAC5_FailoverSlotPG17(t *testing.T) {
	inst, err := startContainer(context.Background(), primaryRequest("postgres:17", pgArgs, "", ""))
	if err != nil {
		t.Fatalf("không khởi động được postgres:17: %v", err)
	}
	db := inst.db
	pub := setupTables(t, db, "p17", "CREATE TABLE p17 (id int PRIMARY KEY)", "p17")
	r := newReader(t, pg.Config{DSN: inst.dsn, Slot: "p17_slot", Publication: pub, CreateSlot: true, Failover: true})
	var failover bool
	if err := db.QueryRow(`SELECT failover FROM pg_replication_slots WHERE slot_name = 'p17_slot'`).Scan(&failover); err != nil || !failover {
		t.Fatalf("PG17: failover phải = true (%v)", err)
	}
	ch, err := r.Start(context.Background(), nil)
	if err != nil {
		t.Fatalf("Start trên PG17 failover slot: %v", err)
	}
	v, s, tp := r.Protocol()
	t.Logf("PG17 failover slot: proto=%d streaming=%v two_phase=%v", v, s, tp)
	if v != 4 || !s {
		t.Fatalf("PG17 phải nhận proto 4 + streaming")
	}
	mustExec(t, db, "BEGIN; INSERT INTO p17 VALUES (1); PREPARE TRANSACTION 'g17_rb';")
	mustExec(t, db, "ROLLBACK PREPARED 'g17_rb'")
	mustExec(t, db, "BEGIN; INSERT INTO p17 VALUES (2); PREPARE TRANSACTION 'g17';")
	mustExec(t, db, "COMMIT PREPARED 'g17'")
	insertMarker(t, db, "p17", "end")
	got, _ := newTxnReader(t, ch).untilMarker("end", readTimeout)
	if len(got) != 1 || col(t, txnRows(t, got[0])[0], "id") != int64(2) {
		t.Fatalf("phải phát đúng Txn g17 một lần, được %d Txn", len(got))
	}
}

// Kiểm điều kiện tiên quyết: wal_level sai → exit 3; slot không tồn tại / mất WAL → exit 5;
// tên slot sai → exit 3.
func TestPreflightErrors(t *testing.T) {
	inst := sharedPG(t)
	db := inst.db

	t.Run("invalid_slot_name", func(t *testing.T) {
		_, err := pg.New(pg.Config{DSN: inst.dsn, Slot: "Bad-Name", Publication: "x"})
		if exitOf(err) != exitcode.Rejected {
			t.Fatalf("muốn exit 3, được %v", err)
		}
	})

	t.Run("missing_slot", func(t *testing.T) {
		_, err := pg.New(pg.Config{DSN: inst.dsn, Slot: "khong_ton_tai", Publication: "x"})
		var e *exitcode.Error
		if !errors.As(err, &e) || e.Code != exitcode.ErrLogGap || e.ExitCode != exitcode.DataError {
			t.Fatalf("muốn log_gap exit 5, được %v", err)
		}
	})

	t.Run("wal_level_replica", func(t *testing.T) {
		other, err := startContainer(context.Background(), primaryRequest("postgres:18", nil, "", ""))
		if err != nil {
			t.Fatal(err)
		}
		_, err = pg.New(pg.Config{DSN: other.dsn, Slot: "s", Publication: "x", CreateSlot: true})
		var e *exitcode.Error
		if !errors.As(err, &e) || e.Code != exitcode.ErrBinlogConfigInvalid || e.ExitCode != exitcode.Rejected {
			t.Fatalf("wal_level=replica phải là binlog_config_invalid exit 3, được %v", err)
		}
	})

	t.Run("lost_slot", func(t *testing.T) {
		slot := "lost_slot"
		dropSlotLater(t, db, slot)
		r := newReader(t, pg.Config{DSN: inst.dsn, Slot: slot, Publication: "x", CreateSlot: true})
		_ = r.Close()
		mustExec(t, db, "CREATE TABLE lost_fill (id int, pad text)")
		mustExec(t, db, "ALTER SYSTEM SET max_slot_wal_keep_size = '1MB'")
		mustExec(t, db, "SELECT pg_reload_conf()")
		t.Cleanup(func() {
			mustExec(t, db, "ALTER SYSTEM RESET max_slot_wal_keep_size")
			mustExec(t, db, "SELECT pg_reload_conf()")
		})
		waitFor(t, 2*time.Minute, "wal_status = lost", func() bool {
			mustExec(t, db, "INSERT INTO lost_fill SELECT g, repeat(md5(g::text), 20) FROM generate_series(1, 20000) g")
			mustExec(t, db, "SELECT pg_switch_wal()")
			mustExec(t, db, "CHECKPOINT")
			var st string
			_ = db.QueryRow(`SELECT coalesce(wal_status, '') FROM pg_replication_slots WHERE slot_name = $1`, slot).Scan(&st)
			return st == "lost"
		})
		_, err := pg.New(pg.Config{DSN: inst.dsn, Slot: slot, Publication: "x"})
		var e *exitcode.Error
		if !errors.As(err, &e) || e.Code != exitcode.ErrLogGap || e.ExitCode != exitcode.DataError {
			t.Fatalf("slot mất WAL phải là log_gap exit 5, được %v", err)
		}
	})
}
