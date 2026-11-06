package e2e

// G2-6 (01 §6 Gate 2, HANDOFF §8): transaction mở 30 phút ở commit_order=relaxed, PG→PG và
// MySQL→PG. Một Txn mở suốt thời gian chạy, ghi rải rác các row riêng của nó, có những quãng im
// lặng dài, trong khi các Txn khác chạy song song và được apply; cuối cùng Txn dài commit.
// Kiểm: checkpoint không phình (CP-4, 02 §6: row delta ≤ 64 KB, holes ≤ 4.096 — lấy mẫu mỗi giây
// suốt lúc chạy), checkpoint vẫn tiến khi Txn dài còn mở, không row nào của Txn dài tới target trước
// commit, lag hồi về ≤ lag_slo sau commit, không mất row, 0 lệch.
//
// Thời lượng Txn dài: STRAND_E2E_LONG_TXN (mặc định 2m cho CI; bản gate đặt 30m). Nhịp tải song song:
// STRAND_E2E_LONG_TXN_OPS (mặc định 100 Txn/giây, giãn nhịp để dữ liệu không phình tới mức bước so
// sánh cuối hết RAM khi chạy 30 phút). Luôn chạy relaxed (cấu hình ép commit_order: relaxed, bỏ qua
// STRAND_E2E_COMMIT_ORDER) vì đây là kịch bản có holes.

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"math/rand"
	"strings"
	"testing"
	"time"

	"strand/internal/checkpoint"
	"strand/internal/model"
)

const (
	longTxnEventsBase   = int64(9_000_000_000_000) // id row events/products riêng của Txn dài
	longTxnItemsBase    = int64(9_100_000_000_000) // id row items riêng của Txn dài (PG, có TOAST)
	longTxnLagSLO       = 5 * time.Second          // khớp LagSLO mặc định của strandCfg
	longTxnLagRecoverBy = 3 * time.Minute
)

// longTxnResult: thống kê lịch ghi của Txn dài (xem runLongTxn).
type longTxnResult struct {
	inserted, updated, bursts, quiet int
	elapsed                          time.Duration
}

// runLongTxn giữ một Txn mở trong d: ghi từng cụm nhỏ cách nhau 0,5–3,5 s, có hai quãng im lặng dài
// (d/8, tối thiểu 5 s) ở khoảng 1/4 và 5/8 thời lượng. insert(n) ghi row mới thứ n, update(k) sửa row cũ
// k < n, cả hai chạy trong Txn caller đã BEGIN. alive() khác nil thì dừng ngay (strand chết, ...).
func runLongTxn(ctx context.Context, d time.Duration, rng *rand.Rand, insert func(n int) error, update func(k int) error, alive func() error) (longTxnResult, error) {
	var res longTxnResult
	start := time.Now()
	quietLen := max(d/8, 5*time.Second)
	nextQuiet := start.Add(d / 4)
	sleep := func(x time.Duration) error {
		end := time.Now().Add(x)
		for time.Now().Before(end) {
			if err := alive(); err != nil {
				return err
			}
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(min(time.Until(end), time.Second)):
			}
		}
		return nil
	}
	for time.Since(start) < d {
		if err := alive(); err != nil {
			return res, err
		}
		if time.Now().After(nextQuiet) {
			if err := sleep(min(quietLen, d-time.Since(start))); err != nil {
				return res, err
			}
			res.quiet++
			nextQuiet = nextQuiet.Add(d * 3 / 8)
			continue
		}
		for k := 1 + rng.Intn(8); k > 0; k-- {
			if err := insert(res.inserted); err != nil {
				return res, fmt.Errorf("long txn insert #%d: %w", res.inserted, err)
			}
			res.inserted++
			if res.inserted > 1 && rng.Intn(3) == 0 {
				if err := update(rng.Intn(res.inserted - 1)); err != nil {
					return res, fmt.Errorf("long txn update: %w", err)
				}
				res.updated++
			}
		}
		res.bursts++
		if err := sleep(time.Duration(500+rng.Intn(3000)) * time.Millisecond); err != nil {
			return res, err
		}
	}
	res.elapsed = time.Since(start)
	return res, nil
}

func strandAlive(p *strandProc) func() error {
	return func() error {
		if p.exited() {
			return fmt.Errorf("strand exited (code %d) while the long transaction was open\n%s", p.exitCode(), p.tail())
		}
		return nil
	}
}

// waitLagRecovers: sau khi Txn dài commit (tải song song vẫn chạy), strand_lag_seconds{phase=synced}
// phải về ≤ lag_slo trong 3 mẫu liên tiếp, trong thời hạn timeout.
func waitLagRecovers(t *testing.T, cfg *strandCfg, p *strandProc, slo, timeout time.Duration) {
	t.Helper()
	start := time.Now()
	ok, last := 0, -1.0
	var seen []string
	for {
		if p.exited() {
			t.Fatalf("strand exited (code %d) while waiting for lag to recover\n%s", p.exitCode(), p.tail())
		}
		text, err := scrapeMetrics(cfg.MetricsAddr)
		if err == nil {
			if v, found := metricSum(text, "strand_lag_seconds", map[string]string{"pipeline": cfg.Name, "phase": "synced"}); found {
				last = v
				if len(seen) < 40 {
					seen = append(seen, fmt.Sprintf("%.1f", v))
				}
				if v <= slo.Seconds() {
					ok++
				} else {
					ok = 0
				}
			}
		}
		if ok >= 3 {
			t.Logf("lag recovered to %.1fs (≤ %s) %s after the long transaction committed", last, slo, time.Since(start).Round(time.Second))
			return
		}
		if time.Since(start) > timeout {
			t.Fatalf("lag did not recover to ≤ %s within %s after commit (last %.1fs, samples %v, scrape err %v)\n%s",
				slo, timeout, last, seen, err, p.tail())
		}
		time.Sleep(500 * time.Millisecond)
	}
}

func readGlobalApplied(t *testing.T, dst *sql.DB, pipelineID string) string {
	var s string
	if err := dst.QueryRow("SELECT applied FROM _strand_checkpoint WHERE pipeline_id = $1", pipelineID).Scan(&s); err != nil {
		t.Fatalf("read _strand_checkpoint: %v", err)
	}
	return s
}

// G2-6, PG→PG.
func TestG2_E2E_LongTxnRelaxedPG(t *testing.T) {
	seed := seedFromEnv(t)
	d := envDuration("STRAND_E2E_LONG_TXN", 2*time.Minute)
	ops := float64(envInt("STRAND_E2E_LONG_TXN_OPS", 100))
	t.Logf("long transaction open for %s, parallel load %.0f txn/s, commit_order relaxed", d, ops)
	slot, pub := "strand_g2_long", "strand_g2_pub"
	src, dst, lsn := setupPGPair(t, slot, pub)

	cfg := strandCfg{Name: "g2-long-pg", SourceKind: "postgres", TargetKind: "postgres",
		SourceDSN: src.DSN, TargetDSN: dst.DSN, CommitOrder: "relaxed", MetricsAddr: freeAddr(t),
		SourceExtra: fmt.Sprintf(", slot: %s, publication: %s", slot, pub),
		Include:     []string{"app.*"}, Dir: testDir(t), Bootstrap: "start_at"}
	cfgPath := cfg.write(t)
	p := startStrand(t, cfgPath, "--start-at", lsn)
	defer func() {
		if !p.exited() {
			p.kill9()
		}
	}()

	wl := newPGWorkload(src.DB, seed)
	load := startPacedLoad(seed, 4, ops, func(r *rand.Rand) error { return wl.one(r, 3000) })
	defer load.Stop()
	waitPGCaughtUp(t, src, dst, &cfg, p, 3*time.Minute)
	vs := startPGVerSampler(dst, "app.items", "app.events", "app.accounts")
	mon := startCkptMonitor(dst.DB, cfg.Name, model.EnginePG, cfg.MetricsAddr)
	before := readGlobalApplied(t, dst.DB, cfg.Name)

	ctx := context.Background()
	conn, err := src.DB.Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	if _, err := conn.ExecContext(ctx, "BEGIN"); err != nil {
		t.Fatal(err)
	}
	rng := rand.New(rand.NewSource(seed ^ 0x6c6f6e67))
	// Row riêng của Txn dài: events (id dành riêng) và mỗi row thứ 5 thêm một item có doc TOAST > 2 KB.
	insert := func(n int) error {
		if _, err := conn.ExecContext(ctx, `INSERT INTO app.events VALUES ($1, nextval('app.ver_seq'), $2)`,
			longTxnEventsBase+int64(n), fmt.Sprintf("long-%d", n)); err != nil {
			return err
		}
		if n%5 == 0 {
			_, err := conn.ExecContext(ctx, `INSERT INTO app.items VALUES ($1, nextval('app.ver_seq'), 1, 1.00, 'long',
				repeat(md5($1::text), 100), '{"long": true}', now())`, longTxnItemsBase+int64(n))
			return err
		}
		return nil
	}
	update := func(k int) error {
		_, err := conn.ExecContext(ctx, `UPDATE app.events SET ver = nextval('app.ver_seq'), body = body || '+' WHERE id = $1`, longTxnEventsBase+int64(k))
		return err
	}
	res, err := runLongTxn(ctx, d, rng, insert, update, strandAlive(p))
	if err != nil {
		_, _ = conn.ExecContext(ctx, "ROLLBACK")
		t.Fatalf("long transaction: %v", err)
	}
	t.Logf("long transaction: %d rows inserted, %d updates, %d bursts, %d quiet periods over %s",
		res.inserted, res.updated, res.bursts, res.quiet, res.elapsed.Round(time.Second))
	if res.quiet < 1 || res.inserted < 10 {
		t.Fatalf("long transaction schedule degenerate: %+v", res)
	}

	// Trước commit: không row nào của Txn dài ở target; checkpoint đã tiến (lwd không bị Txn mở giữ lại).
	var early int
	if err := dst.DB.QueryRow("SELECT count(*) FROM app.events WHERE id >= $1", longTxnEventsBase).Scan(&early); err != nil {
		t.Fatal(err)
	}
	if early != 0 {
		t.Fatalf("%d rows of the still-open long transaction are already on the target", early)
	}
	during := readGlobalApplied(t, dst.DB, cfg.Name)
	lwdOf := func(s string) uint64 {
		var pj checkpoint.PGAppliedJSON
		if err := json.Unmarshal([]byte(s), &pj); err != nil {
			t.Fatalf("parse applied %q: %v", s, err)
		}
		l, err := checkpoint.ParseLSN(pj.Lwd)
		if err != nil {
			t.Fatalf("parse lwd %q: %v", pj.Lwd, err)
		}
		return l
	}
	if lb, ld := lwdOf(before), lwdOf(during); ld <= lb {
		t.Fatalf("checkpoint lwd did not advance while the long transaction was open (%s -> %s)",
			checkpoint.FormatLSN(lb), checkpoint.FormatLSN(ld))
	} else {
		t.Logf("checkpoint lwd advanced %s -> %s while the long transaction was open", checkpoint.FormatLSN(lb), checkpoint.FormatLSN(ld))
	}
	if _, err := conn.ExecContext(ctx, "COMMIT"); err != nil {
		t.Fatalf("commit long transaction: %v", err)
	}
	waitLagRecovers(t, &cfg, p, longTxnLagSLO, longTxnLagRecoverBy)

	load.Stop()
	t.Logf("parallel load: %s", load)
	if load.ops.Load() < int64(d.Seconds()*ops/4) {
		t.Fatalf("parallel load too thin: %s over %s", load, d)
	}
	waitPGCaughtUp(t, src, dst, &cfg, p, 5*time.Minute)
	stats := mon.finish()
	if code := p.stop(t); code != 0 {
		t.Fatalf("strand exit code %d\n%s", code, p.tail())
	}
	vs.check(t)
	comparePG(t, src, dst, pgAppTables...)
	checkPGInvariants(t, dst)
	var got int
	if err := dst.DB.QueryRow("SELECT count(*) FROM app.events WHERE id >= $1", longTxnEventsBase).Scan(&got); err != nil {
		t.Fatal(err)
	}
	if got != res.inserted {
		t.Fatalf("long transaction rows on target: %d, want %d", got, res.inserted)
	}
	checkCP4(t, stats)
}

// G2-6, MySQL→PG.
func TestG2_E2E_LongTxnRelaxedMySQLToPG(t *testing.T) {
	seed := seedFromEnv(t)
	d := envDuration("STRAND_E2E_LONG_TXN", 2*time.Minute)
	ops := float64(envInt("STRAND_E2E_LONG_TXN_OPS", 100))
	t.Logf("long transaction open for %s, parallel load %.0f txn/s, commit_order relaxed", d, ops)
	src := startMySQL(t, mysqlImage("source"), "src", 1, nil)
	dst := startPG(t, "pgdst")
	src.exec(t, myPGSourceSQL...)
	dst.exec(t, myPGTargetSQL...)
	src.exec(t, "SET GLOBAL time_zone = '+00:00'")
	const preload = 2000
	tx, err := src.DB.Begin()
	if err != nil {
		t.Fatal(err)
	}
	for i := 1; i <= preload; i++ {
		if _, err := tx.Exec(`INSERT INTO shop.products VALUES (?, ?, ?, ?, ?, NOW(6), NOW(6), UUID_SHORT())`,
			i, fmt.Sprintf("SKU-%d", i), fmt.Sprintf("%d.%02d", i%9999, i%100), uint64(1)<<40+uint64(i),
			fmt.Sprintf(`{"color": "c%d"}`, i%7)); err != nil {
			t.Fatal(err)
		}
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}

	cfg := strandCfg{Name: "g2-long-mypg", TargetKind: "postgres", SourceDSN: src.RootDSN, TargetDSN: dst.DSN,
		CommitOrder: "relaxed", MetricsAddr: freeAddr(t), Include: []string{"shop.*"}, Dir: testDir(t)}
	cfgPath := cfg.write(t)
	p := startStrand(t, cfgPath)
	defer func() {
		if !p.exited() {
			p.kill9()
		}
	}()
	// shop.users phải còn rỗng lúc Strand kiểm bảng (như TestP2_E2E_MySQLToPGSnapshotAndStream).
	deadline := time.Now().Add(2 * time.Minute)
	for !strings.Contains(p.output.String(), "joins the pipeline without snapshot") {
		if p.exited() || time.Now().After(deadline) {
			t.Fatalf("strand did not accept the empty varchar-PK table (exited=%v)\n%s", p.exited(), p.tail())
		}
		time.Sleep(100 * time.Millisecond)
	}
	wl := &myPGWorkload{db: src.DB, rng: rand.New(rand.NewSource(seed))}
	wl.nextID.Store(preload + 1000)
	load := startPacedLoad(seed, 4, ops, func(r *rand.Rand) error { return wl.one(r, preload) })
	defer load.Stop()
	waitSynced(t, &cfg, p, 5*time.Minute)
	waitMySQLPGCaughtUp(t, src, dst, &cfg, p, 3*time.Minute)
	mon := startCkptMonitor(dst.DB, cfg.Name, model.EngineMySQL, cfg.MetricsAddr)
	before := readGlobalApplied(t, dst.DB, cfg.Name)

	ctx := context.Background()
	conn, err := src.DB.Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	if _, err := conn.ExecContext(ctx, "BEGIN"); err != nil {
		t.Fatal(err)
	}
	rng := rand.New(rand.NewSource(seed ^ 0x6c6f6e67))
	insert := func(n int) error {
		id := longTxnEventsBase + int64(n)
		_, err := conn.ExecContext(ctx, `INSERT INTO shop.products VALUES (?, ?, ?, ?, ?, NOW(6), NOW(6), UUID_SHORT())`,
			id, fmt.Sprintf("LONG-%d", n), fmt.Sprintf("%d.%02d", n%9999, n%100), uint64(n), fmt.Sprintf(`{"long": %d}`, n))
		return err
	}
	update := func(k int) error {
		_, err := conn.ExecContext(ctx, `UPDATE shop.products SET qty = qty + 1, ver = UUID_SHORT(), modified = NOW(6),
			attrs = JSON_SET(attrs, '$.u', qty) WHERE id = ?`, longTxnEventsBase+int64(k))
		return err
	}
	res, err := runLongTxn(ctx, d, rng, insert, update, strandAlive(p))
	if err != nil {
		_, _ = conn.ExecContext(ctx, "ROLLBACK")
		t.Fatalf("long transaction: %v", err)
	}
	t.Logf("long transaction: %d rows inserted, %d updates, %d bursts, %d quiet periods over %s",
		res.inserted, res.updated, res.bursts, res.quiet, res.elapsed.Round(time.Second))
	if res.quiet < 1 || res.inserted < 10 {
		t.Fatalf("long transaction schedule degenerate: %+v", res)
	}
	var early int
	if err := dst.DB.QueryRow("SELECT count(*) FROM shop.products WHERE id >= $1", longTxnEventsBase).Scan(&early); err != nil {
		t.Fatal(err)
	}
	if early != 0 {
		t.Fatalf("%d rows of the still-open long transaction are already on the target", early)
	}
	// MySQL: Txn mở chưa có trong binlog; checkpoint phải tiếp tục ghi nhận các Txn song song.
	if during := readGlobalApplied(t, dst.DB, cfg.Name); during == before {
		t.Fatalf("checkpoint did not advance while the long transaction was open (still %q)", before)
	}
	if _, err := conn.ExecContext(ctx, "COMMIT"); err != nil {
		t.Fatalf("commit long transaction: %v", err)
	}
	waitLagRecovers(t, &cfg, p, longTxnLagSLO, longTxnLagRecoverBy)

	load.Stop()
	t.Logf("parallel load: %s", load)
	if load.ops.Load() < int64(d.Seconds()*ops/4) {
		t.Fatalf("parallel load too thin: %s over %s", load, d)
	}
	waitMySQLPGCaughtUp(t, src, dst, &cfg, p, 5*time.Minute)
	stats := mon.finish()
	if code := p.stop(t); code != 0 {
		t.Fatalf("strand exit code %d\n%s", code, p.tail())
	}
	compareMySQLPG(t, src, dst)
	var got int
	if err := dst.DB.QueryRow("SELECT count(*) FROM shop.products WHERE id >= $1", longTxnEventsBase).Scan(&got); err != nil {
		t.Fatal(err)
	}
	if got != res.inserted {
		t.Fatalf("long transaction rows on target: %d, want %d", got, res.inserted)
	}
	checkCP4(t, stats)
}
