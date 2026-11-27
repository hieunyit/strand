package e2e

import (
	"database/sql"
	"fmt"
	"strings"
	"testing"
	"time"

	"strand/internal/checkpoint"
)

// Source PG mà các bảng của pipeline không có ghi mới, trong khi một database khác cùng cluster ghi
// nhiều: slot của pipeline vẫn phải tiến (heartbeat, source.heartbeat_interval), nếu không WAL bị giữ
// tăng mãi tới đầy đĩa source (như AWS DMS HeartbeatEnable, Debezium heartbeat.action.query).
// Trước heartbeat: confirmed_flush đứng ở commit cuối của bảng pipeline, WAL giữ tăng theo ghi ở database kia.
func TestHB_E2E_PGIdlePublicationSlotAdvances(t *testing.T) {
	slot, pub := "strand_e2e_idle", "strand_e2e_idle_pub"
	src, dst, lsn := setupPGPair(t, slot, pub)
	// Ghi ở một database khác trên cùng cluster: walsender của slot (database strand) không gửi gì cho
	// Strand, nên chỉ heartbeat mới cho Strand thấy vị trí mới để xác nhận.
	src.exec(t, "CREATE DATABASE otherdb")
	other, err := sql.Open("pgx", strings.Replace(src.DSN, "/strand?", "/otherdb?", 1))
	if err != nil {
		t.Fatal(err)
	}
	defer other.Close()
	if _, err := other.Exec("CREATE TABLE noise (id bigserial PRIMARY KEY, pad text)"); err != nil {
		t.Fatal(err)
	}

	cfg := strandCfg{Name: "e2e-pg-idle", SourceKind: "postgres", TargetKind: "postgres",
		SourceDSN: src.DSN, TargetDSN: dst.DSN,
		SourceExtra: fmt.Sprintf(", slot: %s, publication: %s", slot, pub),
		Include:     []string{"app.*"}, Dir: testDir(t), Bootstrap: "start_at"}
	cfgPath := cfg.write(t)
	p := startStrand(t, cfgPath, "--start-at", lsn)
	waitPGCaughtUp(t, src, dst, &cfg, p, 3*time.Minute)

	// Chỉ ghi vào bảng ngoài publication: ~64 MB WAL.
	for i := 0; i < 32; i++ {
		if _, err := other.Exec("INSERT INTO noise (pad) SELECT repeat('x', 1000) FROM generate_series(1, 2000)"); err != nil {
			t.Fatal(err)
		}
	}
	var head string
	if err := src.DB.QueryRow("SELECT pg_current_wal_lsn()::text").Scan(&head); err != nil {
		t.Fatal(err)
	}
	headLSN, err := checkpoint.ParseLSN(head)
	if err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(90 * time.Second)
	for {
		c := slotConfirmed(t, src, slot)
		var retained int64
		if err := src.DB.QueryRow("SELECT pg_wal_lsn_diff(pg_current_wal_lsn(), restart_lsn)::bigint FROM pg_replication_slots WHERE slot_name = $1", slot).Scan(&retained); err != nil {
			t.Fatal(err)
		}
		if c >= headLSN {
			t.Logf("slot confirmed_flush passed the idle WAL head %s; WAL kept %d MB", head, retained>>20)
			break
		}
		if p.exited() {
			t.Fatalf("strand exited (code %d)\n%s", p.exitCode(), p.tail())
		}
		if time.Now().After(deadline) {
			t.Fatalf("slot stuck at confirmed_flush %X while WAL head moved to %s on unpublished tables (WAL kept: %d MB)", c, head, retained>>20)
		}
		time.Sleep(time.Second)
	}
	// restart_lsn đi theo bản ghi running_xacts (bgwriter, ~15 s): WAL giữ phải về dưới 1 segment dư.
	deadline = time.Now().Add(90 * time.Second)
	for {
		var retained int64
		if err := src.DB.QueryRow("SELECT pg_wal_lsn_diff(pg_current_wal_lsn(), restart_lsn)::bigint FROM pg_replication_slots WHERE slot_name = $1", slot).Scan(&retained); err != nil {
			t.Fatal(err)
		}
		if retained < 32<<20 {
			t.Logf("WAL kept by the slot dropped to %d MB", retained>>20)
			break
		}
		if time.Now().After(deadline) {
			var info string
			_ = src.DB.QueryRow(`SELECT format('restart=%s confirmed=%s head=%s', restart_lsn, confirmed_flush_lsn, pg_current_wal_lsn()) FROM pg_replication_slots WHERE slot_name = $1`, slot).Scan(&info)
			var rep string
			_ = src.DB.QueryRow(`SELECT coalesce(string_agg(format('sent=%s write=%s flush=%s', sent_lsn, write_lsn, flush_lsn), '; '), 'none') FROM pg_stat_replication`).Scan(&rep)
			t.Fatalf("slot restart_lsn stuck: WAL kept %d MB after the idle period (%s; walsender %s)", retained>>20, info, rep)
		}
		time.Sleep(time.Second)
	}
	if code := p.stop(t); code != 0 {
		t.Fatalf("strand exit code %d\n%s", code, p.tail())
	}
}
