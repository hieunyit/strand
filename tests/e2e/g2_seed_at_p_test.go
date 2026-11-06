package e2e

// Gate 2 (G2-3, 02 §9): Txn có commit LSN ĐÚNG BẰNG P (start_position.lsn của seed.json PG) và Txn có
// commit LSN = P−1 — biên chính xác của `--start-at seed`: tập đã apply là "≤ P−1", reader nhận
// confirmed_flush = lwd+1 (gap check `cf > lwd+1`), server stream mọi Txn có commit LSN ≥ confirmed_flush.

import (
	"database/sql"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"strand/internal/checkpoint"
	"strand/internal/seed"
)

// Gate 2 (G2-3): seed PG→PG mà P rơi đúng vào commit record của một Txn T_k.
//
// consistent_point thật của slot luôn là cuối một record xl_running_xacts: commit record chỉ bắt đầu
// đúng tại đó trong một race rất hẹp (Txn nhận xid sau khi running_xacts chụp nhưng ghi WAL trước), nên
// không thể tái hiện tất định bằng `strand export`. Test dựng đúng tình huống đó bằng
// pg_replication_slot_advance: slot (pgoutput) được tạo trước chuỗi Txn T_1..T_7 tuần tự, commit LSN của
// từng Txn đọc từ chính slot (message 'B' của pgoutput mang final_lsn = commit LSN), rồi slot được đẩy
// tới confirmed_flush = P với
//   - commit_lsn_eq_P:          P = commit LSN của T_k → T_k KHÔNG có trong seed, phải đến từ slot;
//   - commit_lsn_eq_P_minus_1:  P = commit LSN của T_k + 1 → T_k có trong seed, slot không được gửi lại.
//
// Target được "restore" bằng cách chạy lại đúng các câu lệnh của mọi Txn có commit LSN < P (dữ liệu tất
// định, ver ghi tường minh). seed.json (kind strand-export, slot, source_identity thật) trỏ vào P.
// Mỗi Txn ghi các row riêng (không gộp được) nên trigger apply_log ở target chứng minh từng phiên bản
// được ghi ĐÚNG MỘT LẦN: 0 = hổng (Txn bị bỏ ở P), 2 = trùng (Txn của seed bị apply lại).
func TestG2_E2E_PGSeedCommitExactlyAtP(t *testing.T) {
	t.Run("commit_lsn_eq_P", func(t *testing.T) { runPGSeedAtP(t, 0) })
	t.Run("commit_lsn_eq_P_minus_1", func(t *testing.T) { runPGSeedAtP(t, 1) })
}

// atPTxn là các câu lệnh của Txn thứ i (row riêng của Txn: items/accounts id = i, events id = 900000+i).
func atPTxn(i int) []string {
	v := 5_000_000 + i
	return []string{
		// doc (TOAST, lưu ngoài dòng) không đổi: pgoutput gửi 'u' — partial update ngay tại biên P.
		fmt.Sprintf("UPDATE app.items SET qty = qty + 1, ver = %d, note = 'at-p %d' WHERE id = %d", v, i, i),
		fmt.Sprintf("INSERT INTO app.events VALUES (%d, %d, 'at-p')", 900000+i, v),
		fmt.Sprintf("UPDATE app.accounts SET balance = balance + %d, ver = %d WHERE id = %d", i, v, i),
	}
}

// runAtPTxn chạy Txn i trong một transaction và trả xid (32 bit thấp của pg_current_xact_id()).
func runAtPTxn(t *testing.T, db *sql.DB, i int) uint32 {
	t.Helper()
	tx, err := db.Begin()
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	for _, q := range atPTxn(i) {
		if _, err := tx.Exec(q); err != nil {
			t.Fatalf("txn %d: %s: %v", i, q, err)
		}
	}
	var xs string
	if err := tx.QueryRow("SELECT pg_current_xact_id()::text").Scan(&xs); err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	x, err := strconv.ParseUint(xs, 10, 64)
	if err != nil {
		t.Fatal(err)
	}
	return uint32(x)
}

// pgoutputCommitLSNs đọc (không tiêu thụ) slot: xid → commit LSN, lấy từ message Begin của pgoutput
// ('B', Int64 final_lsn, Int64 commit time, Int32 xid) — đúng giá trị Strand dùng làm vị trí Txn.
func pgoutputCommitLSNs(t *testing.T, db *sql.DB, slot, pub string) map[uint32]uint64 {
	t.Helper()
	rows, err := db.Query(`SELECT data FROM pg_logical_slot_peek_binary_changes($1, NULL, NULL,
		'proto_version', '1', 'publication_names', $2)`, slot, pub)
	if err != nil {
		t.Fatalf("peek slot %s: %v", slot, err)
	}
	defer rows.Close()
	out := map[uint32]uint64{}
	for rows.Next() {
		var b []byte
		if err := rows.Scan(&b); err != nil {
			t.Fatal(err)
		}
		if len(b) >= 21 && b[0] == 'B' {
			out[binary.BigEndian.Uint32(b[17:21])] = binary.BigEndian.Uint64(b[1:9])
		}
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	return out
}

func runPGSeedAtP(t *testing.T, off uint64) {
	const (
		slot = "strand_seed_at_p"
		pub  = "strand_e2e_pub"
		k    = 4 // T_k là Txn có commit LSN = P (hoặc P−1)
		pre  = 7 // T_1..T_7 commit trước khi Strand chạy
		post = 3 // T_8..T_10 commit khi Strand đang stream
	)
	// synchronous_commit=on: commit LSN của T_7 đã flush khi Exec trả về, nên pg_replication_slot_advance
	// (bị kẹp ở flush LSN) tới đúng P.
	src := startPG(t, "pgsrc", "-c", "synchronous_commit=on")
	dst := startPG(t, "pgdst")
	src.exec(t, pgSchemaSQL...)
	dst.exec(t, pgSchemaSQL...)
	pgPreload(t, src, 300, 100, 300)
	pgPreload(t, dst, 300, 100, 300)
	dst.exec(t, pgApplyLogSQL...)
	src.exec(t, fmt.Sprintf("CREATE PUBLICATION %s FOR TABLE %s WITH (publish = 'insert, update, delete')", pub, strings.Join(pgAppTables, ", ")))
	var cp string
	if err := src.DB.QueryRow("SELECT lsn::text FROM pg_create_logical_replication_slot($1, 'pgoutput')", slot).Scan(&cp); err != nil {
		t.Fatalf("create slot: %v", err)
	}

	xids := make([]uint32, pre+post+1)
	for i := 1; i <= pre; i++ {
		xids[i] = runAtPTxn(t, src.DB, i)
	}
	lsns := pgoutputCommitLSNs(t, src.DB, slot, pub)
	commit := make([]uint64, pre+1)
	for i := 1; i <= pre; i++ {
		l, ok := lsns[xids[i]]
		if !ok {
			t.Fatalf("txn %d (xid %d) not decoded from slot %s (decoded %d txns)", i, xids[i], slot, len(lsns))
		}
		commit[i] = l
		if i > 1 && commit[i] <= commit[i-1] {
			t.Fatalf("commit LSN not increasing: T%d %s, T%d %s", i-1, checkpoint.FormatLSN(commit[i-1]), i, checkpoint.FormatLSN(commit[i]))
		}
	}
	P := commit[k] + off
	var desc []string
	for i := 1; i <= pre; i++ {
		desc = append(desc, fmt.Sprintf("T%d=%s", i, checkpoint.FormatLSN(commit[i])))
	}
	t.Logf("slot consistent_point %s; commit LSNs %s; P = %s (T%d commit LSN + %d)", cp, strings.Join(desc, " "), checkpoint.FormatLSN(P), k, off)

	var adv string
	if err := src.DB.QueryRow("SELECT end_lsn::text FROM pg_replication_slot_advance($1, $2::pg_lsn)", slot, checkpoint.FormatLSN(P)).Scan(&adv); err != nil {
		t.Fatalf("advance slot to P: %v", err)
	}
	if cf := slotConfirmed(t, src, slot); cf != P {
		t.Fatalf("slot confirmed_flush = %s after advance (returned %s), want P = %s", checkpoint.FormatLSN(cf), adv, checkpoint.FormatLSN(P))
	}

	// "Restore": target = source sau mọi Txn có commit LSN < P (trigger ghi apply_log cnt = 1 cho chúng).
	inSeed := map[int]bool{}
	for i := 1; i <= pre; i++ {
		if commit[i] < P {
			inSeed[i] = true
			runAtPTxn(t, dst.DB, i)
		}
	}
	if inSeed[k] != (off > 0) || !inSeed[k-1] || inSeed[k+1] {
		t.Fatalf("seed boundary wrong: in seed %v (P = %s)", inSeed, checkpoint.FormatLSN(P))
	}

	var sysID string
	if err := src.DB.QueryRow("SELECT system_identifier::text FROM pg_control_system()").Scan(&sysID); err != nil {
		t.Fatal(err)
	}
	dir := testDir(t)
	manifest := filepath.Join(dir, "seed.json")
	m := seed.SeedManifest{
		Version: 1, Engine: "pg", Kind: "strand-export", ToolVersion: "e2e", SourceIdentity: sysID,
		BackupRef: "e2e-replay", StartPosition: seed.StartPosition{LSN: checkpoint.FormatLSN(P), Slot: slot},
		SchemaSnapshot: "e2e-replay", Tables: []string{"app.*"}, Integrity: &seed.IntegrityInfo{PrepareOK: true},
		Target:    seed.TargetInfo{Version: "any", Compatible: true},
		CreatedAt: time.Now().UTC().Format(time.RFC3339),
	}
	raw, _ := json.MarshalIndent(m, "", "  ")
	if err := os.WriteFile(manifest, raw, 0o600); err != nil {
		t.Fatal(err)
	}

	cfg := strandCfg{Name: "e2e-pg-seed-at-p", SourceKind: "postgres", TargetKind: "postgres",
		SourceDSN: src.DSN, TargetDSN: dst.DSN, SourceExtra: fmt.Sprintf(", publication: %s", pub),
		Include: []string{"app.*"}, Dir: dir, Bootstrap: "seed"}
	p := startStrand(t, cfg.write(t), "--start-at", "seed", "--seed-manifest", manifest)
	waitSynced(t, &cfg, p, 2*time.Minute)
	for i := pre + 1; i <= pre+post; i++ {
		xids[i] = runAtPTxn(t, src.DB, i)
	}
	waitPGCaughtUp(t, src, dst, &cfg, p, 3*time.Minute)
	if code := p.stop(t); code != 0 {
		t.Fatalf("strand exit code %d\n%s", code, p.tail())
	}
	out := p.output.String()
	if !strings.Contains(out, `"p":"`+checkpoint.FormatLSN(P)+`"`) {
		t.Fatalf("strand did not start from the seed at P = %s\n%s", checkpoint.FormatLSN(P), p.tail())
	}

	// Mỗi phiên bản của mỗi Txn: đúng một lần ở target (seed hoặc stream, không cả hai, không thiếu).
	var bad []string
	for i := 1; i <= pre+post; i++ {
		from := "stream"
		if inSeed[i] {
			from = "seed"
		}
		for _, tb := range []string{"items", "events", "accounts"} {
			var cnt sql.NullInt64
			err := dst.DB.QueryRow("SELECT cnt FROM public.apply_log WHERE tbl = $1 AND ver = $2", tb, 5_000_000+i).Scan(&cnt)
			if err != nil && err != sql.ErrNoRows {
				t.Fatal(err)
			}
			if cnt.Int64 != 1 {
				l := "?"
				if i <= pre {
					l = checkpoint.FormatLSN(commit[i])
				}
				bad = append(bad, fmt.Sprintf("T%d (commit %s, via %s) %s written %d times", i, l, from, tb, cnt.Int64))
			}
		}
	}
	if len(bad) > 0 {
		t.Fatalf("P = %s: row versions not applied exactly once:\n%s\n%s", checkpoint.FormatLSN(P), strings.Join(bad, "\n"), p.tail())
	}
	t.Logf("P = %s: T%d (commit LSN %s, %s) and all %d Txns applied exactly once", checkpoint.FormatLSN(P), k,
		checkpoint.FormatLSN(commit[k]), map[bool]string{true: "from the seed", false: "from the slot"}[inSeed[k]], pre+post)
	comparePG(t, src, dst, pgAppTables...)
	checkPGInvariants(t, dst)
}
