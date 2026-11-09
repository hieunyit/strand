package e2e

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"strand/internal/checkpoint"
	"strand/internal/model"
)

// Nhân chứng WAL vật lý độc lập pgoutput/assembler: giữ WAL bằng slot riêng, chỉ quét SAU khi dừng tải.
// Không đổi dữ liệu nguồn hoặc bất biến CP/RS/CO; extension/slot chỉ ở container của test này.
type f11Witness struct {
	src        *pgNode
	dir, start string
}

func beginF11Witness(t *testing.T, src *pgNode, dir, start string) *f11Witness {
	t.Helper()
	if os.Getenv("STRAND_E2E_F11_WITNESS") != "1" {
		return nil
	}
	src.exec(t, "CREATE EXTENSION IF NOT EXISTS pg_walinspect", "SELECT pg_create_physical_replication_slot('f11_witness',true)")
	w := &f11Witness{src: src, dir: dir, start: start}
	t.Cleanup(func() {
		if _, err := src.DB.Exec("SELECT pg_drop_replication_slot('f11_witness')"); err != nil {
			t.Errorf("drop own witness slot: %v", err)
		}
	})
	return w
}

func (w *f11Witness) capture(t *testing.T, dst *pgNode, cfg *strandCfg) {
	t.Helper()
	if w == nil {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	var end string
	if err := w.src.DB.QueryRowContext(ctx, "SELECT pg_current_wal_insert_lsn()::text").Scan(&end); err != nil {
		t.Fatal(err)
	}
	for {
		var flushed bool
		if err := w.src.DB.QueryRowContext(ctx, "SELECT pg_current_wal_flush_lsn() >= $1::pg_lsn", end).Scan(&flushed); err != nil {
			t.Fatal(err)
		}
		if flushed {
			break
		}
		select {
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		case <-time.After(10 * time.Millisecond):
		}
	}
	rows, err := w.src.DB.QueryContext(ctx, `SELECT start_lsn::text,end_lsn::text,xid::text,record_type,description
  FROM pg_get_wal_records_info($1::pg_lsn,$2::pg_lsn)
  WHERE resource_manager='Transaction' AND record_type IN ('COMMIT','COMMIT_PREPARED')`, w.start, end)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	f, err := os.Create(filepath.Join(w.dir, "f11-source-commits.jsonl"))
	if err != nil {
		t.Fatal(err)
	}
	enc := json.NewEncoder(f)
	n := 0
	for rows.Next() {
		var x struct{ Start, End, XID, Kind, Description string }
		if err := rows.Scan(&x.Start, &x.End, &x.XID, &x.Kind, &x.Description); err != nil {
			f.Close()
			t.Fatal(err)
		}
		if err := enc.Encode(x); err != nil {
			f.Close()
			t.Fatal(err)
		}
		n++
	}
	if err := rows.Err(); err != nil {
		f.Close()
		t.Fatal(err)
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}
	applied, err := checkpoint.LoadAppliedSnapshotPG(ctx, dst.DB, cfg.Name, model.EnginePG)
	if err != nil {
		t.Fatal(err)
	}
	final, err := json.MarshalIndent(map[string]any{"applied": fmt.Sprint(applied), "start": w.start, "end": end}, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(w.dir, "f11-final.json"), final, 0600); err != nil {
		t.Fatal(err)
	}
	t.Logf("F11 witness: %d physical commits in [%s,%s], file=%s", n, w.start, end, f.Name())
}

// Ghi cả mốc cutoff lẫn applied set bền: khi có row thiếu mới xác định được checkpoint đã vượt nó chưa.
func recordF11Barrier(t *testing.T, src, dst *pgNode, cfg *strandCfg, cutoff string, marker int64, got checkpoint.AppliedSet) {
	t.Helper()
	if os.Getenv("STRAND_E2E_F11_WITNESS") != "1" {
		return
	}
	var write, insert string
	if err := src.DB.QueryRow("SELECT pg_current_wal_lsn()::text,pg_current_wal_insert_lsn()::text").Scan(&write, &insert); err != nil {
		t.Fatal(err)
	}
	x := map[string]any{"cutoff": cutoff, "write": write, "insert": insert, "marker": marker, "applied": fmt.Sprint(got), "unsafe_barrier": os.Getenv("STRAND_E2E_F11_UNSAFE_BARRIER") == "1"}
	for _, q := range []string{"SELECT applied FROM _strand_checkpoint WHERE pipeline_id=$1", "SELECT applied FROM _strand_worker_delta WHERE pipeline_id=$1"} {
		rows, err := dst.DB.Query(q, cfg.Name)
		if err != nil {
			t.Fatal(err)
		}
		var vals []string
		for rows.Next() {
			var v string
			if err := rows.Scan(&v); err != nil {
				rows.Close()
				t.Fatal(err)
			}
			vals = append(vals, v)
		}
		err = rows.Err()
		rows.Close()
		if err != nil {
			t.Fatal(err)
		}
		x[q] = vals
	}
	b, err := json.MarshalIndent(x, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(cfg.Dir, "f11-barrier.json"), b, 0600); err != nil {
		t.Fatal(err)
	}
	t.Logf("F11 barrier cutoff=%s lwd=%s high=%s holes=%v", cutoff, checkpoint.FormatLSN(got.Lwd().LSN), checkpoint.FormatLSN(got.High().LSN), got.Holes())
}
