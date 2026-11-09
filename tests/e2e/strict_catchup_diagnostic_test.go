package e2e

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"strand/internal/admin"
	"strand/internal/checkpoint"
	"strand/internal/model"
)

// Chỉ dùng khi điều tra hai lượt strict: không thay timeout, tải hay barrier.
func enableStrictDiagnostic(t interface{ Logf(string, ...any) }, cfg *strandCfg) {
	if os.Getenv("STRAND_E2E_STRICT_DIAG") != "1" {
		return
	}
	cfg.MetricsAddr = "127.0.0.1:0"
	t.Logf("strict diagnostic: metrics listener enabled; config still uses the existing commit_order, workload and timeout")
}

func cleanupStrictDiagnostic(t *testing.T, p *strandProc) {
	if os.Getenv("STRAND_E2E_STRICT_DIAG") != "1" {
		return
	}
	t.Cleanup(func() {
		if !p.exited() {
			p.kill9()
		}
	})
}

func strictMetricAddress(log string) string {
	for _, line := range strings.Split(log, "\n") {
		var e struct {
			Msg  string `json:"msg"`
			Addr string `json:"addr"`
		}
		if json.Unmarshal([]byte(line), &e) == nil && e.Msg == "metrics and healthz server listening" {
			return e.Addr
		}
	}
	return ""
}

func startStrictDiagnostic(src, dst *pgNode, cfg *strandCfg, p *strandProc, cutoff string, marker int64) func() {
	if os.Getenv("STRAND_E2E_STRICT_DIAG") != "1" || p == nil {
		return func() {}
	}
	path := filepath.Join(cfg.Dir, "strict-catchup-diagnostic.jsonl")
	f, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
	if err != nil {
		fmt.Fprintf(os.Stderr, "strict diagnostic file error: %v\n", err)
		return func() {}
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		defer f.Close()
		sample := func() {
			c, stop := context.WithTimeout(ctx, 4*time.Second)
			defer stop()
			out := map[string]any{"at": time.Now().UTC().Format(time.RFC3339Nano), "cutoff": cutoff, "marker_id": marker, "pid": p.cmd.Process.Pid}
			var n int
			if e := dst.DB.QueryRowContext(c, "SELECT count(*) FROM app.marker WHERE id=$1", marker).Scan(&n); e != nil {
				out["marker_error"] = e.Error()
			} else {
				out["marker_count"] = n
			}
			applied, e := checkpoint.LoadAppliedSnapshotPG(c, dst.DB, cfg.Name, model.EnginePG)
			if e != nil {
				out["checkpoint_error"] = e.Error()
			} else {
				out["checkpoint"] = fmt.Sprint(applied)
				out["lwd"] = checkpoint.FormatLSN(applied.Lwd().LSN)
				out["high"] = checkpoint.FormatLSN(applied.High().LSN)
			}
			top, e := admin.NewClient(cfg.Socket).Top(c)
			if e != nil {
				out["admin_error"] = e.Error()
			} else {
				out["admin"] = top
			}
			const q = `SELECT coalesce(json_agg(row_to_json(s)), '[]'::json)::text FROM (SELECT pid,application_name,backend_type,state,wait_event_type,wait_event,left(query,256) AS query FROM pg_stat_activity WHERE datname=current_database() ORDER BY pid) s`
			query := func(db *sql.DB, key string) {
				var x string
				if e := db.QueryRowContext(c, q).Scan(&x); e != nil {
					out[key+"_error"] = e.Error()
				} else {
					out[key] = json.RawMessage(x)
				}
			}
			var wal string
			if e := src.DB.QueryRowContext(c, `SELECT json_build_object('write',pg_current_wal_lsn()::text,'insert',pg_current_wal_insert_lsn()::text)::text`).Scan(&wal); e != nil {
				out["source_wal_error"] = e.Error()
			} else {
				out["source_wal"] = json.RawMessage(wal)
			}
			query(src.DB, "source_activity")
			query(dst.DB, "target_activity")
			var slot string
			e = src.DB.QueryRowContext(c, `SELECT coalesce(json_agg(row_to_json(s)), '[]'::json)::text FROM (SELECT slot_name,active,restart_lsn,confirmed_flush_lsn,wal_status FROM pg_replication_slots WHERE database=current_database()) s`).Scan(&slot)
			if e != nil {
				out["slot_error"] = e.Error()
			} else {
				out["slot"] = json.RawMessage(slot)
			}
			addr := strictMetricAddress(p.output.String())
			if addr != "" {
				req, _ := http.NewRequestWithContext(c, http.MethodGet, "http://"+addr+"/metrics", nil)
				resp, e := http.DefaultClient.Do(req)
				if e != nil {
					out["metrics_error"] = e.Error()
				} else {
					b, e := io.ReadAll(io.LimitReader(resp.Body, 256<<10))
					resp.Body.Close()
					if e != nil {
						out["metrics_error"] = e.Error()
					} else {
						out["metrics"] = string(b)
					}
				}
			} else {
				out["metrics_error"] = "listener address not yet logged"
			}
			b, e := json.Marshal(out)
			if e == nil {
				fmt.Fprintln(f, string(b))
			} else {
				fmt.Fprintf(os.Stderr, "strict diagnostic marshal: %v\n", e)
			}
		}
		sample()
		ticker := time.NewTicker(15 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				sample()
			}
		}
	}()
	return func() { cancel(); <-done }
}

func TestStrictDiagnosticMetricAddress(t *testing.T) {
	log := "not json\n{\"msg\":\"other\",\"addr\":\"wrong\"}\n{\"msg\":\"metrics and healthz server listening\",\"addr\":\"127.0.0.1:24680\"}\n"
	if got := strictMetricAddress(log); got != "127.0.0.1:24680" {
		t.Fatalf("metrics address %q", got)
	}
	if got := strictMetricAddress("invalid"); got != "" {
		t.Fatalf("unexpected metrics address %q", got)
	}
}
