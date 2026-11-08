package e2e

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"net/url"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"strand/internal/checkpoint"
	"strand/internal/model"
)

// Chặn từ bên ngoài ngay SAU một watermark, trước mọi event của commit kế tiếp.
// Không cài failpoint trong production: CP-1/2 phải đúng kể cả reader bị đứng rồi kill -9.
type f11WALGate struct {
	ln      net.Listener
	ctx     context.Context
	cancel  context.CancelFunc
	ready   chan uint64
	release chan struct{}
	once    sync.Once
	fired   atomic.Bool
	wg      sync.WaitGroup
	mu      sync.Mutex
	clients []net.Conn
	accept  chan struct{}
}

func startF11WALGate(t *testing.T, dsn, content string) (string, *f11WALGate) {
	t.Helper()
	u, err := url.Parse(dsn)
	if err != nil {
		t.Fatal(err)
	}
	upstream := u.Host
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	g := &f11WALGate{ln: ln, ctx: ctx, cancel: cancel, ready: make(chan uint64, 1), release: make(chan struct{}), accept: make(chan struct{})}
	go func() {
		defer close(g.accept)
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			g.mu.Lock()
			g.clients = append(g.clients, c)
			g.mu.Unlock()
			g.wg.Add(1)
			go g.forward(c, upstream, content)
		}
	}()
	t.Cleanup(func() {
		cancel()
		_ = ln.Close()
		<-g.accept // Không còn Add khi Wait bắt đầu.
		g.mu.Lock()
		for _, c := range g.clients {
			_ = c.Close()
		}
		g.mu.Unlock()
		g.wg.Wait()
	})
	u.Host = ln.Addr().String()
	return u.String(), g
}

func (g *f11WALGate) unblock() { g.once.Do(func() { close(g.release) }) }

func (g *f11WALGate) forward(client net.Conn, upstream, content string) {
	defer g.wg.Done()
	defer client.Close()
	server, err := net.DialTimeout("tcp", upstream, 5*time.Second)
	if err != nil {
		return
	}
	defer server.Close()
	frontDone := make(chan struct{})
	go func() {
		defer close(frontDone)
		defer server.Close() // Client chết thì đánh thức ReadFull ở chiều ngược.
		_, _ = io.Copy(server, client)
	}()
	defer func() { _ = client.Close(); _ = server.Close(); <-frontDone }()
	for {
		var header [5]byte
		if _, err := io.ReadFull(server, header[:]); err != nil {
			return
		}
		n := int(binary.BigEndian.Uint32(header[1:]))
		if n < 4 || n > 64<<20 {
			return
		}
		payload := make([]byte, n-4)
		if _, err := io.ReadFull(server, payload); err != nil {
			return
		}
		if _, err := io.Copy(client, bytes.NewReader(append(header[:], payload...))); err != nil {
			return
		}
		// CopyData: XLogData gồm byte 'w' + ba uint64; sau đó là message pgoutput.
		if header[0] != 'd' || len(payload) < 36 || payload[0] != 'w' {
			continue
		}
		m := payload[25:]
		if m[0] != 'M' || m[1] != 0 {
			continue
		}
		prefixEnd := bytes.IndexByte(m[10:], 0) + 10
		if prefixEnd < 10 || prefixEnd+5 > len(m) || string(m[10:prefixEnd]) != "strand_watermark" {
			continue
		}
		size := int(binary.BigEndian.Uint32(m[prefixEnd+1 : prefixEnd+5]))
		if size != len(m)-prefixEnd-5 || string(m[prefixEnd+5:]) != content || !g.fired.CompareAndSwap(false, true) {
			continue
		}
		g.ready <- binary.BigEndian.Uint64(m[2:10])
		select {
		case <-g.release:
		case <-g.ctx.Done():
			return
		}
	}
}

// Tái hiện biên F11 trên PG thật: message end == commit start, checkpoint bền trong
// khi event Txn chưa tới reader, kill -9 rồi restart. Phải replay đủ, đúng một lần (CP-1/2).
func TestF11_E2E_WatermarkBoundaryCrashRestart(t *testing.T) {
	seedFromEnv(t)
	src, dst, start := setupPGPair(t, "f11_boundary", "f11_boundary_pub")
	src.exec(t, "CREATE EXTENSION IF NOT EXISTS pg_walinspect")
	dst.exec(t, `CREATE TABLE app.f11_marker_audit (id bigint PRIMARY KEY, n integer NOT NULL);
		CREATE FUNCTION app.f11_audit_marker() RETURNS trigger LANGUAGE plpgsql AS $$
		BEGIN
			INSERT INTO app.f11_marker_audit VALUES (NEW.id, 1)
			ON CONFLICT (id) DO UPDATE SET n = app.f11_marker_audit.n + 1;
			RETURN NEW;
		END $$;
		CREATE TRIGGER f11_audit_marker AFTER INSERT OR UPDATE ON app.marker
		FOR EACH ROW EXECUTE FUNCTION app.f11_audit_marker()`)
	const content = "f11_boundary_gate"
	proxied, gate := startF11WALGate(t, src.DSN, content)
	cfg := strandCfg{Name: "f11-boundary", SourceKind: "postgres", TargetKind: "postgres", SourceDSN: proxied, TargetDSN: dst.DSN,
		SourceExtra: ", slot: f11_boundary, publication: f11_boundary_pub", Include: []string{"app.*"}, Dir: testDir(t), Bootstrap: "start_at", SourceIOPS: 20000}
	path := cfg.write(t)
	p := startStrand(t, path, "--start-at", start)
	t.Cleanup(func() {
		if !p.exited() {
			p.kill9()
		}
	})
	waitSynced(t, &cfg, p, time.Minute)
	waitPGCaughtUp(t, src, dst, &cfg, p, time.Minute)

	// Ghi row trước message, rồi COMMIT ngay sau message: pgoutput phát message trước
	// BEGIN/row/COMMIT của Txn này. Kiểm WAL vật lý thay vì giả định hai vị trí trùng nhau.
	tx, err := src.DB.Begin()
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	const id = int64(900000001)
	var xid, end string
	if err := tx.QueryRow("INSERT INTO app.marker VALUES ($1, 'must-replay') RETURNING xmin::text", id).Scan(&xid); err != nil {
		t.Fatal(err)
	}
	if err := tx.QueryRow("SELECT pg_logical_emit_message(false, 'strand_watermark', $1)::text", content).Scan(&end); err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	endLSN, err := checkpoint.ParseLSN(end)
	if err != nil {
		t.Fatal(err)
	}
	select {
	case got := <-gate.ready:
		if got != endLSN {
			t.Fatalf("wire end LSN %X differs from SQL %s", got, end)
		}
	case <-time.After(30 * time.Second):
		t.Fatalf("watermark never reached gate\n%s", p.tail())
	}
	deadline := time.Now().Add(30 * time.Second)
	for {
		var flushed bool
		if err := src.DB.QueryRow("SELECT pg_current_wal_flush_lsn() > $1::pg_lsn", end).Scan(&flushed); err != nil {
			t.Fatal(err)
		}
		if flushed {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("commit WAL not flushed")
		}
		time.Sleep(10 * time.Millisecond)
	}
	var recordStart, recordXID, resource, kind string
	if err := src.DB.QueryRow("SELECT start_lsn::text,xid::text,resource_manager,record_type FROM pg_get_wal_record_info($1::pg_lsn)", end).
		Scan(&recordStart, &recordXID, &resource, &kind); err != nil {
		t.Fatal(err)
	}
	if recordStart != end || recordXID != xid || resource != "Transaction" || kind != "COMMIT" {
		t.Fatalf("required boundary absent: message end=%s next record=%s xid=%s (want %s) %s/%s", end, recordStart, recordXID, xid, resource, kind)
	}
	t.Logf("physical WAL proves message end=commit start=%s xid=%s; next transaction blocked at proxy", end, xid)
	var applied checkpoint.AppliedSet
	for {
		var encoded string
		err := dst.DB.QueryRow("SELECT applied FROM _strand_checkpoint WHERE pipeline_id=$1", cfg.Name).Scan(&encoded)
		if err == nil {
			applied, err = checkpoint.ParseAppliedSet(model.EnginePG, encoded)
		}
		if err == nil && applied.Lwd().LSN >= endLSN-1 {
			break
		}
		if p.exited() || time.Now().After(deadline) {
			t.Fatalf("watermark checkpoint not durable: %v\n%s", err, p.tail())
		}
		time.Sleep(20 * time.Millisecond)
	}
	var n int
	if err := dst.DB.QueryRow("SELECT count(*) FROM app.marker WHERE id=$1", id).Scan(&n); err != nil || n != 0 {
		t.Fatalf("gate failed to hold transaction: rows=%d err=%v", n, err)
	}
	if err := dst.DB.QueryRow("SELECT count(*) FROM app.f11_marker_audit WHERE id=$1", id).Scan(&n); err != nil || n != 0 {
		t.Fatalf("marker audit present before crash: rows=%d err=%v", n, err)
	}
	if applied.Contains(model.Pos{Engine: model.EnginePG, LSN: endLSN}) {
		t.Errorf("durable checkpoint %s covers missing xid=%s commit=%s before crash", fmt.Sprint(applied), xid, end)
	}
	p.kill9()
	gate.unblock()
	p = startStrand(t, path)
	waitSynced(t, &cfg, p, time.Minute)
	waitPGCaughtUp(t, src, dst, &cfg, p, time.Minute)
	if code := p.stop(t); code != 0 {
		t.Fatalf("restart exit=%d\n%s", code, p.tail())
	}
	if err := dst.DB.QueryRow("SELECT count(*) FROM app.marker WHERE id=$1", id).Scan(&n); err != nil || n != 1 {
		t.Fatalf("restart lost entire transaction xid=%s commit=%s rows=%d err=%v", xid, end, n, err)
	}
	if err := dst.DB.QueryRow("SELECT n FROM app.f11_marker_audit WHERE id=$1", id).Scan(&n); err != nil || n != 1 {
		t.Fatalf("marker applied incorrect number of times: n=%d err=%v", n, err)
	}
	comparePG(t, src, dst, "app.marker")
}
