package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"net/http/httputil"
	"net/url"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"sync/atomic"
	"syscall"
	"time"

	"golang.org/x/crypto/bcrypt"

	"strand/internal/admin"
	"strand/internal/web"
)

const (
	FixturePassword = "strand_browser_test_pw_2026"
)

type mockPipeline struct {
	events []admin.EventRecord
}

func (m *mockPipeline) Top(ctx context.Context) (*admin.TopInfo, error) {
	return &admin.TopInfo{PipelineID: "orders", State: "synced", LagSeconds: 0.12, Throughput: 1500, Zone: "green"}, nil
}
func (m *mockPipeline) Pause(ctx context.Context) (*admin.PauseResult, error)  { return nil, nil }
func (m *mockPipeline) Resume(ctx context.Context, so bool) (*admin.ResumeResult, error) {
	return nil, nil
}
func (m *mockPipeline) ThrottleSet(ctx context.Context, r admin.Request) (*admin.ThrottleResult, error) {
	return nil, nil
}
func (m *mockPipeline) CheckpointShow(ctx context.Context) (*admin.CheckpointResult, error) {
	return &admin.CheckpointResult{PipelineID: "orders", Epoch: 3, Applied: "uuid-1234:1-100", UpdatedAt: time.Now()}, nil
}
func (m *mockPipeline) CheckpointRewind(ctx context.Context, to string) (*admin.CheckpointResult, error) {
	return nil, nil
}
func (m *mockPipeline) Status(ctx context.Context) (*admin.StatusInfo, error) {
	return &admin.StatusInfo{
		PipelineID: "orders", State: "synced", Epoch: 3, StartedAt: time.Now().Add(-2 * time.Hour), UptimeSeconds: 7200,
		StartPosition: "uuid-1234:1-1", SourceKind: "mysql", TargetKind: "postgres", CommitOrder: "strict", Workers: 4,
		AppliedTxns: 1200, AppliedRows: 98000, Governor: admin.GovernorInfo{Algorithm: "grace-v2", Zone: "Green", BackgroundRate: 500},
	}, nil
}
func (m *mockPipeline) Tables(ctx context.Context) ([]admin.TableInfo, error) {
	return []admin.TableInfo{
		{Table: "app.users", State: "done", ProgressPct: 100, PK: []string{"id"}},
		{Table: "app.orders", State: "running", Ranges: map[string]int{"Done": 3, "Pending": 7}, RowsEstimate: 500000, RowsCopied: 175000, ProgressPct: 35.0, ETASeconds: 45, PK: []string{"id"}},
		{Table: "app.products", State: "pending", GenericPK: true, PK: []string{"sku"}},
	}, nil
}
func (m *mockPipeline) Events(ctx context.Context) ([]admin.EventRecord, error) {
	return m.events, nil
}

type FixtureServer struct {
	Dir            string
	DirectURL      string
	ProxyURL       string
	AttackerURL    string
	AuditFile      string
	directListener net.Listener
	proxyListener  net.Listener
	attackListener net.Listener
	webServer      *web.Server
	tableDelayMs   atomic.Int64
}

func main() {
	directPort := flag.Int("direct-port", 0, "Direct port (0 for random)")
	proxyPort := flag.Int("proxy-port", 0, "Reverse proxy port (0 for random)")
	attackPort := flag.Int("attacker-port", 0, "Attacker cross-origin port (0 for random)")
	secureCookies := flag.Bool("secure-cookies", false, "Enable SecureCookies on Strand web server")
	trustForwarded := flag.Bool("trust-forwarded", true, "Enable TrustForwardedFor on Strand web server")
	infoOut := flag.String("info-json", "", "Path to write fixture ports and paths as JSON")
	flag.Parse()

	f, err := StartFixture(*directPort, *proxyPort, *attackPort, *secureCookies, *trustForwarded)
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to start fixture: %v\n", err)
		os.Exit(1)
	}

	info := map[string]any{
		"direct_url":   f.DirectURL,
		"proxy_url":    f.ProxyURL,
		"attacker_url": f.AttackerURL,
		"audit_file":   f.AuditFile,
		"state_dir":    f.Dir,
		"password":     FixturePassword,
	}
	b, _ := json.MarshalIndent(info, "", "  ")
	if *infoOut != "" {
		_ = os.WriteFile(*infoOut, b, 0644)
	}
	fmt.Println(string(b))

	// Keep running until SIGINT or SIGTERM
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, os.Interrupt, syscall.SIGTERM)
	<-sigCh
}

func StartFixture(directPort, proxyPort, attackPort int, secureCookies, trustForwarded bool) (*FixtureServer, error) {
	dir, err := os.MkdirTemp("", "strand-browser-repro-*")
	if err != nil {
		return nil, err
	}

	stateDir := filepath.Join(dir, "state")
	spoolDir := filepath.Join(dir, "spool")
	if err := os.MkdirAll(stateDir, 0755); err != nil {
		return nil, err
	}
	if err := os.MkdirAll(spoolDir, 0755); err != nil {
		return nil, err
	}

	sock := filepath.Join(dir, "admin.sock")
	mock := &mockPipeline{
		events: []admin.EventRecord{
			{Time: time.Now(), Level: "INFO", Message: "fixture pipeline ready", Attrs: map[string]string{"table": "app.orders"}},
		},
	}
	asrv := admin.NewServer(sock, mock, nil)
	if err := asrv.Start(); err != nil {
		return nil, fmt.Errorf("start admin socket: %w", err)
	}

	cfgPath := filepath.Join(dir, "orders.yaml")
	pipelineYAML := fmt.Sprintf(`version: 1
name: orders
source:
  kind: mysql
  dsn: "mysql://strand:FakeSrcPass@127.0.0.1:3306/app"
target:
  kind: postgres
  host: 127.0.0.2
  user: strand
  password: FakeTargetPass
  database: app
tables:
  include: ["app.*"]
resources:
  spool:
    path: %s
observability:
  admin_socket: %s
`, spoolDir, sock)
	if err := os.WriteFile(cfgPath, []byte(pipelineYAML), 0640); err != nil {
		return nil, err
	}

	fakeBin := filepath.Join(dir, "strand-bin")
	if err := os.WriteFile(fakeBin, []byte("#!/bin/sh\nexit 0\n"), 0755); err != nil {
		return nil, err
	}

	pwHash, err := bcrypt.GenerateFromPassword([]byte(FixturePassword), bcrypt.MinCost)
	if err != nil {
		return nil, err
	}

	cfg := &web.Config{
		AllowPlainHTTP:    true,
		Listen:            fmt.Sprintf("127.0.0.1:%d", directPort),
		PasswordHash:      string(pwHash),
		StateDir:          stateDir,
		StrandBinary:      fakeBin,
		SecureCookies:     secureCookies,
		TrustForwardedFor: trustForwarded,
		Pipelines: []web.PipelineEntry{
			{Name: "orders", Config: cfgPath},
		},
	}

	srv, err := web.New(web.Options{
		Config:  cfg,
		Version: "browser-repro-1.0",
		Logger:  slog.Default(),
		Now:     time.Now,
	})
	if err != nil {
		return nil, fmt.Errorf("web.New: %w", err)
	}

	directListener, err := net.Listen("tcp", fmt.Sprintf("127.0.0.1:%d", directPort))
	if err != nil {
		return nil, fmt.Errorf("listen direct: %w", err)
	}
	directAddr := directListener.Addr().(*net.TCPAddr)
	directURL := fmt.Sprintf("http://127.0.0.1:%d", directAddr.Port)

	fix := &FixtureServer{
		Dir:            dir,
		DirectURL:      directURL,
		AuditFile:      filepath.Join(stateDir, "audit.log"),
		directListener: directListener,
		webServer:      srv,
	}

	// Direct server mux: Strand handler + control endpoints
	directMux := http.NewServeMux()
	directMux.HandleFunc("/_control/audit", func(w http.ResponseWriter, r *http.Request) {
		b, err := os.ReadFile(fix.AuditFile)
		if err != nil {
			http.Error(w, err.Error(), 500)
			return
		}
		w.Header().Set("Content-Type", "text/plain")
		w.Write(b)
	})
	directMux.HandleFunc("/_control/set_delay", func(w http.ResponseWriter, r *http.Request) {
		dStr := r.URL.Query().Get("ms")
		var d int64
		fmt.Sscanf(dStr, "%d", &d)
		fix.tableDelayMs.Store(d)
		w.Write([]byte(fmt.Sprintf(`{"table_delay_ms":%d}`, d)))
	})
	directMux.Handle("/", srv.Handler())

	go http.Serve(directListener, directMux)

	// Reverse Proxy Listener
	proxyListener, err := net.Listen("tcp", fmt.Sprintf("127.0.0.1:%d", proxyPort))
	if err != nil {
		return nil, fmt.Errorf("listen proxy: %w", err)
	}
	proxyAddr := proxyListener.Addr().(*net.TCPAddr)
	fix.ProxyURL = fmt.Sprintf("http://127.0.0.1:%d", proxyAddr.Port)
	fix.proxyListener = proxyListener

	targetURL, _ := url.Parse(directURL)
	proxyHandler := httputil.NewSingleHostReverseProxy(targetURL)
	origDirector := proxyHandler.Director
	proxyMux := http.NewServeMux()
	proxyMux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		// Simulate table delay if configured or requested via header/query
		if strings.Contains(r.URL.Path, "/tables") {
			d := fix.tableDelayMs.Load()
			if d > 0 {
				time.Sleep(time.Duration(d) * time.Millisecond)
			}
		}
		// Simulate load balancer returning repeated/stale response from /tables for another tab
		if r.Header.Get("X-Simulate-Mismatched-Response") == "tables" {
			r.URL.Path = "/pipelines/orders/tables"
		}
		origDirector(r)
		// Set reverse proxy headers as documented
		if r.Header.Get("X-Forwarded-For") == "" {
			r.Header.Set("X-Forwarded-For", "203.0.113.195")
		}
		r.Header.Set("X-Forwarded-Host", fmt.Sprintf("127.0.0.1:%d", proxyAddr.Port))
		r.Header.Set("X-Forwarded-Proto", "http")
		proxyHandler.ServeHTTP(w, r)
	})
	go http.Serve(proxyListener, proxyMux)

	// Attacker Server Listener (Cross-Origin site)
	attackListener, err := net.Listen("tcp", fmt.Sprintf("127.0.0.1:%d", attackPort))
	if err != nil {
		return nil, fmt.Errorf("listen attacker: %w", err)
	}
	attackAddr := attackListener.Addr().(*net.TCPAddr)
	fix.AttackerURL = fmt.Sprintf("http://127.0.0.1:%d", attackAddr.Port)
	fix.attackListener = attackListener

	attackMux := http.NewServeMux()
	attackMux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		targetLogin := directURL + "/login"
		targetAPI := directURL + "/api/v1/login"
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		fmt.Fprintf(w, `<!DOCTYPE html>
<html>
<head><title>Attacker Site</title></head>
<body>
  <h1>Attacker CSRF Simulation Page</h1>
  <form id="csrf-form" action="%s" method="POST">
    <input type="hidden" name="password" value="%s">
    <input type="hidden" name="next" value="/pipelines">
    <input type="submit" id="submit-form" value="Auto Submit CSRF Form">
  </form>

  <button id="trigger-fetch" onclick="attackFetch()">Attack Fetch</button>
  <pre id="result"></pre>

  <script>
    async function attackFetch() {
      try {
        const res = await fetch("%s", {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({ password: "%s" })
        });
        const text = await res.text();
        document.getElementById("result").textContent = "STATUS: " + res.status + "\nBODY: " + text;
      } catch (err) {
        document.getElementById("result").textContent = "FETCH_ERR: " + err.message;
      }
    }
  </script>
</body>
</html>`, targetLogin, FixturePassword, targetAPI, FixturePassword)
	})
	go http.Serve(attackListener, attackMux)

	return fix, nil
}
