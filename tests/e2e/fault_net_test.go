package e2e

import (
	"bytes"
	"context"
	"fmt"
	"math/rand"
	"net"
	"net/http"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/docker/go-connections/nat"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/network"
	"github.com/testcontainers/testcontainers-go/wait"
)

type toxiproxy struct {
	api   string
	ports map[string]string // proxy name -> host:port từ máy test
}

func startToxiproxy(t *testing.T, nw *testcontainers.DockerNetwork, proxies map[string]string) *toxiproxy {
	t.Helper()
	ctx := context.Background()
	exposed := []string{"8474/tcp"}
	i := 0
	listen := map[string]string{}
	for name := range proxies {
		port := fmt.Sprintf("%d", 8666+i)
		exposed = append(exposed, port+"/tcp")
		listen[name] = port
		i++
	}
	req := testcontainers.GenericContainerRequest{ContainerRequest: testcontainers.ContainerRequest{
		Image: "ghcr.io/shopify/toxiproxy:latest", ExposedPorts: exposed,
		WaitingFor: wait.ForListeningPort("8474/tcp").WithStartupTimeout(60 * time.Second),
	}, Started: true}
	if err := network.WithNetwork([]string{"toxiproxy"}, nw)(&req); err != nil {
		t.Fatal(err)
	}
	c, err := testcontainers.GenericContainer(ctx, req)
	if err != nil {
		t.Fatalf("start toxiproxy: %v", err)
	}
	t.Cleanup(func() { _ = c.Terminate(context.Background()) })
	host, _ := c.Host(ctx)
	api, _ := c.MappedPort(ctx, "8474")
	tp := &toxiproxy{api: "http://" + net.JoinHostPort(host, api.Port()), ports: map[string]string{}}
	for name, upstream := range proxies {
		body := fmt.Sprintf(`{"name":%q,"listen":"0.0.0.0:%s","upstream":%q}`, name, listen[name], upstream)
		tp.post(t, "/proxies", body)
		mp, _ := c.MappedPort(ctx, nat.Port(listen[name]+"/tcp"))
		tp.ports[name] = net.JoinHostPort(host, mp.Port())
	}
	return tp
}

func (tp *toxiproxy) post(t *testing.T, path, body string) {
	resp, err := http.Post(tp.api+path, "application/json", bytes.NewBufferString(body))
	if err != nil {
		t.Fatalf("toxiproxy %s: %v", path, err)
	}
	resp.Body.Close()
	if resp.StatusCode >= 300 {
		t.Fatalf("toxiproxy %s: status %d", path, resp.StatusCode)
	}
}

func (tp *toxiproxy) del(path string) {
	req, _ := http.NewRequest(http.MethodDelete, tp.api+path, nil)
	if resp, err := http.DefaultClient.Do(req); err == nil {
		resp.Body.Close()
	}
}

func (tp *toxiproxy) setEnabled(t *testing.T, name string, on bool) {
	tp.post(t, "/proxies/"+name, fmt.Sprintf(`{"enabled":%v}`, on))
}

// supervisor mô phỏng systemd (03 §4): restart khi exit code không thuộc RestartPreventExitStatus.
type supervisor struct {
	t        *testing.T
	cfgPath  string
	mu       sync.Mutex
	cur      *strandProc
	stop     atomic.Bool
	restarts atomic.Int64
	fatal    atomic.Value
	done     chan struct{}
}

func superviseStrand(t *testing.T, cfgPath string) *supervisor {
	s := &supervisor{t: t, cfgPath: cfgPath, done: make(chan struct{})}
	go func() {
		defer close(s.done)
		for !s.stop.Load() {
			p := startStrand(t, cfgPath)
			s.mu.Lock()
			s.cur = p
			s.mu.Unlock()
			code := p.exitCode()
			if s.stop.Load() {
				return
			}
			switch code {
			case 2, 3, 4, 5, 10:
				s.fatal.Store(fmt.Sprintf("strand exited with non-restartable code %d\n%s", code, p.tail()))
				return
			}
			s.restarts.Add(1)
			time.Sleep(time.Second) // RestartSec
		}
	}()
	return s
}

func (s *supervisor) current() *strandProc {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.cur
}

// Fault injection (04 §4, Gate 1): mạng tới source (latency, cắt, reset) và tới target (cắt
// giữa lúc commit batch). Kỳ vọng: reconnect/restart, resume đúng vị trí, không apply trùng, 0 lệch.
func TestG1_E2E_NetworkFaultsSourceAndTarget(t *testing.T) {
	seed := seedFromEnv(t)
	rng := rand.New(rand.NewSource(seed))
	ctx := context.Background()
	nw, err := network.New(ctx)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = nw.Remove(context.Background()) })
	src := startMySQL(t, mysqlImage("source"), "src", 1, nw)
	dst := startMySQL(t, mysqlImage("target"), "dst", 2, nw)
	createSchema(t, src, dst)
	dst.exec(t, "CREATE DATABASE IF NOT EXISTS strand")
	tp := startToxiproxy(t, nw, map[string]string{"source": "src:3306", "target": "dst:3306"})

	wl := newWorkload(src.DB, seed)
	wl.Preload(t, 5000)
	wl.Start(6, 5000)

	cfg := strandCfg{Name: "e2e-netfault", Dir: testDir(t),
		SourceDSN: fmt.Sprintf("root:dev@tcp(%s)/?timeout=5s&readTimeout=30s&writeTimeout=30s", tp.ports["source"]),
		TargetDSN: fmt.Sprintf("root:dev@tcp(%s)/strand?timeout=5s&readTimeout=30s&writeTimeout=30s", tp.ports["target"]),
		Include:   []string{"app.*"}, Exclude: []string{"app.ignored"}}
	sup := superviseStrand(t, cfg.write(t))
	time.Sleep(15 * time.Second)

	for round := 0; round < 6; round++ {
		name := []string{"source", "target"}[round%2]
		switch round % 3 {
		case 0: // latency 200 ms
			tp.post(t, "/proxies/"+name+"/toxics", `{"name":"lat","type":"latency","attributes":{"latency":200,"jitter":50}}`)
			time.Sleep(time.Duration(5+rng.Intn(5)) * time.Second)
			tp.del("/proxies/" + name + "/toxics/lat")
		case 1: // cắt hẳn rồi nối lại
			tp.setEnabled(t, name, false)
			time.Sleep(time.Duration(2+rng.Intn(5)) * time.Second)
			tp.setEnabled(t, name, true)
		case 2: // reset_peer
			tp.post(t, "/proxies/"+name+"/toxics", `{"name":"rst","type":"reset_peer","attributes":{"timeout":500}}`)
			time.Sleep(3 * time.Second)
			tp.del("/proxies/" + name + "/toxics/rst")
		}
		t.Logf("fault round %d on %s done (restarts so far %d)", round, name, sup.restarts.Load())
		time.Sleep(8 * time.Second)
		if f := sup.fatal.Load(); f != nil {
			t.Fatal(f)
		}
	}
	wl.Stop()

	deadline := time.Now().Add(5 * time.Minute)
	for !pipelineSynced(cfg.Socket) && time.Now().Before(deadline) {
		time.Sleep(time.Second)
	}
	waitCaughtUp(t, src, dst, &cfg, sup.current(), 5*time.Minute)
	sup.stop.Store(true)
	sup.current().stop(t)
	<-sup.done
	compareFinal(t, src, dst, appTables...)
	t.Logf("network faults: %d supervised restarts, 0 deviation", sup.restarts.Load())
}
