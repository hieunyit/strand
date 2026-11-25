package runner

import (
	"context"
	"database/sql"
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	_ "github.com/go-sql-driver/mysql"
	_ "github.com/jackc/pgx/v5/stdlib"

	"strand/internal/backup"
	"strand/internal/objectstore"
	"strand/internal/testutil"
	"strand/internal/web/store"
)

// Integration fixtures: real database containers named bkp-* on the host network (unique ports), with a
// shared temp directory mounted at the same path so native tools run inside the containers through
// docker-exec wrappers see the files Strand writes ({dir}).

func requireDocker(t *testing.T) {
	t.Helper()
	if testing.Short() {
		t.Skip("integration test (-short)")
	}
	if err := exec.Command("docker", "version").Run(); err != nil {
		t.Fatalf("docker is required for backup integration tests: %v", err)
	}
}

var portMu sync.Mutex
var nextPort = 0

func freePort(t *testing.T) int {
	t.Helper()
	portMu.Lock()
	defer portMu.Unlock()
	for i := 0; i < 200; i++ {
		l, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		p := l.Addr().(*net.TCPAddr).Port
		l.Close()
		if p != nextPort {
			nextPort = p
			return p
		}
	}
	t.Fatal("no free port")
	return 0
}

// shared is a temp dir visible at the same path on the host and in every container.
func shared(t *testing.T) string {
	t.Helper()
	d, err := os.MkdirTemp("", "bkp-shared-")
	if err != nil {
		t.Fatal(err)
	}
	_ = os.Chmod(d, 0o777)
	t.Cleanup(func() {
		// Files written by container users: remove through a container.
		_ = exec.Command("docker", "run", "--rm", "--name", fmt.Sprintf("%s-cleanup-%d-%d", containerPrefix("bkp"), os.Getpid(), time.Now().UnixNano()),
			"-v", d+":"+d, "debian:bookworm-slim", "rm", "-rf", d+"/.").Run()
		_ = os.RemoveAll(d)
	})
	return d
}

func docker(t *testing.T, args ...string) string {
	t.Helper()
	out, err := exec.Command("docker", args...).CombinedOutput()
	if err != nil {
		t.Fatalf("docker %s: %v\n%s", strings.Join(args, " "), err, out)
	}
	return strings.TrimSpace(string(out))
}

// containerPrefix names test containers: bkp, or STRAND_TEST_CONTAINER_PREFIX on a shared host.
func containerPrefix(fallback string) string {
	if p := os.Getenv("STRAND_TEST_CONTAINER_PREFIX"); p != "" {
		return p + "-" + fallback
	}
	return fallback
}

func startContainer(t *testing.T, name, dir string, args ...string) string {
	t.Helper()
	full := fmt.Sprintf("%s-%s-%d", containerPrefix("bkp"), name, os.Getpid())
	_ = exec.Command("docker", "rm", "-f", full).Run()
	run := append([]string{"run", "-d", "--name", full, "--network", "host", "-v", dir + ":" + dir}, args...)
	docker(t, run...)
	t.Cleanup(func() {
		if t.Failed() {
			out, _ := exec.Command("docker", "logs", "--tail", "40", full).CombinedOutput()
			t.Logf("%s logs:\n%s", full, out)
		}
		_ = exec.Command("docker", "rm", "-f", "-v", full).Run()
	})
	return full
}

// wrapper writes tools/<tool> running the tool inside container with the listed env vars forwarded.
func wrapper(t *testing.T, toolsDir, tool, container, inner string, env ...string) {
	t.Helper()
	var b strings.Builder
	b.WriteString("#!/bin/sh\nexec docker exec -i")
	for _, e := range append(env, "TZ") {
		b.WriteString(" -e " + e)
	}
	b.WriteString(" " + container + " " + inner + " \"$@\"\n")
	if err := os.WriteFile(filepath.Join(toolsDir, tool), []byte(b.String()), 0o755); err != nil {
		t.Fatal(err)
	}
}

func waitSQL(t *testing.T, driver, dsn string) *sql.DB {
	t.Helper()
	db, err := sql.Open(driver, dsn)
	if err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(3 * time.Minute)
	for {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		err = db.PingContext(ctx)
		cancel()
		if err == nil {
			t.Cleanup(func() { db.Close() })
			return db
		}
		if time.Now().After(deadline) {
			t.Fatalf("%s not ready: %v", driver, err)
		}
		time.Sleep(time.Second)
	}
}

func mustExec(t *testing.T, db *sql.DB, q string, args ...any) {
	t.Helper()
	if _, err := db.Exec(q, args...); err != nil {
		t.Fatalf("%s: %v", q, err)
	}
}

type mysqlFixture struct {
	Container string
	Port      int
	DB        *sql.DB
}

func startMySQL(t *testing.T, dir, image string) mysqlFixture {
	port := freePort(t)
	c := startContainer(t, "mysql", dir, "-e", "MYSQL_ROOT_PASSWORD=rootpw", image, fmt.Sprintf("--port=%d", port),
		"--mysqlx=OFF", "--gtid-mode=ON", "--enforce-gtid-consistency=ON", "--server-id=1", "--log-bin=binlog",
		"--binlog-format=ROW", "--skip-name-resolve")
	db := waitSQL(t, "mysql", fmt.Sprintf("root:rootpw@tcp(127.0.0.1:%d)/?multiStatements=true&parseTime=true", port))
	return mysqlFixture{Container: c, Port: port, DB: db}
}

type pgFixture struct {
	Container string
	Port      int
	DB        *sql.DB
}

func startPG(t *testing.T, dir string, extra ...string) pgFixture {
	port := freePort(t)
	args := []string{"-e", "POSTGRES_PASSWORD=pgpw", "postgres:17", "-p", fmt.Sprint(port), "-c", "wal_level=replica",
		"-c", "max_wal_senders=5", "-c", "listen_addresses=127.0.0.1"}
	c := startContainer(t, "pg", dir, append(args, extra...)...)
	db := waitSQL(t, "pgx", fmt.Sprintf("postgres://postgres:pgpw@127.0.0.1:%d/postgres?sslmode=disable", port))
	// The entrypoint restarts the server once after init: wait until it is stable.
	time.Sleep(2 * time.Second)
	db = waitSQL(t, "pgx", fmt.Sprintf("postgres://postgres:pgpw@127.0.0.1:%d/postgres?sslmode=disable", port))
	return pgFixture{Container: c, Port: port, DB: db}
}

// minio starts MinIO and returns an env with STRAND_BACKUP_S3_* set and the bucket created.
func startMinIO(t *testing.T, dir string, buckets ...string) map[string]string {
	port := freePort(t)
	startContainer(t, "minio", dir, "-e", "MINIO_ROOT_USER=minioadmin", "-e", "MINIO_ROOT_PASSWORD=minioadmin", "--entrypoint", testutil.MinIOEntrypoint, testutil.PinnedMinIOImage,
		"server", "/tmp/minio-data", "--address", fmt.Sprintf("127.0.0.1:%d", port), "--console-address", fmt.Sprintf("127.0.0.1:%d", freePort(t)))
	env := map[string]string{"STRAND_BACKUP_S3_ENDPOINT": fmt.Sprintf("http://127.0.0.1:%d", port), "STRAND_BACKUP_S3_ACCESS_KEY": "minioadmin",
		"STRAND_BACKUP_S3_SECRET_KEY": "minioadmin", "STRAND_BACKUP_S3_ALLOW_INSECURE_LOCAL": "true"}
	for _, b := range buckets {
		lock := strings.HasPrefix(b, "locked")
		deadline := time.Now().Add(time.Minute)
		for {
			obj, err := objectstore.New(objectstore.Config{Endpoint: env["STRAND_BACKUP_S3_ENDPOINT"], Bucket: b, AccessKey: "minioadmin",
				SecretKey: "minioadmin", AllowInsecureLocal: true, MaxRetries: -1})
			if err != nil {
				t.Fatal(err)
			}
			if lock {
				err = obj.CreateBucketWithObjectLock(context.Background())
			} else {
				err = obj.CreateBucket(context.Background())
			}
			if err == nil {
				break
			}
			if time.Now().After(deadline) {
				t.Fatalf("create bucket %s: %v", b, err)
			}
			time.Sleep(500 * time.Millisecond)
		}
	}
	return env
}

func testEnv(t *testing.T, vars map[string]string, st store.Store) *Env {
	t.Helper()
	cat, err := backup.NewCatalog(st)
	if err != nil {
		t.Fatal(err)
	}
	get := func(k string) string {
		if v, ok := vars[k]; ok {
			return v
		}
		return ""
	}
	keys, err := backup.LoadKeyring(get)
	if err != nil {
		t.Fatal(err)
	}
	return &Env{Getenv: get, Catalog: cat, Keys: keys, Log: testWriter{t},
		Repo: func(ctx context.Context, id string) (backup.Repo, bool, error) { return st.GetBackupRepo(ctx, id) }}
}

type testWriter struct{ t *testing.T }

func (w testWriter) Write(p []byte) (int, error) {
	w.t.Log(strings.TrimRight(string(p), "\n"))
	return len(p), nil
}
