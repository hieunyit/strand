package archive_test

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"strconv"
	"testing"
	"time"

	gomysql "github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/replication"
	_ "github.com/go-sql-driver/mysql"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"

	"strand/internal/exitcode"
	"strand/internal/reader"
	"strand/internal/reader/archive"
)

type mysqlNode struct {
	db   *sql.DB
	host string
	port int
}

func startMySQL(t *testing.T) *mysqlNode {
	t.Helper()
	ctx := context.Background()
	c, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: testcontainers.ContainerRequest{
			Image:        "mysql:8.4",
			ExposedPorts: []string{"3306/tcp"},
			Env:          map[string]string{"MYSQL_ROOT_PASSWORD": "dev"},
			Cmd: []string{"--server-id=11", "--gtid-mode=ON", "--enforce-gtid-consistency=ON", "--binlog-format=ROW",
				"--binlog-row-image=FULL", "--binlog-row-metadata=FULL", "--innodb-use-native-aio=OFF"},
			WaitingFor: wait.ForLog("port: 3306  MySQL Community Server").WithStartupTimeout(180 * time.Second),
		},
		Started: true,
	})
	if err != nil {
		t.Fatalf("start mysql: %v", err)
	}
	t.Cleanup(func() { _ = c.Terminate(context.Background()) })
	host, _ := c.Host(ctx)
	p, _ := c.MappedPort(ctx, "3306")
	port, _ := strconv.Atoi(p.Port())
	db, err := sql.Open("mysql", fmt.Sprintf("root:dev@tcp(%s)/?multiStatements=true", net.JoinHostPort(host, p.Port())))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	for i := 0; ; i++ {
		if err = db.PingContext(ctx); err == nil {
			break
		}
		if i == 30 {
			t.Fatalf("ping mysql: %v", err)
		}
		time.Sleep(time.Second)
	}
	return &mysqlNode{db: db, host: host, port: port}
}

func (n *mysqlNode) exec(t *testing.T, q string, args ...any) {
	t.Helper()
	if _, err := n.db.Exec(q, args...); err != nil {
		t.Fatalf("%s: %v", q, err)
	}
}

func (n *mysqlNode) executed(t *testing.T) *gomysql.MysqlGTIDSet {
	t.Helper()
	var s string
	if err := n.db.QueryRow("SELECT @@GLOBAL.gtid_executed").Scan(&s); err != nil {
		t.Fatal(err)
	}
	g, err := gomysql.ParseMysqlGTIDSet(s)
	if err != nil {
		t.Fatal(err)
	}
	return g.(*gomysql.MysqlGTIDSet)
}

func (n *mysqlNode) insert(t *testing.T, from, count int) {
	t.Helper()
	for i := from; i < from+count; i++ {
		n.exec(t, "INSERT INTO app.t (id, v) VALUES (?, ?)", i, fmt.Sprintf("row-%d", i))
	}
}

type runningArchiver struct {
	a      *archive.Archiver
	cancel context.CancelFunc
	done   chan error
}

func runArchiver(t *testing.T, n *mysqlNode, dir string) *runningArchiver {
	t.Helper()
	a, err := archive.OpenArchiver(archive.ArchiverConfig{
		Dir: dir,
		Syncer: replication.BinlogSyncerConfig{ServerID: 4242, Flavor: "mysql", Host: n.host, Port: uint16(n.port),
			User: "root", Password: "dev"},
		Heartbeat: 2 * time.Second,
		RetryWait: 500 * time.Millisecond,
		Log:       slog.New(slog.NewTextHandler(io.Discard, nil)),
	})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	r := &runningArchiver{a: a, cancel: cancel, done: make(chan error, 1)}
	go func() { r.done <- a.Run(ctx) }()
	return r
}

func (r *runningArchiver) stop(t *testing.T) {
	t.Helper()
	r.cancel()
	if err := <-r.done; err != nil {
		t.Fatalf("archiver: %v", err)
	}
	if err := r.a.Close(); err != nil {
		t.Fatal(err)
	}
}

// waitArchived chờ tới khi tập GTID đã chốt trong archive chứa want.
func waitArchived(t *testing.T, r *runningArchiver, want *gomysql.MysqlGTIDSet) {
	t.Helper()
	deadline := time.Now().Add(60 * time.Second)
	for {
		g, err := gomysql.ParseMysqlGTIDSet(r.a.Executed())
		if err != nil {
			t.Fatal(err)
		}
		if g.Contain(want) {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("archive has %s, want %s", g, want)
		}
		select {
		case err := <-r.done:
			t.Fatalf("archiver stopped: %v", err)
		case <-time.After(200 * time.Millisecond):
		}
	}
}

// Ghi, rotate, dừng archiver giữa chừng, ghi tiếp khi nó dừng, chạy lại, purge binlog trên source:
// ArchiveReader (seed apply --archive) đọc lại đúng mọi transaction của source, mỗi GTID đúng một lần.
func TestArchiverIntegration_ResumesWithoutGapOrDuplicate(t *testing.T) {
	n := startMySQL(t)
	n.exec(t, "CREATE DATABASE app; CREATE TABLE app.t (id INT PRIMARY KEY, v VARCHAR(32))")
	n.insert(t, 0, 40)
	n.exec(t, "FLUSH BINARY LOGS")
	n.insert(t, 40, 40)

	dir := t.TempDir()
	r := runArchiver(t, n, dir)
	n.exec(t, "FLUSH BINARY LOGS")
	waitArchived(t, r, n.executed(t))
	n.insert(t, 80, 30) // nằm trong file đang ghi dở khi dừng
	time.Sleep(time.Second)
	r.stop(t)

	n.insert(t, 110, 30) // khi archiver không chạy
	n.exec(t, "FLUSH BINARY LOGS")
	n.insert(t, 140, 30)

	r = runArchiver(t, n, dir)
	n.exec(t, "FLUSH BINARY LOGS")
	want := n.executed(t)
	waitArchived(t, r, want)
	r.stop(t)
	// Source không còn file cũ: archive phải tự đủ.
	n.exec(t, "PURGE BINARY LOGS BEFORE NOW() + INTERVAL 1 DAY")

	ar, err := archive.New(archive.Config{Dir: dir})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	evs, err := ar.Start(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	seen := map[string]int{}
	got := gomysql.MysqlGTIDSet{}
	ids := map[int64]bool{}
	for ev := range evs {
		switch ev.Type {
		case reader.EventError:
			t.Fatalf("archive read: %v", ev.Err)
		case reader.EventGTID:
			seen[ev.GTID]++
			if err := got.Update(ev.GTID); err != nil {
				t.Fatal(err)
			}
		case reader.EventRows:
			if ev.Schema == "app" && ev.Table == "t" {
				for _, row := range ev.Rows {
					ids[toInt64(t, row[0])] = true
				}
			}
		}
	}
	_ = ar.Close()
	for g, c := range seen {
		if c != 1 {
			t.Fatalf("GTID %s archived %d times", g, c)
		}
	}
	if !got.Equal(want) {
		t.Fatalf("archive GTIDs %s, source executed %s", got.String(), want.String())
	}
	if len(ids) != 170 {
		t.Fatalf("archive has %d rows of app.t, want 170", len(ids))
	}
}

// Source purge phần chưa được lưu khi archiver dừng: chạy lại phải báo log_gap (exit 5), không im lặng
// bỏ qua phần hở.
func TestArchiverIntegration_PurgedGapIsLogGap(t *testing.T) {
	n := startMySQL(t)
	n.exec(t, "CREATE DATABASE app; CREATE TABLE app.t (id INT PRIMARY KEY, v VARCHAR(32))")
	dir := t.TempDir()
	r := runArchiver(t, n, dir)
	n.insert(t, 0, 10)
	n.exec(t, "FLUSH BINARY LOGS")
	waitArchived(t, r, n.executed(t))
	r.stop(t)

	n.insert(t, 10, 10)
	n.exec(t, "FLUSH BINARY LOGS")
	n.exec(t, "PURGE BINARY LOGS BEFORE NOW() + INTERVAL 1 DAY")

	r = runArchiver(t, n, dir)
	select {
	case err := <-r.done:
		var se *exitcode.Error
		if !errors.As(err, &se) || se.Code != exitcode.ErrLogGap {
			t.Fatalf("want log_gap, got %v", err)
		}
	case <-time.After(60 * time.Second):
		t.Fatal("archiver did not report the gap")
	}
	_ = r.a.Close()
}

func toInt64(t *testing.T, v any) int64 {
	t.Helper()
	switch x := v.(type) {
	case int32:
		return int64(x)
	case int64:
		return x
	case int:
		return int64(x)
	case uint32:
		return int64(x)
	}
	t.Fatalf("unexpected id type %T", v)
	return 0
}
