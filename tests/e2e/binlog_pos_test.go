package e2e

// Chế độ vị trí binlog: source MySQL KHÔNG có GTID (5.5; 5.6/5.7 với gtid_mode=OFF) → MySQL 8.4.
// Strand chạy như process riêng với --start-at <file>:<pos>, source chịu tải (InnoDB nhiều row, rollback,
// bảng MyISAM không giao dịch, rotate binlog liên tục), kill -9 ngẫu nhiên ở cả relaxed và strict, rồi
// so dữ liệu. Image: STRAND_E2E_POS_IMAGES (mặc định mysql:5.5,mysql:5.6,mysql:5.7).
// STRAND_E2E_POS_KILLS: số lần kill -9 mỗi chế độ (mặc định 8).

import (
	"context"
	"database/sql"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"

	"strand/internal/checkpoint"
	"strand/internal/model"
)

func startMySQLNoGTID(t *testing.T, image, alias string, serverID int, extra ...string) *mysqlNode {
	t.Helper()
	ctx := context.Background()
	args := []string{
		fmt.Sprintf("--server-id=%d", serverID),
		"--log-bin=mysql-bin", "--binlog-format=ROW", "--max-connections=500",
		"--character-set-server=utf8mb4", "--innodb-use-native-aio=OFF",
	}
	args = append(args, extra...)
	req := testcontainers.ContainerRequest{
		Image:        image,
		ExposedPorts: []string{"3306/tcp"},
		Env:          map[string]string{"MYSQL_ROOT_PASSWORD": "dev"},
		Cmd:          args,
		WaitingFor:   wait.ForLog("port: 3306  MySQL Community Server").WithStartupTimeout(240 * time.Second),
	}
	c, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{ContainerRequest: req, Started: true})
	if err != nil {
		t.Fatalf("start %s: %v", image, err)
	}
	t.Cleanup(func() { _ = c.Terminate(context.Background()) })
	host, _ := c.Host(ctx)
	port, _ := c.MappedPort(ctx, "3306")
	n := &mysqlNode{c: c, Alias: alias, Addr: fmt.Sprintf("%s:%s", host, port.Port())}
	n.RootDSN = fmt.Sprintf("root:dev@tcp(%s)/", n.Addr)
	n.DB, err = sql.Open("mysql", n.RootDSN+"?parseTime=false&multiStatements=true")
	if err != nil {
		t.Fatal(err)
	}
	n.DB.SetMaxOpenConns(32)
	for i := 0; i < 120; i++ {
		if err = n.DB.Ping(); err == nil {
			break
		}
		time.Sleep(500 * time.Millisecond)
	}
	if err != nil {
		t.Fatalf("ping %s: %v", alias, err)
	}
	t.Cleanup(func() { _ = n.DB.Close() })
	return n
}

var posTables = []string{"app.t1", "app.t2", "app.m"}

func posSchema(engineOfM string) []string {
	return []string{
		"CREATE DATABASE IF NOT EXISTS app",
		`CREATE TABLE app.t1 (id BIGINT PRIMARY KEY, ver BIGINT UNSIGNED NOT NULL, v VARCHAR(64), n INT NOT NULL DEFAULT 0) ENGINE=InnoDB`,
		`CREATE TABLE app.t2 (id BIGINT PRIMARY KEY, ver BIGINT UNSIGNED NOT NULL, v VARCHAR(64), d DATETIME) ENGINE=InnoDB`,
		`CREATE TABLE app.m (id BIGINT PRIMARY KEY, ver BIGINT UNSIGNED NOT NULL, v VARCHAR(32)) ENGINE=` + engineOfM,
	}
}

// masterPos: (file, position) hiện tại của source.
func (n *mysqlNode) masterPos(t *testing.T) (string, uint64) {
	t.Helper()
	rows, err := n.DB.Query("SHOW MASTER STATUS")
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	cols, _ := rows.Columns()
	if !rows.Next() {
		t.Fatal("SHOW MASTER STATUS returned no row")
	}
	vals := make([]sql.RawBytes, len(cols))
	ptr := make([]any, len(cols))
	for i := range ptr {
		ptr[i] = &vals[i]
	}
	if err := rows.Scan(ptr...); err != nil {
		t.Fatal(err)
	}
	var pos uint64
	fmt.Sscan(string(vals[1]), &pos)
	return string(vals[0]), pos
}

type posWorkload struct {
	db      *sql.DB
	ops     atomic.Int64
	errs    atomic.Int64
	stopped atomic.Bool
	wg      sync.WaitGroup
}

func (w *posWorkload) Start(clients int, seed int64, span int64) {
	for i := 0; i < clients; i++ {
		r := rand.New(rand.NewSource(seed + int64(i)))
		w.wg.Add(1)
		go func() {
			defer w.wg.Done()
			for !w.stopped.Load() {
				if err := w.one(r, span); err != nil {
					w.errs.Add(1)
				}
				w.ops.Add(1)
				time.Sleep(time.Duration(r.Intn(3)) * time.Millisecond)
			}
		}()
	}
}

func (w *posWorkload) Stop() { w.stopped.Store(true); w.wg.Wait() }

func (w *posWorkload) one(r *rand.Rand, span int64) error {
	ctx := context.Background()
	id := func() int64 { return int64(1 + r.Intn(int(span))) }
	str := func() string { return fmt.Sprintf("s%d", r.Intn(1000000)) }
	upT1 := func(x interface {
		ExecContext(context.Context, string, ...any) (sql.Result, error)
	}) error {
		_, err := x.ExecContext(ctx, `INSERT INTO app.t1 (id, ver, v, n) VALUES (?, UUID_SHORT(), ?, 1) ON DUPLICATE KEY UPDATE ver = UUID_SHORT(), v = VALUES(v), n = n + 1`, id(), str())
		return err
	}
	upT2 := func(x interface {
		ExecContext(context.Context, string, ...any) (sql.Result, error)
	}) error {
		_, err := x.ExecContext(ctx, `INSERT INTO app.t2 (id, ver, v, d) VALUES (?, UUID_SHORT(), ?, NOW()) ON DUPLICATE KEY UPDATE ver = UUID_SHORT(), v = VALUES(v), d = NOW()`, id(), str())
		return err
	}
	upM := func(x interface {
		ExecContext(context.Context, string, ...any) (sql.Result, error)
	}) error {
		_, err := x.ExecContext(ctx, `INSERT INTO app.m (id, ver, v) VALUES (?, UUID_SHORT(), ?) ON DUPLICATE KEY UPDATE ver = UUID_SHORT(), v = VALUES(v)`, id(), str())
		return err
	}
	switch x := r.Intn(100); {
	case x < 30:
		return upT1(w.db)
	case x < 40:
		return upT2(w.db)
	case x < 48:
		_, err := w.db.ExecContext(ctx, "DELETE FROM app.t1 WHERE id = ?", id())
		return err
	case x < 52:
		_, err := w.db.ExecContext(ctx, "DELETE FROM app.t2 WHERE id = ?", id())
		return err
	case x < 60:
		return upM(w.db) // không giao dịch (MyISAM): BEGIN/COMMIT query trong binlog
	case x < 82: // Txn nhiều row, nhiều bảng
		tx, err := w.db.BeginTx(ctx, nil)
		if err != nil {
			return err
		}
		for i := 0; i < 1+r.Intn(5); i++ {
			if err = upT1(tx); err == nil {
				err = upT2(tx)
			}
			if err != nil {
				_ = tx.Rollback()
				return err
			}
		}
		return tx.Commit()
	case x < 90: // rollback: không vào binlog
		tx, err := w.db.BeginTx(ctx, nil)
		if err != nil {
			return err
		}
		_ = upT1(tx)
		return tx.Rollback()
	case x < 96: // Txn hỗn hợp InnoDB + MyISAM rồi ROLLBACK: phần MyISAM đã có hiệu lực và nằm trong binlog
		tx, err := w.db.BeginTx(ctx, nil)
		if err != nil {
			return err
		}
		_ = upM(tx)
		_ = upT1(tx)
		return tx.Rollback()
	default:
		_, err := w.db.ExecContext(ctx, "FLUSH LOGS")
		return err
	}
}

func posSnapshot(db *sql.DB, table string) (string, error) {
	rows, err := db.Query(fmt.Sprintf("SELECT id, ver, IFNULL(v,'~') FROM %s ORDER BY id", table))
	if err != nil {
		return "", err
	}
	defer rows.Close()
	var sb strings.Builder
	for rows.Next() {
		var id, ver int64
		var v string
		if err := rows.Scan(&id, &ver, &v); err != nil {
			return "", err
		}
		fmt.Fprintf(&sb, "%d|%d|%s\n", id, ver, v)
	}
	return sb.String(), rows.Err()
}

// posEqual: mọi bảng source == target (trả bảng đầu tiên lệch).
func posEqual(src, dst *sql.DB) (string, bool) {
	for _, tb := range posTables {
		a, err1 := posSnapshot(src, tb)
		b, err2 := posSnapshot(dst, tb)
		if err1 != nil || err2 != nil || a != b {
			return tb, false
		}
	}
	return "", true
}

func posStrandCfg(t *testing.T, name string, src, dst *mysqlNode, order string, dir string) strandCfg {
	return strandCfg{Name: name, SourceDSN: src.RootDSN, TargetDSN: dst.RootDSN + "strand", Include: []string{"app.*"},
		Dir: dir, CommitOrder: order, Workers: 4, CreateSchema: "off"}
}

// hasCheckpoint: row toàn cục của pipeline đã có (khác rỗng) trên target.
func hasCheckpoint(dst *mysqlNode, name string) (string, bool) {
	var applied string
	err := dst.DB.QueryRow("SELECT applied FROM strand._strand_checkpoint WHERE pipeline_id = ?", name).Scan(&applied)
	return applied, err == nil && strings.TrimSpace(applied) != ""
}

func posImages() []string {
	if v := os.Getenv("STRAND_E2E_POS_IMAGES"); v != "" {
		return strings.Split(v, ",")
	}
	return []string{"mysql:5.5", "mysql:5.6", "mysql:5.7"}
}

func TestBinlogPos_E2E_Kill9Restarts(t *testing.T) {
	seed := seedFromEnv(t)
	kills := envInt("STRAND_E2E_POS_KILLS", 8)
	for _, image := range posImages() {
		image := image
		t.Run(strings.ReplaceAll(image, ":", "_"), func(t *testing.T) {
			rng := rand.New(rand.NewSource(seed))
			// max_binlog_size nhỏ: rotate liên tục ngoài FLUSH LOGS.
			src := startMySQLNoGTID(t, image, "src", 1, "--max-binlog-size=4096")
			dst := startMySQL(t, mysqlImage("target"), "dst", 2, nil)
			src.exec(t, posSchema("MyISAM")...)
			dst.exec(t, posSchema("InnoDB")...)
			dst.exec(t, "CREATE DATABASE IF NOT EXISTS strand")
			var gtidMode sql.NullString
			if err := src.DB.QueryRow("SELECT @@GLOBAL.gtid_mode").Scan(&gtidMode); err == nil {
				t.Logf("source %s gtid_mode=%s", image, gtidMode.String)
			} else {
				t.Logf("source %s has no gtid_mode (%v)", image, err)
			}
			// Dữ liệu ban đầu giống hệt hai phía.
			for _, db := range []*sql.DB{src.DB, dst.DB} {
				for i := 1; i <= 200; i++ {
					if _, err := db.Exec("INSERT INTO app.t1 (id, ver, v, n) VALUES (?, ?, 'init', 0)", i, 1000+i); err != nil {
						t.Fatal(err)
					}
					if i%2 == 0 {
						if _, err := db.Exec("INSERT INTO app.t2 (id, ver, v, d) VALUES (?, ?, 'init', '2020-01-02 03:04:05')", i, 1000+i); err != nil {
							t.Fatal(err)
						}
					}
					if i%5 == 0 {
						if _, err := db.Exec("INSERT INTO app.m (id, ver, v) VALUES (?, ?, 'init')", i, 1000+i); err != nil {
							t.Fatal(err)
						}
					}
				}
			}
			vs := startVerSampler(dst, posTables...)
			for _, order := range []string{"relaxed", "strict"} {
				order := order
				t.Run(order, func(t *testing.T) {
					name := "pos-" + order
					dir := testDir(t)
					cfg := posStrandCfg(t, name, src, dst, order, dir)
					cfgPath := cfg.write(t)
					// Mỗi chế độ bắt đầu ở vị trí hiện tại của source; hai phía đang bằng nhau.
					f, pos := src.masterPos(t)
					if tb, ok := posEqual(src.DB, dst.DB); !ok {
						t.Fatalf("precondition: %s differs before start", tb)
					}
					start := fmt.Sprintf("%s:%d", f, pos)
					t.Logf("start-at %s (%s)", start, order)
					wl := &posWorkload{db: src.DB}
					wl.Start(4, seed, 300)
					for i := 1; i <= kills; i++ {
						var extra []string
						if _, ok := hasCheckpoint(dst, name); !ok {
							extra = []string{"--start-at", start}
						}
						p := startStrand(t, cfgPath, extra...)
						select {
						case <-p.done:
							t.Fatalf("iteration %d: strand exited by itself (code %d)\n%s", i, p.exitCode(), p.tail())
						case <-time.After(time.Duration(300+rng.Intn(2500)) * time.Millisecond):
						}
						p.kill9()
					}
					wl.Stop()
					t.Logf("workload: %d ops, %d client errors", wl.ops.Load(), wl.errs.Load())
					var extra []string
					if _, ok := hasCheckpoint(dst, name); !ok {
						extra = []string{"--start-at", start}
					}
					p := startStrand(t, cfgPath, extra...)
					deadline := time.Now().Add(4 * time.Minute)
					var tb string
					for {
						var ok bool
						if tb, ok = posEqual(src.DB, dst.DB); ok {
							break
						}
						if p.exited() {
							t.Fatalf("strand exited (code %d) before catching up\n%s", p.exitCode(), p.tail())
						}
						if time.Now().After(deadline) {
							t.Fatalf("target did not catch up in time: %s differs\n%s", tb, p.tail())
						}
						time.Sleep(300 * time.Millisecond)
					}
					// Trạng thái synced (post-start-at verify dùng ApplyMark theo vị trí, caught-up theo SHOW MASTER STATUS).
					synced := false
					for i := 0; i < 240 && !synced; i++ {
						synced = pipelineSynced(cfg.Socket)
						time.Sleep(500 * time.Millisecond)
					}
					if !synced {
						t.Errorf("pipeline never reported synced in binlog position mode\n%s", p.tail())
					}
					if code := p.stop(t); code != 0 {
						t.Fatalf("final run exit code %d\n%s", code, p.tail())
					}
					// Checkpoint cuối: JSON vị trí binlog hợp lệ, không hole, mốc tới đầu binlog (đủ cho resume).
					applied, ok := hasCheckpoint(dst, name)
					if !ok {
						t.Fatal("no checkpoint row")
					}
					set, err := checkpoint.ParseAppliedSet(model.EngineMySQL, applied)
					if err != nil {
						t.Fatal(err)
					}
					ps, isPos := set.(*checkpoint.MySQLPosAppliedSet)
					if !isPos || ps.NumHoles() != 0 {
						t.Fatalf("final checkpoint %T holes=%v: %s", set, isPos && ps.NumHoles() != 0, applied)
					}
					if !strings.Contains(applied, `"mode":"binlog_pos"`) || !strings.Contains(applied, `"file_base":"mysql-bin"`) {
						t.Fatalf("unexpected checkpoint: %s", applied)
					}
					t.Logf("final checkpoint: %s", applied)
					if tb, ok := posEqual(src.DB, dst.DB); !ok {
						t.Fatalf("final state: %s differs", tb)
					}
				})
			}
			vs.check(t)
			// Kiểm bằng chứng việc cắt/rotate thật sự diễn ra.
			var nfiles int
			rows, err := src.DB.Query("SHOW BINARY LOGS")
			if err == nil {
				for rows.Next() {
					nfiles++
				}
				rows.Close()
			}
			t.Logf("source has %d binlog files", nfiles)
		})
	}
}

// Từ chối rõ ràng khi chế độ vị trí không thể chạy an toàn.
func TestBinlogPos_E2E_Rejections(t *testing.T) {
	image := "mysql:5.7"
	if v := os.Getenv("STRAND_E2E_POS_IMAGES"); v != "" {
		image = strings.Split(v, ",")[0]
	}
	src := startMySQLNoGTID(t, image, "src", 1)
	dst := startMySQL(t, mysqlImage("target"), "dst", 2, nil)
	src.exec(t, posSchema("InnoDB")...)
	dst.exec(t, posSchema("InnoDB")...)
	dst.exec(t, "CREATE DATABASE IF NOT EXISTS strand")
	dir := testDir(t)
	cfg := posStrandCfg(t, "pos-reject", src, dst, "relaxed", dir)
	cfgPath := cfg.write(t)
	runOnce := func(extra ...string) (int, string) {
		p := startStrand(t, cfgPath, extra...)
		select {
		case <-p.done:
		case <-time.After(60 * time.Second):
			p.kill9()
			t.Fatalf("strand did not exit\n%s", p.tail())
		}
		return p.exitCode(), p.tail()
	}
	mustReject := func(what string, wantSub string, extra ...string) {
		t.Helper()
		code, out := runOnce(extra...)
		if code == 0 || !strings.Contains(out, wantSub) {
			t.Fatalf("%s: exit %d, want a rejection containing %q\n%s", what, code, wantSub, out)
		}
		if _, ok := hasCheckpoint(dst, "pos-reject"); ok && what != "log gap after RESET MASTER" {
			t.Fatalf("%s: a rejected start must not write a checkpoint", what)
		}
	}
	// Seed: manifest không có vị trí binlog / không có manifest → từ chối rõ (không ghi checkpoint).
	mustReject("seed without a manifest", "read seed manifest", "--start-at", "seed", "--seed-manifest", filepath.Join(dir, "none.json"))
	noPos := filepath.Join(dir, "nopos.json")
	if err := os.WriteFile(noPos, []byte(`{"version":1,"engine":"mysql","kind":"mysqldump","start_position":{}}`), 0o600); err != nil {
		t.Fatal(err)
	}
	mustReject("seed manifest without a binlog position", "no binlog position", "--start-at", "seed", "--seed-manifest", noPos)
	gone := filepath.Join(dir, "gone.json")
	if err := os.WriteFile(gone, []byte(`{"version":1,"engine":"mysql","kind":"mysqldump","start_position":{"binlog_file":"mysql-bin.009999","binlog_pos":120}}`), 0o600); err != nil {
		t.Fatal(err)
	}
	mustReject("seed position in a purged/missing file", "log gap", "--start-at", "seed", "--seed-manifest", gone)
	mustReject("GTID start-at", "invalid start position", "--start-at", "3e11fa47-71ca-11e1-9e33-c80aa9429562:1-5")
	mustReject("start-at in a missing file", "log gap", "--start-at", "mysql-bin.009999:4")
	f, _ := src.masterPos(t)
	mustReject("start-at in the middle of an event", "not an event boundary", "--start-at", fmt.Sprintf("%s:5", f))
	mustReject("start-at beyond EOF", "beyond the end", "--start-at", fmt.Sprintf("%s:99999999", f))

	// Hợp lệ → chạy, rồi dừng; RESET MASTER làm vị trí đã lưu vô nghĩa → resume phải dừng.
	f, pos := src.masterPos(t)
	p := startStrand(t, cfgPath, "--start-at", fmt.Sprintf("%s:%d", f, pos))
	for i := 0; i < 100; i++ {
		if _, ok := hasCheckpoint(dst, "pos-reject"); ok {
			break
		}
		time.Sleep(200 * time.Millisecond)
	}
	if _, ok := hasCheckpoint(dst, "pos-reject"); !ok {
		t.Fatalf("no checkpoint after a valid start\n%s", p.tail())
	}
	if code := p.stop(t); code != 0 {
		t.Fatalf("clean stop: code %d\n%s", code, p.tail())
	}
	if _, err := src.DB.Exec("INSERT INTO app.t1 (id, ver, v) VALUES (1, 1, 'x')"); err != nil {
		t.Fatal(err)
	}
	if _, err := src.DB.Exec("FLUSH LOGS"); err != nil {
		t.Fatal(err)
	}
	src.exec(t, "RESET MASTER")
	mustReject("log gap after RESET MASTER", "log gap")
}

// MySQL không GTID → PostgreSQL: cùng đường vị trí binlog, checkpoint nằm ở PG (_strand_checkpoint).
func TestBinlogPos_E2E_ToPostgres(t *testing.T) {
	seed := seedFromEnv(t)
	kills := envInt("STRAND_E2E_POS_KILLS", 8)
	image := "mysql:5.6"
	if v := os.Getenv("STRAND_E2E_POS_IMAGES"); v != "" {
		image = strings.Split(v, ",")[0]
	}
	rng := rand.New(rand.NewSource(seed))
	src := startMySQLNoGTID(t, image, "src", 1, "--max-binlog-size=4096")
	dst := startPG(t, "pgdst")
	src.exec(t, posSchema("MyISAM")...)
	dst.exec(t,
		"CREATE SCHEMA app",
		`CREATE TABLE app.t1 (id bigint PRIMARY KEY, ver numeric(20,0) NOT NULL, v varchar(64), n int NOT NULL DEFAULT 0)`,
		`CREATE TABLE app.t2 (id bigint PRIMARY KEY, ver numeric(20,0) NOT NULL, v varchar(64), d timestamp)`,
		`CREATE TABLE app.m (id bigint PRIMARY KEY, ver numeric(20,0) NOT NULL, v varchar(32))`)
	for i := 1; i <= 100; i++ {
		src.exec(t, fmt.Sprintf("INSERT INTO app.t1 (id, ver, v, n) VALUES (%d, %d, 'init', 0)", i, 1000+i))
		dst.exec(t, fmt.Sprintf("INSERT INTO app.t1 (id, ver, v, n) VALUES (%d, %d, 'init', 0)", i, 1000+i))
	}
	snapPG := func(tb string) (string, error) {
		rows, err := dst.DB.Query(fmt.Sprintf("SELECT id, ver::bigint, COALESCE(v,'~') FROM %s ORDER BY id", tb))
		if err != nil {
			return "", err
		}
		defer rows.Close()
		var sb strings.Builder
		for rows.Next() {
			var id, ver int64
			var v string
			if err := rows.Scan(&id, &ver, &v); err != nil {
				return "", err
			}
			fmt.Fprintf(&sb, "%d|%d|%s\n", id, ver, v)
		}
		return sb.String(), rows.Err()
	}
	equal := func() (string, bool) {
		for _, tb := range posTables {
			a, e1 := posSnapshot(src.DB, tb)
			b, e2 := snapPG(tb)
			if e1 != nil || e2 != nil || a != b {
				return tb, false
			}
		}
		return "", true
	}
	for _, order := range []string{"relaxed", "strict"} {
		order := order
		t.Run(order, func(t *testing.T) {
			name := "pos-pg-" + order
			cfg := strandCfg{Name: name, TargetKind: "postgres", SourceDSN: src.RootDSN, TargetDSN: dst.DSN, Include: []string{"app.*"},
				Dir: testDir(t), CommitOrder: order, Workers: 4, CreateSchema: "off"}
			cfgPath := cfg.write(t)
			f, pos := src.masterPos(t)
			if tb, ok := equal(); !ok {
				t.Fatalf("precondition: %s differs", tb)
			}
			start := fmt.Sprintf("%s:%d", f, pos)
			has := func() bool {
				var a string
				err := dst.DB.QueryRow("SELECT applied FROM _strand_checkpoint WHERE pipeline_id = $1", name).Scan(&a)
				return err == nil && strings.TrimSpace(a) != ""
			}
			wl := &posWorkload{db: src.DB}
			wl.Start(4, seed, 300)
			for i := 1; i <= kills; i++ {
				var extra []string
				if !has() {
					extra = []string{"--start-at", start}
				}
				p := startStrand(t, cfgPath, extra...)
				select {
				case <-p.done:
					t.Fatalf("iteration %d: strand exited by itself (code %d)\n%s", i, p.exitCode(), p.tail())
				case <-time.After(time.Duration(300+rng.Intn(2500)) * time.Millisecond):
				}
				p.kill9()
			}
			wl.Stop()
			var extra []string
			if !has() {
				extra = []string{"--start-at", start}
			}
			p := startStrand(t, cfgPath, extra...)
			deadline := time.Now().Add(4 * time.Minute)
			for {
				tb, ok := equal()
				if ok {
					break
				}
				if p.exited() {
					t.Fatalf("strand exited (code %d)\n%s", p.exitCode(), p.tail())
				}
				if time.Now().After(deadline) {
					t.Fatalf("target did not catch up: %s differs\n%s", tb, p.tail())
				}
				time.Sleep(300 * time.Millisecond)
			}
			if code := p.stop(t); code != 0 {
				t.Fatalf("final run exit code %d\n%s", code, p.tail())
			}
			var applied string
			if err := dst.DB.QueryRow("SELECT applied FROM _strand_checkpoint WHERE pipeline_id = $1", name).Scan(&applied); err != nil {
				t.Fatal(err)
			}
			if _, err := checkpoint.ParseAppliedSet(model.EngineMySQL, applied); err != nil || !strings.Contains(applied, `"mode":"binlog_pos"`) {
				t.Fatalf("checkpoint %q: %v", applied, err)
			}
			t.Logf("final checkpoint: %s", applied)
		})
	}
}

// ---------------------------------------------------------------------------
// Seed / snapshot logic / lịch sử schema ở chế độ vị trí binlog
// ---------------------------------------------------------------------------

func posImage(def string) string {
	if v := os.Getenv("STRAND_E2E_POS_IMAGES"); v != "" {
		return strings.Split(v, ",")[0]
	}
	return def
}

// waitPosEqual: chờ mọi bảng source == target (và strand còn sống).
func waitPosEqual(t *testing.T, src, dst *sql.DB, p *strandProc, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for {
		tb, ok := posEqual(src, dst)
		if ok {
			return
		}
		if p.exited() {
			t.Fatalf("strand exited (code %d) before catching up (%s differs)\n%s", p.exitCode(), tb, p.tail())
		}
		if time.Now().After(deadline) {
			t.Fatalf("target did not catch up: %s differs\n%s", tb, p.tail())
		}
		time.Sleep(300 * time.Millisecond)
	}
}

func posPreload(t *testing.T, n int, dbs ...*sql.DB) {
	t.Helper()
	for _, db := range dbs {
		for i := 1; i <= n; i++ {
			if _, err := db.Exec("INSERT INTO app.t1 (id, ver, v, n) VALUES (?, ?, 'init', 0)", i, 1000+i); err != nil {
				t.Fatal(err)
			}
			if i%2 == 0 {
				if _, err := db.Exec("INSERT INTO app.t2 (id, ver, v, d) VALUES (?, ?, 'init', '2020-01-02 03:04:05')", i, 1000+i); err != nil {
					t.Fatal(err)
				}
			}
			if i%5 == 0 {
				if _, err := db.Exec("INSERT INTO app.m (id, ver, v) VALUES (?, ?, 'init')", i, 1000+i); err != nil {
					t.Fatal(err)
				}
			}
		}
	}
}

// (a) Seed từ mysqldump --master-data=2 của MySQL 5.5 (không GTID) → 8.4: vị trí CHANGE MASTER là điểm
// bắt đầu; nguồn ghi liên tục trong lúc dump/restore; CDC bắt kịp, hai bên bằng nhau.
func TestBinlogPos_E2E_SeedMysqldump(t *testing.T) {
	seed := seedFromEnv(t)
	src := startMySQLNoGTID(t, posImage("mysql:5.5"), "src", 1)
	dst := startMySQL(t, mysqlImage("target"), "dst", 2, nil)
	src.exec(t, posSchema("InnoDB")...)
	posPreload(t, 600, src.DB)
	wl := &posWorkload{db: src.DB}
	wl.Start(4, seed, 800)
	defer wl.Stop()
	time.Sleep(2 * time.Second)

	dir := testDir(t)
	dumpFile := filepath.Join(dir, "dump.sql")
	srcID, dstID := src.c.GetContainerID(), dst.c.GetContainerID()
	runDockerCmd(t, 2*time.Minute, "exec", srcID, "sh", "-c",
		"mysqldump -uroot -pdev --default-character-set=utf8mb4 --single-transaction --master-data=2 --databases app > /tmp/dump.sql")
	runDockerCmd(t, time.Minute, "cp", srcID+":/tmp/dump.sql", dumpFile)
	runDockerCmd(t, time.Minute, "cp", dumpFile, dstID+":/tmp/dump.sql")
	// Tải vẫn chạy trong lúc restore: Txn sau vị trí dump phải tới đích qua CDC.
	runDockerCmd(t, 2*time.Minute, "exec", dstID, "sh", "-c", "mysql -uroot -pdev < /tmp/dump.sql")
	dst.exec(t, "SET SESSION sql_log_bin=0; CREATE DATABASE IF NOT EXISTS strand; SET SESSION sql_log_bin=1")

	manifest := filepath.Join(dir, "seed.json")
	out, code := runStrandCLI(t, time.Minute, "seed", "plan", "--type", "mysqldump", "--backup", dumpFile,
		"--source", src.RootDSN, "--target", dst.RootDSN, "--out", manifest)
	if code != 0 {
		t.Fatalf("seed plan: exit %d\n%s", code, out)
	}
	raw, _ := os.ReadFile(manifest)
	if !strings.Contains(string(raw), `"binlog_file"`) || strings.Contains(string(raw), `"gtid_set"`) {
		t.Fatalf("manifest must carry the binlog position and no GTID set:\n%s", raw)
	}
	out, code = runStrandCLI(t, 5*time.Minute, "seed", "apply", "--manifest", manifest, "--target", dst.RootDSN,
		"--spool", filepath.Join(dir, "state", "prefetch"), "--source", src.RootDSN)
	if code != 0 {
		t.Fatalf("seed apply: exit %d\n%s", code, out)
	}
	cfg := posStrandCfg(t, "pos-seed", src, dst, "", dir)
	cfg.Bootstrap = "seed"
	p := startStrand(t, cfg.write(t), "--start-at", "seed", "--seed-manifest", manifest)
	time.Sleep(10 * time.Second) // tải tiếp tục trong lúc catch-up
	wl.Stop()
	waitPosEqual(t, src.DB, dst.DB, p, 4*time.Minute)
	waitSynced(t, &cfg, p, 3*time.Minute) // post-seed verify chạy xong
	if code := p.stop(t); code != 0 {
		t.Fatalf("exit %d\n%s", code, p.tail())
	}
	if tb, ok := posEqual(src.DB, dst.DB); !ok {
		t.Fatalf("final state: %s differs", tb)
	}
}

// (b) Bootstrap logical (snapshot theo chunk + stream) từ 5.6 gtid_mode=OFF dưới tải ghi đồng thời → 8.4;
// so sánh dữ liệu và `verify --mode full`.
func TestBinlogPos_E2E_LogicalBootstrap(t *testing.T) {
	seed := seedFromEnv(t)
	src := startMySQLNoGTID(t, posImage("mysql:5.6"), "src", 1, "--max-binlog-size=65536")
	dst := startMySQL(t, mysqlImage("target"), "dst", 2, nil)
	src.exec(t, posSchema("InnoDB")...)
	dst.exec(t, posSchema("InnoDB")...)
	dst.exec(t, "CREATE DATABASE IF NOT EXISTS strand")
	posPreload(t, 3000, src.DB)
	wl := &posWorkload{db: src.DB}
	wl.Start(4, seed, 3500)
	defer wl.Stop()

	dir := testDir(t)
	cfg := posStrandCfg(t, "pos-boot", src, dst, "", dir)
	cfg.ChunkRows = "200"
	cfgPath := cfg.write(t)
	p := startStrand(t, cfgPath) // không --start-at: bootstrap logical từ đầu binlog hiện tại
	time.Sleep(15 * time.Second)
	wl.Stop()
	waitPosEqual(t, src.DB, dst.DB, p, 4*time.Minute)
	waitSynced(t, &cfg, p, 3*time.Minute)
	if code := p.stop(t); code != 0 {
		t.Fatalf("exit %d\n%s", code, p.tail())
	}
	if !strings.Contains(p.output.String(), "logical bootstrap in binlog position mode") {
		t.Fatalf("pipeline did not bootstrap logically\n%s", p.tail())
	}
	out, code := runStrandCLI(t, 10*time.Minute, "verify", "-c", cfgPath, "--mode", "full")
	if code != 0 {
		t.Fatalf("verify full: exit %d\n%s", code, out)
	}
	if tb, ok := posEqual(src.DB, dst.DB); !ok {
		t.Fatalf("final state: %s differs", tb)
	}
}

// (c) `snapshot add` một bảng mới (MyISAM ở source) khi pipeline đang stream dưới tải, chế độ vị trí.
func TestBinlogPos_E2E_SnapshotAdd(t *testing.T) {
	seed := seedFromEnv(t)
	src := startMySQLNoGTID(t, posImage("mysql:5.5"), "src", 1)
	dst := startMySQL(t, mysqlImage("target"), "dst", 2, nil)
	src.exec(t, posSchema("MyISAM")...)
	dst.exec(t, posSchema("InnoDB")...)
	dst.exec(t, "CREATE DATABASE IF NOT EXISTS strand")
	posPreload(t, 1500, src.DB, dst.DB)
	wl := &posWorkload{db: src.DB}
	wl.Start(4, seed, 1800)
	defer wl.Stop()

	dir := testDir(t)
	cfg := posStrandCfg(t, "pos-add", src, dst, "", dir)
	cfg.Include = []string{"app.t1", "app.t2"} // app.m chưa thuộc pipeline
	cfg.ChunkRows = "100"
	// Hai bảng của pipeline phải bằng nhau tại vị trí bắt đầu: dừng tải một nhịp, sao chép t1/t2 sang target
	// tại đúng vị trí đó, rồi chạy lại tải.
	wl.Stop()
	f, pos := src.masterPos(t)
	for _, tb := range []string{"app.t1", "app.t2"} {
		dst.exec(t, "DELETE FROM "+tb)
		rows, err := src.DB.Query("SELECT * FROM " + tb)
		if err != nil {
			t.Fatal(err)
		}
		cols, _ := rows.Columns()
		ph := strings.TrimSuffix(strings.Repeat("?,", len(cols)), ",")
		for rows.Next() {
			vals := make([]any, len(cols))
			ptr := make([]any, len(cols))
			for i := range ptr {
				ptr[i] = &vals[i]
			}
			if err := rows.Scan(ptr...); err != nil {
				t.Fatal(err)
			}
			if _, err := dst.DB.Exec("INSERT INTO "+tb+" VALUES ("+ph+")", vals...); err != nil {
				t.Fatal(err)
			}
		}
		rows.Close()
	}
	dst.exec(t, "DELETE FROM app.m")
	wl2 := &posWorkload{db: src.DB}
	wl2.Start(4, seed+7, 1800)
	defer wl2.Stop()
	cfgPath := cfg.write(t)
	p := startStrand(t, cfgPath, "--start-at", fmt.Sprintf("%s:%d", f, pos))
	// Tải ghi liên tục nên pipeline có thể chưa báo "synced": chỉ cần đã chạy (có checkpoint) và stream đang áp.
	for i := 0; i < 100; i++ {
		if _, ok := hasCheckpoint(dst, "pos-add"); ok {
			break
		}
		time.Sleep(200 * time.Millisecond)
	}
	time.Sleep(3 * time.Second)
	out, code := runStrandCLI(t, time.Minute, "snapshot", "add", "--table", "app.m", "--socket", cfg.Socket)
	if code != 0 {
		t.Fatalf("snapshot add: exit %d\n%s", code, out)
	}
	deadline := time.Now().Add(3 * time.Minute)
	for !strings.Contains(p.output.String(), "snapshot done") || !strings.Contains(p.output.String(), "app.m") {
		if p.exited() || time.Now().After(deadline) {
			t.Fatalf("snapshot add did not finish (exited=%v)\n%s", p.exited(), p.tail())
		}
		time.Sleep(300 * time.Millisecond)
	}
	time.Sleep(5 * time.Second)
	wl2.Stop()
	// app.m không thuộc include của pipeline nên không được ghi lại sau snapshot add khi restart; trong phiên này
	// bảng đã vào pipeline (Strand giữ bảng cho tới khi dừng), nên cả ba bảng phải hội tụ.
	waitPosEqual(t, src.DB, dst.DB, p, 4*time.Minute)
	if code := p.stop(t); code != 0 {
		t.Fatalf("exit %d\n%s", code, p.tail())
	}
}

// (d) DROP COLUMN khi Strand dừng, nguồn 5.6 không GTID: binlog 5.6 không có tên cột; row ghi trước DDL phải
// giải mã theo cấu trúc cũ (lịch sử schema theo vị trí), row sau DDL theo cấu trúc mới.
func TestBinlogPos_E2E_SchemaHistoryDropColumn(t *testing.T) {
	src := startMySQLNoGTID(t, posImage("mysql:5.6"), "src", 1)
	dst := startMySQL(t, mysqlImage("target"), "dst", 2, nil)
	src.exec(t, posSchema("InnoDB")...)
	dst.exec(t, posSchema("InnoDB")...)
	const altDDL = "CREATE TABLE app.alt (id BIGINT PRIMARY KEY, v VARCHAR(20) NOT NULL, u BIGINT UNSIGNED NOT NULL, w INT NULL)"
	src.exec(t, altDDL)
	dst.exec(t, altDDL)
	dst.exec(t, "CREATE DATABASE IF NOT EXISTS strand")
	for i := 1; i <= 100; i++ {
		src.exec(t, fmt.Sprintf("INSERT INTO app.alt VALUES (%d, 'v%d', %d, %d)", i, i, i, i))
		dst.exec(t, fmt.Sprintf("INSERT INTO app.alt VALUES (%d, 'v%d', %d, %d)", i, i, i, i))
	}
	dir := testDir(t)
	cfg := posStrandCfg(t, "pos-ddl", src, dst, "", dir)
	cfgPath := cfg.write(t)
	f, pos := src.masterPos(t)
	p := startStrand(t, cfgPath, "--start-at", fmt.Sprintf("%s:%d", f, pos))
	waitSynced(t, &cfg, p, 3*time.Minute)
	src.exec(t, "INSERT INTO app.alt VALUES (500, 'live', 5, 5)")
	waitPosEqualAlt(t, src, dst, p)
	if code := p.stop(t); code != 0 {
		t.Fatalf("exit %d\n%s", code, p.tail())
	}
	// Strand dừng: row theo cấu trúc cũ, DROP COLUMN (đích chưa áp), row theo cấu trúc mới.
	for i := 2001; i <= 2100; i++ {
		src.exec(t, fmt.Sprintf("INSERT INTO app.alt VALUES (%d, 'old%d', %d, %d)", i, i, i, i%9))
	}
	src.exec(t, "ALTER TABLE app.alt DROP COLUMN w")
	for i := 3001; i <= 3100; i++ {
		src.exec(t, fmt.Sprintf("INSERT INTO app.alt VALUES (%d, 'new%d', %d)", i, i, i))
	}
	p2 := startStrand(t, cfgPath)
	deadline := time.Now().Add(2 * time.Minute)
	for !p2.exited() && time.Now().Before(deadline) {
		time.Sleep(200 * time.Millisecond)
	}
	if !p2.exited() || p2.exitCode() != 10 || !strings.Contains(p2.output.String(), "schema_change_pending") {
		t.Fatalf("want exit 10 schema_change_pending, exited=%v code=%d\n%s", p2.exited(), p2.exitCode(), p2.tail())
	}
	var cnt int
	if err := dst.DB.QueryRow("SELECT COUNT(*) FROM app.alt WHERE id BETWEEN 2001 AND 2100").Scan(&cnt); err != nil || cnt != 100 {
		t.Fatalf("rows before DROP COLUMN at target: %d (%v), want 100\n%s", cnt, err, p2.tail())
	}
	if err := dst.DB.QueryRow("SELECT COUNT(*) FROM app.alt WHERE id BETWEEN 3001 AND 3100").Scan(&cnt); err != nil || cnt != 0 {
		t.Fatalf("rows after DROP COLUMN reached the target (%d) before its DDL was applied", cnt)
	}
	dst.exec(t, "ALTER TABLE app.alt DROP COLUMN w")
	p3 := startStrand(t, cfgPath)
	waitPosEqualAlt(t, src, dst, p3)
	if code := p3.stop(t); code != 0 {
		t.Fatalf("exit %d\n%s", code, p3.tail())
	}
}

func waitPosEqualAlt(t *testing.T, src, dst *mysqlNode, p *strandProc) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Minute)
	for {
		a, e1 := posAltDump(src.DB)
		b, e2 := posAltDump(dst.DB)
		if e1 == nil && e2 == nil && a == b {
			return
		}
		if p.exited() || time.Now().After(deadline) {
			t.Fatalf("app.alt differs (exited=%v): %v %v\n%s", p.exited(), e1, e2, p.tail())
		}
		time.Sleep(300 * time.Millisecond)
	}
}

func posAltDump(db *sql.DB) (string, error) {
	rows, err := db.Query("SELECT id, v, u FROM app.alt ORDER BY id")
	if err != nil {
		return "", err
	}
	defer rows.Close()
	var sb strings.Builder
	for rows.Next() {
		var id, u int64
		var v string
		if err := rows.Scan(&id, &v, &u); err != nil {
			return "", err
		}
		fmt.Fprintf(&sb, "%d|%s|%d\n", id, v, u)
	}
	return sb.String(), rows.Err()
}
