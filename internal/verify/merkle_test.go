package verify_test

// Test P2-08: verify --mode full bằng cây Merkle, chạy trên MySQL 8.4 và PostgreSQL 18 thật
// (testcontainers). Container do test tạo được dừng ở TestMain.

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"net"
	"net/url"
	"os"
	"reflect"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	_ "github.com/go-sql-driver/mysql"
	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"

	"strand/internal/checkpoint"
	"strand/internal/model"
	"strand/internal/verify"
)

var (
	mysqlOnce sync.Once
	mysqlSrc  *sql.DB
	mysqlDst  *sql.DB
	mysqlErr  error

	pgOnce sync.Once
	pgDB   *sql.DB
	pgErr  error

	stopMu sync.Mutex
	stops  []func()
)

func TestMain(m *testing.M) {
	code := m.Run()
	stopMu.Lock()
	for _, f := range stops {
		f()
	}
	stopMu.Unlock()
	os.Exit(code)
}

func addStop(f func()) {
	stopMu.Lock()
	stops = append(stops, f)
	stopMu.Unlock()
}

// getMySQL trả hai kết nối tới hai schema verify_src / verify_dst trên cùng một MySQL. Session
// time_zone là +07:00 để chứng minh TIMESTAMP được quy về UTC khi hash.
func getMySQL(t *testing.T) (*sql.DB, *sql.DB) {
	t.Helper()
	mysqlOnce.Do(func() {
		ctx := context.Background()
		c, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
			ContainerRequest: testcontainers.ContainerRequest{
				Image:        "mysql:8.4",
				ExposedPorts: []string{"3306/tcp"},
				Env:          map[string]string{"MYSQL_ROOT_PASSWORD": "dev"},
				Cmd: []string{"--server-id=7001", "--gtid-mode=ON", "--enforce-gtid-consistency=ON",
					"--binlog-format=ROW", "--binlog-row-image=FULL", "--innodb-use-native-aio=OFF"},
				WaitingFor: wait.ForLog("port: 3306  MySQL Community Server").WithStartupTimeout(180 * time.Second),
			},
			Started: true,
		})
		if err != nil {
			mysqlErr = err
			return
		}
		addStop(func() { _ = c.Terminate(context.Background()) })
		host, _ := c.Host(ctx)
		port, err := c.MappedPort(ctx, "3306")
		if err != nil {
			mysqlErr = err
			return
		}
		addr := net.JoinHostPort(host, port.Port())
		tz := url.QueryEscape("'+07:00'")
		root, err := sql.Open("mysql", fmt.Sprintf("root:dev@tcp(%s)/?multiStatements=true", addr))
		if err != nil {
			mysqlErr = err
			return
		}
		defer root.Close()
		for i := 0; i < 60; i++ {
			if err = root.PingContext(ctx); err == nil {
				break
			}
			time.Sleep(500 * time.Millisecond)
		}
		if _, err = root.ExecContext(ctx, "CREATE DATABASE IF NOT EXISTS verify_src; CREATE DATABASE IF NOT EXISTS verify_dst"); err != nil {
			mysqlErr = err
			return
		}
		mysqlSrc, _ = sql.Open("mysql", fmt.Sprintf("root:dev@tcp(%s)/verify_src?time_zone=%s", addr, tz))
		mysqlDst, _ = sql.Open("mysql", fmt.Sprintf("root:dev@tcp(%s)/verify_dst?time_zone=%s", addr, tz))
	})
	if mysqlErr != nil {
		t.Fatalf("start mysql: %v", mysqlErr)
	}
	return mysqlSrc, mysqlDst
}

// getPostgres trả kết nối tới PG; TimeZone của session là America/New_York để chứng minh
// timestamptz được quy về UTC khi hash.
func getPostgres(t *testing.T) *sql.DB {
	t.Helper()
	pgOnce.Do(func() {
		ctx := context.Background()
		c, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
			ContainerRequest: testcontainers.ContainerRequest{
				Image:        "postgres:18",
				ExposedPorts: []string{"5432/tcp"},
				Env:          map[string]string{"POSTGRES_USER": "strand", "POSTGRES_PASSWORD": "dev", "POSTGRES_DB": "strand"},
				WaitingFor:   wait.ForLog("database system is ready to accept connections").WithOccurrence(2).WithStartupTimeout(120 * time.Second),
			},
			Started: true,
		})
		if err != nil {
			pgErr = err
			return
		}
		addStop(func() { _ = c.Terminate(context.Background()) })
		host, _ := c.Host(ctx)
		port, err := c.MappedPort(ctx, "5432")
		if err != nil {
			pgErr = err
			return
		}
		db, err := sql.Open("pgx", fmt.Sprintf("postgres://strand:dev@%s/strand?sslmode=disable&timezone=America/New_York",
			net.JoinHostPort(host, port.Port())))
		if err != nil {
			pgErr = err
			return
		}
		for i := 0; i < 60; i++ {
			if err = db.PingContext(ctx); err == nil {
				break
			}
			time.Sleep(500 * time.Millisecond)
		}
		pgDB, pgErr = db, err
	})
	if pgErr != nil {
		t.Fatalf("start postgres: %v", pgErr)
	}
	return pgDB
}

func mustExec(t *testing.T, db *sql.DB, q string, args ...any) {
	t.Helper()
	if _, err := db.Exec(q, args...); err != nil {
		t.Fatalf("%s: %v", q, err)
	}
}

func toDst(t model.TableID) model.TableID { return model.TableID{Schema: "verify_dst", Table: t.Table} }

// mysqlPair tạo bảng id INT PK, val INT, title VARCHAR ở verify_src và verify_dst, nạp n row
// (1..n) vào source rồi chép sang target.
func mysqlPair(t *testing.T, src, dst *sql.DB, name string, n int) model.TableID {
	t.Helper()
	tbl := model.TableID{Schema: "verify_src", Table: name}
	for _, q := range []string{"DROP TABLE IF EXISTS verify_src.%[1]s", "DROP TABLE IF EXISTS verify_dst.%[1]s",
		"CREATE TABLE verify_src.%[1]s (id INT PRIMARY KEY, val INT NOT NULL, title VARCHAR(50))",
		"CREATE TABLE verify_dst.%[1]s LIKE verify_src.%[1]s"} {
		mustExec(t, src, fmt.Sprintf(q, name))
	}
	// Nhân đôi từ 10 row tới >= n rồi cắt, nhanh hơn INSERT từng lô.
	mustExec(t, src, fmt.Sprintf("INSERT INTO verify_src.%s VALUES (1,1,'row-1'),(2,2,'row-2'),(3,3,'row-3'),(4,4,'row-4'),(5,5,'row-5'),(6,6,'row-6'),(7,7,'row-7'),(8,8,'row-8'),(9,9,'row-9'),(10,10,'row-10')", name))
	for c := 10; c < n; c *= 2 {
		mustExec(t, src, fmt.Sprintf("INSERT INTO verify_src.%[1]s SELECT id + %[2]d, val + %[2]d, CONCAT('row-', id + %[2]d) FROM verify_src.%[1]s", name, c))
	}
	mustExec(t, src, fmt.Sprintf("DELETE FROM verify_src.%s WHERE id > %d", name, n))
	mustExec(t, dst, fmt.Sprintf("INSERT INTO verify_dst.%[1]s SELECT * FROM verify_src.%[1]s", name))
	return tbl
}

// standaloneLV là LiveVerifier dùng khi verify chạy độc lập (không có pipeline): không chờ apply.
func standaloneLV(src, dst *sql.DB, pipeline string) *verify.LiveVerifier {
	return verify.NewLiveVerifier(verify.LiveConfig{Source: src, Target: dst, Meta: dst, PipelineID: pipeline,
		TargetName: toDst, SkipApplyWait: true})
}

func mysqlFull(src, dst *sql.DB, pipeline string, chunk int, lv *verify.LiveVerifier, mod func(*verify.FullVerifyConfig)) *verify.MerkleVerifier {
	cfg := verify.FullVerifyConfig{Source: src, Target: dst, Meta: dst, PipelineID: pipeline,
		SourceEngine: model.EngineMySQL, TargetEngine: model.EngineMySQL, TargetName: toDst, ChunkRows: chunk, LiveVerifier: lv}
	if mod != nil {
		mod(&cfg)
	}
	return verify.NewMerkleVerifier(cfg)
}

func sorted(xs []string) []string {
	out := append([]string(nil), xs...)
	sort.Strings(out)
	return out
}

// AC1: bảng 1.000.000 row, sửa 1 cột của 1 row ở target → full verify báo đúng PK đó và chỉ đúng
// 1 chunk (10.000 row) phải so từng row; 101 chunk còn lại được loại nhờ so hash cây Merkle.
// Không có pipeline / _strand_checkpoint: drill-down không được chờ target apply.
func TestAC1_SingleMismatchOneMillionRows(t *testing.T) {
	src, dst := getMySQL(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	tbl := mysqlPair(t, src, dst, "ac1_million", 1_000_000)
	var n int
	if err := src.QueryRow("SELECT COUNT(*) FROM verify_src.ac1_million").Scan(&n); err != nil || n != 1_000_000 {
		t.Fatalf("source rows = %d (%v), want 1000000", n, err)
	}
	mustExec(t, dst, "UPDATE verify_dst.ac1_million SET val = val + 1 WHERE id = 543210")

	start := time.Now()
	rep, err := mysqlFull(src, dst, "pipe_ac1", 10000, standaloneLV(src, dst, "pipe_ac1"), nil).VerifyTableFull(ctx, tbl, "id")
	if err != nil {
		t.Fatalf("VerifyTableFull: %v", err)
	}
	t.Logf("run 1 (LiveVerifier drill-down) in %s: %+v", time.Since(start), *rep)
	if want := []string{"543210"}; !reflect.DeepEqual(rep.Mismatches, want) {
		t.Fatalf("mismatches = %v, want %v", rep.Mismatches, want)
	}
	// 100 chunk dữ liệu + 2 chunk biên.
	if rep.TotalChunks != 102 || rep.HashedChunks != 102 {
		t.Fatalf("chunks total=%d hashed=%d, want 102/102", rep.TotalChunks, rep.HashedChunks)
	}
	if rep.MismatchedChunks != 1 || rep.RangesChecked != 1 {
		t.Fatalf("Merkle did not localize: mismatched chunks=%d, chunks compared row-by-row=%d, want 1/1", rep.MismatchedChunks, rep.RangesChecked)
	}
	if rep.RowsChecked != 10000 {
		t.Fatalf("rows compared = %d, want 10000 (one chunk)", rep.RowsChecked)
	}

	// Lượt 2: drill-down bằng so chuỗi chính tắc (không LiveVerifier). --resume sau một lượt đã chạy
	// hết không dùng lại hash cũ (review R12): mọi chunk được hash lại.
	rep2, err := mysqlFull(src, dst, "pipe_ac1", 10000, nil, func(c *verify.FullVerifyConfig) { c.Resume = true }).VerifyTableFull(ctx, tbl, "id")
	if err != nil {
		t.Fatalf("VerifyTableFull (resume, canonical compare): %v", err)
	}
	if !reflect.DeepEqual(rep2.Mismatches, []string{"543210"}) || rep2.RangesChecked != 1 || rep2.HashedChunks != 102 || rep2.ResumedChunks != 0 {
		t.Fatalf("run 2: %+v", *rep2)
	}
}

// AC2: thiếu 1 row / thừa 1 row ở target (trong dải và ngoài [min, max]) đều được báo đúng, với cả
// hai cách so từng row.
func TestAC2_ExtraOrMissingRow(t *testing.T) {
	src, dst := getMySQL(t)
	ctx := context.Background()
	tbl := mysqlPair(t, src, dst, "ac2_missing", 20000)
	for _, lv := range []*verify.LiveVerifier{standaloneLV(src, dst, "pipe_ac2"), nil} {
		run := func(want ...string) {
			t.Helper()
			rep, err := mysqlFull(src, dst, "pipe_ac2", 5000, lv, nil).VerifyTableFull(ctx, tbl, "id")
			if err != nil {
				t.Fatalf("VerifyTableFull: %v", err)
			}
			if got := sorted(rep.Mismatches); !reflect.DeepEqual(got, want) && !(len(got) == 0 && len(want) == 0) {
				t.Fatalf("live=%v mismatches = %v, want %v (%+v)", lv != nil, got, want, *rep)
			}
		}
		mustExec(t, dst, "DELETE FROM verify_dst.ac2_missing WHERE id = 7777")
		run("7777")
		mustExec(t, dst, "INSERT INTO verify_dst.ac2_missing VALUES (7777, 7777, 'row-7777')")
		run()
		mustExec(t, dst, "INSERT INTO verify_dst.ac2_missing VALUES (999999, 1, 'extra'), (-5, 1, 'extra')")
		run("-5", "999999")
		mustExec(t, dst, "DELETE FROM verify_dst.ac2_missing WHERE id IN (999999, -5)")
		// NULL khác chuỗi rỗng.
		mustExec(t, src, "UPDATE verify_src.ac2_missing SET title = NULL WHERE id = 12345")
		mustExec(t, dst, "UPDATE verify_dst.ac2_missing SET title = '' WHERE id = 12345")
		run("12345")
		mustExec(t, src, "UPDATE verify_src.ac2_missing SET title = 'row-12345' WHERE id = 12345")
		mustExec(t, dst, "UPDATE verify_dst.ac2_missing SET title = 'row-12345' WHERE id = 12345")
	}
}

// AC3: dừng sau 2/7 chunk (~30%), chạy lại với Resume → 2 chunk đã xong không bị hash lại
// (đếm HashedChunks / ResumedChunks), kết quả đúng.
func TestAC3_ResumeAfterPartialVerify(t *testing.T) {
	src, dst := getMySQL(t)
	ctx := context.Background()
	tbl := mysqlPair(t, src, dst, "ac3_resume", 50000)
	mustExec(t, dst, "UPDATE verify_dst.ac3_resume SET val = val + 1 WHERE id = 45000")

	lv := standaloneLV(src, dst, "pipe_ac3")
	_, err := mysqlFull(src, dst, "pipe_ac3", 10000, lv, func(c *verify.FullVerifyConfig) { c.StopAfter = 2 }).VerifyTableFull(ctx, tbl, "id")
	if !errors.Is(err, verify.ErrVerifyInterrupted) {
		t.Fatalf("run 1: err = %v, want ErrVerifyInterrupted", err)
	}
	var saved int
	if err := dst.QueryRow("SELECT COUNT(*) FROM _strand_verify WHERE pipeline_id = 'pipe_ac3' AND table_name = ?", tbl.String()).Scan(&saved); err != nil || saved != 2 {
		t.Fatalf("saved chunks = %d (%v), want 2", saved, err)
	}
	// Đổi một row thuộc chunk đã hash ở lượt 1 (id 5 thuộc chunk [1, 10000]). Lượt resume dùng lại
	// hash đã lưu nên không thấy thay đổi này — bằng chứng chunk đó không bị hash lại.
	mustExec(t, dst, "UPDATE verify_dst.ac3_resume SET val = val + 1 WHERE id = 5")

	rep, err := mysqlFull(src, dst, "pipe_ac3", 10000, lv, func(c *verify.FullVerifyConfig) { c.Resume = true }).VerifyTableFull(ctx, tbl, "id")
	if err != nil {
		t.Fatalf("run 2 (resume): %v", err)
	}
	if rep.TotalChunks != 7 || rep.ResumedChunks != 2 || rep.HashedChunks != 5 {
		t.Fatalf("run 2: total=%d resumed=%d hashed=%d, want 7/2/5", rep.TotalChunks, rep.ResumedChunks, rep.HashedChunks)
	}
	if !reflect.DeepEqual(rep.Mismatches, []string{"45000"}) {
		t.Fatalf("run 2 mismatches = %v, want [45000]", rep.Mismatches)
	}
	// Lượt mới (không Resume) xóa tiến độ cũ, hash lại hết và thấy cả hai row.
	rep, err = mysqlFull(src, dst, "pipe_ac3", 10000, lv, nil).VerifyTableFull(ctx, tbl, "id")
	if err != nil {
		t.Fatalf("run 3: %v", err)
	}
	if rep.HashedChunks != 7 || rep.ResumedChunks != 0 || !reflect.DeepEqual(sorted(rep.Mismatches), []string{"45000", "5"}) {
		t.Fatalf("run 3: %+v", *rep)
	}
}

// AC4: hai bảng giống nhau → 0 lệch, không chunk nào phải so từng row.
func TestAC4_IdenticalTablesZeroMismatch(t *testing.T) {
	src, dst := getMySQL(t)
	tbl := mysqlPair(t, src, dst, "ac4_identical", 25000)
	rep, err := mysqlFull(src, dst, "pipe_ac4", 10000, standaloneLV(src, dst, "pipe_ac4"), nil).VerifyTableFull(context.Background(), tbl, "id")
	if err != nil {
		t.Fatalf("VerifyTableFull: %v", err)
	}
	if len(rep.Mismatches) != 0 || rep.MismatchedChunks != 0 || rep.RangesChecked != 0 || rep.TotalChunks != 5 {
		t.Fatalf("report = %+v", *rep)
	}
}

// LiveVerifier mặc định (SkipApplyWait=false) vẫn chờ target apply như trước: không ai apply thì lỗi.
func TestLiveVerifierDefaultStillWaitsForApply(t *testing.T) {
	src, dst := getMySQL(t)
	ctx := context.Background()
	tbl := mysqlPair(t, src, dst, "lv_wait", 100)
	mustExec(t, dst, "UPDATE verify_dst.lv_wait SET val = 0 WHERE id = 50")
	store := checkpoint.NewMySQLStore(dst, "pipe_lv", model.EngineMySQL)
	if err := store.EnsureTables(ctx); err != nil {
		t.Fatal(err)
	}
	mustExec(t, dst, `INSERT INTO _strand_pipeline (pipeline_id, epoch, owner, config_hash, meta_version, started_at)
		VALUES ('pipe_lv', 1, 't', 'h', 1, NOW(6)) ON DUPLICATE KEY UPDATE epoch = 1`)
	cfg := verify.LiveConfig{Source: src, Target: dst, Meta: dst, PipelineID: "pipe_lv", TargetName: toDst, CatchUpLimit: 300 * time.Millisecond}
	_, err := verify.NewLiveVerifier(cfg).VerifyRanges(ctx, tbl, "id", [][2]int64{{1, 100}})
	if err == nil || !strings.Contains(err.Error(), "did not apply") {
		t.Fatalf("default LiveVerifier: err = %v, want 'did not apply'", err)
	}
	cfg.SkipApplyWait = true
	rep, err := verify.NewLiveVerifier(cfg).VerifyRanges(ctx, tbl, "id", [][2]int64{{1, 100}})
	if err != nil || !reflect.DeepEqual(rep.Mismatches, []string{"50"}) {
		t.Fatalf("SkipApplyWait: rep=%+v err=%v", rep, err)
	}
}

// DDL của _strand_verify gọi lại nhiều lần không lỗi, trên cả MySQL và PG.
func TestEnsureVerifyTableIdempotent(t *testing.T) {
	_, dst := getMySQL(t)
	pg := getPostgres(t)
	ctx := context.Background()
	for i := 0; i < 3; i++ {
		if err := verify.EnsureVerifyTable(ctx, dst, model.EngineMySQL); err != nil {
			t.Fatalf("mysql call %d: %v", i, err)
		}
		if err := verify.EnsureVerifyTable(ctx, pg, model.EnginePG); err != nil {
			t.Fatalf("pg call %d: %v", i, err)
		}
	}
}

// crossRow là một row dùng chung cho MySQL và PG; nil là NULL.
type crossRow struct {
	id                      int
	big, amount             any // bigint, decimal(12,3)
	f, d                    any // float (4 byte), double
	name, note              any // varchar, text
	bin                     any // varbinary / bytea
	day, dt, tsMySQL, tsPG  any // date, datetime/timestamp, TIMESTAMP (giờ +07:00) / timestamptz
	tm                      any // time
	active                  any // bool gốc; activeMySQL = 0/1 cho TINYINT(1), activeOnPG cho boolean
	activeMySQL, activeOnPG any
}

func crossRows() []crossRow {
	rows := []crossRow{
		{id: 1, big: int64(9007199254740993), amount: "10.500", f: float32(0.1), d: 1.0 / 3, name: "alpha", note: "a|b:c\nTiếng Việt 🙂",
			bin: []byte{0x00, 0x01, 0xff, 0x00}, day: "2026-11-03", dt: "2026-01-02 03:04:05.678901",
			tsMySQL: "2026-11-03 17:00:00.123456", tsPG: "2026-11-03 10:00:00.123456+00", tm: "10:11:12.5", active: true},
		{id: 2, big: int64(-1), amount: "0.000", f: float32(-2.5), d: 1e300, name: "", note: "", bin: []byte{},
			day: "1999-12-31", dt: "2000-01-01 00:00:00", tsMySQL: "2000-01-01 07:00:00", tsPG: "2000-01-01 00:00:00+00", tm: "00:00:00", active: false},
		{id: 3, big: int64(0), amount: "-123456789.120", f: float32(16777217), d: 0.30000000000000004, name: "gamma", note: "x",
			bin: []byte("abc"), day: "2026-02-28", dt: "2026-02-28 23:59:59.5", tsMySQL: "2026-03-01 06:59:59.999999",
			tsPG: "2026-02-28 23:59:59.999999+00", tm: "23:59:59.999999", active: true},
		{id: 4, big: int64(42), amount: "7.000", f: float32(1e-3), d: -1e-12, name: "delta", note: "0", bin: []byte{0x0a},
			day: "2026-07-01", dt: "2026-07-01 12:00:00", tsMySQL: "2026-07-01 19:00:00", tsPG: "2026-07-01 12:00:00+00", tm: "12:00:00", active: false},
		{id: 5}, // mọi cột (trừ PK) đều NULL
	}
	for i := range rows {
		if rows[i].active != nil {
			rows[i].activeOnPG = rows[i].active
			rows[i].activeMySQL = 0
			if rows[i].active.(bool) {
				rows[i].activeMySQL = 1
			}
		}
	}
	return rows
}

// AC5: MySQL → PG. Cùng dữ liệu (số nguyên, decimal, float, double, chuỗi UTF-8, nhị phân có byte 0,
// date, datetime, timestamp ở hai múi giờ session khác nhau, time, boolean, NULL) cho cùng hash;
// sửa từng giá trị ở PG thì báo đúng PK.
func TestAC5_CrossEngineMySQLToPostgres(t *testing.T) {
	src, _ := getMySQL(t)
	pg := getPostgres(t)
	ctx := context.Background()
	mustExec(t, src, "DROP TABLE IF EXISTS verify_src.ac5_cross")
	mustExec(t, src, `CREATE TABLE verify_src.ac5_cross (id INT PRIMARY KEY, big BIGINT, amount DECIMAL(12,3), f FLOAT, d DOUBLE,
		name VARCHAR(50), note TEXT, bin VARBINARY(16), day DATE, dt DATETIME(6), ts TIMESTAMP(6) NULL, tm TIME(6), active TINYINT(1))
		DEFAULT CHARSET utf8mb4`)
	mustExec(t, pg, "DROP TABLE IF EXISTS public.ac5_cross")
	mustExec(t, pg, `CREATE TABLE public.ac5_cross (id int PRIMARY KEY, big bigint, amount numeric(12,3), f real, d double precision,
		name varchar(50), note text, bin bytea, day date, dt timestamp(6), ts timestamptz(6), tm time(6), active boolean,
		extra_on_target int DEFAULT 7)`)
	for _, r := range crossRows() {
		mustExec(t, src, "INSERT INTO verify_src.ac5_cross VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)",
			r.id, r.big, r.amount, r.f, r.d, r.name, r.note, r.bin, r.day, r.dt, r.tsMySQL, r.tm, r.activeMySQL)
		mustExec(t, pg, "INSERT INTO public.ac5_cross (id, big, amount, f, d, name, note, bin, day, dt, ts, tm, active) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13)",
			r.id, r.big, r.amount, r.f, r.d, r.name, r.note, r.bin, r.day, r.dt, r.tsPG, r.tm, r.activeOnPG)
	}
	tbl := model.TableID{Schema: "verify_src", Table: "ac5_cross"}
	mv := verify.NewMerkleVerifier(verify.FullVerifyConfig{Source: src, Target: pg, PipelineID: "pipe_ac5",
		SourceEngine: model.EngineMySQL, TargetEngine: model.EnginePG, ChunkRows: 2,
		TargetName: func(model.TableID) model.TableID { return model.TableID{Schema: "public", Table: "ac5_cross"} }})

	rep, err := mv.VerifyTableFull(ctx, tbl, "id")
	if err != nil {
		t.Fatalf("identical data: %v", err)
	}
	if len(rep.Mismatches) != 0 || rep.MismatchedChunks != 0 {
		t.Fatalf("identical data: %+v", *rep)
	}

	for _, q := range []string{
		"UPDATE public.ac5_cross SET amount = 10.501 WHERE id = 1",
		"UPDATE public.ac5_cross SET ts = ts + interval '1 microsecond' WHERE id = 2",
		"UPDATE public.ac5_cross SET bin = '\\x616263ff' WHERE id = 3",
		"UPDATE public.ac5_cross SET active = true WHERE id = 4",
		"UPDATE public.ac5_cross SET name = '' WHERE id = 5",
	} {
		mustExec(t, pg, q)
	}
	rep, err = mv.VerifyTableFull(ctx, tbl, "id")
	if err != nil {
		t.Fatalf("modified data: %v", err)
	}
	if got, want := sorted(rep.Mismatches), []string{"1", "2", "3", "4", "5"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("mismatches = %v, want %v (%+v)", got, want, *rep)
	}
	// Mỗi chunk dữ liệu 2 row: chỉ chunk lệch được so từng row.
	if rep.RangesChecked != rep.MismatchedChunks || rep.RangesChecked != 3 {
		t.Fatalf("ranges checked = %d, mismatched chunks = %d, want 3/3", rep.RangesChecked, rep.MismatchedChunks)
	}
}

// PG → PG (hai schema trên cùng server) dùng cùng thuật toán hash.
func TestFullVerifyPostgresToPostgres(t *testing.T) {
	pg := getPostgres(t)
	ctx := context.Background()
	mustExec(t, pg, "CREATE SCHEMA IF NOT EXISTS s1; CREATE SCHEMA IF NOT EXISTS s2")
	for _, s := range []string{"s1", "s2"} {
		mustExec(t, pg, fmt.Sprintf("DROP TABLE IF EXISTS %s.pp", s))
		mustExec(t, pg, fmt.Sprintf(`CREATE TABLE %s.pp (id bigint PRIMARY KEY, v numeric, ts timestamptz, b bytea, j jsonb)`, s))
		mustExec(t, pg, fmt.Sprintf(`INSERT INTO %s.pp SELECT g, g / 7.0, timestamptz '2026-01-01Z' + g * interval '1 second',
			decode(md5(g::text), 'hex'), jsonb_build_object('k', g) FROM generate_series(1, 5000) g`, s))
	}
	mustExec(t, pg, "UPDATE s2.pp SET j = '{\"k\": -1}' WHERE id = 4321")
	mv := verify.NewMerkleVerifier(verify.FullVerifyConfig{Source: pg, Target: pg, PipelineID: "pipe_pp",
		SourceEngine: model.EnginePG, TargetEngine: model.EnginePG, ChunkRows: 1000,
		TargetName: func(t model.TableID) model.TableID { return model.TableID{Schema: "s2", Table: t.Table} }})
	rep, err := mv.VerifyTableFull(ctx, model.TableID{Schema: "s1", Table: "pp"}, "id")
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(rep.Mismatches, []string{"4321"}) || rep.RangesChecked != 1 {
		t.Fatalf("report = %+v", *rep)
	}
}
