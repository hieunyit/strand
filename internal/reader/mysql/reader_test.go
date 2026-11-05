package mysql_test

import (
	"context"
	"database/sql"
	"fmt"
	"net"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"

	"strand/internal/checkpoint"
	"strand/internal/exitcode"
	"strand/internal/model"
	"strand/internal/reader"
	readermysql "strand/internal/reader/mysql"
)

func init() {
	_ = os.Setenv("TESTCONTAINERS_RYUK_DISABLED", "true")
}

func startMySQLServer(t *testing.T, serverID int, extraArgs ...string) (*sql.DB, string, int, func()) {
	t.Helper()
	ctx := context.Background()

	cmd := []string{
		fmt.Sprintf("--server-id=%d", serverID),
		"--gtid-mode=ON",
		"--enforce-gtid-consistency=ON",
		"--binlog-format=ROW",
		"--binlog-row-image=FULL",
		"--binlog-row-metadata=FULL",
	}
	cmd = append(cmd, "--innodb-use-native-aio=OFF") // không phụ thuộc fs.aio-max-nr khi nhiều MySQL chạy cùng lúc
	cmd = append(cmd, extraArgs...)

	req := testcontainers.ContainerRequest{
		Image:        "mysql:8.4",
		ExposedPorts: []string{"3306/tcp"},
		Env: map[string]string{
			"MYSQL_ROOT_PASSWORD": "dev",
		},
		Cmd:        cmd,
		WaitingFor: wait.ForLog("port: 3306  MySQL Community Server").WithStartupTimeout(180 * time.Second),
	}

	container, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: req,
		Started:          true,
	})
	if err != nil {
		t.Fatalf("failed to start mysql:8.4 container: %v", err)
	}

	host, err := container.Host(ctx)
	if err != nil {
		container.Terminate(ctx)
		t.Fatalf("failed to get container host: %v", err)
	}

	portNat, err := container.MappedPort(ctx, "3306")
	if err != nil {
		container.Terminate(ctx)
		t.Fatalf("failed to get mapped port: %v", err)
	}
	port, _ := strconv.Atoi(portNat.Port())

	addr := net.JoinHostPort(host, portNat.Port())
	dsn := fmt.Sprintf("root:dev@tcp(%s)/?multiStatements=true", addr)

	var db *sql.DB
	for i := 0; i < 30; i++ {
		db, err = sql.Open("mysql", dsn)
		if err == nil {
			if err = db.PingContext(ctx); err == nil {
				break
			}
			db.Close()
		}
		time.Sleep(1 * time.Second)
	}
	if err != nil {
		container.Terminate(ctx)
		t.Fatalf("failed to connect to mysql %s: %v", addr, err)
	}

	cleanup := func() {
		db.Close()
		container.Terminate(ctx)
	}

	return db, host, port, cleanup
}

func TestNew_InvalidDSN(t *testing.T) {
	_, err := readermysql.New(readermysql.Config{
		DSN: "invalid-dsn-format",
	})
	if err == nil {
		t.Fatalf("expected error for invalid DSN, got nil")
	}
	code := exitcode.GetExitCode(err)
	if code != exitcode.Rejected {
		t.Errorf("expected exit code %d (Rejected), got %d", exitcode.Rejected, code)
	}
}

// AC1: Đọc stream có tagged GTID (MySQL 8.4+)
func TestAC1_TaggedGTIDStream(t *testing.T) {
	db, host, port, cleanup := startMySQLServer(t, 2001)
	defer cleanup()

	const tagName = "strand_tag"
	queries := []string{
		"CREATE DATABASE IF NOT EXISTS test_ac1;",
		"CREATE TABLE test_ac1.t (id INT PRIMARY KEY, val VARCHAR(64));",
		fmt.Sprintf("SET @@SESSION.gtid_next = 'automatic:%s';", tagName),
		"INSERT INTO test_ac1.t VALUES (100, 'tagged_data');",
		"COMMIT;",
		"SET @@SESSION.gtid_next = 'automatic';",
	}
	for _, q := range queries {
		if _, err := db.Exec(q); err != nil {
			t.Fatalf("Exec query %q failed: %v", q, err)
		}
	}

	r, err := readermysql.New(readermysql.Config{
		Host:     host,
		Port:     uint16(port),
		User:     "root",
		Password: "dev",
		ServerID: 3001,
	})
	if err != nil {
		t.Fatalf("readermysql.New failed: %v", err)
	}
	defer r.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	events, err := r.Start(ctx, nil)
	if err != nil {
		t.Fatalf("r.Start failed: %v", err)
	}

	foundTaggedGTID := false
	var detectedGTID string

	for !foundTaggedGTID {
		select {
		case ev, ok := <-events:
			if !ok {
				t.Fatalf("events channel closed without finding tagged GTID")
			}
			if ev.Type == reader.EventGTID && strings.Contains(ev.GTID, tagName) {
				foundTaggedGTID = true
				detectedGTID = ev.GTID
				t.Logf("Found tagged GTID event: %s", ev.GTID)
			}
		case <-ctx.Done():
			t.Fatalf("timeout waiting for tagged GTID event")
		}
	}

	if !foundTaggedGTID {
		t.Fatalf("AC1 FAIL: không tìm thấy tagged GTID trong binlog stream")
	}

	pos := r.Position()
	if !strings.Contains(pos.GTID, tagName) {
		t.Errorf("expected Position().GTID to contain tag %s, got %s", tagName, pos.GTID)
	}
	t.Logf("AC1 PASS: Đọc thành công stream có tagged GTID: %s, position: %s", detectedGTID, pos.GTID)
}

// AC2: Resume từ AppliedSet đúng
func TestAC2_ResumeFromAppliedSet(t *testing.T) {
	db, host, port, cleanup := startMySQLServer(t, 2002)
	defer cleanup()

	var serverUUID string
	if err := db.QueryRow("SELECT @@server_uuid;").Scan(&serverUUID); err != nil {
		t.Fatalf("Get @@server_uuid failed: %v", err)
	}

	if _, err := db.Exec("CREATE DATABASE IF NOT EXISTS test_ac2; CREATE TABLE test_ac2.txns (id INT PRIMARY KEY, val INT);"); err != nil {
		t.Fatalf("Setup table failed: %v", err)
	}

	// Tạo 5 transactions riêng biệt
	for i := 1; i <= 5; i++ {
		q := fmt.Sprintf("INSERT INTO test_ac2.txns VALUES (%d, %d);", i, i*100)
		if _, err := db.Exec(q); err != nil {
			t.Fatalf("Exec insert %d failed: %v", i, err)
		}
	}

	// Đọc Executed_Gtid_Set hiện tại
	var binlogFile, executedGtidSet string
	var binlogPos uint64
	var doDB, ignoreDB sql.NullString
	if err := db.QueryRow("SHOW BINARY LOG STATUS;").Scan(&binlogFile, &binlogPos, &doDB, &ignoreDB, &executedGtidSet); err != nil {
		t.Fatalf("SHOW BINARY LOG STATUS failed: %v", err)
	}
	t.Logf("Executed_Gtid_Set before resume test: %s", executedGtidSet)

	// Lấy số GNO lớn nhất đã commit
	// Giả sử serverUUID có dạng uuid:1-N. Ta đánh dấu các txn 1..3 là đã apply trong AppliedSet.
	// Server phải chỉ gửi lại các txn từ 4 trở đi.
	applied := checkpoint.NewMySQLAppliedSet(serverUUID)
	// Tìm khoảng GNO hiện tại
	var baseGNO uint64 = 1
	applied.Add(model.Pos{Engine: model.EngineMySQL, GTID: fmt.Sprintf("%s:%d", serverUUID, 1)})
	applied.Add(model.Pos{Engine: model.EngineMySQL, GTID: fmt.Sprintf("%s:%d", serverUUID, 2)})
	applied.Add(model.Pos{Engine: model.EngineMySQL, GTID: fmt.Sprintf("%s:%d", serverUUID, 3)})

	// Khởi tạo Reader với AppliedSet đã apply 1..3
	r, err := readermysql.New(readermysql.Config{
		Host:     host,
		Port:     uint16(port),
		User:     "root",
		Password: "dev",
		ServerID: 3002,
	})
	if err != nil {
		t.Fatalf("readermysql.New failed: %v", err)
	}
	defer r.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	events, err := r.Start(ctx, applied)
	if err != nil {
		t.Fatalf("r.Start with applied set failed: %v", err)
	}

	// Thu thập các GTID nhận được từ stream
	receivedGTIDs := make(map[string]bool)
	timer := time.After(3 * time.Second)

collectLoop:
	for {
		select {
		case ev, ok := <-events:
			if !ok {
				break collectLoop
			}
			if ev.Type == reader.EventGTID {
				receivedGTIDs[ev.GTID] = true
				t.Logf("Received GTID: %s", ev.GTID)
			}
		case <-timer:
			break collectLoop
		case <-ctx.Done():
			break collectLoop
		}
	}

	// Đảm bảo không nhận được các GTID đã có trong AppliedSet (1, 2, 3)
	for gno := baseGNO; gno <= 3; gno++ {
		gtid := fmt.Sprintf("%s:%d", serverUUID, gno)
		if receivedGTIDs[gtid] {
			t.Fatalf("AC2 FAIL: Txn đã có trong AppliedSet (%s) lại bị stream lại", gtid)
		}
	}

	t.Logf("AC2 PASS: Resume từ AppliedSet thành công, không bị lặp các GTID đã apply")
}

// AC3: Đổi replica giữa hai lần chạy vẫn resume đúng (TestCP2)
// invariant: CP-2
func TestCP2_SwitchReplicaResume(t *testing.T) {
	// Khởi chạy 2 MySQL servers hoàn toàn riêng biệt: Replica 1 (port1) và Replica 2 (port2)
	db1, host1, port1, cleanup1 := startMySQLServer(t, 2003)
	defer cleanup1()

	db2, host2, port2, cleanup2 := startMySQLServer(t, 2004)
	defer cleanup2()

	// Cả 2 replica cùng đồng bộ từ 1 source chung có sourceUUID
	const sourceUUID = "11111111-2222-3333-4444-555555555555"

	// Khởi tạo schema trên cả 2 replica
	for _, db := range []*sql.DB{db1, db2} {
		if _, err := db.Exec("CREATE DATABASE IF NOT EXISTS test_cp2; CREATE TABLE test_cp2.items (id INT PRIMARY KEY);"); err != nil {
			t.Fatalf("Setup db failed: %v", err)
		}
	}

	// Ghi các transaction từ source vào Replica 1 (txn 1..3)
	for i := 1; i <= 3; i++ {
		gtid := fmt.Sprintf("%s:%d", sourceUUID, i)
		queries := []string{
			fmt.Sprintf("SET @@SESSION.gtid_next = '%s';", gtid),
			fmt.Sprintf("INSERT INTO test_cp2.items VALUES (%d);", i),
			"COMMIT;",
			"SET @@SESSION.gtid_next = 'automatic';",
		}
		for _, q := range queries {
			if _, err := db1.Exec(q); err != nil {
				t.Fatalf("db1 insert %d failed: %v", i, err)
			}
		}
	}

	// Ghi các transaction từ source vào Replica 2 (txn 1..5)
	for i := 1; i <= 5; i++ {
		gtid := fmt.Sprintf("%s:%d", sourceUUID, i)
		queries := []string{
			fmt.Sprintf("SET @@SESSION.gtid_next = '%s';", gtid),
			fmt.Sprintf("INSERT INTO test_cp2.items VALUES (%d);", i),
			"COMMIT;",
			"SET @@SESSION.gtid_next = 'automatic';",
		}
		for _, q := range queries {
			if _, err := db2.Exec(q); err != nil {
				t.Fatalf("db2 insert %d failed: %v", i, err)
			}
		}
	}

	// Lần chạy 1 trên Replica 1 (host1:port1): Đọc đến Txn 2
	r1, err := readermysql.New(readermysql.Config{
		Host:     host1,
		Port:     uint16(port1),
		User:     "root",
		Password: "dev",
		ServerID: 3003,
	})
	if err != nil {
		t.Fatalf("New r1 failed: %v", err)
	}

	ctx1, cancel1 := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel1()

	appliedSet := checkpoint.NewMySQLAppliedSet(sourceUUID)
	events1, err := r1.Start(ctx1, nil)
	if err != nil {
		t.Fatalf("r1.Start failed: %v", err)
	}

	// Đọc cho đến khi gặp ít nhất 2 GTID thuộc sourceUUID
	readCount := 0
	for ev := range events1 {
		if ev.Type == reader.EventGTID && strings.Contains(ev.GTID, sourceUUID) {
			readCount++
			appliedSet.Add(ev.Pos)
			if readCount >= 2 {
				break
			}
		}
	}
	r1.Close()

	t.Logf("Replica 1 (host %s:%d) read %d GTIDs. AppliedSet: %s", host1, port1, readCount, appliedSet.String())

	// Lần chạy 2: Chuyển hoàn toàn sang Replica 2 độc lập (host2:port2)
	// Resume với AppliedSet đã ghi nhận các txn từ Replica 1
	r2, err := readermysql.New(readermysql.Config{
		Host:     host2, // Endpoint Replica 2 riêng biệt
		Port:     uint16(port2),
		User:     "root",
		Password: "dev",
		ServerID: 3004,
	})
	if err != nil {
		t.Fatalf("New r2 failed: %v", err)
	}
	defer r2.Close()

	ctx2, cancel2 := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel2()

	events2, err := r2.Start(ctx2, appliedSet)
	if err != nil {
		t.Fatalf("r2.Start on replica 2 failed: %v", err)
	}

	// Đảm bảo không nhận lại bất kỳ Txn nào đã nằm trong appliedSet (TestCP2: Không Txn nào được apply hai lần)
	timer := time.After(3 * time.Second)
	duplicated := false
	resumedCount := 0

checkLoop:
	for {
		select {
		case ev, ok := <-events2:
			if !ok {
				break checkLoop
			}
			if ev.Type == reader.EventGTID && strings.Contains(ev.GTID, sourceUUID) {
				if appliedSet.Contains(ev.Pos) {
					t.Errorf("CP-2 VIOLATION: Txn %s đã có trong appliedSet từ Replica 1 lại xuất hiện trên Replica 2!", ev.Pos.GTID)
					duplicated = true
				} else {
					resumedCount++
					t.Logf("Replica 2 (%s:%d) resumed new GTID: %s", host2, port2, ev.Pos.GTID)
				}
			}
		case <-timer:
			break checkLoop
		case <-ctx2.Done():
			break checkLoop
		}
	}

	if duplicated {
		t.Fatalf("TestCP2 FAIL: Có transaction bị đọc lặp lại sau khi đổi replica")
	}
	if resumedCount == 0 {
		t.Fatalf("TestCP2 FAIL: Không nhận được transaction mới nào từ Replica 2 sau khi resume")
	}

	t.Logf("AC3 (TestCP2) PASS: Đổi replica từ (%s:%d) sang (%s:%d) resume đúng, nhận %d Txn mới, 0 Txn trùng lặp",
		host1, port1, host2, port2, resumedCount)
}
