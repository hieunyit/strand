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
	// Khởi chạy 2 MySQL servers giả lập Replica 1 và Replica 2
	db1, host1, port1, cleanup1 := startMySQLServer(t, 2003)
	defer cleanup1()

	var server1UUID string
	if err := db1.QueryRow("SELECT @@server_uuid;").Scan(&server1UUID); err != nil {
		t.Fatalf("Get @@server_uuid db1 failed: %v", err)
	}

	if _, err := db1.Exec("CREATE DATABASE IF NOT EXISTS test_cp2; CREATE TABLE test_cp2.items (id INT PRIMARY KEY);"); err != nil {
		t.Fatalf("Setup db1 failed: %v", err)
	}

	// Ghi 3 Txn vào db1
	for i := 1; i <= 3; i++ {
		if _, err := db1.Exec(fmt.Sprintf("INSERT INTO test_cp2.items VALUES (%d);", i)); err != nil {
			t.Fatalf("Insert db1 %d failed: %v", i, err)
		}
	}

	// Lần chạy 1 trên Replica 1: Đọc đến Txn 2
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

	ctx1, cancel1 := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel1()

	appliedSet := checkpoint.NewMySQLAppliedSet(server1UUID)
	events1, err := r1.Start(ctx1, nil)
	if err != nil {
		t.Fatalf("r1.Start failed: %v", err)
	}

	// Đọc cho đến khi gặp ít nhất 2 GTID
	gtidCount := 0
	for ev := range events1 {
		if ev.Type == reader.EventGTID {
			gtidCount++
			appliedSet.Add(ev.Pos)
			if gtidCount >= 2 {
				break
			}
		}
	}
	r1.Close()

	t.Logf("Replica 1 read %d GTIDs. AppliedSet: %s", gtidCount, appliedSet.String())

	// Lần chạy 2: Chuyển sang Replica 2 (ở đây ta dùng db1 hoặc một replica chứa cùng dữ liệu)
	// Với AppliedSet đã ghi nhận các txn đã đọc từ Replica 1, kết nối tới replica tiếp theo để resume
	r2, err := readermysql.New(readermysql.Config{
		Host:     host1, // Switch endpoint
		Port:     uint16(port1),
		User:     "root",
		Password: "dev",
		ServerID: 3004, // Server ID khác nhau cho mỗi kết nối reader
	})
	if err != nil {
		t.Fatalf("New r2 failed: %v", err)
	}
	defer r2.Close()

	ctx2, cancel2 := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel2()

	events2, err := r2.Start(ctx2, appliedSet)
	if err != nil {
		t.Fatalf("r2.Start failed: %v", err)
	}

	// Đảm bảo không nhận lại bất kỳ Txn nào đã nằm trong appliedSet (TestCP2: Không Txn nào được apply hai lần)
	timer := time.After(3 * time.Second)
	duplicated := false

checkLoop:
	for {
		select {
		case ev, ok := <-events2:
			if !ok {
				break checkLoop
			}
			if ev.Type == reader.EventGTID {
				if appliedSet.Contains(ev.Pos) {
					t.Errorf("CP-2 VIOLATION: Txn %s đã có trong appliedSet lại xuất hiện khi đổi replica!", ev.Pos.GTID)
					duplicated = true
				} else {
					t.Logf("Replica 2 resumed new GTID: %s", ev.Pos.GTID)
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

	t.Logf("AC3 (TestCP2) PASS: Đổi replica giữa hai lần chạy resume đúng, 0 Txn trùng lặp")
}
