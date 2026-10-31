package mysql_audit

import (
	"context"
	"database/sql"
	"fmt"
	"net"
	"os"
	"strconv"
	"testing"
	"time"

	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/replication"
	_ "github.com/go-sql-driver/mysql"
	"github.com/google/uuid"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

func init() {
	_ = os.Setenv("TESTCONTAINERS_RYUK_DISABLED", "true")
}

func startMySQL(t *testing.T, image string, extraArgs ...string) (*sql.DB, string, int, func()) {
	t.Helper()
	ctx := context.Background()

	cmd := []string{
		"--server-id=1",
		"--gtid-mode=ON",
		"--enforce-gtid-consistency=ON",
		"--binlog-format=ROW",
		"--binlog-row-image=FULL",
		"--binlog-row-metadata=FULL",
	}
	cmd = append(cmd, extraArgs...)

	req := testcontainers.ContainerRequest{
		Image:        image,
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
		t.Fatalf("failed to start container %s: %v", image, err)
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

// AC1: Đọc được binlog qua stream có tagged GTID của MySQL 8.4.
func TestAC1_TaggedGTID_MySQL84(t *testing.T) {
	db, host, port, cleanup := startMySQL(t, "mysql:8.4")
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

	cfg := replication.BinlogSyncerConfig{
		ServerID: 1001,
		Flavor:   "mysql",
		Host:     host,
		Port:     uint16(port),
		User:     "root",
		Password: "dev",
	}
	syncer := replication.NewBinlogSyncer(cfg)
	defer syncer.Close()

	streamer, err := syncer.StartSync(mysql.Position{Name: "binlog.000001", Pos: 4})
	if err != nil {
		t.Fatalf("StartSync failed: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	foundTaggedGTID := false
	var detectedTag string
	var detectedGNO int64

	for {
		ev, err := streamer.GetEvent(ctx)
		if err != nil {
			t.Fatalf("GetEvent failed: %v", err)
		}

		if ev.Header.EventType == replication.GTID_TAGGED_LOG_EVENT {
			taggedEv, ok := ev.Event.(*replication.GtidTaggedLogEvent)
			if !ok {
				t.Fatalf("Event is GTID_TAGGED_LOG_EVENT but type is %T", ev.Event)
			}
			detectedTag = taggedEv.Tag.String()
			detectedGNO = taggedEv.GNO
			if detectedTag == tagName {
				foundTaggedGTID = true
				t.Logf("AC1: Nhận được GtidTaggedLogEvent: Tag=%s, GNO=%d", detectedTag, detectedGNO)
				break
			}
		}
	}

	if !foundTaggedGTID {
		t.Fatalf("AC1 FAIL: không đọc được tagged GTID '%s' từ stream MySQL 8.4", tagName)
	}
}

// AC2: Parse đủ event của MySQL 9.7 (testcontainers mysql:9.7).
func TestAC2_Events_MySQL97(t *testing.T) {
	db, host, port, cleanup := startMySQL(t, "mysql:9.7")
	defer cleanup()

	setupSQL := []string{
		"CREATE DATABASE IF NOT EXISTS test_ac2;",
		"CREATE TABLE test_ac2.items (id INT PRIMARY KEY, name VARCHAR(64), payload JSON);",
		"INSERT INTO test_ac2.items VALUES (1, 'item1', '{\"status\":\"active\"}');",
		"UPDATE test_ac2.items SET name = 'item1_mod' WHERE id = 1;",
		"DELETE FROM test_ac2.items WHERE id = 1;",
	}
	for _, q := range setupSQL {
		if _, err := db.Exec(q); err != nil {
			t.Fatalf("Exec setup query %q failed: %v", q, err)
		}
	}

	cfg := replication.BinlogSyncerConfig{
		ServerID: 1002,
		Flavor:   "mysql",
		Host:     host,
		Port:     uint16(port),
		User:     "root",
		Password: "dev",
	}
	syncer := replication.NewBinlogSyncer(cfg)
	defer syncer.Close()

	streamer, err := syncer.StartSync(mysql.Position{Name: "binlog.000001", Pos: 4})
	if err != nil {
		t.Fatalf("StartSync failed: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	receivedEvents := make(map[replication.EventType]int)
	expectedEvents := []replication.EventType{
		replication.FORMAT_DESCRIPTION_EVENT,
		replication.QUERY_EVENT,
		replication.TABLE_MAP_EVENT,
		replication.WRITE_ROWS_EVENTv2,
		replication.UPDATE_ROWS_EVENTv2,
		replication.DELETE_ROWS_EVENTv2,
		replication.XID_EVENT,
	}

	for {
		ev, err := streamer.GetEvent(ctx)
		if err != nil {
			t.Fatalf("GetEvent failed: %v", err)
		}
		receivedEvents[ev.Header.EventType]++
		t.Logf("MySQL 9.7 Event: %s (pos: %d)", ev.Header.EventType, ev.Header.LogPos)

		allFound := true
		for _, exp := range expectedEvents {
			if receivedEvents[exp] == 0 {
				allFound = false
				break
			}
		}
		if allFound {
			break
		}
	}

	for _, exp := range expectedEvents {
		if receivedEvents[exp] == 0 {
			t.Fatalf("AC2 FAIL: thiếu event %s từ MySQL 9.7", exp)
		}
	}
	t.Logf("AC2: Parse đủ mọi loại event của MySQL 9.7: %+v", receivedEvents)
}

// AC3: Resume bằng COM_BINLOG_DUMP_GTID với GTID set có lỗ: server gửi lại đúng các transaction còn thiếu.
func TestAC3_Resume_Holes_GTID(t *testing.T) {
	db, host, port, cleanup := startMySQL(t, "mysql:8.4")
	defer cleanup()

	var serverUUID string
	if err := db.QueryRow("SELECT @@server_uuid;").Scan(&serverUUID); err != nil {
		t.Fatalf("Get @@server_uuid failed: %v", err)
	}
	t.Logf("Server UUID: %s", serverUUID)

	if _, err := db.Exec("CREATE DATABASE IF NOT EXISTS test_ac3; CREATE TABLE test_ac3.txns (id INT PRIMARY KEY, val INT);"); err != nil {
		t.Fatalf("Setup table failed: %v", err)
	}

	for i := 1; i <= 5; i++ {
		q := fmt.Sprintf("INSERT INTO test_ac3.txns VALUES (%d, %d);", i, i*100)
		if _, err := db.Exec(q); err != nil {
			t.Fatalf("Exec insert %d failed: %v", i, err)
		}
	}

	var binlogFile, executedGtidSet string
	var pos uint64
	var doDB, ignoreDB sql.NullString
	row := db.QueryRow("SHOW BINARY LOG STATUS;")
	if err := row.Scan(&binlogFile, &pos, &doDB, &ignoreDB, &executedGtidSet); err != nil {
		t.Fatalf("SHOW BINARY LOG STATUS failed: %v", err)
	}
	t.Logf("Executed_Gtid_Set: %s", executedGtidSet)

	gset, err := mysql.ParseMysqlGTIDSet(executedGtidSet)
	if err != nil {
		t.Fatalf("ParseMysqlGTIDSet failed: %v", err)
	}

	mysqlSet, ok := gset.(*mysql.MysqlGTIDSet)
	if !ok {
		t.Fatalf("Not a MysqlGTIDSet")
	}

	sUUID, err := uuid.Parse(serverUUID)
	if err != nil {
		t.Fatalf("uuid.Parse failed: %v", err)
	}
	emptyTag := mysql.NewTag("")
	tagMap := (*mysqlSet)[sUUID]
	if tagMap == nil {
		t.Fatalf("No tagMap for server %s", serverUUID)
	}
	intervals := tagMap[emptyTag]
	if len(intervals) == 0 {
		t.Fatalf("No intervals in UUIDSet")
	}

	lastInterval := intervals[len(intervals)-1]
	startGNO := lastInterval.Start
	stopGNO := lastInterval.Stop // [Start, Stop)

	if stopGNO-startGNO < 4 {
		t.Fatalf("Need at least 4 transactions in interval, got [%d, %d)", startGNO, stopGNO)
	}

	missingGNO := stopGNO - 3 // transaction cần gửi lại

	var gtidWithHoleStr string
	if missingGNO == 1 || missingGNO == startGNO {
		gtidWithHoleStr = fmt.Sprintf("%s:%d-%d", serverUUID, missingGNO+1, stopGNO-1)
	} else {
		gtidWithHoleStr = fmt.Sprintf("%s:%d-%d:%d-%d", serverUUID, startGNO, missingGNO-1, missingGNO+1, stopGNO-1)
	}

	t.Logf("GTID set có lỗ gửi lên (omitted GNO=%d): %s", missingGNO, gtidWithHoleStr)

	clientGSet, err := mysql.ParseMysqlGTIDSet(gtidWithHoleStr)
	if err != nil {
		t.Fatalf("Parse client GTID set with hole failed: %v", err)
	}

	cfg := replication.BinlogSyncerConfig{
		ServerID: 1003,
		Flavor:   "mysql",
		Host:     host,
		Port:     uint16(port),
		User:     "root",
		Password: "dev",
	}
	syncer := replication.NewBinlogSyncer(cfg)
	defer syncer.Close()

	streamer, err := syncer.StartSyncGTID(clientGSet)
	if err != nil {
		t.Fatalf("StartSyncGTID failed: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	receivedGNOs := make([]int64, 0)
	for {
		ev, err := streamer.GetEvent(ctx)
		if err != nil {
			break
		}

		switch e := ev.Event.(type) {
		case *replication.GTIDEvent:
			receivedGNOs = append(receivedGNOs, e.GNO)
			t.Logf("Nhận GTIDEvent: GNO=%d", e.GNO)
		case *replication.GtidTaggedLogEvent:
			receivedGNOs = append(receivedGNOs, e.GNO)
			t.Logf("Nhận GtidTaggedLogEvent: GNO=%d", e.GNO)
		}

		hasMissing := false
		for _, g := range receivedGNOs {
			if g == missingGNO {
				hasMissing = true
			}
		}
		if hasMissing {
			break
		}
	}

	foundMissing := false
	for _, g := range receivedGNOs {
		if g == missingGNO {
			foundMissing = true
		}
		if g < missingGNO && g >= startGNO {
			t.Fatalf("AC3 FAIL: Server gửi lại transaction %d vốn đã có trong client GTID set!", g)
		}
	}

	if !foundMissing {
		t.Fatalf("AC3 FAIL: Server không gửi lại transaction còn thiếu GNO=%d! Các GNO nhận được: %v", missingGNO, receivedGNOs)
	}

	t.Logf("AC3 PASS: Server gửi lại đúng transaction thiếu GNO=%d và không gửi lại các transaction đã có", missingGNO)
}
