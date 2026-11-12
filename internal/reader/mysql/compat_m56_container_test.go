package mysql

import (
	"context"
	"database/sql"
	"fmt"
	"net"
	"os"
	"strconv"
	"testing"
	"time"

	gomysql "github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/replication"
	_ "github.com/go-sql-driver/mysql"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"

	"strand/internal/model"
	"strand/internal/reader"
)

func init() { _ = os.Setenv("TESTCONTAINERS_RYUK_DISABLED", "true") }

// startLegacyMySQL chạy mysql:5.5 / 5.6 với binlog ROW (không GTID), trả DB root + host/port.
func startLegacyMySQL(t *testing.T, image string, extra ...string) (*sql.DB, string, uint16) {
	t.Helper()
	ctx := context.Background()
	cmd := append([]string{"--server-id=1", "--log-bin=mysql-bin", "--binlog-format=ROW", "--innodb-use-native-aio=0"}, extra...)
	c, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: testcontainers.ContainerRequest{
			Image:        image,
			ExposedPorts: []string{"3306/tcp"},
			Env:          map[string]string{"MYSQL_ROOT_PASSWORD": "dev"},
			Cmd:          cmd,
			WaitingFor:   wait.ForLog("ready for connections").WithOccurrence(2).WithStartupTimeout(180 * time.Second),
		},
		Started: true,
	})
	if err != nil {
		t.Fatalf("start %s: %v", image, err)
	}
	t.Cleanup(func() { _ = c.Terminate(ctx) })
	host, _ := c.Host(ctx)
	pn, _ := c.MappedPort(ctx, "3306")
	port, _ := strconv.Atoi(pn.Port())
	dsn := fmt.Sprintf("root:dev@tcp(%s)/?multiStatements=true", net.JoinHostPort(host, pn.Port()))
	var db *sql.DB
	for i := 0; i < 60; i++ {
		db, err = sql.Open("mysql", dsn)
		if err == nil && db.PingContext(ctx) == nil {
			break
		}
		time.Sleep(time.Second)
	}
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	t.Cleanup(func() { db.Close() })
	return db, host, uint16(port)
}

type decodedRow struct {
	op   model.Op
	vals []any
}

// collectLegacyRows đọc binlog từ (file,pos) bằng BinlogSyncer (StartSync theo vị trí: không cần GTID),
// đưa từng event qua handleEvent của reader (cùng đường NormalizeRows như production) và gom các row.
func collectLegacyRows(t *testing.T, host string, port uint16, st BinlogStatus, want int) ([]decodedRow, *replication.TableMapEvent) {
	t.Helper()
	syncer := replication.NewBinlogSyncer(replication.BinlogSyncerConfig{
		ServerID: 4242, Flavor: "mysql", Host: host, Port: port, User: "root", Password: "dev",
		ParseTime: false, TimestampStringLocation: time.UTC,
	})
	defer syncer.Close()
	streamer, err := syncer.StartSync(gomysql.Position{Name: st.File, Pos: uint32(st.Position)})
	if err != nil {
		t.Fatalf("StartSync: %v", err)
	}
	r := &MySQLReader{}
	state := &streamState{tableMap: map[uint64]*replication.TableMapEvent{}}
	var out []decodedRow
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	for len(out) < want {
		ev, err := streamer.GetEvent(ctx)
		if err != nil {
			t.Fatalf("GetEvent (have %d/%d rows): %v", len(out), want, err)
		}
		r.handleEvent(state, ev, func(e reader.Event) bool {
			if e.Type == reader.EventError {
				t.Fatalf("reader error: %v", e.Err)
			}
			if e.Type == reader.EventRows {
				for _, row := range e.Rows {
					out = append(out, decodedRow{e.Op, row})
				}
			}
			return true
		})
	}
	var tm *replication.TableMapEvent
	for _, x := range state.tableMap {
		tm = x
	}
	return out, tm
}

const legacyDDL = `CREATE TABLE m56.t (
  id INT NOT NULL PRIMARY KEY,
  dt DATETIME NULL, ts TIMESTAMP NULL DEFAULT NULL, tm TIME NULL, d DATE NULL,
  yr YEAR(4) NULL, y2 YEAR(2) NULL, b BIT(10) NULL,
  e ENUM('a','b','c') NULL, s SET('x','y','z') NULL,
  u8 VARCHAR(20) CHARACTER SET utf8 NULL, l1 VARCHAR(20) CHARACTER SET latin1 NULL,
  u4 VARCHAR(20) CHARACTER SET utf8mb4 NULL, dec2 DECIMAL(10,2) NULL, tu TINYINT UNSIGNED NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8`

func runLegacyDecode(t *testing.T, image string, extra []string, wantTemporalType byte) {
	ctx := context.Background()
	db, host, port := startLegacyMySQL(t, image, extra...)
	if _, err := db.ExecContext(ctx, "CREATE DATABASE m56"); err != nil {
		t.Fatal(err)
	}
	if _, err := db.ExecContext(ctx, legacyDDL); err != nil {
		t.Fatal(err)
	}
	// ReadBinlogStatus chạy được trên 5.5/5.6 (SHOW BINARY LOG STATUS lỗi cú pháp -> lùi về SHOW MASTER STATUS).
	st, err := ReadBinlogStatus(ctx, db)
	if err != nil || st.File == "" {
		t.Fatalf("ReadBinlogStatus: %+v %v", st, err)
	}
	conn, err := db.Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	for _, q := range []string{
		"SET time_zone='+00:00'", "SET sql_mode=''", "SET NAMES utf8mb4",
		`INSERT INTO m56.t VALUES (1,'2020-01-02 03:04:05','2020-01-02 03:04:05','01:02:03','2020-01-02',2020,99,517,'b','x,z','héllo€','abc','ok',12.50,200)`,
		`INSERT INTO m56.t VALUES (2,'0000-00-00 00:00:00','0000-00-00 00:00:00','-01:02:03','0000-00-00',0,5,0,'c','', 'x','y','z',-1.00,0)`,
		`INSERT INTO m56.t VALUES (3,'2020-00-10 00:00:00',NULL,'838:59:59','2020-05-00',1999,70,1023,'a','y',NULL,NULL,NULL,NULL,NULL)`,
		`INSERT INTO m56.t (id,tm) VALUES (4,'-838:59:59')`,
		`UPDATE m56.t SET dt='2021-12-31 23:59:59', tm='-00:00:01' WHERE id=1`,
		`DELETE FROM m56.t WHERE id=2`,
	} {
		if _, err := conn.ExecContext(ctx, q); err != nil {
			t.Fatalf("%s: %v", q, err)
		}
	}
	rows, tm := collectLegacyRows(t, host, port, st, 4+2+1)
	// Định dạng nhị phân thật của cột thời gian (5.5: cũ; 5.6.4+: *2).
	if tm == nil || tm.ColumnType[1] != wantTemporalType {
		t.Fatalf("DATETIME wire type: want %d, table map %+v", wantTemporalType, tm)
	}

	// cột: 0 id,1 dt,2 ts,3 tm,4 d,5 yr,6 y2,7 b,8 e,9 s,10 u8,11 l1,12 u4,13 dec2,14 tu
	r1 := rows[0].vals
	want1 := map[int]any{
		1: "2020-01-02 03:04:05", 2: "2020-01-02 03:04:05", 3: "01:02:03", 4: "2020-01-02",
		5: 2020, 6: 1999, 7: int64(517), 8: int64(2), 9: int64(5), 10: "héllo€", 11: "abc", 12: "ok",
		13: "12.50", 14: int8(-56), // go-mysql trả TINYINT có dấu; writer/typemap dùng cờ unsigned của cột
	}
	for i, w := range want1 {
		if fmt.Sprint(r1[i]) != fmt.Sprint(w) {
			t.Errorf("row1 col %d: got %#v want %#v", i, r1[i], w)
		}
	}
	// zero date: giữ nguyên chuỗi zero (typemap/writer quyết định chính sách)
	r2 := rows[1].vals
	for i, w := range map[int]string{1: "0000-00-00 00:00:00", 2: "0000-00-00 00:00:00", 3: "-01:02:03", 4: "0000-00-00"} {
		if fmt.Sprint(r2[i]) != w {
			t.Errorf("row2 col %d: got %#v want %q", i, r2[i], w)
		}
	}
	if fmt.Sprint(r2[6]) != "2005" || fmt.Sprint(r2[5]) != "0" {
		t.Errorf("YEAR zero/YEAR(2): yr=%v y2=%v", r2[5], r2[6])
	}
	r3 := rows[2].vals
	if fmt.Sprint(r3[1]) != "2020-00-10 00:00:00" || fmt.Sprint(r3[3]) != "838:59:59" || fmt.Sprint(r3[4]) != "2020-05-00" || r3[2] != nil {
		t.Errorf("row3: %#v", r3)
	}
	if fmt.Sprint(r3[6]) != "1970" || fmt.Sprint(r3[7]) != "1023" {
		t.Errorf("row3 y2/bit: %v %v", r3[6], r3[7])
	}
	if fmt.Sprint(rows[3].vals[3]) != "-838:59:59" {
		t.Errorf("row4 min TIME: %#v", rows[3].vals[3])
	}
	// UPDATE: before-image rồi after-image
	if rows[4].op != model.OpUpdate || rows[5].op != model.OpUpdate {
		t.Fatalf("expected update images, got %v %v", rows[4].op, rows[5].op)
	}
	if fmt.Sprint(rows[5].vals[1]) != "2021-12-31 23:59:59" || fmt.Sprint(rows[5].vals[3]) != "-00:00:01" {
		t.Errorf("update after: %#v", rows[5].vals)
	}
	if rows[6].op != model.OpDelete || fmt.Sprint(rows[6].vals[0]) != "2" || fmt.Sprint(rows[6].vals[1]) != "0000-00-00 00:00:00" {
		t.Errorf("delete: %#v", rows[6])
	}
}

// MySQL 5.5: không có binlog checksum, định dạng TIME/DATETIME/TIMESTAMP cũ, ROWS event v1.
func TestCompatM56_Container_Decode_MySQL55(t *testing.T) {
	runLegacyDecode(t, "mysql:5.5", nil, gomysql.MYSQL_TYPE_DATETIME)
}

// MySQL 5.6 mặc định CRC32 (syncer đàm phán @master_binlog_checksum='NONE'), DATETIME2/TIME2/TIMESTAMP2.
func TestCompatM56_Container_Decode_MySQL56(t *testing.T) {
	runLegacyDecode(t, "mysql:5.6", nil, gomysql.MYSQL_TYPE_DATETIME2)
}

// MySQL 5.6 với checksum NONE (trước 5.6.6 là mặc định).
func TestCompatM56_Container_Decode_MySQL56_ChecksumNone(t *testing.T) {
	runLegacyDecode(t, "mysql:5.6", []string{"--binlog-checksum=NONE"}, gomysql.MYSQL_TYPE_DATETIME2)
}
