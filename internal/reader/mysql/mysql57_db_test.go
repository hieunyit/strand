package mysql_test

// Test DB với mysql:5.7 thật (không có binlog_row_metadata): stream insert/update/delete và kiểm giá trị
// giải mã (unsigned BIGINT > MaxInt64, ENUM, SET, latin1, JSON, DATETIME(6)), rồi ALTER TABLE khi đang stream.

import (
	"context"
	"database/sql"
	"fmt"
	"math"
	"net"
	"reflect"
	"strconv"
	"testing"
	"time"

	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"

	"strand/internal/model"
	"strand/internal/reader"
	readermysql "strand/internal/reader/mysql"
)

func startMySQL57(t *testing.T) (*sql.DB, string, int, func()) {
	t.Helper()
	ctx := context.Background()
	req := testcontainers.ContainerRequest{
		Image:        "mysql:5.7",
		ExposedPorts: []string{"3306/tcp"},
		Env:          map[string]string{"MYSQL_ROOT_PASSWORD": "dev"},
		Cmd: []string{"--server-id=1", "--gtid-mode=ON", "--enforce-gtid-consistency=ON", "--log-bin=mysql-bin",
			"--binlog-format=ROW", "--binlog-row-image=FULL", "--innodb-use-native-aio=OFF"},
		WaitingFor: wait.ForLog("port: 3306  MySQL Community Server").WithStartupTimeout(180 * time.Second),
	}
	c, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{ContainerRequest: req, Started: true})
	if err != nil {
		t.Fatalf("start mysql:5.7: %v", err)
	}
	host, _ := c.Host(ctx)
	pn, err := c.MappedPort(ctx, "3306")
	if err != nil {
		c.Terminate(ctx)
		t.Fatal(err)
	}
	port, _ := strconv.Atoi(pn.Port())
	dsn := fmt.Sprintf("root:dev@tcp(%s)/?multiStatements=true&charset=utf8mb4", net.JoinHostPort(host, pn.Port()))
	var db *sql.DB
	for i := 0; i < 60; i++ {
		if db, err = sql.Open("mysql", dsn); err == nil {
			if err = db.PingContext(ctx); err == nil {
				break
			}
			db.Close()
		}
		time.Sleep(time.Second)
	}
	if err != nil {
		c.Terminate(ctx)
		t.Fatalf("connect: %v", err)
	}
	return db, host, port, func() { db.Close(); c.Terminate(ctx) }
}

func TestM57DB_StreamDecodesWithoutRowMetadata(t *testing.T) {
	db, host, port, cleanup := startMySQL57(t)
	defer cleanup()
	var ver string
	if err := db.QueryRow("SELECT @@version").Scan(&ver); err != nil || ver[:3] != "5.7" {
		t.Fatalf("version=%q err=%v", ver, err)
	}
	for _, q := range []string{
		"CREATE DATABASE m57",
		`CREATE TABLE m57.t (id BIGINT UNSIGNED NOT NULL PRIMARY KEY, u8 TINYINT UNSIGNED, u24 MEDIUMINT UNSIGNED, u32 INT UNSIGNED,
		   i32 INT, e ENUM('small','medium','large'), s SET('a','b','c','d'), l VARCHAR(20) CHARACTER SET latin1,
		   b BINARY(4), j JSON, dt DATETIME(6)) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4`,
	} {
		if _, err := db.Exec(q); err != nil {
			t.Fatalf("%s: %v", q, err)
		}
	}
	charsets, err := readermysql.LoadCollationCharsets(context.Background(), db)
	if err != nil {
		t.Fatal(err)
	}
	r, err := readermysql.New(readermysql.Config{Host: host, Port: uint16(port), User: "root", Password: "dev", ServerID: 3057,
		CollationCharsets: charsets, TableInfo: readermysql.NewInfoSchemaProvider(db)})
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	events, err := r.Start(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}

	next := func(want reader.EventType) reader.Event {
		for {
			select {
			case ev, ok := <-events:
				if !ok {
					t.Fatal("stream closed")
				}
				if ev.Type == reader.EventError {
					t.Fatalf("reader error: %v", ev.Err)
				}
				if ev.Type == want && (ev.Table == "t" || ev.Type != reader.EventRows && ev.Type != reader.EventTableMap) {
					return ev
				}
			case <-ctx.Done():
				t.Fatalf("timeout waiting for %s", want)
			}
		}
	}

	if _, err := db.Exec(`INSERT INTO m57.t VALUES (18446744073709551615, 255, 16777215, 4294967295, -5, 'large', 'a,c,d',
	   CONCAT('caf', CHAR(233 USING latin1)), 'ab', '{"k": [1, 2]}', '2024-05-06 07:08:09.123456')`); err != nil {
		t.Fatal(err)
	}
	tmEv := next(reader.EventTableMap)
	if !reflect.DeepEqual(tmEv.PKColumns, []string{"id"}) || len(tmEv.Columns) != 11 || tmEv.Columns[5] != "e" {
		t.Fatalf("table map columns=%v pk=%v", tmEv.Columns, tmEv.PKColumns)
	}
	ins := next(reader.EventRows)
	if ins.Op != model.OpInsert {
		t.Fatalf("op=%v", ins.Op)
	}
	row := ins.Rows[0]
	asStr := func(v any) string {
		switch x := v.(type) {
		case []byte:
			return string(x)
		case string:
			return x
		}
		return fmt.Sprint(v)
	}
	if row[0] != uint64(math.MaxUint64) || row[1] != uint8(255) || row[2] != uint32(16777215) || row[3] != uint32(math.MaxUint32) || row[4] != int32(-5) {
		t.Fatalf("ints=%#v", row[:5])
	}
	if row[5] != "large" || row[6] != "a,c,d" || asStr(row[7]) != "café" || asStr(row[8]) != "ab\x00\x00" ||
		asStr(row[9]) != `{"k":[1,2]}` && asStr(row[9]) != `{"k": [1, 2]}` || asStr(row[10]) != "2024-05-06 07:08:09.123456" {
		t.Fatalf("row=%#v", row)
	}

	if _, err := db.Exec("UPDATE m57.t SET e='small', s='', u8=0 WHERE id=18446744073709551615"); err != nil {
		t.Fatal(err)
	}
	upd := next(reader.EventRows)
	if len(upd.Rows) != 2 || upd.Rows[0][5] != "large" || upd.Rows[1][5] != "small" || upd.Rows[1][6] != "" || upd.Rows[1][0] != uint64(math.MaxUint64) {
		t.Fatalf("update rows=%#v", upd.Rows)
	}
	if _, err := db.Exec("DELETE FROM m57.t"); err != nil {
		t.Fatal(err)
	}
	if del := next(reader.EventRows); del.Op != model.OpDelete || del.Rows[0][0] != uint64(math.MaxUint64) {
		t.Fatalf("delete rows=%#v", del.Rows)
	}

	// ALTER TABLE khi đang stream: thêm cột + đổi INT thành UNSIGNED; reader phải nạp lại cấu trúc.
	if _, err := db.Exec("ALTER TABLE m57.t ADD COLUMN extra VARCHAR(5), MODIFY i32 INT UNSIGNED"); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec("INSERT INTO m57.t (id, i32, extra) VALUES (1, 4294967295, 'x')"); err != nil {
		t.Fatal(err)
	}
	tm2 := next(reader.EventTableMap)
	if len(tm2.Columns) != 12 || tm2.Columns[11] != "extra" {
		t.Fatalf("after ALTER columns=%v", tm2.Columns)
	}
	if rows := next(reader.EventRows).Rows[0]; rows[4] != uint32(math.MaxUint32) || asStr(rows[11]) != "x" {
		t.Fatalf("after ALTER row=%#v", rows)
	}
}
