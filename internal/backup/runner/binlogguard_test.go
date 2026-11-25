package runner

import (
	"io"
	"strings"
	"testing"
)

// mysqlbinlog --skip-gtids output (MySQL 8.4, trimmed): one file, a row event and statement events.
const binlogHead = "/*!50530 SET @@SESSION.PSEUDO_SLAVE_MODE=1*/;\n" +
	"/*!50003 SET @OLD_COMPLETION_TYPE=@@COMPLETION_TYPE,COMPLETION_TYPE=0*/;\n" +
	"DELIMITER /*!*/;\n" +
	"# at 4\n" +
	"#261125 20:34:37 server id 1  end_log_pos 127 CRC32 0x75e74486 \tStart: binlog v 4, server v 8.4.13 created 261125 20:34:37\n" +
	"BINLOG '\nXUYHaw8BAAAAewAAAH8AAAABAAQAOC40LjEzAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA\n'/*!*/;\n" +
	"/*!50616 SET @@SESSION.GTID_NEXT='AUTOMATIC'*//*!*/;\n"

const binlogTail = "DELIMITER ;\n# End of log file\n/*!50003 SET COMPLETION_TYPE=@OLD_COMPLETION_TYPE*/;\n"

func binlogQuery(at, db, stmt string) string {
	s := "# at " + at + "\n#261125 20:34:38 server id 1  end_log_pos 9 CRC32 0xbab6d5c4 \tQuery\tthread_id=9\texec_time=0\terror_code=0\tXid = 16\n"
	if db != "" {
		s += "use `" + db + "`/*!*/;\n"
	}
	return s + "SET TIMESTAMP=1795638878/*!*/;\n/*!80013 SET @@session.sql_require_primary_key=0*//*!*/;\n" + stmt + "\n/*!*/;\n"
}

const binlogRows = "# at 870\n#261125 20:34:37 server id 1  end_log_pos 945 CRC32 0x94a2722c \tQuery\tthread_id=9\texec_time=1\terror_code=0\n" +
	"SET TIMESTAMP=1795638877/*!*/;\nBEGIN\n/*!*/;\n" +
	"# at 945\n#261125 20:34:37 server id 1  end_log_pos 1005 CRC32 0x120ba9e2 \tTable_map: `shop`.`orders` mapped to number 85\n" +
	"# at 1005\n#261125 20:34:37 server id 1  end_log_pos 1054 CRC32 0x3a7863cb \tWrite_rows: table id 85 flags: STMT_END_F\n\n" +
	"BINLOG '\nXUYHaxMBAAAAPgAAAO0DAAAAAFUAAAAAAAEABnNob3BfcgAGb3JkZXJzAAIDDwIoAAIBAQACA/z/\nAOKpCxI=\n'/*!*/;\n" +
	"# at 1054\n#261125 20:34:37 server id 1  end_log_pos 1085 CRC32 0x4b3675a9 \tXid = 14\nCOMMIT/*!*/;\n"

func runGuard(t *testing.T, input, src, dst string, targetDBs []string, replay bool) (*binlogGuard, string, error) {
	t.Helper()
	g := newBinlogGuard(strings.NewReader(input), src, dst, binlogForbiddenDatabases(src, dst, targetDBs), []string{"binlog.000004"}, replay)
	out, err := io.ReadAll(g)
	return g, string(out), err
}

func TestBinlogGuardRefusesStatementsThatEscapeTheTarget(t *testing.T) {
	server := []string{"information_schema", "mysql", "performance_schema", "sys", "shop", "other"}
	for _, c := range []struct {
		name, body, want string
	}{
		{"qualified source table", binlogQuery("1373", "shop", "ALTER TABLE shop.orders ADD COLUMN c2 int"), "names database `shop` explicitly"},
		{"quoted, other case", binlogQuery("1373", "shop", "ALTER TABLE `SHOP` . `orders` ADD COLUMN c2 int"), "names database `SHOP` explicitly"},
		{"executable comment", binlogQuery("1373", "shop", "CREATE TABLE t2 /*!50100 LIKE shop.orders */"), "names database `shop`"},
		{"another database of the server", binlogQuery("1373", "shop", "DROP TABLE `other`.`x`"), "names database `other` explicitly"},
		{"database DDL of the source", binlogQuery("1373", "", "DROP DATABASE shop"), "DROP DATABASE `shop`"},
		{"database DDL of the source, stale use", binlogQuery("1373", "other", "CREATE DATABASE /*!32312 IF NOT EXISTS*/ `shop`"), "CREATE DATABASE `shop`"},
		{"drift: another default database", binlogQuery("1862", "other", "CREATE TABLE shop.drift (id int)"), "while its default database was `other`"},
		{"drift: no default database", binlogQuery("1862", "", "ALTER TABLE `shop`.orders ADD COLUMN x int"), "while its default database was (none)"},
		{"database DDL of the source with another default", binlogQuery("1862", "other", "ALTER DATABASE shop CHARACTER SET utf8mb4"), "ALTER DATABASE `shop`"},
		{"drift: database DDL in another letter case", binlogQuery("1862", "other", "ALTER DATABASE SHOP CHARACTER SET utf8mb4"), "another letter case"},
	} {
		t.Run(c.name, func(t *testing.T) {
			_, _, err := runGuard(t, binlogHead+binlogRows+c.body+binlogTail, "shop", "shop_restore", server, false)
			if err == nil || !strings.Contains(err.Error(), c.want) || !strings.Contains(err.Error(), "binlog.000004 position") {
				t.Fatalf("prescan: %v", err)
			}
		})
	}
	// The replay filter never forwards the offending statement.
	_, out, err := runGuard(t, binlogHead+binlogRows+binlogQuery("1373", "shop_restore", "ALTER TABLE shop.orders ADD COLUMN c2 int")+binlogTail,
		"shop", "shop_restore", server, true)
	if err == nil || strings.Contains(out, "ADD COLUMN c2") || !strings.Contains(out, "COMMIT/*!*/;") {
		t.Fatalf("replay forwarded the statement or failed open: %v\n%s", err, out)
	}
	if !strings.Contains(err.Error(), "separate server under the same database name") || !strings.Contains(err.Error(), "position 1373") {
		t.Fatalf("unclear error: %v", err)
	}
}

func TestBinlogGuardPassesStatementsConfinedToTheTarget(t *testing.T) {
	server := []string{"information_schema", "mysql", "performance_schema", "sys", "shop", "other"}
	body := binlogRows +
		binlogQuery("1162", "shop", "ALTER TABLE orders ADD COLUMN note varchar(10)") +
		binlogQuery("1200", "shop", "INSERT INTO orders (v) VALUES ('shop.orders'), (\"x\") -- shop.orders\n/* other.x */") +
		binlogQuery("1300", "shop", "CREATE DEFINER=`root`@`localhost` TRIGGER trg BEFORE INSERT ON orders FOR EACH ROW SET NEW.v = orders.v") +
		binlogQuery("1400", "shop", "CREATE VIEW v AS SELECT o.id FROM orders o JOIN information_schema.tables x ON 1=1") +
		binlogQuery("1500", "shop", "ALTER DATABASE CHARACTER SET utf8mb4")
	// Database DDL of another database is logged with that database as default: never replayed.
	others := binlogQuery("1550", "shop", "CREATE DATABASE scratch") +
		binlogQuery("1600", "other", "CREATE TABLE o (id int)") + binlogQuery("1700", "other", "DROP DATABASE scratch")
	// Statements of other default databases are not replayed (mysqlbinlog --database leaves them out of the
	// replay stream); they are fine when they do not touch the source.
	for _, replay := range []bool{false, true} {
		in := binlogHead + body + binlogTail
		if !replay {
			in = binlogHead + body + others + binlogTail
		}
		g, out, err := runGuard(t, in, "shop", "shop_restore", server, replay)
		if err != nil || out != in {
			t.Fatalf("replay=%v: %v\n%s", replay, err, out)
		}
		if replay && len(g.positions) == 0 {
			t.Fatal("replay recorded no statements")
		}
	}
	// Same name on another server: qualified names of the database itself are fine.
	same := binlogHead + binlogQuery("1373", "shop", "ALTER TABLE shop.orders ADD COLUMN c2 int") + binlogTail
	if _, _, err := runGuard(t, same, "shop", "shop", []string{"mysql", "sys"}, false); err != nil {
		t.Fatal(err)
	}
}

// A statement the prescan saw touching the source with the source as default database must be in the
// replay; if mysqlbinlog --database left it out (its event had no default database) the restore fails.
func TestBinlogGuardDetectsStatementsLeftOutOfTheReplay(t *testing.T) {
	in := binlogHead + binlogQuery("1373", "shop", "ALTER TABLE shop.orders ADD COLUMN c2 int") + binlogQuery("1500", "", "ALTER TABLE shop.orders ADD COLUMN c3 int") + binlogTail
	prescan, _, err := runGuard(t, in, "shop", "shop", nil, false)
	if err != nil {
		t.Fatal(err)
	}
	replayed := binlogHead + binlogQuery("1373", "shop", "ALTER TABLE shop.orders ADD COLUMN c2 int") + binlogTail
	replay, _, err := runGuard(t, replayed, "shop", "shop", nil, true)
	if err != nil {
		t.Fatal(err)
	}
	if missed := prescan.unreplayed(replay); len(missed) != 1 || !strings.Contains(missed[0], "position 1500") {
		t.Fatalf("missed %v", missed)
	}
}
