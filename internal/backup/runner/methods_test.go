package runner

import (
	"strings"
	"testing"

	"strand/internal/backup"
)

func TestParseDumpHeadersAndVersions(t *testing.T) {
	head := []byte("-- MySQL dump 10.13\n--\n-- Position to start replication or point-in-time recovery from\n--\n\n" +
		"-- CHANGE REPLICATION SOURCE TO SOURCE_LOG_FILE='binlog.000003', SOURCE_LOG_POS=1577;\n\n" +
		"SET @@GLOBAL.GTID_PURGED=/*!80000 '+'*/ '3E11FA47-71CA-11E1-9E33-C80AA9429562:1-77,\n" +
		"5C11FA47-71CA-11E1-9E33-C80AA9429562:1-3';\n")
	p := ParseMySQLDumpHeader(head)
	if p.BinlogFile != "binlog.000003" || p.BinlogPos != 1577 ||
		p.GTIDSet != "3E11FA47-71CA-11E1-9E33-C80AA9429562:1-77,5C11FA47-71CA-11E1-9E33-C80AA9429562:1-3" {
		t.Fatalf("%+v", p)
	}
	old := ParseMySQLDumpHeader([]byte("-- CHANGE MASTER TO MASTER_LOG_FILE='mysql-bin.000002', MASTER_LOG_POS=154;\nSET @@GLOBAL.GTID_PURGED='u:1-5';\n"))
	if old.BinlogFile != "mysql-bin.000002" || old.BinlogPos != 154 || old.GTIDSet != "u:1-5" {
		t.Fatalf("%+v", old)
	}
	maria := ParseMySQLDumpHeader([]byte("-- CHANGE MASTER TO MASTER_LOG_FILE='m.000001', MASTER_LOG_POS=328;\n-- SET GLOBAL gtid_slave_pos='0-1-5';\n"))
	if maria.GTIDSet != "0-1-5" {
		t.Fatalf("%+v", maria)
	}
	for in, want := range map[string]MySQLDumpFlavor{
		"mysqldump  Ver 8.4.3 for Linux on x86_64 (MySQL Community Server - GPL)": {Major: 8, Minor: 4, Patch: 3},
		"mysqldump  Ver 10.19 Distrib 10.11.6-MariaDB, for debian-linux-gnu":      {MariaDB: true, Major: 10, Minor: 11, Patch: 6},
		"mariadb-dump from 11.4.2-MariaDB, client 10.19":                          {MariaDB: true, Major: 11, Minor: 4, Patch: 2},
	} {
		if got := ParseDumpVersion(in); got != want {
			t.Fatalf("%q => %+v", in, got)
		}
	}
	get := func(k string) string { return map[string]string{"PW": "s3cret"}[k] }
	c := backup.ConnSpec{Host: "db", Port: 3306, User: "bk", PasswordEnv: "PW", Databases: []string{"shop"}}
	inv := MySQLDumpInvocation(get, "mysqldump", c, MySQLDumpFlavor{Major: 8, Minor: 0, Patch: 25})
	joined := strings.Join(inv.Args, " ")
	if !strings.Contains(joined, "--master-data=2") || strings.Contains(joined, "s3cret") || inv.Env[0] != "MYSQL_PWD=s3cret" ||
		!strings.HasSuffix(joined, "-- shop") {
		t.Fatalf("argv %v env %v", inv.Args, inv.Env)
	}
	if j := strings.Join(MySQLDumpInvocation(get, "mysqldump", c, MySQLDumpFlavor{Major: 8, Minor: 4}).Args, " "); !strings.Contains(j, "--source-data=2") {
		t.Fatal(j)
	}
	if j := strings.Join(MySQLDumpInvocation(get, "mariadb-dump", c, MySQLDumpFlavor{MariaDB: true}).Args, " "); !strings.Contains(j, "--gtid") {
		t.Fatal(j)
	}
}

func TestPhysicalParsers(t *testing.T) {
	s, e, tl := ParsePGBaseBackupLog("pg_basebackup: initiating base backup, waiting for checkpoint to complete\n" +
		"pg_basebackup: write-ahead log start point: 0/2000028 on timeline 1\npg_basebackup: write-ahead log end point: 0/2000100\n")
	if s.LSN != "0/2000028" || e.LSN != "0/2000100" || tl != "1" {
		t.Fatal(s, e, tl)
	}
	m := ParseMydumperMetadata("Started dump at: 2026-11-20 10:00:00\nSHOW MASTER STATUS:\n\tLog: binlog.000004\n\tPos: 777\n\tGTID:u:1-12\n\nFinished\n")
	if m.BinlogFile != "binlog.000004" || m.BinlogPos != 777 || m.GTIDSet != "u:1-12" {
		t.Fatal(m)
	}
	get := func(k string) string { return map[string]string{"PW": "pa:ss"}[k] }
	uri, _ := MongoURI(get, backup.ConnSpec{Host: "m1", Port: 27017, User: "u@x", PasswordEnv: "PW"})
	if uri != "mongodb://u%40x:pa%3Ass@m1:27017/?directConnection=true" {
		t.Fatal(uri)
	}
	mi, _ := MongodumpInvocation(uri, backup.ConnSpec{})
	if strings.Contains(strings.Join(mi.Args, " "), "pa%3Ass") || !strings.Contains(strings.Join(mi.Args, " "), "--oplog") {
		t.Fatal(mi.Args)
	}
	if secrets := secretsOf(mi); !strings.Contains(strings.Join(secrets, "\n"), uri) {
		t.Fatalf("secrets %v", secrets)
	}
}

func TestGTIDPurgedFilter(t *testing.T) {
	in := "SET @@SESSION.SQL_LOG_BIN= 0;\nSET @@GLOBAL.GTID_PURGED=/*!80000 '+'*/ 'a:1-5,\nb:1-3';\nINSERT INTO t VALUES (1);\n"
	out := string(StripGTIDPurged([]byte(in)))
	if !strings.Contains(out, "-- [strand] SET @@GLOBAL.GTID_PURGED") || !strings.Contains(out, "-- b:1-3';") || !strings.Contains(out, "\nINSERT INTO t VALUES (1);") {
		t.Fatal(out)
	}
}
