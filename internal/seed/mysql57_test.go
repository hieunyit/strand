package seed_test

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"strand/internal/exitcode"
	"strand/internal/seed"
)

const (
	u57a = "3E11FA47-71CA-11E1-9E33-C80AA9429562"
	u57b = "4E22FB58-82DB-22F2-AF44-D91BB0530673"
)

// XtraBackup 2.4 (nguồn 5.7): xtrabackup_binlog_info có GTID set nhiều UUID, ngăn bằng ",\n"; xtrabackup_info
// ghi server_version = 5.7.x-log. Backup vật lý 5.7 chỉ restore được lên target 5.7 (SD-4), không lên 8.x.
func TestMySQL57_XtraBackup24_Metadata(t *testing.T) {
	gtid := u57a + ":1-100,\n" + u57b + ":1-20"
	dir := createMockXtraBackup(t, map[string]string{
		"xtrabackup_checkpoints": "backup_type = full-prepared\nfrom_lsn = 0\nto_lsn = 1\nlast_lsn = 1\n",
		"xtrabackup_binlog_info": "mysql-bin.000004\t7788\t" + gtid + "\n",
		"xtrabackup_info":        "uuid = abc\nname =\ntool_name = xtrabackup\ntool_command = --backup\ntool_version = 2.4.29\nibbackup_version = 2.4.29\nserver_version = 5.7.44-log\nstart_time = 2026-11-12 10:00:00\n",
	})
	p := seed.NewXtraBackupPlanner()
	m, err := p.Plan(context.Background(), seed.SeedSource{
		Engine: "mysql", Kind: "xtrabackup", BackupRef: dir,
		TargetVersion: "5.7.44-log", SourceArch: "x86_64", TargetArch: "x86_64",
	})
	if err != nil {
		t.Fatalf("5.7 → 5.7 plan: %v", err)
	}
	if m.ToolVersion != "2.4.29" || m.ToolPrerelease {
		t.Errorf("tool version %q prerelease=%v", m.ToolVersion, m.ToolPrerelease)
	}
	if !strings.Contains(m.StartPosition.GTIDSet, u57a) || !strings.Contains(m.StartPosition.GTIDSet, u57b) {
		t.Errorf("P = %q", m.StartPosition.GTIDSet)
	}

	_, err = p.Plan(context.Background(), seed.SeedSource{
		Engine: "mysql", Kind: "xtrabackup", BackupRef: dir,
		TargetVersion: "8.4.0", SourceArch: "x86_64", TargetArch: "x86_64",
	})
	if err == nil || !strings.Contains(err.Error(), "SD-4") || !strings.Contains(err.Error(), "logical seed") {
		t.Fatalf("5.7 → 8.4 physical must be rejected with a logical-seed hint, got %v", err)
	}
	if exitcode.GetExitCode(err) != exitcode.Rejected {
		t.Errorf("exit code %d", exitcode.GetExitCode(err))
	}
}

// mysqldump 5.7: GTID_PURGED không có tiền tố /*!80000 '+'*/, CHANGE MASTER TO (không comment) và dòng
// "Distrib 5.7.x".
func TestMySQL57_Mysqldump_Format(t *testing.T) {
	dump := "-- MySQL dump 10.13  Distrib 5.7.44, for Linux (x86_64)\n--\n" +
		"SET @MYSQLDUMP_TEMP_LOG_BIN = @@SESSION.SQL_LOG_BIN;\nSET @@SESSION.SQL_LOG_BIN= 0;\n--\n-- GTID state at the beginning of the backup \n--\n\n" +
		"SET @@GLOBAL.GTID_PURGED='" + u57a + ":1-100,\n" + u57b + ":1-20';\n\n" +
		"--\n-- Position to start replication or point-in-time recovery from\n--\n\n" +
		"CHANGE MASTER TO MASTER_LOG_FILE='mysql-bin.000004', MASTER_LOG_POS=7788;\n" +
		"CREATE TABLE `t` (`id` int NOT NULL, PRIMARY KEY (`id`)) ENGINE=InnoDB DEFAULT CHARSET=latin1;\n-- Dump completed on 2026-11-12 10:00:05\n"
	dir := t.TempDir()
	path := filepath.Join(dir, "dump.sql")
	if err := os.WriteFile(path, []byte(dump), 0o644); err != nil {
		t.Fatal(err)
	}
	m, err := seed.NewMysqldumpPlanner().Plan(context.Background(), seed.SeedSource{
		Engine: "mysql", Kind: "mysqldump", BackupRef: path,
		TargetRestoredGTID: u57a + ":1-100," + u57b + ":1-20",
	})
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(m.StartPosition.GTIDSet, u57a+":1-100") || !strings.Contains(m.StartPosition.GTIDSet, u57b+":1-20") {
		t.Errorf("P = %q", m.StartPosition.GTIDSet)
	}
}

// mydumper trên nguồn 5.7 ghi metadata dạng cũ: "SHOW MASTER STATUS:" với Log/Pos/GTID.
func TestMySQL57_Mydumper_OldMetadata(t *testing.T) {
	meta := "Started dump at: 2026-11-12 10:00:00\nSHOW MASTER STATUS:\n\tLog: mysql-bin.000004\n\tPos: 7788\n\tGTID:" +
		u57a + ":1-100,\n" + u57b + ":1-20\n\nFinished dump at: 2026-11-12 10:00:05\n"
	dir := createMockMydumperBackup(t, meta)
	m, err := seed.NewMydumperPlanner().Plan(context.Background(), seed.SeedSource{
		Engine: "mysql", Kind: "mydumper", BackupRef: dir, AcceptUnverifiedLockMode: true,
		TargetRestoredGTID: u57a + ":1-100," + u57b + ":1-20",
	})
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(m.StartPosition.GTIDSet, u57a+":1-100") || !strings.Contains(m.StartPosition.GTIDSet, u57b+":1-20") {
		t.Errorf("P = %q", m.StartPosition.GTIDSet)
	}
}

// Clone plugin cần donor 8.0.17+: nguồn 5.7 bị từ chối trước mọi kiểm tra khác.
func TestMySQL57_Clone_Rejected(t *testing.T) {
	src := seed.SeedSource{Engine: "mysql", Kind: "clone", TargetRestoredGTID: u57a + ":1-100", TargetVersion: "8.4.0"}
	for _, v := range []string{"5.5.62", "5.6.51-log", "5.7.44-log", "8.0.16"} {
		src.SourceMySQLVersion = v
		_, err := seed.NewClonePlanner().Plan(context.Background(), src)
		if err == nil || !strings.Contains(err.Error(), "8.0.17") || exitcode.GetExitCode(err) != exitcode.Rejected {
			t.Errorf("%s: want clone rejection, got %v", v, err)
		}
	}
	src.SourceMySQLVersion = "5.1.73"
	if _, err := seed.NewClonePlanner().Plan(context.Background(), src); err == nil || !strings.Contains(err.Error(), "5.5 or later is required") {
		t.Errorf("5.1: %v", err)
	}
	src.SourceMySQLVersion = "8.4.0"
	if _, err := seed.NewClonePlanner().Plan(context.Background(), src); err != nil {
		t.Errorf("8.4 donor must pass: %v", err)
	}
}
