package runner

import "testing"

func TestNeedsMySQLDump80(t *testing.T) {
	d84 := ParseDumpVersion("mysqldump  Ver 8.4.13 for Linux on x86_64 (MySQL Community Server - GPL)")
	d97 := ParseDumpVersion("mysqldump  Ver 9.7.4 for Linux on x86_64 (MySQL Community Server - GPL)")
	d80 := ParseDumpVersion("mysqldump  Ver 8.0.46 for Linux on x86_64 (MySQL Community Server - GPL)")
	maria := ParseDumpVersion("mariadb-dump from 12.3.4-MariaDB, client 10.20 for debian-linux-gnu (x86_64)")
	for _, c := range []struct {
		server string
		tool   MySQLDumpFlavor
		want   bool
	}{
		{"8.0.46", d84, true},
		{"8.0.46-log", d97, true},
		{"8.1.0", d84, true},
		{"8.0.46", d80, false},     // already an 8.0 client
		{"5.7.44-log", d84, false}, // 8.4 client sends SHOW MASTER STATUS to 5.7 (verified live)
		{"8.4.7", d84, false},
		{"9.7.0", d84, false},
		{"10.11.10-MariaDB-log", d84, false},
		{"8.0.46", maria, false},
		{"", d84, false}, // server not reachable: run the default tool, which reports the error
	} {
		if got := needsMySQLDump80(c.server, c.tool); got != c.want {
			t.Errorf("needsMySQLDump80(%q, %+v) = %v, want %v", c.server, c.tool, got, c.want)
		}
	}
}
