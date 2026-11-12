package schemagen

import (
	"strings"
	"testing"
)

// SHOW CREATE TABLE của MariaDB 10.6 (lấy nguyên từ server thật): MySQL 8.4 báo lỗi cú pháp 1064 với
// DEFAULT uuid()/rand()/curdate()/concat() và lỗi 1101 với TEXT/BLOB/JSON DEFAULT 'literal'.
func TestMariaDBDefaultsRewrittenForMySQL(t *testing.T) {
	in := "CREATE TABLE `d` (\n" +
		"  `id` int(11) NOT NULL,\n" +
		"  `a` int(11) DEFAULT 3,\n" +
		"  `b` int(11) DEFAULT (`a` + 1),\n" +
		"  `r` double DEFAULT rand(),\n" +
		"  `s` varchar(20) DEFAULT concat('x','y'),\n" +
		"  `t` text DEFAULT 'hello',\n" +
		"  `bl` blob DEFAULT 'zz',\n" +
		"  `j` longtext CHARACTER SET utf8mb4 COLLATE utf8mb4_bin DEFAULT '{}' CHECK (json_valid(`j`)),\n" +
		"  `n` int(11) DEFAULT -1,\n" +
		"  `bt` bit(1) DEFAULT b'1',\n" +
		"  `dt` date DEFAULT curdate(),\n" +
		"  `e` enum('a','b') DEFAULT 'b',\n" +
		"  `ts6` datetime(6) DEFAULT current_timestamp(6),\n" +
		"  `upd` timestamp NOT NULL DEFAULT current_timestamp() ON UPDATE current_timestamp(),\n" +
		"  `q` varchar(10) DEFAULT 'it''s',\n" +
		"  `nl` int(11) DEFAULT NULL,\n" +
		"  `ud` varchar(30) DEFAULT current_user(),\n" +
		"  `tok` varchar(36) DEFAULT uuid(),\n" +
		"  PRIMARY KEY (`id`)\n" +
		") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4"
	want := "CREATE TABLE `d` (\n" +
		"  `id` int(11) NOT NULL,\n" +
		"  `a` int(11) DEFAULT 3,\n" +
		"  `b` int(11) DEFAULT (`a` + 1),\n" +
		"  `r` double DEFAULT (rand()),\n" +
		"  `s` varchar(20) DEFAULT (concat('x','y')),\n" +
		"  `t` text DEFAULT ('hello'),\n" +
		"  `bl` blob DEFAULT ('zz'),\n" +
		"  `j` longtext CHARACTER SET utf8mb4 COLLATE utf8mb4_bin DEFAULT ('{}') CHECK (json_valid(`j`)),\n" +
		"  `n` int(11) DEFAULT -1,\n" +
		"  `bt` bit(1) DEFAULT b'1',\n" +
		"  `dt` date DEFAULT (curdate()),\n" +
		"  `e` enum('a','b') DEFAULT 'b',\n" +
		"  `ts6` datetime(6) DEFAULT current_timestamp(6),\n" +
		"  `upd` timestamp NOT NULL DEFAULT current_timestamp() ON UPDATE current_timestamp(),\n" +
		"  `q` varchar(10) DEFAULT 'it''s',\n" +
		"  `nl` int(11) DEFAULT NULL,\n" +
		"  `ud` varchar(30) DEFAULT (current_user()),\n" +
		"  `tok` varchar(36) DEFAULT (uuid()),\n" +
		"  PRIMARY KEY (`id`)\n" +
		") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4"
	got, err := mariaDBDefaults(in)
	if err != nil {
		t.Fatal(err)
	}
	if got != want {
		t.Fatalf("got:\n%s\nwant:\n%s", got, want)
	}
}

func TestMariaDBTypesRewrite(t *testing.T) {
	in := "CREATE TABLE `t` (\n  `id` int(11) NOT NULL,\n  `ip` inet6 DEFAULT NULL,\n  `a` inet4 NOT NULL,\n  `u` uuid DEFAULT NULL,\n  `inet6` int DEFAULT NULL COMMENT 'uuid inet4',\n  PRIMARY KEY (`id`)\n) ENGINE=InnoDB"
	got, err := mariaDBTypes(in)
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{"`ip` varchar(39) CHARACTER SET ascii COLLATE ascii_bin DEFAULT NULL", "`a` varchar(15) CHARACTER SET ascii COLLATE ascii_bin NOT NULL",
		"`u` char(36) CHARACTER SET ascii COLLATE ascii_bin DEFAULT NULL", "`inet6` int DEFAULT NULL COMMENT 'uuid inet4'"} {
		if !strings.Contains(got, want) {
			t.Errorf("missing %q in:\n%s", want, got)
		}
	}
}

func TestMariaDBVersioningStripped(t *testing.T) {
	in := "CREATE TABLE `t` (\n  `id` int(11) NOT NULL,\n  `c` int(11) WITHOUT SYSTEM VERSIONING DEFAULT NULL,\n  PRIMARY KEY (`id`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 WITH SYSTEM VERSIONING\n PARTITION BY SYSTEM_TIME INTERVAL 1 MONTH STARTS TIMESTAMP'2026-11-12 00:00:00'\n(PARTITION `p0` HISTORY ENGINE = InnoDB,\n PARTITION `pn` CURRENT ENGINE = InnoDB)"
	got, err := mariaDBVersioning(in)
	if err != nil {
		t.Fatal(err)
	}
	for _, bad := range []string{"VERSIONING", "SYSTEM_TIME", "PARTITION"} {
		if strings.Contains(got, bad) {
			t.Errorf("still contains %q:\n%s", bad, got)
		}
	}
	if !strings.Contains(got, "ENGINE=InnoDB DEFAULT CHARSET=utf8mb4") || !strings.Contains(got, "`c` int(11)  DEFAULT NULL") {
		t.Errorf("unexpected:\n%s", got)
	}
}

func TestMariaDBHasPeriod(t *testing.T) {
	for ddl, want := range map[string]bool{
		"CREATE TABLE t (id int, s date, e date, PERIOD FOR p(s, e)) ENGINE=InnoDB":  true,
		"CREATE TABLE t (id int, `period` int COMMENT 'PERIOD FOR x') ENGINE=InnoDB": false,
		"CREATE TABLE t (id int) ENGINE=InnoDB COMMENT='PERIOD FOR'":                 false,
	} {
		if got, err := mariaDBHasPeriod(ddl); err != nil || got != want {
			t.Errorf("%s: got %v %v want %v", ddl, got, err, want)
		}
	}
}
