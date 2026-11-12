package schemagen

import "testing"

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
