package schemagen

import (
	"strings"
	"testing"
)

// SHOW CREATE TABLE của 5.7 bỏ COLLATE khi là mặc định của charset; target 8.x sẽ dùng utf8mb4_0900_ai_ci.
func TestPinUTF8MB4Collation57(t *testing.T) {
	cases := []struct{ name, in, want string }{
		{
			"table charset only",
			"CREATE TABLE `t` (\n  `id` int(11) NOT NULL,\n  `s` varchar(10) DEFAULT NULL,\n  PRIMARY KEY (`id`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4",
			"CREATE TABLE `t` (\n  `id` int(11) NOT NULL,\n  `s` varchar(10) DEFAULT NULL,\n  PRIMARY KEY (`id`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci",
		},
		{
			"table has collate already",
			"CREATE TABLE `t` (\n  `id` int(11) NOT NULL\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin",
			"CREATE TABLE `t` (\n  `id` int(11) NOT NULL\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin",
		},
		{
			"column charset in latin1 table",
			"CREATE TABLE `t` (\n  `a` varchar(10) CHARACTER SET utf8mb4 DEFAULT NULL,\n  `b` varchar(10) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin DEFAULT NULL\n) ENGINE=InnoDB DEFAULT CHARSET=latin1",
			"CREATE TABLE `t` (\n  `a` varchar(10) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci DEFAULT NULL,\n  `b` varchar(10) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin DEFAULT NULL\n) ENGINE=InnoDB DEFAULT CHARSET=latin1",
		},
		{
			"utf8 (utf8mb3) and latin1 untouched",
			"CREATE TABLE `t` (\n  `a` varchar(10) DEFAULT NULL\n) ENGINE=InnoDB DEFAULT CHARSET=utf8",
			"CREATE TABLE `t` (\n  `a` varchar(10) DEFAULT NULL\n) ENGINE=InnoDB DEFAULT CHARSET=utf8",
		},
		{
			"literal containing charset text is not touched",
			"CREATE TABLE `t` (\n  `a` varchar(30) DEFAULT 'CHARSET=utf8mb4' COMMENT 'CHARACTER SET utf8mb4'\n) ENGINE=InnoDB DEFAULT CHARSET=latin1",
			"CREATE TABLE `t` (\n  `a` varchar(30) DEFAULT 'CHARSET=utf8mb4' COMMENT 'CHARACTER SET utf8mb4'\n) ENGINE=InnoDB DEFAULT CHARSET=latin1",
		},
	}
	for _, c := range cases {
		got, err := pinUTF8MB4Collation57(c.in)
		if err != nil {
			t.Fatalf("%s: %v", c.name, err)
		}
		if got != c.want {
			t.Errorf("%s:\n got %s\nwant %s", c.name, got, c.want)
		}
		if !strings.HasPrefix(got, "CREATE TABLE") {
			t.Errorf("%s: prefix lost", c.name)
		}
	}
}
