package mysql

import (
	"fmt"
	"strings"

	"github.com/go-mysql-org/go-mysql/mysql"
	"strand/internal/checkpoint"
	"strand/internal/exitcode"
)

// ToMysqlGTIDSet chuyển đổi checkpoint.AppliedSet thành mysql.GTIDSet của go-mysql để truyền vào COM_BINLOG_DUMP_GTID.
// spec: 02 §3, D §1
func ToMysqlGTIDSet(applied checkpoint.AppliedSet) (mysql.GTIDSet, error) {
	if applied == nil {
		return mysql.ParseMysqlGTIDSet("")
	}

	mySet, ok := applied.(*checkpoint.MySQLAppliedSet)
	if !ok {
		return nil, exitcode.New(
			exitcode.ErrBinlogConfigInvalid,
			exitcode.Rejected,
			"expected *checkpoint.MySQLAppliedSet",
			nil,
		)
	}

	gtidStr := mySet.String()
	if strings.TrimSpace(gtidStr) == "" {
		return mysql.ParseMysqlGTIDSet("")
	}

	gset, err := mysql.ParseMysqlGTIDSet(gtidStr)
	if err != nil {
		return nil, exitcode.New(
			exitcode.ErrBinlogConfigInvalid,
			exitcode.Rejected,
			fmt.Sprintf("failed to parse GTID set %q: %v", gtidStr, err),
			err,
		)
	}

	return gset, nil
}
