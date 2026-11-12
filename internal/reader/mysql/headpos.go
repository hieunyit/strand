package mysql

import (
	"context"
	"database/sql"
	"fmt"
	"strings"

	"strand/internal/checkpoint"
)

// MasterStatus đọc (file, position) hiện tại của source (SHOW MASTER STATUS; MySQL 8.2+ đổi tên
// BINARY LOG STATUS).
func MasterStatus(ctx context.Context, db *sql.DB) (file string, pos uint64, err error) {
	for _, q := range []string{"SHOW MASTER STATUS", "SHOW BINARY LOG STATUS"} {
		rows, qerr := db.QueryContext(ctx, q)
		if qerr != nil {
			err = qerr
			continue
		}
		cols, _ := rows.Columns()
		if rows.Next() {
			vals := make([]sql.RawBytes, len(cols))
			ptr := make([]any, len(cols))
			for i := range ptr {
				ptr[i] = &vals[i]
			}
			if serr := rows.Scan(ptr...); serr != nil {
				rows.Close()
				return "", 0, serr
			}
			rows.Close()
			if len(vals) < 2 {
				return "", 0, fmt.Errorf("%s: unexpected columns", q)
			}
			if _, serr := fmt.Sscan(string(vals[1]), &pos); serr != nil {
				return "", 0, fmt.Errorf("%s: bad position %q", q, vals[1])
			}
			return string(vals[0]), pos, nil
		}
		rows.Close()
		return "", 0, fmt.Errorf("%s returned no row: binary logging is off", q)
	}
	return "", 0, err
}

// nonTxnEventKinds: loại event của SHOW BINLOG EVENTS không thuộc Txn nào.
func nonTxnEvent(kind string) bool {
	switch strings.ToLower(kind) {
	case "rotate", "format_desc", "previous_gtids", "stop", "heartbeat":
		return true
	}
	return false
}

// TxnFreeToHead: không còn event của Txn nào trong binlog của source từ khóa from tới đầu binlog hiện
// tại (nghĩa là mọi Txn đã có trong binlog đều kết thúc ≤ from). from phải là ranh giới event.
// Dùng để biết "đã theo kịp" khi đầu binlog chỉ còn event không thuộc Txn (Rotate, Format_desc...).
func TxnFreeToHead(ctx context.Context, db *sql.DB, fileBase string, from uint64) (bool, error) {
	hf, hp, err := MasterStatus(ctx, db)
	if err != nil {
		return false, err
	}
	hbase, hseq, err := checkpoint.SplitBinlogName(hf)
	if err != nil {
		return false, err
	}
	if hbase != fileBase {
		return false, fmt.Errorf("source binlog base name is %q, expected %q", hbase, fileBase)
	}
	head, err := checkpoint.BinlogKey(hseq, hp)
	if err != nil {
		return false, err
	}
	if from >= head {
		return true, nil
	}
	seq, off := checkpoint.SplitBinlogKey(from)
	// Duyệt từ (seq, off) qua các file tới đầu: mọi event phải là non-Txn.
	for steps := 0; steps < 64; steps++ {
		file := checkpoint.FormatBinlogName(fileBase, seq)
		rows, err := db.QueryContext(ctx, fmt.Sprintf("SHOW BINLOG EVENTS IN '%s' FROM %d LIMIT 200", strings.ReplaceAll(file, "'", "''"), off))
		if err != nil {
			return false, err
		}
		cols, _ := rows.Columns()
		n := 0
		var last uint64
		var rotateTo string
		for rows.Next() {
			vals := make([]sql.RawBytes, len(cols))
			ptr := make([]any, len(cols))
			for i := range ptr {
				ptr[i] = &vals[i]
			}
			if err := rows.Scan(ptr...); err != nil {
				rows.Close()
				return false, err
			}
			// Log_name, Pos, Event_type, Server_id, End_log_pos, Info
			if len(vals) < 6 {
				rows.Close()
				return false, fmt.Errorf("SHOW BINLOG EVENTS: unexpected columns")
			}
			if !nonTxnEvent(string(vals[2])) {
				rows.Close()
				return false, nil
			}
			fmt.Sscan(string(vals[4]), &last)
			if strings.EqualFold(string(vals[2]), "rotate") {
				if name, _, ok := strings.Cut(string(vals[5]), ";"); ok {
					rotateTo = name
				}
			}
			n++
		}
		rows.Close()
		if err := rows.Err(); err != nil {
			return false, err
		}
		if n == 200 {
			off = last // còn nữa trong file này
			continue
		}
		if file == hf {
			return true, nil
		}
		nseq := seq + 1 // file không kết thúc bằng Rotate (server chết): file kế tiếp là seq+1
		if rotateTo != "" {
			_, ns, err := checkpoint.SplitBinlogName(rotateTo)
			if err != nil || ns <= seq {
				return false, nil
			}
			nseq = ns
		}
		seq, off = nseq, 4
	}
	return false, nil
}
