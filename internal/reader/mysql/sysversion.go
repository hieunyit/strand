package mysql

import (
	"fmt"

	"strand/internal/model"
)

// Bảng MariaDB WITH SYSTEM VERSIONING (row_start/row_end ẩn): binlog ghi thêm hai cột TIMESTAMP(6) ở CUỐI row
// (kiểm trên binlog thật, kể cả sau ALTER ADD COLUMN FIRST/AFTER). Hàng hiện hành có row_end = mốc tối đa.
//   - INSERT hiện hành: row mới;
//   - UPDATE: UPDATE (hiện hành → hiện hành, row_start mới) rồi INSERT bản lịch sử (row_end = lúc sửa);
//   - DELETE: UPDATE đặt row_end của hàng hiện hành (không có DELETE event);
//   - DELETE HISTORY / purge: DELETE các hàng lịch sử.
//
// Đích là bảng thường chỉ giữ hàng hiện hành: bỏ mọi thay đổi trên hàng lịch sử, đổi UPDATE "đóng" hàng thành
// DELETE, và cắt hai cột ẩn.
const (
	sysVerHidden    = 2
	sysVerRowEndMax = "2038-01-19 03:14:07.999999" // TIMESTAMP_MAX_VALUE.999999, giờ UTC (TimestampStringLocation=UTC)
)

type rowSeg struct {
	op   model.Op
	rows [][]any
}

func sysVerCurrent(row []any, n int) (bool, error) {
	if len(row) != n+sysVerHidden {
		return false, fmt.Errorf("system-versioned row has %d columns, want %d", len(row), n+sysVerHidden)
	}
	switch v := row[n+1].(type) {
	case string:
		return v == sysVerRowEndMax, nil
	case []byte:
		return string(v) == sysVerRowEndMax, nil
	}
	return false, fmt.Errorf("system-versioned row_end has unexpected binlog value %T", row[n+1])
}

// splitSystemVersioned đổi các row của một ROWS event (n = số cột người dùng) thành các đoạn row-image đã cắt
// cột ẩn, theo thứ tự, chỉ gồm thay đổi trên hàng hiện hành.
func splitSystemVersioned(op model.Op, rows [][]any, n int) ([]rowSeg, error) {
	var segs []rowSeg
	add := func(o model.Op, rs ...[]any) {
		if len(segs) > 0 && segs[len(segs)-1].op == o {
			segs[len(segs)-1].rows = append(segs[len(segs)-1].rows, rs...)
			return
		}
		segs = append(segs, rowSeg{op: o, rows: rs})
	}
	switch op {
	case model.OpInsert, model.OpDelete:
		for _, r := range rows {
			cur, err := sysVerCurrent(r, n)
			if err != nil {
				return nil, err
			}
			if cur {
				add(op, r[:n:n])
			}
		}
	case model.OpUpdate:
		if len(rows)%2 != 0 {
			return nil, fmt.Errorf("system-versioned UPDATE has an odd number of row images (%d)", len(rows))
		}
		for i := 0; i < len(rows); i += 2 {
			b, a := rows[i], rows[i+1]
			bc, err := sysVerCurrent(b, n)
			if err != nil {
				return nil, err
			}
			ac, err := sysVerCurrent(a, n)
			if err != nil {
				return nil, err
			}
			switch {
			case bc && ac:
				add(model.OpUpdate, b[:n:n], a[:n:n])
			case bc: // hàng hiện hành bị đóng = DELETE ở bảng thường
				add(model.OpDelete, b[:n:n])
			case ac: // hàng lịch sử thành hiện hành (chỉ khi người dùng chủ động sửa lịch sử)
				add(model.OpInsert, a[:n:n])
			}
		}
	}
	return segs, nil
}
