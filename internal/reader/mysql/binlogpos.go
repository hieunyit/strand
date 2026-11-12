package mysql

import (
	"fmt"

	"strand/internal/checkpoint"
	"strand/internal/exitcode"
)

// Chế độ vị trí binlog của reader (source không GTID). Reader theo dõi file hiện tại qua ROTATE
// (kể cả rotate giả lúc kết nối) và gắn mỗi event Pos = (file, end_log_pos). LogPos trong header
// event của go-mysql là vị trí KẾT THÚC của event (= đầu event kế tiếp), nên Txn kết thúc tại LogPos
// của event XID/COMMIT, và bắt đầu tại LogPos − EventSize của event đầu tiên.

// initPosMode khởi tạo trạng thái cho luồng đọc từ file startFile với tên gốc fileBase.
func (st *streamState) initPosMode(fileBase, startFile string) error {
	base, seq, err := checkpoint.SplitBinlogName(startFile)
	if err != nil {
		return exitcode.New(exitcode.ErrBinlogConfigInvalid, exitcode.Rejected, err.Error(), err)
	}
	if base != fileBase {
		return exitcode.New(exitcode.ErrBinlogConfigInvalid, exitcode.Rejected,
			fmt.Sprintf("resume file %q does not have the checkpoint's binlog base name %q", startFile, fileBase), nil)
	}
	st.posMode, st.fileBase, st.curFile, st.curSeq = true, fileBase, startFile, seq
	return nil
}

// rotate xử lý ROTATE tới file next. Số thứ tự file phải KHÔNG GIẢM và tên gốc không đổi:
//   - giảm (RESET MASTER / đổi index): khóa vị trí sẽ lùi, mọi suy luận "≤ base đã apply" sai (CP-1/2);
//   - đổi tên gốc (log-bin basename đổi): không so sánh được với tập đã apply.
//
// Bằng nhau hợp lệ (rotate giả lúc kết nối chỉ lặp lại file đang đọc). Cả hai trường hợp lỗi đều fail
// closed: cần seed lại.
func (st *streamState) rotate(next string) error {
	base, seq, err := checkpoint.SplitBinlogName(next)
	if err != nil {
		return exitcode.New(exitcode.ErrBinlogConfigInvalid, exitcode.Rejected, err.Error(), err)
	}
	if base != st.fileBase {
		return exitcode.New(exitcode.ErrBinlogConfigInvalid, exitcode.Rejected, fmt.Sprintf(
			"binlog rotated to %q: base name %q differs from the checkpoint's %q (log-bin basename changed); re-seed the pipeline", next, base, st.fileBase), nil)
	}
	if seq < st.curSeq {
		return exitcode.New(exitcode.ErrBinlogConfigInvalid, exitcode.Rejected, fmt.Sprintf(
			"binlog rotated backwards from %s to %q (RESET MASTER or a replaced binlog index?): positions are no longer comparable with the checkpoint; re-seed the pipeline",
			checkpoint.FormatBinlogName(st.fileBase, st.curSeq), next), nil)
	}
	st.curFile, st.curSeq = next, seq
	return nil
}
