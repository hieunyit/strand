package pipeline

import (
	"context"
	"time"

	"strand/internal/model"
)

// batchLimits là giới hạn group commit của worker (target.batch, 02 §6 / design §7):
// đóng batch khi đạt max_rows, max_bytes, 256 Txn, hoặc hết max_wait (0 = không chờ thêm).
type batchLimits struct {
	maxRows  int64
	maxBytes int64
	maxWait  time.Duration
	strict   bool
	// chain (relaxed, tùy chọn): lấy thêm Txn đang chờ trong DAG mà mọi phụ thuộc đã nằm trong batch
	// (sched.Scheduler.TakeChained) — chuỗi Txn của một row nóng được group commit thay vì mỗi Txn một
	// commit cộng max_wait.
	chain func(batch []*model.Txn, accept func(*model.Txn) bool) []*model.Txn
}

// txnBytes ước lượng kích thước Txn để áp max_bytes: dùng Txn.Bytes nếu có; Txn đã tràn ra đĩa
// coi như lớn (tự thành một batch); ngược lại cộng độ dài giá trị cột (chuỗi/byte) và 8 byte
// cho mỗi giá trị khác.
func txnBytes(t *model.Txn, maxBytes int64) int64 {
	if t.Bytes > 0 {
		return t.Bytes
	}
	if t.IsSpilled() {
		return maxBytes
	}
	var n int64
	for i := range t.Rows {
		for _, c := range t.Rows[i].Cols {
			switch v := c.Value.(type) {
			case []byte:
				n += int64(len(v))
			case string:
				n += int64(len(v))
			default:
				n += 8
			}
		}
	}
	return n
}

// gatherBatch gom thêm Txn đã sẵn sàng sau first. Ở strict, batch chỉ gồm seq liên tiếp; Txn
// không liền seq được trả về làm stash cho lượt sau (CP-5).
func gatherBatch(ctx context.Context, first *model.Txn, ready <-chan *model.Txn, lim batchLimits) (batch []*model.Txn, rows int64, stash *model.Txn) {
	batch = []*model.Txn{first}
	rows = first.TotalRows()
	size := txnBytes(first, lim.maxBytes)
	var timeout <-chan time.Time
	if lim.maxWait > 0 {
		timer := time.NewTimer(lim.maxWait)
		defer timer.Stop()
		timeout = timer.C
	}
	full := func() bool {
		return rows >= lim.maxRows || len(batch) >= 256 || (lim.maxBytes > 0 && size >= lim.maxBytes)
	}
	// takeChain gom chuỗi phụ thuộc của batch; true nếu lấy được ít nhất một Txn.
	takeChain := func() bool {
		if lim.chain == nil || lim.strict || full() {
			return false
		}
		got := lim.chain(batch, func(t *model.Txn) bool {
			if full() {
				return false
			}
			rows += t.TotalRows()
			size += txnBytes(t, lim.maxBytes)
			return true
		})
		batch = append(batch, got...)
		return len(got) > 0
	}
	for !full() {
		var t *model.Txn
		// Txn đã sẵn sàng hoặc chuỗi phụ thuộc của batch: lấy ngay, không chờ.
		select {
		case t = <-ready:
		default:
			if takeChain() {
				continue
			}
			if timeout == nil {
				return
			}
			select {
			case t = <-ready:
			case <-timeout:
				takeChain() // Txn tới trong lúc chờ có thể phụ thuộc batch
				return
			case <-ctx.Done():
				return
			}
		}
		if t == nil {
			return
		}
		if lim.strict && t.Seq() != batch[len(batch)-1].Seq()+1 {
			return batch, rows, t
		}
		batch = append(batch, t)
		rows += t.TotalRows()
		size += txnBytes(t, lim.maxBytes)
	}
	return
}
