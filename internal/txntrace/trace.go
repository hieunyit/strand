// Package txntrace: vết theo Txn (không theo row) cho điều tra F11 — chỉ nhánh debug. Bật bằng
// STRAND_TXN_TRACE=<file>; ghi vào mảng cấp sẵn, không định dạng, không cấp phát; dump khi pipeline dừng.
package txntrace

import (
	"bufio"
	"fmt"
	"os"
	"sync/atomic"
	"time"
)

// Kind của một bản ghi.
const (
	Routed  = 1 // router xong: Seq (0 = không Admit), A = số row vào, B = số row giữ lại
	Skipped = 2 // router bỏ cả Txn (applied0 / rỗng): A = lý do
	Done    = 3 // worker báo Done: A = worker, B = batch id
	Applied = 4 // ApplyBatch trả nil: A = worker, B = batch id, Seq = số Txn trong batch
)

type rec struct {
	ready    atomic.Bool
	ts       int64
	kind     uint8
	lsn, xid uint64
	seq      uint64
	a, b     int64
}

var (
	path  = os.Getenv("STRAND_TXN_TRACE")
	On    = path != ""
	recs  []rec
	n     atomic.Int64
	batch atomic.Int64
)

func init() {
	if On {
		recs = make([]rec, 3_000_000)
	}
}

// Put ghi một bản ghi (lock-free).
func Put(kind uint8, lsn, xid, seq uint64, a, b int64) {
	if !On {
		return
	}
	i := n.Add(1) - 1
	if int(i) >= len(recs) {
		return
	}
	r := &recs[i]
	r.ts, r.kind, r.lsn, r.xid, r.seq, r.a, r.b = time.Now().UnixNano(), kind, lsn, xid, seq, a, b
	// Publish sau khi ghi đủ trường; Dump không đọc slot chưa sẵn sàng (không đổi CP/RS/CO).
	r.ready.Store(true)
}

// NextBatch trả id batch mới.
func NextBatch() int64 { return batch.Add(1) }

// Dump ghi vết ra file STRAND_TXN_TRACE.
func Dump() (err error) {
	if !On {
		return nil
	}
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	defer func() {
		if e := f.Close(); err == nil {
			err = e
		}
	}()
	w := bufio.NewWriterSize(f, 64*1024)
	allocated := n.Load()
	m := min(allocated, int64(len(recs)))
	var stored, unpublished int64
	for i := int64(0); i < m; i++ {
		r := &recs[i]
		if !r.ready.Load() {
			unpublished++
			continue
		}
		if _, err := fmt.Fprintf(w, "%d k=%d lsn=%X xid=%d seq=%d a=%d b=%d\n", r.ts, r.kind, r.lsn, r.xid, r.seq, r.a, r.b); err != nil {
			return err
		}
		stored++
	}
	// Có slot bị bỏ hoặc còn producer thì sự vắng mặt của record không chứng minh mất Txn.
	final := n.Load()
	if _, err := fmt.Fprintf(w, "# allocated=%d stored=%d dropped=%d unpublished=%d observed_after_dump=%d\n", allocated, stored, max(int64(0), allocated-int64(len(recs))), unpublished, final); err != nil {
		return err
	}
	return w.Flush()
}
