// Package txntrace: vết theo Txn (không theo row) cho điều tra F11 — chỉ nhánh debug. Bật bằng
// STRAND_TXN_TRACE=<file>; ghi vào mảng cấp sẵn, không định dạng, không cấp phát; dump khi pipeline dừng.
package txntrace

import (
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
	recs[i] = rec{ts: time.Now().UnixNano(), kind: kind, lsn: lsn, xid: xid, seq: seq, a: a, b: b}
}

// NextBatch trả id batch mới.
func NextBatch() int64 { return batch.Add(1) }

// Dump ghi vết ra file STRAND_TXN_TRACE.
func Dump() {
	if !On {
		return
	}
	f, err := os.Create(path)
	if err != nil {
		return
	}
	defer f.Close()
	m := int(n.Load())
	if m > len(recs) {
		m = len(recs)
	}
	for _, r := range recs[:m] {
		fmt.Fprintf(f, "%d k=%d lsn=%X xid=%d seq=%d a=%d b=%d\n", r.ts, r.kind, r.lsn, r.xid, r.seq, r.a, r.b)
	}
}
