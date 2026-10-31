# Strand CDC — 02 Technical Specifications

Oct 31, 2026 · @hieuny

## 1. Glossary và quy ước tên

Cả 5 tài liệu dùng đúng các tên trong bảng này. Cột "Không dùng" liệt kê những từ dễ gây nhầm. Nguồn sự thật là [thiết kế rev 89](https://claude.ai/artifact/GKQWmLMMgJ6M1MMHcmsJ1V); kế hoạch nằm trong Strand CDC — 01 Implementation Plan.

| Tên | Nghĩa | Không dùng |
| --- | --- | --- |
| Strand, `strand` | Tên dự án và tên binary CLI | strand-cdc, StrandCDC |
| Pipeline | Một cặp source→target cùng tập bảng, định danh bằng `pipeline_id` | job, task, stream |
| Txn | Transaction nguồn đã lắp ráp đủ. Định danh là GTID (MySQL) hoặc commit LSN (PG) | event (event là một row change) |
| `seq` | Số thứ tự nội bộ để scheduler sắp việc. Chỉ nằm trong RAM, **không bao giờ ghi xuống** | offset, position |
| Reader, Txn Assembler, Spool, Router, Scheduler, Writer, Commit Orderer, Coordinator | Các tầng của pipeline (mục 3) | consumer, sink worker |
| Spool | Hàng đợi trên đĩa (Pebble), phân tầng: local rồi bucket staging | buffer, cache |
| `lwd` / `high` / `holes` | Mọi Txn ≤ `lwd` đã apply / mốc cao nhất đã gộp / các Txn chưa apply trong khoảng (`lwd`, `high`\] | low watermark (từ này dành cho chunk) |
| Row toàn cục, row delta | `_strand_checkpoint` và `_strand_worker_delta` trên target | offset table |
| Chế độ A / B / C | Snapshot logic / seed từ backup (tùy chọn) / `--start-at` | full load, initial load |
| P | Vị trí log tương ứng thời điểm nhất quán của bản seed | seed point, binlog pos |
| Range, chunk | Range: khoảng PK đang được snapshot. Chunk: một lần `SELECT` trong range đó | batch (batch là nhóm Txn của writer) |
| low / high watermark | Vị trí log đọc ngay trước và ngay sau một chunk | lwd |
| Writeset | Tập khóa mà một Txn chạm tới, dùng để dựng DAG phụ thuộc | lock set |
| `strict` / `relaxed` | Giá trị của `commit_order`: commit tuần tự / commit song song có holes | ordered, unordered |
| GRACE v2 | Governor mặc định dự kiến; chỉ giữ mặc định nếu qua Gate 2 | GRACE (không ghi phiên bản) |
| `onoff` | Governor bật/tắt kiểu gh-ost, là fallback của GRACE | bang-bang (chỉ dùng trong văn mô tả) |
| Zone Green / Yellow / Red | Vùng áp lực của GRACE | level, tier |
| Breaker | Circuit breaker theo p99 latency của ứng dụng trên source | kill switch |
| `synced` | Trạng thái pipeline khi bootstrap xong và đang stream | live, caught-up |
| Verify `sample` / `delta` / `full` | Ba mức đối chiếu dữ liệu | checksum |
| Promote | Chuyển target thành nơi nhận ghi: fence, read-only, setval, mở ghi | failover, switchover |
| Gate | Mốc kiểm định cuối phase, xem tài liệu 01 | milestone |

**Quy ước đặt tên:**

- CLI: `strand <danh từ> [<động từ>]` (Cobra). Mọi lệnh ghi có `--dry-run`.
- Biến môi trường `STRAND_*`; key cấu hình viết `snake_case`.
- Metric có tiền tố `strand_`, theo quy ước Prometheus: đơn vị nằm ở hậu tố (`_seconds`, `_bytes`, `_total`).
- Mã lỗi và cảnh báo viết `snake_case` và ổn định giữa các bản, ví dụ `log_gap`, `prefetch_exhausted` (mục 12).
- Bảng metadata trên target có tiền tố `_strand_`.

## 2. Bất biến cốt lõi

Mười lăm bất biến dưới đây là hợp đồng tính đúng của Strand. Mọi thay đổi code chạm vào cơ chế giữ một bất biến phải chạy lại test tương ứng. Bốn bất biến đánh dấu TLA+ được kiểm bằng mô hình ở P0 (mục 12).

| ID | Bất biến | Cơ chế giữ | Kiểm ở |
| --- | --- | --- | --- |
| CP-1 | Khi restart, tập đã apply chứa mọi Txn mà dữ liệu đã commit ở target (không mất) | Row delta ghi cùng transaction với dữ liệu; compaction chỉ sinh chồng lấn, không sinh thiếu | Gate 1 (kill -9), TLA+ |
| CP-2 | Không Txn nào được apply hai lần | Scheduler bỏ mọi Txn thuộc tập đã apply | Gate 1, TLA+ |
| CP-3 | Chỉ GTID hoặc commit LSN được ghi xuống; `seq` không bao giờ được ghi | Kiểu `Checkpoint` không có trường `seq`; lint chặn | Gate 1 (đổi bộ lọc, đổi replica) |
| CP-4 | \|holes\| ≤ 4.096; row delta ≤ 64 KB | Vượt ngưỡng thì scheduler ngừng nhận Txn mới; delta đầy thì compaction đồng bộ, không được thì worker ngừng | Gate 2 (Txn 30 phút) |
| CP-5 | Ở `strict`, trạng thái target luôn là một prefix của thứ tự commit ở source | Commit tuần tự; checkpoint là một vị trí, ghi trong Txn cuối của batch | Gate 2 |
| RS-1 | Target không bao giờ hiển thị một row cũ hơn trạng thái nó đã có | Range-state machine (mục 5): bỏ, gộp theo PK, rendezvous, merge có thứ tự | Gate 1 (`snapshot add`), TLA+ |
| RS-2 | Mọi GTID trong low watermark đã visible với read view của chunk | Thứ tự đọc low → `SELECT` → high; trên replica chạy `WAIT_FOR_EXECUTED_GTID_SET(low)` trước | Gate 0 (P0-06) |
| CO-1 | Hai Txn có writeset giao nhau commit ở target theo đúng thứ tự source | DAG chỉ có cạnh từ Txn trước sang Txn sau, nên không có chu trình | Gate 1, TLA+ |
| CO-2 | RAM của writeset ≤ `writeset_budget` | Nâng lên mức bảng khi một Txn chạm > 50.000 khóa | Gate 2 (100 × 49.999 khóa) |
| SD-1 | Backup nhất quán tại đúng một P; P đọc từ metadata của backup, không nhận P gõ tay (trừ chế độ C) | `seed plan` đối chiếu chéo các nguồn metadata | Gate 1 (seed hỏng) |
| SD-2 | Log liền mạch từ P đến hiện tại; retention ≥ 1,5 × (tuổi backup + thời gian restore) | Kiểm ở `seed plan`; prefetch; báo `log_gap` | Gate 1 |
| SD-3 | Biết schema tại P | Lấy schema từ chính backup | Gate 1 |
| SD-4 | Target tương thích: cùng engine, cùng dòng version, cùng kiến trúc CPU (backup vật lý) | `seed plan` từ chối nếu lệch | Gate 1 |
| PR-1 | Ứng dụng không ghi được vào target trước khi setval xong | Fence source, read-only, setval, rồi mới mở ghi | Gate 1, 2 |
| SG-1 (mới) | Mỗi `pipeline_id` có nhiều nhất một writer active | Khóa trên target (P1–P3); lease etcd + fencing token (P4) | Gate 1, Gate 4 |

## 3. Interface Go giữa các module

Interface dưới đây là bản nháp, chốt ở P0-04. Sau tuần 2 của P1, muốn đổi phải qua review của cả hai kỹ sư. Quy tắc chung: mọi hàm nhận `context.Context`, không dùng state toàn cục, lỗi bọc theo mã ở mục 12.

| Package | Trách nhiệm |
| --- | --- |
| `cmd/strand` | Cobra CLI, nạp cấu hình |
| `internal/reader/{mysql,pg,archive}` | Đọc binlog, pgoutput, hoặc file binlog trong archive |
| `internal/assemble` | Lắp event thành `Txn` |
| `internal/spool` | Pebble, phân tầng local rồi bucket |
| `internal/snapshot` | Chunk, watermark, range-state machine |
| `internal/sched` | Writeset, DAG, holes |
| `internal/writer/{mysql,pg}` | Apply batch, ghi row delta |
| `internal/checkpoint` | Row toàn cục, compaction, khóa pipeline |
| `internal/governor/{grace,onoff,static}` | Ba governor, cùng một interface |
| `internal/seed`, `internal/verify`, `internal/promote` | Chế độ B, verify, promote |

```go
// Pos là thứ duy nhất về vị trí được ghi xuống (CP-3).
type Pos struct {
	Engine Engine // EngineMySQL | EnginePG
	GTID   string // MySQL: "uuid[:tag]:n"
	LSN    uint64 // PG: commit LSN
}

type Txn struct {
	seq       uint64 // chỉ trong RAM, không serialize
	SourcePos Pos
	SchemaVer uint32
	Rows      []RowChange
	Bytes     int64
}

type RowChange struct {
	Table  TableID
	Op     Op         // OpInsert | OpUpdate | OpDelete
	Key    []Value    // PK sau thay đổi
	OldKey []Value    // khác nil khi UPDATE đổi PK
	Cols   []ColValue // ColValue.Unchanged = true với cột TOAST vắng mặt
}

// AppliedSet: GTID set với MySQL; tập LSN kèm holes với PG.
type AppliedSet interface {
	Contains(Pos) bool
	Add(Pos)
	Merge(AppliedSet)
	Lwd() Pos
	High() Pos
	Holes() []Pos
}

type Reader interface {
	Start(ctx context.Context, from AppliedSet) (<-chan Event, error)
	Position() Pos
}

type Spool interface {
	Append(ctx context.Context, t *Txn) error
	Iter(ctx context.Context, from Pos) (SpoolIter, error)
	Truncate(ctx context.Context, applied AppliedSet) error
	Usage() SpoolUsage // số byte theo từng tầng
}

type Scheduler interface {
	Admit(ctx context.Context, t *Txn) error // chặn khi holes hoặc writeset_budget đầy
	Ready() <-chan *Txn
	Done(seq uint64, err error)
}

type Writer interface {
	// Dữ liệu và row delta nằm trong cùng một transaction ở target (CP-1).
	ApplyBatch(ctx context.Context, batch []*Txn, delta DeltaRow) error
}

type CheckpointStore interface {
	LoadApplied(ctx context.Context) (AppliedSet, error) // row toàn cục ∪ mọi row delta
	WriteGlobal(ctx context.Context, g Global) error
	Lock(ctx context.Context, pipelineID string) (release func() error, err error) // SG-1
}

type Governor interface {
	Decide(ctx context.Context, s Signals) Decision // Decision{Rate, InFlight, Zone}
	Mode() string                                   // "grace" | "onoff" | "static"
}

type Seeder interface {
	Plan(ctx context.Context, src SeedSource) (SeedPlan, error) // kiểm SD-1..SD-4
	Apply(ctx context.Context, p SeedPlan) (SeedManifest, error)
}

type Verifier interface {
	Verify(ctx context.Context, mode VerifyMode, t TableID) (VerifyReport, error)
}
```

## 4. Lược đồ metadata trên target

Chỉ có 3 bảng, số row cố định: 1 row pipeline, 1 row toàn cục, mỗi worker 1 row delta. Không bảng nào phình theo thời gian, không có lệnh DELETE. Cột `applied` mã hóa theo engine của **source**, không theo target, nên MySQL→PG dùng cùng lược đồ:

- Source MySQL: một chuỗi GTID set đã gộp, ví dụ `uuid:1-999:1001-50000`.
- Source PG: JSON `{"lwd":"0/1A2B3C","high":"0/1A9F00","holes":["0/1A4000"]}`.

```sql
-- PostgreSQL target
CREATE TABLE _strand_pipeline (
  pipeline_id  text PRIMARY KEY,
  epoch        bigint NOT NULL,      -- fencing token, tăng mỗi lần một instance nhận pipeline
  owner        text   NOT NULL,      -- host/pod, pid
  config_hash  text   NOT NULL,
  meta_version int    NOT NULL,      -- phiên bản lược đồ _strand_*, xem 03 mục 8
  started_at   timestamptz NOT NULL
);
CREATE TABLE _strand_checkpoint (
  pipeline_id  text PRIMARY KEY,
  epoch        bigint NOT NULL,
  applied      text   NOT NULL,
  updated_at   timestamptz NOT NULL
) WITH (fillfactor = 50);
CREATE TABLE _strand_worker_delta (
  pipeline_id  text     NOT NULL,
  worker_id    smallint NOT NULL,
  epoch        bigint   NOT NULL,
  batch_no     bigint   NOT NULL,    -- luôn tăng, để UPDATE luôn đổi row
  applied      text     NOT NULL,    -- ≤ 64 KB (CP-4)
  PRIMARY KEY (pipeline_id, worker_id)
) WITH (fillfactor = 50);
```

Bản MySQL giống hệt: `text` đổi thành `VARCHAR(64)` cho khóa và `MEDIUMTEXT` cho `applied`, `timestamptz` đổi thành `TIMESTAMP(6)`, engine InnoDB, bỏ `fillfactor`.

**Quy tắc ghi:**

1. **Khởi động:** lấy khóa `strand:<pipeline_id>` (`GET_LOCK(name, 0)` với MySQL; `pg_try_advisory_lock` dạng hai số int4 với PG). Không lấy được thì thoát với mã `pipeline_locked`.
2. Trong một transaction: `epoch = epoch + 1` trên `_strand_pipeline`, rồi gán epoch mới vào row toàn cục và mọi row delta. Sau đó mới `LoadApplied`.
3. **Mỗi batch của writer:** trong cùng transaction với dữ liệu, chạy `UPDATE _strand_worker_delta SET applied = ?, batch_no = batch_no + 1 WHERE pipeline_id = ? AND worker_id = ? AND epoch = ?`. Nếu số row bị ảnh hưởng khác 1 thì rollback và dừng với mã `fenced`. `batch_no` khiến UPDATE luôn đổi row, nên MySQL trả đúng số row kể cả khi không bật `CLIENT_FOUND_ROWS`.
4. **Coordinator:** mỗi 100–500 ms ghi row toàn cục trong transaction riêng, có cùng điều kiện `epoch`.
5. **Chế độ `strict`:** không dùng row delta. Row toàn cục được ghi trong transaction commit cuối của mỗi batch, cũng có điều kiện `epoch`.
6. Mất kết nối giữ khóa thì ngừng nhận Txn mới. Khóa chỉ chặn lúc khởi động; `epoch` mới chặn writer cũ đang chạy dở.

Cơ chế `epoch` ở bước 2–4 là đề xuất mới cho SG-1, cần đưa vào thiết kế rev 90. Đến P4, lease etcd thay cho khóa, nhưng fencing vẫn dùng cột `epoch` này.

## 5. Range-state machine

Khi snapshot chạy song song với stream, mỗi range PK đi qua 5 trạng thái theo một chiều. Event từ stream được xử lý theo trạng thái của range chứa nó. PG dùng exported snapshot nên snapshot ban đầu không cần buffer; quy tắc này chỉ áp dụng cho PG khi `snapshot add`.

&#91;embedded content: range-state machine · 5 trạng thái\]

Bảo đảm nằm ở bước Flush: snapshot và buffer được merge trong cùng một transaction, nên target không có lúc nào lùi về trạng thái cũ hơn.

**Watermark.** MySQL: mỗi chunk đọc low = `@@GLOBAL.gtid_executed` (Percona dùng `Binlog_snapshot_gtid_executed`), rồi `SELECT … ORDER BY pk`, rồi đọc high. Đọc từ replica thì chạy `WAIT_FOR_EXECUTED_GTID_SET(low)` trước `SELECT` (RS-2).

**Chia chunk** (`chunk_strategy: auto`):

- PK số, phân bố đều ((max − min) / count trong \[0,5, 2\]): chia theo dải số, O(1) query.
- PK lệch hoặc PK chuỗi: keyset walk `SELECT pk FROM t WHERE pk > ? ORDER BY pk LIMIT 1 OFFSET n`, chạy dần và cũng bị governor điều tiết.
- Biên chunk luôn do DB so sánh theo collation của cột. Thiếu bộ so sánh tương thích ở phía Go thì dùng `numeric_range` hoặc tuần tự hóa.

**Giới hạn:**

| Tham số | Mặc định | Quy tắc |
| --- | --- | --- |
| Timeout `SELECT` của chunk | 5 s | Hủy, chia đôi, chạy lại. Không áp khi chunk chỉ còn 1 row |
| `max_chunk_bytes` | 64 MB | Giới hạn cả row lẫn byte; row vượt ngân sách được ghi thẳng thành run trên đĩa |
| `min_chunk_rows` | 1 | Chặn vòng lặp chia chunk |
| Chunk thăm dò | `LIMIT` nhỏ | Chunk đầu của mỗi bảng, để hiệu chỉnh ước lượng byte |
| Buffer | Chặn theo số khóa khác nhau | Vượt RAM thì spill xuống Pebble dạng sorted run; flush bằng k-way merge tuần tự; có thể nén zstd |
| Chunk đuôi `[b_last, +∞)` | — | Đọc `max(pk)` sau low; khóa lớn hơn thuộc range "Xong" ngay từ đầu |

Governor điều tiết bằng cách giãn khoảng cách giữa các chunk, không làm chậm một `SELECT` đang chạy, nên cửa sổ \[low, high\] luôn ngắn.

## 6. Scheduler và writeset phân cấp

Scheduler dựng DAG phụ thuộc từ writeset. Mọi cạnh đều trỏ về Txn đứng trước theo thứ tự commit của source, nên DAG không thể có vòng (CO-1). Rủi ro còn lại là thiếu cạnh hoặc thừa cạnh, phần này TLA+ kiểm ở P0.

**Writeset của Txn T** là tập hash xxh3 của:

- `(table, PK)` cho mọi row T chạm tới. Với UPDATE đổi PK, lấy cả PK cũ lẫn PK mới.
- `(table, unique_key)` cho mọi unique index.
- `(parent_table, parent_key)` cho FK.

**Tham số:**

| Key | Mặc định | Khi vượt |
| --- | --- | --- |
| `table_level_threshold` | 50.000 khóa/bảng/Txn | Txn được nâng thành khóa cấp bảng; map row của bảng bị xóa |
| `writeset_budget` | 2.000.000 entry (khoảng 100 MB) | Bảng chiếm nhiều entry nhất chuyển sang chế độ cấp bảng cho đến khi map được dọn hết |
| `max_holes` | 4.096 | Ngừng nhận Txn mới |
| `max_pending_cross_range` | 1.024 thao tác | Spill xuống spool; cùng với chờ > 30 s, đẩy range liên quan lên đầu hàng đợi đọc |
| `group_commit.max_rows` / `max_bytes` / `max_wait` | Chốt sau P0 | Đóng batch |

**Thuật toán `Admit`** (chạy tuần tự theo thứ tự commit của source):

```go
type tableState struct {
	rows        map[uint64]uint64   // hash khóa -> seq của writer cuối
	tableWriter uint64              // seq của Txn cấp bảng gần nhất
	active      map[uint64]struct{} // seq đang chạy có chạm bảng này
	forcedTableLevel bool           // bật khi vượt writeset_budget
}

func (s *Scheduler) Admit(t *Txn) {
	var deps []uint64
	for tbl, keys := range t.writesetByTable() {
		st := s.table(tbl)
		if st.tableWriter != 0 {
			deps = append(deps, st.tableWriter)
		}
		if len(keys) > s.tableLevelThreshold || st.forcedTableLevel {
			for p := range st.active { // phụ thuộc mọi Txn đang chạy trên bảng
				deps = append(deps, p)
			}
			st.tableWriter = t.seq
			clear(st.rows)
		} else {
			for _, k := range keys {
				if p, ok := st.rows[k]; ok {
					deps = append(deps, p)
				}
				st.rows[k] = t.seq
			}
		}
		st.active[t.seq] = struct{}{}
	}
	s.dag.Add(t.seq, dedup(deps)) // mọi dep < t.seq, nên DAG không có vòng
}
```

Khi Txn commit xong: xóa nó khỏi `active`, xóa entry `rows[k]` nếu giá trị vẫn là seq của nó, và xóa `tableWriter` nếu bằng seq của nó.

**Writer:**

- Một Txn nguồn không bao giờ bị tách thành nhiều lần commit ở target. Group commit chỉ gộp các Txn đứng liền nhau và không phụ thuộc nhau.
- MySQL dùng multi-row upsert. PG dùng `COPY` vào bảng tạm rồi `MERGE` (PG 15+).
- UPDATE đổi PK trên range đã "Xong": `UPDATE … SET pk = <mới> WHERE pk = <cũ>`. Trường hợp partial TOAST nằm ở mục 8.
- `commit_order: strict` commit theo đúng thứ tự source, không có holes. `relaxed` commit khi Txn sẵn sàng; holes được ghi lại trong checkpoint.

## 7. Governor: GRACE v2, `onoff`, `static`

Ba governor dùng chung interface `Governor` và cùng bị chặn bởi hard limit ở token bucket. Breaker đứng trên cả ba, có từ P1. Mặc định là `grace-v2` nếu qua Gate 2; nếu không thì là `onoff`.

**Actuator duy nhất** là x = log R, với R = minᵣ Rᵣ (token/giây; 1 token ≈ 1 page IO, hoặc 1 MiB với băng thông). Hai đại lượng còn lại suy ra từ R:

- Cửa sổ đồng thời L = ⌈R · T\_op · 1,25⌉. L bị chặn bởi trần do deadlock và bởi mức song song mà writeset cho phép.
- Kích thước chunk: rows ← rows · clamp(250 ms / thời gian chunk trước, 0,5, 2). Chi phí ước lượng của chunk phải ≤ R · 0,25 s.

**Áp suất.** Mỗi tín hiệu đi qua median 3 mẫu rồi EWMA với α = 0,3. Tín hiệu mềm phải xuất hiện ở 2 trong 3 tick. Tín hiệu cứng tác dụng ngay, và luôn lấy delta của counter.

```latex
P = \max_r P_r, \qquad P_{lag} = \max\left(\frac{\text{lag}}{\text{lag}_{SLO}},\ \frac{\text{WAL}_{retained}}{\text{max\_slot\_wal\_keep\_size}},\ 1 - \frac{t_{\text{binlog còn lại}}}{t_{\text{ngưỡng}}}\right)
```

| Pha | `lag` trong P\_lag | Khi P\_lag ≥ 1 |
| --- | --- | --- |
| Bootstrap (trước `synced`) | Khoảng cách từ vị trí reader/spool đến head của source | Báo `bootstrap_retention_risk`; **tăng** ưu tiên snapshot và prefetch, vẫn trong hard limit |
| `synced` | Độ trễ apply ở target | Background nhận 0; foreground có sàn để lag không tăng thêm |

**Luật theo zone** (hysteresis: Green→Yellow khi P ≥ 0,70; Yellow→Green khi P < 0,60 trong 3 tick; vào Red khi P ≥ 1,0 hoặc có tín hiệu cứng; rời Red khi P < 0,90 và hết cooldown):

| Zone | Luật trên x | Chống dao động |
| --- | --- | --- |
| Red | x ← x + log β, β = clamp(1 − 0,5(P − 1), 0,5, 0,85); tín hiệu cứng thì β = 0,5. Ghi W\_max = x trước khi giảm | Cooldown d + 1 tick |
| Yellow | x ← x + k(P\* − P), k = κ / max(ŝ, s\_min); P\* = 0,85, hoặc 1,0 khi P\_lag ≥ 1 | Dead band \|e\| < 0,05; κ = 50% của biên 2 sin(π / (2(2d + 1))); bumpless khi vào zone |
| Green | CUBIC quanh W\_max, chỉ tăng khi L đang bị dùng hết | W\_max hết hạn sau 10 phút; tăng tối đa 10% mỗi tick; dither ±1% (tắt bằng `probe: off`) |

**ŝ** ước lượng bằng RLS với hệ số quên 0,95. Hiệp phương sai của RLS phân biệt "ŝ nhỏ" với "chưa đo được". Chưa đo được thì giữ hành động giảm và bật `strand_sensitivity_unknown`. Ở Red, chính bước MD là kích thích: nếu sau τ giây P không giảm thì Strand không phải thủ phạm.

**Arbiter:** foreground nhận đủ nhu cầu trước; background chia phần còn lại theo weight. Nếu sàn của foreground vượt hard limit thì giữ hard limit và báo `lag_unsustainable`.

**Breaker:**

- Kích hoạt khi tín hiệu đọc trực tiếp từ DB vượt ngưỡng: p99 latency query của chính Strand so với baseline, độ trễ commit, hoặc replica lag. Chỉ khi khai báo metrics\_source tùy chọn thì mới dùng thêm p99 của ứng dụng (query PromQL/MetricsQL). Khi đó R giảm ngay về 50% và background dừng.
- Mở lại sau 60 giây liên tục dưới ngưỡng, khởi động lại bumpless.
- Không xuyên qua sàn foreground, trừ khi có tín hiệu cứng.
- Khi ŝ ≈ 0 với độ tin cậy cao trong hơn 5 phút, dừng ở polite floor và báo `pressure_external`.

**`onoff` (fallback, đặc tả mới):** background chỉ có hai trạng thái, chạy ở R = soft limit hoặc dừng hẳn. Dừng khi P ≥ 1,0, khi có tín hiệu cứng, hoặc khi breaker kích hoạt. Chạy lại khi P < 0,90 trong 3 tick liên tiếp. Chunk size vẫn dùng luật 250 ms. Foreground và luật P\_lag theo pha giữ như GRACE.

**`static`:** R cố định bằng soft limit, chỉ dùng làm mốc so sánh trong benchmark Gate 2.

## 8. Ngữ nghĩa dữ liệu

Mục này quy định cách Strand xử lý những trường hợp dễ gây lệch dữ liệu im lặng. Mỗi quy tắc hoặc có kiểm tra ở `validate`, hoặc có test ở một gate.

**Cấu hình source bắt buộc (`validate` từ chối nếu sai):**

- MySQL: `binlog_format=ROW`, `binlog_row_image=FULL`, `gtid_mode=ON`. `binlog_row_value_options` phải rỗng, vì partial JSON bị từ chối. Nên bật `binlog_row_metadata=FULL`.
- PG: `wal_level=logical`. Decode trên standby cần PG 16+ và `hot_standby_feedback=on`.
- Bảng không có PK bị từ chối mặc định. Nếu bật override, bảng đó được apply tuần tự và khớp row theo toàn bộ cột.

**PG TOAST.** Mặc định dùng partial UPDATE: cột được pgoutput đánh dấu "unchanged" không nằm trong `SET`. Row không tồn tại ở target thì báo lỗi và đưa range vào verify. `REPLICA IDENTITY FULL` chỉ là opt-in.

**Đổi PK khi cột TOAST không đổi (PG).** Event này không được gộp và không dùng point read từ source. Cách xử lý phụ thuộc trạng thái của range chứa PK mới:

- **Chưa đọc:** chỉ cần DELETE PK cũ, theo trạng thái của range chứa PK cũ.
- **Đang đọc hoặc Xong:** chờ cả hai range flush xong, rồi chạy trong một transaction ở target:

```sql
UPDATE t SET pk = :new, <các cột đổi>
 WHERE pk = :old
   AND NOT EXISTS (SELECT 1 FROM t WHERE pk = :new);
DELETE FROM t WHERE pk = :old;
```

Thao tác chờ bị chặn bởi `max_pending_cross_range` (mục 6). Metric `strand_pending_cross_range` kèm tuổi của thao tác chờ lâu nhất.

**Collation của cột khóa.** Ngữ nghĩa duy nhất ở target không được lỏng hơn source.

- MySQL `_ci` sang PG `C` hoặc collation deterministic: an toàn. Không dùng `citext`.
- Target lỏng hơn source (case-insensitive, accent-insensitive, PAD SPACE): `validate` từ chối, trừ khi cột ở target dùng `_bin`.
- Pipeline giữa hai loại DB bắt buộc chạy `verify --mode full` sau khi setup.

**Kiểu dữ liệu MySQL→PG.** Zero date, `BIGINT UNSIGNED` vượt int8, `ENUM`/`SET` mỗi loại có policy riêng; `validate` quét giá trị thực tế. `time_zone` của session được cố định. Mapping `TIMESTAMP` sang `timestamptz` và `DATETIME` sang `timestamp` khai báo tường minh.

**Promote.** Đây là cổng duy nhất mở quyền ghi cho target. Các bước chạy tuần tự:

1. Fence source cũ.
2. Giữ target read-only: MySQL dùng `super_read_only`; PG dùng `default_transaction_read_only` và chưa cấp quyền ghi cho role của ứng dụng.
3. Apply nốt phần log còn lại.
4. Chạy setval cho mọi sequence và identity. Sequence gắn với cột (tìm qua `pg_depend`) lấy `max(cột) + margin`. Sequence độc lập lấy `last_value` đọc từ source, kể cả từ physical standby, cộng margin.
5. Mở quyền ghi.
6. Chuyển DNS hoặc VIP.

```latex
\text{margin} = 2 \cdot \text{cache\_size} \cdot \text{increment\_by} + 2 \cdot r_{peak} \cdot \Delta t_{sync} \cdot \text{increment\_by}
```

Standby luôn bằng hoặc đi trước giá trị đã cấp, vì PG ghi WAL trước cho sequence. Rủi ro thật là Δt\_sync, chu kỳ Strand đồng bộ `last_value`. Nếu sau khi fence không đọc được source, `promote` dừng và hỏi người vận hành, kèm giá trị gợi ý (lần đồng bộ cuối cộng margin). MySQL dùng `AUTO_INCREMENT` nên không gặp vấn đề này.

## 9. Seed và archive catch-up

Chế độ B là tùy chọn. Một lần seed chỉ được chấp nhận khi `seed plan` chứng minh được SD-1 đến SD-4 (mục 2). Kết quả được ghi vào `seed.json`, và `strand sync` đọc file này để bắt đầu catch-up từ P.

| Engine | Công cụ | Nguồn của P |
| --- | --- | --- |
| MySQL 8.0 / 8.4 | XtraBackup cùng dòng (8.0.35-xx / 8.4.x) | Metadata của backup, đối chiếu với `@@GLOBAL.gtid_executed` của instance sau restore |
| MySQL 9.7 | XtraBackup 9.7 (chỉ khi GA, hoặc có `--allow-prerelease-tool`), CLONE | Như trên |
| MySQL mọi dòng | mydumper v1.0.3-1 (không chấp nhận `NO_LOCK`) | File metadata của mydumper |
| PG 16–18 | pgBackRest 2.60.0 restore thành standby, PITR với `recovery_target_lsn` + pause; WAL-G | LSN của slot, kiểm bằng `pg_last_wal_replay_lsn()` |
| PG 16–18 | `strand export` (`pg_dump --snapshot` lấy từ slot) | `consistent_point` của slot |

**`seed.json` v1:**

```json
{
  "version": 1,
  "engine": "mysql",
  "kind": "xtrabackup",
  "tool_version": "8.4.0-6",
  "tool_prerelease": false,
  "source_identity": "3E11FA47-71CA-11E1-9E33-C80AA9429562",
  "backup_ref": "s3://mysql-backup/orders-db/full-2026-10-30",
  "start_position": { "gtid_set": "3E11FA47-…:1-982341" },
  "position_cross_check": { "restored_gtid_executed": "…:1-982341", "match": true },
  "schema_snapshot": "s3://mysql-backup/orders-db/full-2026-10-30/schema.sql",
  "tables": ["app.orders", "app.users"],
  "integrity": { "prepare_ok": true },
  "log_coverage": { "source_gtid_purged": "…:1-950000", "archive_until": "…:1-991200", "ok": true },
  "log_prefetch_until": "…:1-993000",
  "legal_hold_ref": "s3://mysql-binlog/orders-db/?hold=seed-20261031",
  "backup_expires_at": null,
  "target": { "version": "8.4.6", "compatible": true },
  "created_at": "2026-10-31T02:10:00Z"
}
```

Hai trường `tool_version` và `tool_prerelease` là mới, dùng cho issue 15 (tài liệu 01, mục 7). Các trường còn lại lấy từ thiết kế rev 89. Thêm trường chỉ được làm theo kiểu cộng thêm; đổi nghĩa một trường thì phải tăng `version`.

**Chống mất log từ lúc `seed plan` đến lúc catch-up** (theo thứ tự ưu tiên):

1. **Prefetch, mặc định bật.** Reader đọc từ P vào spool, song song với restore: đọc archive trước, sau đó đọc phần còn lại từ replica. Prefetch thuộc lớp background và chịu governor điều tiết. Nếu đã có archive bền, prefetch chỉ cần phủ phần log chưa kịp archive.
2. **Legal hold / S3 Object Lock** lên các file archive cần dùng, gỡ khi seed xong.
3. **PG:** kiểm tra backup đang dùng không bị expire trong suốt ETA (`repo1-retention-full`).
4. **Kiểm tra lại trước catch-up.** Thiếu log thì báo `log_gap` và dừng.

Dung lượng log cần giữ = tốc độ sinh log ở peak × (ETA × 2, hoặc p90 của các lần restore trước). Cảnh báo khi vượt 80% tổng dung lượng các tầng spool. Spool local đầy thì tràn sang bucket staging; cả hai tầng chạm hard limit thì báo `prefetch_exhausted` ngay.

**Verify sau seed:** pipeline chỉ chuyển sang `synced` khi cả hai bước sau đều báo 0 lệch: checksum ngẫu nhiên khoảng 1% range PK, và verify mọi range xuất hiện trong 30 phút CDC đầu tiên.

## 10. CLI và cấu hình

CLI là hợp đồng công khai. Tên lệnh, flag và exit code chỉ được thêm, không được đổi nghĩa trong cùng major version. Mọi lệnh ghi có `--dry-run`. Cấu hình đọc theo thứ tự YAML, rồi biến môi trường `STRAND_*`, rồi flag.

| Lệnh | Phase | Mục đích |
| --- | --- | --- |
| `strand validate -c p.yaml` | P1 | Preflight; từ chối hoặc cảnh báo theo mục 8 |
| `strand plan -c p.yaml` | P1 | Ước lượng ETA; từ chối chế độ A khi ETA > 50% retention, trừ khi có `--force` |
| `strand sync -c p.yaml [--start-at seed\|<gtid>\|<lsn>]` | P1 | Chạy pipeline |
| `strand seed plan --backup <uri> --type <kind>` | P1 | Kiểm SD-1..SD-4, ghi `seed.json` |
| `strand seed apply --manifest seed.json [--print-only]` | P1 | Điều phối restore hoặc load; chạy tiếp được khi bị ngắt |
| `strand snapshot add --table <t> [--mode logical\|seed]` | P1 | Thêm bảng khi pipeline đang chạy |
| `strand top` | P1 | TUI: lag, throughput, zone, tiến độ, ETA |
| `strand pause` / `strand resume [--snapshot-only]` | P1 | Tạm dừng, chạy tiếp |
| `strand throttle set --source-iops <n> --soft` | P1 | Đổi limit nóng qua admin socket |
| `strand checkpoint show\|rewind --to <gtid\|lsn>` | P1 | Xem hoặc tua vị trí, có kiểm `gtid_purged` / `restart_lsn` |
| `strand verify --table <t> --mode sample\|delta\|full --repair report\|fix` | P1 (`full`: P2) | Đối chiếu theo range |
| `strand promote --target <dsn> --fence <source>` | P1 | Mở quyền ghi (mục 8) |
| `strand export --source <dsn> --tables … --out <uri>` | P2 | Dump logic gắn với slot (PG) |
| `strand slot drop` | P2 | Dọn slot PG an toàn |
| `strand schema diff` / `strand schema approve <id>` | P3 | Duyệt thay đổi schema phá hủy |
| `strand archive binlog --from <replica> --to <uri>` | P3 | Lưu trữ binlog liên tục |
| `strand config schema` (mới) | P1 | In JSON Schema của file cấu hình, để CI và editor kiểm tra |

**Exit code (đề xuất mới):**

| Code | Nghĩa |
| --- | --- |
| 0 | Thành công |
| 1 | Lỗi không phân loại |
| 2 | Sai cú pháp hoặc cấu hình không hợp lệ |
| 3 | `validate` hoặc `plan` từ chối |
| 4 | `pipeline_locked` hoặc `fenced` |
| 5 | Lỗi dữ liệu hoặc log: `log_gap`, `prefetch_exhausted`, `verify_mismatch` |
| 10 | Cần người vận hành quyết định, ví dụ `promote` khi source không truy cập được |

**File cấu hình** (rút gọn từ thiết kế; `bootstrap` là tùy chọn, mặc định `logical`):

```yaml
version: 1
name: orders-sync                 # = pipeline_id
source: { kind: mysql, dsn: ${SRC_DSN}, server_id: 41001, read_from: replica }
target:
  kind: postgres
  dsn: ${DST_DSN}
  write_mode: upsert
  commit_order: strict            # strict | relaxed
  batch: { max_rows: 2000, max_bytes: 4MiB, max_wait: 50ms }
tables:
  include: ["app.orders", "app.users"]
  rename: { "app.users": "public.app_users" }
bootstrap:
  default: logical                # logical | seed | start_at
  per_table:
    "app.orders": { mode: seed, seed: { type: xtrabackup, uri: s3://mysql-backup/orders-db/full-2026-10-30 } }
  catch_up: { binlog_archive: s3://mysql-binlog/orders-db/ }
  logical: { chunk_rows: auto, max_parallel: 4, windows: ["22:00-06:00"] }
schema: { additive: auto, destructive: pause }
checkpoint: { store: target, flush_every: 500ms }
resources:
  memory: { soft: 1GiB, hard: 2GiB }
  spool:  { path: /var/lib/strand/orders-sync, soft: 20GiB, hard: 50GiB, overflow: s3://strand-staging/orders-sync/ }
  source: { iops: { soft: 600, hard: 900 } }
  target: { iops: { soft: 1500, hard: 2000 } }
  workers: { min: 1, max: 32 }
governor:
  algorithm: grace-v2             # grace-v2 | onoff | static
  tick: 1s
  lag_slo: 5s
  probe: on
  breaker: { strand_query_p99: { max_ratio: 3.0 }, replica_lag: { max: 10s } }
  # metrics_source: { kind: prometheus, url: <url>, app_p99: { query: '<PromQL>', max: 300ms } }   # tùy chọn
verify: { delta: on, full: { cron: "0 2 * * 0", rate: 10MiB } }
observability: { metrics: ":9464", admin_socket: /run/strand/orders-sync.sock, log: { level: info, format: json } }
```

So với thiết kế rev 89 có ba thay đổi: `resources.spool.overflow` cho tầng bucket, `governor.algorithm` nhận thêm giá trị `onoff` và `static`, và `checkpoint.flush_every` mặc định là 500 ms cho khớp chu kỳ 100–500 ms của coordinator.

## 11. Metrics contract

Metric được expose ở `:9464/metrics` theo định dạng Prometheus, scrape bằng VictoriaMetrics. Mọi metric có label `pipeline`. Label `table` chỉ xuất hiện khi ghi rõ trong bảng, để giữ cardinality thấp. Dashboard JSON chỉ xuất ra sau khi có code. Trước đó, bảng này là hợp đồng mà tài liệu 05 dựa vào.

| Metric | Kiểu | Label thêm | Nguồn |
| --- | --- | --- | --- |
| `strand_lag_seconds` | gauge | `phase` (bootstrap\|synced) | Mới |
| `strand_apply_txn_total`, `strand_apply_rows_total` | counter | — | Mới |
| `strand_holes` | gauge | — | Mới |
| `strand_checkpoint_compaction_lag_seconds` | gauge | — | Thiết kế |
| `strand_writeset_entries` | gauge | `table` | Thiết kế |
| `strand_pending_cross_range` | gauge | — | Thiết kế |
| `strand_pending_cross_range_oldest_seconds` | gauge | — | Thiết kế |
| `strand_governor_rate_tokens` | gauge | `resource` | Mới |
| `strand_governor_zone` | gauge (0 Green, 1 Yellow, 2 Red) | — | Mới |
| `strand_governor_pressure` | gauge | `signal` | Mới |
| `strand_sensitivity_unknown` | gauge (0/1) | — | Thiết kế |
| `strand_breaker_open` | gauge (0/1) | — | Mới |
| `strand_spool_bytes` | gauge | `tier` (local\|bucket) | Mới |
| `strand_snapshot_ranges` | gauge | `table`, `state` | Mới |
| `strand_seed_progress_ratio` | gauge | `table` | Mới |
| `strand_retention_headroom_seconds` | gauge | — | Mới |
| `strand_verify_mismatch_total`, `strand_verify_unverifiable_hot_total` | counter | `table` | Mới |
| `strand_fenced_total` | counter | — | Mới (SG-1) |

**Alert mặc định** (ngưỡng là giá trị ban đầu; tài liệu 05 sẽ hiệu chỉnh):

| Alert | Điều kiện |
| --- | --- |
| `StrandCompactionStuck` | `strand_checkpoint_compaction_lag_seconds > 5` trong 1 phút |
| `StrandLagSLO` | `strand_lag_seconds{phase="synced"} > lag_slo` trong 5 phút |
| `StrandRetentionRisk` | `strand_retention_headroom_seconds < 3600` |
| `StrandSpoolNearFull` | `sum(strand_spool_bytes) / spool capacity > 0.8` |
| `StrandVerifyMismatch` | `increase(strand_verify_mismatch_total[15m]) > 0` |
| `StrandFenced` | `increase(strand_fenced_total[5m]) > 0` |
| `StrandBreakerOpen` | `strand_breaker_open == 1` trong 10 phút |

**Panel tối thiểu cho dashboard đầu tiên:** lag theo phase; throughput apply (`rate(strand_apply_rows_total[1m])`); zone và rate của governor chồng lên p99 của ứng dụng; spool theo tầng; tiến độ snapshot/seed theo bảng; holes và độ trễ compaction; kết quả verify.

## 12. Trạng thái pipeline, mã lỗi, đặc tả hình thức

**Trạng thái pipeline:**

| Trạng thái | Nghĩa | Chuyển sang |
| --- | --- | --- |
| `validating` | Preflight và lấy khóa pipeline | `bootstrapping`, `failed` |
| `bootstrapping` | Snapshot (A), restore và catch-up (B), hoặc chờ verify (C) | `synced`, `paused`, `failed` |
| `synced` | Đang stream; verify sau seed đã 0 lệch | `paused`, `promoting`, `failed`, `fenced` |
| `paused` | Dừng theo lệnh, vẫn giữ khóa | Trạng thái trước đó |
| `promoting` | Đang chạy 6 bước promote | `promoted`, `awaiting_operator` |
| `awaiting_operator` | Cần quyết định, ví dụ sequence độc lập khi source không truy cập được | `promoting` |
| `promoted` | Target đã mở ghi; pipeline kết thúc | — |
| `fenced` | Instance khác đã nhận pipeline (`epoch` mới) | Kết thúc process |
| `failed` | Lỗi không tự phục hồi được | — |

**Mã lỗi và cảnh báo** (`snake_case`, ổn định giữa các bản):

| Mã | Loại | Exit | Nghĩa |
| --- | --- | --- | --- |
| `binlog_config_invalid`, `missing_pk`, `collation_looser`, `type_unmappable`, `insufficient_privilege` | Lỗi `validate` | 3 | Mục 8 |
| `seed_invariant_failed` | Lỗi `seed plan` | 3 | Không chứng minh được một trong SD-1..SD-4 |
| `prerelease_tool` (mới) | Lỗi `seed plan` | 3 | Backup tạo bằng công cụ bản RC |
| `pipeline_locked` (mới) | Lỗi | 4 | Instance khác đang giữ khóa |
| `fenced` (mới) | Lỗi | 4 | UPDATE row delta có điều kiện `epoch` trả 0 row |
| `log_gap` | Lỗi | 5 | Thiếu log giữa P và hiện tại |
| `prefetch_exhausted` | Lỗi | 5 | Mọi tầng spool đều chạm hard limit |
| `verify_mismatch` (mới) | Lỗi | 5 | Verify phát hiện lệch |
| `promote_needs_decision` (mới) | Dừng chờ | 10 | Không đọc được sequence độc lập sau khi fence |
| `bootstrap_retention_risk`, `lag_unsustainable`, `pressure_external` | Cảnh báo | — | Mục 7 |
| `unverifiable_hot` | Báo cáo | — | Row quá nóng, không kiểm được trong lượt verify này |

**Đặc tả hình thức.** Tài liệu này không chứa spec TLA+ đầy đủ. Spec là task P0-07, nằm trong repo tại `spec/tla/`, và kiểm bằng TLC ở biên nhỏ:

| Module | Bất biến kiểm | Biên TLC ban đầu |
| --- | --- | --- |
| `Scheduler` | CO-1; không thiếu cạnh khi trộn Txn nhỏ với Txn cấp bảng | 2 bảng, 6 Txn, 1 Txn cấp bảng, 3 worker |
| `Checkpoint` | CP-1, CP-2, CP-4; crash ở mọi bước compaction | 3 worker, 8 Txn, 1 crash |
| `RangeState` | RS-1, kể cả đổi PK qua hai range | 2 range, 4 khóa, 5 event |
| `Handover` | Archive sang live stream: không trùng, không hổng | 2 file archive, 6 Txn |

Nếu TLC tìm thấy vi phạm, thiết kế phải được sửa trước khi viết code cho phần đó (điều kiện Gate 0). Mở rộng biên là việc của P1, không chặn gate.

**Bổ sung từ tài liệu 03–05:**

- `GET /healthz` trên cổng metric trả 200 khi vòng lặp chính còn tiến, dùng cho liveness probe.
- Metric `strand_spool_capacity_bytes{tier}`, để viết alert `StrandSpoolNearFull`.
- Cột `meta_version` trong `_strand_pipeline`. Binary mới migrate tiến; binary cũ gặp `meta_version` cao hơn thì từ chối chạy.
- Log không chứa giá trị row. Lỗi apply chỉ log PK đã hash.
- `promote` có thể chạy lại: mỗi bước idempotent và bước đang dở được ghi lại, nên chạy lại sẽ làm tiếp từ bước đó.
- P1–P2: gặp DDL trên bảng thuộc pipeline thì dừng với mã `schema_change_pending` (exit 10). `resume` chỉ chạy tiếp khi schema ở target đã khớp.
