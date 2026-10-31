# Báo cáo Audit thư viện pglogrepl

- **Thư viện**: `github.com/jackc/pglogrepl`
- **Commit SHA đã ghim**: `662581bb6bb6de4da82805879643f1dc39e41fad` (v0.0.0-20261003132456-662581bb6bb6)
- **Tác giả audit**: Implementer Agent (Strand CDC)
- **Phạm vi audit**:
  1. Hỗ trợ pgoutput proto v1–v4 trên PostgreSQL 16, 17, 18
  2. Xử lý transaction đang streaming (proto v2+)
  3. Two-phase commit của proto v3
  4. Nhận diện cột TOAST không đổi (unchanged TOAST marker)
  5. Tạo slot kèm exported snapshot và `consistent_point`
  6. Khả năng logical decoding trên standby PostgreSQL 16+

---

## 1. Hỗ trợ giao thức pgoutput proto v1–v4 trên PG 16, 17, 18

### 1.1 Kiểm tra mã nguồn thư viện tại commit 662581bb6bb6de4da82805879643f1dc39e41fad
- **Hàm khởi tạo stream**: `pglogrepl.go:307-333` định nghĩa `StartReplication`, nhận `StartReplicationOptions` cho phép truyền các tham số plugin như `proto_version '1'..'4'`, `publication_names`, `streaming 'on'`.
- **Định nghĩa loại message**: `message.go:56-70` khai báo các hằng số nhận dạng message chuẩn:
  - `MessageTypeBegin = 'B'`
  - `MessageTypeCommit = 'C'`
  - `MessageTypeRelation = 'R'`
  - `MessageTypeInsert = 'I'`
  - `MessageTypeUpdate = 'U'`
  - `MessageTypeDelete = 'D'`
  - `MessageTypeTruncate = 'T'`
  - `MessageTypeType = 'Y'`
  - `MessageTypeOrigin = 'O'`
  - `MessageTypeMessage = 'M'`
- **Parser cho proto v1**: `message.go:98-105` (`pglogrepl.Parse(data)`).
- **Parser cho proto v2+**: `messageV2.go:113-157` (`pglogrepl.ParseV2(data, inStream)`), hỗ trợ giải mã cả các message thông thường lẫn message bọc trong streaming segment.

### 1.2 Kiểm chứng bằng kiểm thử thực tế
- Test thực thi: `tests/audit/pg/audit_test.go:TestAC1_ProtoV1toV4_PG16_17_18`
- Kết quả: Chạy lần lượt trên 3 phiên bản PostgreSQL (`postgres:16`, `postgres:17`, `postgres:18`) qua Testcontainers. Với mỗi phiên bản DB, cả 4 proto version (1, 2, 3, 4) đều khởi tạo replication slot, start stream với `pgoutput`, nhận và giải mã thành công `InsertMessage` / `InsertMessageV2`.

---

## 2. Nhận đúng transaction đang streaming (proto v2+)

### 2.1 Kiểm tra mã nguồn thư viện tại commit 662581bb6bb6de4da82805879643f1dc39e41fad
- **Cấu trúc message streaming**:
  - `messageV2.go:15-37` định nghĩa `StreamStartMessageV2` (`MessageTypeStreamStart = 'S'`) chứa `Xid uint32` và `FirstSegment uint8`.
  - `messageV2.go:40-50` định nghĩa `StreamStopMessageV2` (`MessageTypeStreamStop = 'E'`).
  - `messageV2.go:53-82` định nghĩa `StreamCommitMessageV2` (`MessageTypeStreamCommit = 'c'`) chứa `Xid`, `Flags`, `CommitLSN`, `TransactionEndLSN`, `CommitTime`.
  - `messageV2.go:85-106` định nghĩa `StreamAbortMessageV2` (`MessageTypeStreamAbort = 'A'`) chứa `Xid`, `SubXid`.
- **Cấu trúc message bên trong stream**:
  - `messageV2.go:160-310`: các struct `InsertMessageV2`, `UpdateMessageV2`, `DeleteMessageV2`, `RelationMessageV2`, `TypeMessageV2`, `TruncateMessageV2`, `LogicalDecodingMessageV2` kế thừa message tương ứng và nhúng thêm `InStreamMessageV2WithXid` chứa `Xid uint32`.
  - Hàm `ParseV2(data, inStream)`: Khi `inStream = true`, parser tự động trích xuất `Xid` (4 byte đầu) trước khi parse phần payload còn lại của tuple/relation.

### 2.2 Kiểm chứng bằng kiểm thử thực tế
- Test thực thi: `tests/audit/pg/audit_test.go:TestAC2_StreamingTxn_ProtoV2`
- Cấu hình server: `logical_decoding_work_mem = 64kB`.
- Kịch bản: Client kết nối với `proto_version '2'`, `streaming 'on'`. Server thực thi transaction lớn (100 row x 2KB = 200KB > 64kB).
- Kết quả: Client nhận đúng chuỗi message streaming trước và khi commit:
  1. `StreamStartMessageV2` (bắt đầu một segment stream)
  2. Các `InsertMessageV2` có `Xid` đi kèm
  3. `StreamStopMessageV2` (kết thúc segment stream)
  4. `StreamCommitMessageV2` khi transaction kết thúc thành công.

---

## 3. Tạo slot kèm exported snapshot và consistent_point

### 3.1 Kiểm tra mã nguồn thư viện tại commit 662581bb6bb6de4da82805879643f1dc39e41fad
- **Tạo slot logic**: `pglogrepl.go:218-231` hàm `CreateReplicationSlot` thực thi lệnh:
  ```sql
  CREATE_REPLICATION_SLOT <slotName> [TEMPORARY] LOGICAL <outputPlugin> [EXPORT_SNAPSHOT]
  ```
- **Kết quả trả về**: `pglogrepl.go:210-215` `CreateReplicationSlotResult` bao gồm:
  ```go
  type CreateReplicationSlotResult struct {
      SlotName        string
      ConsistentPoint string
      SnapshotName    string
      OutputPlugin    string
  }
  ```
- **Parse LSN**: `pglogrepl.go:96-102` hàm `ParseLSN` chuyển đổi chuỗi `ConsistentPoint` (ví dụ `0/16B2D80`) thành `LSN` (uint64).

### 3.2 Kiểm chứng bằng kiểm thử thực tế
- Test thực thi: `tests/audit/pg/audit_test.go:TestAC3_ExportSnapshot_ConsistentPoint`
- Kịch bản:
  1. Bảng có sẵn row 1 (`before_snap`).
  2. Tạo slot với `SnapshotAction: "EXPORT_SNAPSHOT"`. Trả về `SnapshotName` (khác rỗng) và `ConsistentPoint` (khác rỗng).
  3. Mở transaction `REPEATABLE READ`, gán `SET TRANSACTION SNAPSHOT '<SnapshotName>'`, kiểm tra đọc đúng dữ liệu tại điểm snapshot.
  4. Ghi tiếp row 2 (`after_snap`) vào database sau khi tạo snapshot.
  5. Bắt đầu replication stream từ vị trí `consistent_point`.
- Kết quả: Stream bắt đầu chính xác từ `consistent_point`, chỉ nhận row 2 (`after_snap`) và không gửi lại row 1 đã thuộc snapshot ban đầu.

---

## 4. Logical decoding trên standby PostgreSQL 16+

### 4.1 Đánh giá cơ chế Standby Logical Decoding của PostgreSQL 16+
- Từ PostgreSQL 16, PostgreSQL chính thức hỗ trợ Standby Logical Decoding: một standby server (replica nhận WAL từ primary) có thể tạo logical replication slot và stream logical decoding ra bên ngoài.
- Yêu cầu cấu hình:
  - Primary: `wal_level = logical`, `max_wal_senders >= 10`, `max_replication_slots >= 10`.
  - Standby: `wal_level = logical`, `hot_standby_feedback = on`.
  - Đồng bộ WAL: Standby cần nhận checkpoint / running transactions WAL record từ primary khi khởi tạo slot.
- Thư viện `pglogrepl`: Sử dụng giao thức chuẩn `CREATE_REPLICATION_SLOT ... LOGICAL pgoutput` và `START_REPLICATION ...`, tương thích tự nhiên khi kết nối tới Standby node mà không cần code patch riêng.

### 4.2 Kiểm chứng bằng kiểm thử thực tế
- Test thực thi: `tests/audit/pg/audit_test.go:TestAC4_StandbyLogicalDecoding_PG16`
- Kịch bản:
  - Tạo Docker network riêng liên kết Primary container (`postgres:16`) và Standby container (`postgres:16`).
  - Standby được dựng từ Primary bằng `pg_basebackup -R` và chạy với `wal_level=logical -c hot_standby_feedback=on`.
  - Kết nối replication trực tiếp tới Standby, tạo logical slot `standby_slot` và bắt đầu stream.
  - Insert dữ liệu trên Primary: bản ghi được WAL replicate sang Standby.
- Kết quả: Standby giải mã WAL thành logical replication message (`InsertMessage`) và gửi tới client `pglogrepl` thành công 100%.

---

## 5. Đánh giá Two-Phase Commit (proto v3) và TOAST Unchanged Marker

### 5.1 TOAST Unchanged Marker
- `message.go:361-364` định nghĩa:
  ```go
  TupleDataTypeNull   = uint8('n')
  TupleDataTypeToast  = uint8('u')
  TupleDataTypeText   = uint8('t')
  TupleDataTypeBinary = uint8('b')
  ```
- `message.go:372` chú thích rõ: `Byte1('u') Identifies unchanged TOASTed value (the actual value is not sent)`.
- `message.go:422-423`: hàm `TupleData.Decode` xử lý trường hợp `TupleDataTypeToast` mà không sinh lỗi, giữ `Data` là nil/empty. Điều này hoàn toàn khớp với trường `ColValue.Unchanged = true` trong đặc tả lõi của Strand CDC (`spec/02-specs.md §3` dòng 102).

### 5.2 Two-Phase Commit (proto v3)
- Tại SHA `662581bb6bb6de4da82805879643f1dc39e41fad`, `pglogrepl` chưa hỗ trợ các message giải mã 2PC riêng biệt: `'p'` (Prepare), `'P'` (Commit Prepared), `'R'` (Rollback Prepared), `'s'` (Stream Prepare).
- Khi pgoutput không kích hoạt `two_phase 'on'` (chế độ mặc định), mọi transaction kể cả trên PG 16–18 đều được gửi theo dạng tiêu chuẩn (`Begin`/`Commit` hoặc `StreamStart`/`StreamCommit`), `pglogrepl` xử lý hoàn toàn trơn tru.

---

## 6. Khuyến nghị fork: có/không + lý do

- **Khuyến nghị**: **KHÔNG CẦN FORK** cho Phase 0 và Phase 1 của Strand CDC.
- **Lý do**:
  1. Thư viện `github.com/jackc/pglogrepl` tại commit SHA `662581bb6bb6de4da82805879643f1dc39e41fad` đã hỗ trợ đầy đủ toàn bộ tính năng cốt lõi cần thiết:
     - Streaming transaction theo proto v2 (`ParseV2`, `StreamStartMessageV2`, `StreamStopMessageV2`, `StreamCommitMessageV2`).
     - Xuất snapshot và lấy `consistent_point` phục vụ seed ban đầu (`spec/02-specs.md §5`).
     - Nhận diện marker cột TOAST không đổi (`TupleDataTypeToast = 'u'`).
     - Tương thích tốt với PostgreSQL 16, 17, 18 và standby logical decoding.
  2. Đối với Two-Phase Commit (2PC): Strand CDC ở giai đoạn Phase 1 chưa yêu cầu đọc các transaction prepared chưa commit từ nguồn PostgreSQL (nguồn chạy mặc định không cần `two_phase 'on'`). Nếu trong tương lai (Phase 4 hoặc Phase 5) cần xử lý 2PC, việc bổ sung chỉ cần thêm khoảng 4 struct giải mã (khoảng 50 dòng Go) có thể gửi pull request upstream hoặc fork tại thời điểm đó.
  3. Không fork ở giai đoạn này giúp tránh gánh nặng duy trì bản fork riêng và đồng bộ định kỳ với upstream.
- **Quyết định cuối cùng**: Thuộc về người dùng (Owner) theo quy tắc GEMINI.md.
