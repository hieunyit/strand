# Báo cáo Audit thư viện go-mysql

- **Thư viện**: `github.com/go-mysql-org/go-mysql`
- **Commit SHA đã ghim**: `25d16412ce8e38a35fab7d81988e28974ba14a20` (nhánh `master`, ngày 31/10/2026)
- **Tác giả audit**: Implementer Agent (Strand CDC)
- **Phạm vi audit**:
  1. Đọc stream binlog có tagged GTID của MySQL 8.4
  2. Parse các event của MySQL 9.7
  3. Resume bằng `COM_BINLOG_DUMP_GTID` với GTID set có lỗ (gap/holes)

---

## 1. Hỗ trợ Tagged GTID của MySQL 8.4

### 1.1 Kiểm tra mã nguồn thư viện tại commit 25d16412ce8e38a35fab7d81988e28974ba14a20
- **Định nghĩa hằng số sự kiện**: `replication/const.go:100` khai báo hằng `GTID_TAGGED_LOG_EVENT` (giá trị 100), ánh xạ sang chuỗi `"Gtid_tagged_log_event"` tại `replication/const.go:210-211`.
- **Cấu trúc sự kiện**: `replication/event.go:605-625` định nghĩa `type GtidTaggedLogEvent struct` bao gồm các trường `SID`, `Tag mysql.Tag`, `GNO int64`, và cờ transaction metadata.
- **Hàm giải mã sự kiện**: `replication/event.go:609-720` giải mã payload nhị phân của `Gtid_tagged_log_event`, đọc tag qua `msg.GetFieldByName("tag")` tại `replication/event.go:706-711`.
- **Giải mã trong parser**: `replication/parser.go:335-336` bắt `case GTID_TAGGED_LOG_EVENT:` và khởi tạo `&GtidTaggedLogEvent{}`.
- **Cập nhật GTID set trong syncer**: `replication/binlogsyncer.go:994-1005` xử lý `case *GtidTaggedLogEvent:` và gọi `b.currGset.(*mysql.MysqlGTIDSet).AddGTIDWithTag(u, event.Tag, event.GNO)`.
- **Hỗ trợ Tag trong PreviousGTIDsEvent**: `replication/event.go:294-315` hỗ trợ đọc định dạng tagged GTID (`format == mysql.GtidFormatTagged`) từ header của event.
- **Kiểu dữ liệu MysqlGTIDSet**: `mysql/mysql_gtid.go:63` định nghĩa `type MysqlGTIDSet map[uuid.UUID]map[Tag]IntervalSlice`, lưu trữ các khoảng interval theo từng tag riêng biệt.

### 1.2 Kiểm chứng bằng kiểm thử thực tế
- Test thực thi: `tests/audit/mysql/audit_test.go:TestAC1_TaggedGTID_MySQL84`
- Kết quả: Container `mysql:8.4` sinh transaction với `SET @@SESSION.gtid_next = 'automatic:strand_tag'; INSERT ...; COMMIT;`.
- Thư viện `go-mysql` bắt đúng `*replication.GtidTaggedLogEvent` với `Tag = strand_tag` và `GNO = 1`.

---

## 2. Tương thích và Parse event của MySQL 9.7

### 2.1 Kiểm tra mã nguồn thư viện tại commit 25d16412ce8e38a35fab7d81988e28974ba14a20
- **Hỗ trợ Transaction Payload Event (Zstd compression)**: `replication/const.go:98` định nghĩa `TRANSACTION_PAYLOAD_EVENT`, và `replication/parser.go:355` giải nén uncompressed buffer rồi đệ quy giải mã các event bên trong.
- **Hỗ trợ đầy đủ Row Events V2**: `replication/parser.go:343-348` giải mã `WRITE_ROWS_EVENTv2`, `UPDATE_ROWS_EVENTv2`, `DELETE_ROWS_EVENTv2`.
- **Hỗ trợ JSON binary format**: `replication/json_binary.go:15-150` và `replication/json_mysql_text.go:14-30` xử lý kiểu dữ liệu JSON bản mới của MySQL.

### 2.2 Kiểm chứng bằng kiểm thử thực tế
- Test thực thi: `tests/audit/mysql/audit_test.go:TestAC2_Events_MySQL97`
- Kết quả: Khởi chạy `mysql:9.7` qua Testcontainers, thực thi chuỗi DDL, Insert, Update, Delete với cột JSON.
- `go-mysql` parse thành công 100% các event: `FormatDescriptionEvent`, `PreviousGTIDsEvent`, `GTIDEvent`, `QueryEvent`, `TableMapEvent`, `WriteRowsEventV2`, `UpdateRowsEventV2`, `DeleteRowsEventV2`, `XIDEvent`. Không có lỗi giải mã nào xảy ra.

---

## 3. Hành vi Resume bằng COM_BINLOG_DUMP_GTID với GTID set có lỗ

### 3.1 Kiểm tra mã nguồn thư viện tại commit 25d16412ce8e38a35fab7d81988e28974ba14a20
- **Lệnh COM_BINLOG_DUMP_GTID**: `replication/binlogsyncer.go:765-820` hàm `writeBinlogDumpGTIDCommand` đóng gói mã lệnh `COM_BINLOG_DUMP_GTID` (0x1e) kèm flag, server-id, binlog filename, position và chuỗi byte mã hóa của GTID set.
- **Mã hóa GTID set dạng nhị phân**: `mysql/mysql_gtid.go:320-370` hàm `Encode()` tuần tự hóa cấu trúc `MysqlGTIDSet` thành định dạng nhị phân mà MySQL server yêu cầu (gồm số lượng SID, UUID, số khoảng interval, các cặp [start, stop)). Cấu trúc này hỗ trợ tự nhiên nhiều interval không liên tục (khoảng trống / lỗ).

### 3.2 Kiểm chứng bằng kiểm thử thực tế
- Test thực thi: `tests/audit/mysql/audit_test.go:TestAC3_Resume_Holes_GTID`
- Kịch bản: MySQL 8.4 đã commit các transaction `1..12`. Client kết nối lại với GTID set có lỗ (omitted GNO 10, gửi `1-9:11-12`).
- Kết quả: Server MySQL 8.4 gửi lại đúng transaction thiếu `GNO = 10` và bỏ qua các transaction đã có trong set (`1..9` và `11..12`). Streamer của `go-mysql` nhận đúng event và giải mã hoàn chỉnh.

---

## 4. Khuyến nghị fork: có/không + lý do

- **Khuyến nghị**: **KHÔNG CẦN FORK** ở giai đoạn hiện tại.
- **Lý do**:
  1. Thư viện `github.com/go-mysql-org/go-mysql` tại commit SHA `25d16412ce8e38a35fab7d81988e28974ba14a20` đã tích hợp đầy đủ PR #952 và PR #1125, hỗ trợ hoàn chỉnh MySQL 8.4 Tagged GTID (`Gtid_tagged_log_event`, `PreviousGTIDsEvent` với tag, `MysqlGTIDSet` với map Tag).
  2. Tương thích tốt với MySQL 9.7 (đã kiểm chứng qua testcontainers `mysql:9.7`).
  3. Mã hóa và gửi `COM_BINLOG_DUMP_GTID` với GTID set có lỗ hoạt động chuẩn xác theo giao thức MySQL.
  4. Việc duy trì fork riêng sẽ tạo thêm chi phí bảo trì upstream merge; do bản upstream hiện tại đáp ứng đầy đủ yêu cầu của Strand CDC Phase 0 & Phase 1, khuyến nghị tiếp tục ghim bản commit này.
- **Quyết định cuối cùng**: Thuộc về người dùng (Owner) theo quy tắc GEMINI.md.
