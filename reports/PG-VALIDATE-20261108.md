# PostgreSQL preflight — 2026-11-08

`strand validate` trước đây báo thành công khi publication thiếu, hoặc slot cấu hình thiếu/là physical. Nay kiểm tra catalog chỉ đọc: WAL logical, quyền REPLICATION, standby, publication và membership các bảng chọn, ba thao tác insert/update/delete, partition root, slot được khai báo (type/plugin/database/WAL/invalidation). Slot active chỉ cảnh báo vì không xác định được owner; slot thiếu nhưng `create_slot=true` được chấp nhận, validate không tạo slot. Các thay đổi không mở stream hay ghi checkpoint.

`TestPGValidateChecksCDCPrerequisites` trên PostgreSQL 18: code cũ FAIL vì xác nhận sai publication thiếu, slot physical và slot thiếu; bản sửa PASS (20,13 giây) với các ca đó, publication thiếu bảng, role thiếu REPLICATION, slot hợp lệ, slot thiếu cho phép tạo, slot đang được pipeline dùng. SQL kiểm không tạo slot. Bằng chứng `/tmp/strand-matrix-20261108/pg-validate-old-red.log`, `pg-validate-final-green.log`. Reviewer đã đọc source. Chưa test riêng wal_level sai, standby, invalidation và partition root trên database thật trong lượt này.

**Phạm vi còn mở (#33):** Nếu không cấu hình slot tường minh, seed state có thể chọn tên khác; validate cảnh báo và không khẳng định slot nào. Row filter và danh sách cột publication chưa kiểm, luôn cảnh báo; kiểm tra này chưa chứng minh đầy đủ mọi điều kiện CDC/privilege. Slot active có thể thuộc consumer khác; sync quyết định khi bắt đầu.

Slot bị invalid/lost hiện validate trả exit 3 (`binlog_config_invalid`), trong khi reader trả exit 5 (`log_gap`); khác biệt mã lỗi này chưa được đồng nhất.
