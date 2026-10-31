# Strand CDC — 05 Operations Manual

Oct 31, 2026 · @hieuny

## 1. Vòng đời pipeline và lệnh thường dùng

Một pipeline đi qua `validating` → `bootstrapping` → `synced`, rồi kết thúc ở `promoted` hoặc bị hủy. Bảng trạng thái đầy đủ nằm ở tài liệu 02, mục 12; cách cài đặt ở tài liệu 03. Trên VM, mọi lệnh qua admin socket cần `--socket /run/strand/<pipeline>/admin.sock`; trên FKE thì chạy bằng `kubectl exec` vào pod.

| Muốn | Lệnh |
| --- | --- |
| Kiểm tra cấu hình trước khi chạy | `strand validate -c p.yaml` rồi `strand plan -c p.yaml` |
| Xem tình trạng trực tiếp | `strand top` |
| Xem vị trí đã apply | `strand checkpoint show` |
| Tạm dừng snapshot nhưng giữ stream | `strand pause`, rồi `strand resume --snapshot-only` |
| Dừng hẳn | `strand pause` |
| Giảm tải lên source ngay | `strand throttle set --source-iops <n> --soft` |
| Thêm bảng vào pipeline đang chạy | Sửa `tables.include`, rồi `strand snapshot add --table <t>` |
| Kiểm dữ liệu | `strand verify --table <t> --mode sample` (`delta`, `full`) |
| Dừng process an toàn | SIGTERM: `systemctl stop strand@<p>` hoặc scale StatefulSet về 0 |

**Nguyên tắc vận hành:**

- Không bao giờ ghi tay vào `_strand_*` trên target. Muốn đổi vị trí thì dùng `checkpoint rewind`.
- Không dùng `kill -9` cho thao tác thường ngày. Nó vẫn an toàn, nhưng làm Strand phải đọc lại phần chồng lấn.
- Mỗi `pipeline_id` chỉ chạy một process. Thấy `pipeline_locked` thì tìm process kia, đừng xoá khóa.
- Trước mọi thao tác có rủi ro, chạy với `--dry-run` trước.

## 2. Runbook khởi tạo

Chọn chế độ theo kết quả `strand plan`. Chế độ A dùng cho bảng nhỏ và vừa. Chế độ B dùng khi `plan` từ chối A vì ETA vượt 50% retention, hoặc khi source không chịu được IOPS của snapshot. Chế độ C chỉ dùng khi target đã có dữ liệu nhất quán tại một vị trí biết chắc. Một pipeline có thể trộn: bảng lớn dùng B, bảng còn lại dùng A.

**Chế độ A — snapshot logic:**

- [ ] `strand validate` không còn lỗi. Cảnh báo đã được đọc và chấp nhận.
- [ ] `strand plan`: ETA < 50% retention của binlog/WAL.
- [ ] Đặt `resources.source` soft/hard theo năng lực source, khung giờ `bootstrap.logical.windows`, và query p99 cho breaker.
- [ ] Chạy `strand sync`; theo dõi `strand top` và `strand_snapshot_ranges`.
- [ ] Pipeline chuyển `synced`; chạy `verify --mode sample`.

**Chế độ B — seed từ backup:**

- [ ] Archive binlog/WAL phủ từ thời điểm backup đến nay. Bật Object Lock nếu storage hỗ trợ.
- [ ] `strand seed plan --backup <uri> --type <kind>`. Mọi mục trong `seed.json` phải `ok` / `match: true`; `backup_expires_at` phải sau ETA restore.
- [ ] Spool đủ cho prefetch: tốc độ log ở peak × ETA × 2, hoặc có `spool.overflow`.
- [ ] `strand seed apply --manifest seed.json`. Bị ngắt thì chạy lại đúng lệnh này, nó tự chạy tiếp.
- [ ] `strand sync --start-at seed`. Theo dõi `strand_retention_headroom_seconds` và spool trong lúc catch-up.
- [ ] Strand tự verify (1% range + 30 phút CDC đầu). Chỉ khi cả hai báo 0 lệch thì pipeline mới sang `synced`.
- [ ] Gỡ legal hold (Strand tự gỡ; kiểm lại trong bucket).

**Chế độ C — bắt đầu từ vị trí cho trước:**

- [ ] Có bằng chứng target nhất quán tại đúng vị trí X, ví dụ cắt từ replica native đã dừng ở X. Ghi X vào ticket.
- [ ] `strand sync --start-at <gtid|lsn>`.
- [ ] Chạy `verify --mode sample` ngay, không được bỏ. Chế độ C là ngoại lệ duy nhất mà vị trí do người gõ tay, nên verify là lớp kiểm duy nhất.

## 3. Alert → hành động

"Page" là gọi người trực ngay. "Ticket" là xử lý trong giờ làm việc. Ngưỡng trong tài liệu 03 chỉ là giá trị ban đầu; sau 4 tuần chạy thật, hiệu chỉnh lại theo số alert sai.

| Alert / cảnh báo | Mức | Làm ngay | Runbook |
| --- | --- | --- | --- |
| `StrandVerifyMismatch` | Page | Đóng băng mọi promote; ghi lại range lệch | 4.5 |
| `StrandFenced` / `pipeline_locked` | Page | Xác định có hai process; dừng process thừa | 4.3 |
| `StrandRetentionRisk`, `bootstrap_retention_risk` | Page | Kiểm tra retention và archive; cân nhắc tăng retention | 4.2 |
| `StrandSpoolNearFull` | Page nếu đang seed, còn lại là ticket | Thêm `spool.overflow` hoặc nới PVC | 4.1 |
| `StrandCompactionStuck` | Page | Thu thập goroutine dump, rồi restart | 4.4 |
| `StrandLagSLO` | Ticket; page nếu chạm RPO của DR | Xem governor zone, breaker, target | 4.6 |
| `lag_unsustainable` | Ticket | Hard limit quá thấp so với tốc độ ghi; cân nhắc nâng hard limit hoặc tách pipeline | 4.6 |
| `StrandBreakerOpen` | Ticket | Kiểm tra p99 của ứng dụng: do Strand hay do nguồn khác | 4.6 |
| `pressure_external` | Ticket | Áp suất đến từ nguồn khác; báo team sở hữu DB | 4.6 |
| `strand_sensitivity_unknown` kéo dài | Ticket | Governor chưa đo được ŝ; kiểm `probe` và nguồn metric | 4.6 |
| Pod `CrashLoopBackOff` / unit ngừng vì exit code 2–5, 10 | Page | Đọc mã lỗi trong log; scale về 0 trước khi xử lý | Mục 4 theo mã |

Mọi alert page đều có link tới runbook tương ứng trong annotation `runbook_url` của rule vmalert.

## 4. Runbook sự cố

Nguyên tắc chung: Strand dừng lại chứ không đoán. Người vận hành cũng vậy. Không bỏ qua `log_gap`, không sửa tay `_strand_*`, không xóa khóa. Trước khi sửa, luôn lưu lại bằng chứng: log, `checkpoint show`, phiên bản Strand.

### 4.1 Spool đầy, `prefetch_exhausted`

1. Xem `strand_spool_bytes{tier}`. Nếu chỉ tầng local đầy và chưa có `spool.overflow`, thêm bucket staging vào cấu hình rồi restart. Restart là an toàn.
2. Trên FKE, nới PVC nếu StorageClass cho phép.
3. `prefetch_exhausted` (exit 5) nghĩa là mọi tầng đều đầy. Nếu log trên source hoặc archive vẫn còn phủ từ P, tăng dung lượng rồi chạy lại. Nếu không còn, chuyển sang 4.2.

### 4.2 Rủi ro retention, `log_gap`

1. **Khi mới có rủi ro:** tăng tạm `binlog_expire_logs_seconds` trên source. Với PG, xem `wal_status` của slot trong `pg_replication_slots`, và kiểm tra archive còn chạy.
2. **Khi đã có `log_gap`** (exit 5): tìm log còn thiếu ở archive khác. Tìm được thì trỏ `bootstrap.catch_up.binlog_archive` tới đó và chạy lại.
3. **Log mất thật:** bootstrap lại các bảng bị ảnh hưởng, bằng `snapshot add` hoặc seed mới. Slot PG ở trạng thái `lost` không dùng lại được; phải bootstrap lại toàn bộ pipeline.

### 4.3 `fenced`, `pipeline_locked`

1. Đọc `_strand_pipeline.owner` trên target để biết host/pod và pid đang giữ pipeline.
2. Nếu có hai process đang chạy, dừng process không phải owner.
3. Nếu owner đã chết mà khóa vẫn bị giữ: khóa gắn với một session trên target. Sau khi xác nhận chắc chắn process đã chết, kill đúng session đó (`KILL <id>` với MySQL, `pg_terminate_backend(pid)` với PG).
4. `fenced` sau khi force-delete pod là hành vi đúng: pod cũ bị chặn, không có dữ liệu nào bị ghi trùng. Chỉ cần dọn pod cũ.

### 4.4 Compaction bị treo

1. Gửi `SIGQUIT`: Go in stack của mọi goroutine ra log rồi thoát. Restart sau đó vẫn an toàn nhờ checkpoint.
2. Lưu stack vào bug report. Treo lặp lại là bug ưu tiên cao.

### 4.5 Verify báo lệch

1. Đóng băng mọi `promote` của pipeline.
2. Chạy lại verify trên đúng range đó. Row quá nóng được báo là `unverifiable_hot`, không phải lệch.
3. Nếu lệch thật: lưu PK của range, hash row ở hai phía, phiên bản Strand, `checkpoint show`, log quanh thời điểm đó.
4. Sau khi đã lưu bằng chứng, sửa bằng `verify --repair fix` cho range đó. Đây là bug Sev-1 của Strand cho đến khi chứng minh được nguyên nhân nằm ngoài Strand, ví dụ ai đó ghi tay vào target.

### 4.6 Lag cao, governor giữ tốc độ thấp

1. Xem `strand top`: zone, breaker, tín hiệu nào đang có P cao nhất.
2. Breaker mở: so p99 của ứng dụng với lúc Strand đang chạy. Nếu có `pressure_external`, nguồn áp suất nằm ngoài Strand; báo team sở hữu DB.
3. P cao từ phía target: kiểm IO và lock trên target.
4. `lag_unsustainable`: hard limit thấp hơn tốc độ ghi của ứng dụng. Cân nhắc nâng hard limit, hoặc tách bảng sang pipeline khác.
5. Cần giảm lag tạm thời thì dùng `strand throttle set` nâng soft limit. Không bao giờ nâng vượt hard limit đã thống nhất với team DB.

## 5. Runbook promote / switchover

`strand promote` là cách duy nhất để mở quyền ghi cho target. Sau khi target đã mở ghi, không có đường quay lại tự động: muốn chuyển về source cũ phải dựng một pipeline ngược chiều. Có hai tình huống:

|  | Switchover có kế hoạch | Failover khẩn cấp |
| --- | --- | --- |
| Source | Còn truy cập được | Không truy cập được |
| Sequence độc lập | Đọc chính xác lúc fence | Dùng lần đồng bộ cuối cộng margin; cần người quyết (exit 10) |
| Mất dữ liệu | 0 | Bằng lag tại thời điểm source hỏng |

**Trước khi promote:**

- [ ] Hạ TTL của DNS từ trước, hoặc chuẩn bị sẵn VIP.
- [ ] `strand_lag_seconds{phase="synced"}` gần 0; `verify --mode delta` gần nhất sạch.
- [ ] Danh sách sequence độc lập do `validate` liệt kê đã được team ứng dụng xác nhận.
- [ ] Có credential của `strand_admin`. Team ứng dụng đã sẵn sàng chuyển.
- [ ] Chạy `strand promote --target <dsn> --fence <source> --dry-run` và đọc kế hoạch nó in ra.

**Thực hiện:**

1. Chạy `strand promote` bỏ `--dry-run`. Nó chạy tuần tự: fence source, giữ target chặn ghi của ứng dụng, apply nốt log, setval, mở ghi.
2. Exit 10 (`promote_needs_decision`): Strand in giá trị gợi ý cho từng sequence độc lập. Người vận hành chấp nhận hoặc nhập giá trị cao hơn, rồi chạy lại. Promote sẽ tiếp tục từ bước đang dở.
3. Kiểm tra sau promote: gọi `nextval` trên vài sequence và insert thử một row cho mỗi bảng có identity. Không được có lỗi trùng key.
4. Chuyển DNS hoặc VIP. Ứng dụng ghi vào target trước khi DNS chuyển cũng an toàn, vì source đã bị fence và sequence đã đúng.
5. Dọn pipeline cũ: drop slot PG, gỡ legal hold, lưu `_strand_*` làm bằng chứng rồi xóa sau 30 ngày.

**Nếu promote dừng giữa chừng:** trạng thái pipeline cho biết bước đang dở. Chạy lại đúng lệnh promote, Strand làm tiếp từ bước đó. Không tự mở quyền ghi bằng tay trong lúc setval chưa xong.

## 6. Bảo trì

| Việc | Cách làm | Lưu ý |
| --- | --- | --- |
| Nâng cấp Strand | SIGTERM, thay binary/image, chạy lại (tài liệu 03, mục 8) | Đọc release notes về `meta_version` trước khi nâng; có thay đổi thì không rollback được nếu không chạy migrate lùi |
| Đổi credential | Cập nhật Secret hoặc file env, rồi restart | Restart an toàn; tạo user mới trước, xóa user cũ sau |
| DDL trên source (P1–P2) | Strand dừng khi gặp DDL trên bảng thuộc pipeline. Áp DDL tương đương lên target rồi `strand resume`; Strand kiểm schema target khớp trước khi chạy tiếp | Quy trình tạm thời, đến P3 thì thay bằng schema evolution |
| DDL trên source (từ P3) | Thay đổi cộng thêm: tự động. Thay đổi phá hủy: `strand schema diff`, rồi `strand schema approve <id>` | — |
| gh-ost / pt-osc trên source | Trước P3: tránh, hoặc `pause` pipeline trong lúc chạy rồi xử lý như DDL thường. Từ P3: Strand tự nhận diện | Bảng `_gho`/`_new` không được nằm trong `tables.include` |
| Tua checkpoint | `strand checkpoint rewind --to <gtid\|lsn>` | Chỉ dùng khi target đã bị restore về một thời điểm cũ. Tua về trước trạng thái thật của target sẽ khiến row lùi tạm thời cho đến khi replay xong |
| Dọn slot PG | `strand slot drop` sau khi pipeline đã dừng hẳn | Slot bị bỏ quên sẽ giữ WAL trên primary |
| Đổi replica đọc | Sửa `source.dsn`, rồi restart | MySQL resume theo GTID nên an toàn; replica mới phải còn đủ binlog từ `lwd` |
| Nâng cấp major của source | Xem tài liệu 03, mục 8 | PG 16 lên bản mới hơn thì phải bootstrap lại |

**Định kỳ:**

- **Hàng tuần:** xem kết quả `verify --mode full` theo cron, và các alert sai trong tuần.
- **Hàng tháng:** đối chiếu dung lượng spool, archive và retention với tốc độ sinh log hiện tại.
- **Hàng quý:** diễn tập promote trên staging theo mục 5, có bấm giờ. Ghi lại RPO và RTO thực tế.
