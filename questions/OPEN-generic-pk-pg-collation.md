# Snapshot logic cho PK chuỗi PostgreSQL có collation khác "C" (việc C, mức 3)

Phạm vi việc C, 2026-11-05. Không sửa spec. Chưa có quyết định của owner.

## Hiện trạng

Mức 1 và 2 đã xong (`agent/generic-pk-v2`). Với source PG, cột PK `text`/`varchar`/`bpchar` chỉ được nhận khi collation là `"C"`, `"POSIX"`, `ucs_basic`, hoặc collation mặc định của database là C/POSIX (libc) hay builtin C/C.UTF-8 (`internal/snapshot/pkey.go` `pgPKColumn`, `pkey_catalog.go`).

Mọi collation khác bị từ chối khi `sync`/`snapshot add`, với gợi ý dùng `COLLATE "C"` hoặc seed từ backup. Ví dụ: libc `en_US.UTF-8`, ICU `und-x-icu`, ICU nondeterministic.

Các ca bị từ chối:

- PG→PG mà database tạo với `LC_COLLATE=en_US.UTF-8`. Đây là mặc định của nhiều bản cài, nên mọi bảng có PK `text` hoặc `varchar` bị từ chối.
- Bảng có PK ghép, trong đó có một cột chuỗi như vậy.

## Vì sao không làm như mức 2

Snapshot cần thứ tự khóa trong Go giống hệt thứ tự DB dùng cho `ORDER BY` và `WHERE (pk) > (...)`. Thứ tự đó dùng để gán thay đổi của stream vào chunk.

- MySQL có `WEIGHT_STRING()`: DB tự trả khóa sắp xếp, nên mức 2 dùng được.
- PostgreSQL **không** có hàm SQL nào trả khóa sắp xếp (sort key) của một collation.
- Cài lại collation trong Go bị spec cấm, trừ khi chứng minh được giống hệt. libc phụ thuộc phiên bản glibc (glibc 2.28 đổi thứ tự hàng loạt). ICU phụ thuộc phiên bản ICU mà PG được build. `golang.org/x/text/collate` không bảo đảm khớp phiên bản nào trong hai thứ đó.

Phép bằng không phải vấn đề với collation **deterministic** (mặc định). PG phá hòa bằng so byte (`strcmp`), nên hai chuỗi bằng nhau thì bằng nhau từng byte. Danh tính khóa trong buffer theo byte, như mức 1, vẫn đúng. Chỉ **thứ tự** là khác.

Collation **nondeterministic** (ICU `deterministic = false`, ví dụ không phân biệt hoa thường) thì khác cả phép bằng: `'a' = 'A'`. Ca này giống MySQL `_ci`, nhưng PG không có hàm nào chuẩn hóa khóa.

## Các hướng

### 1. Giữ từ chối (hiện trạng)

- Người dùng có ba cách: seed (pgBackRest, WAL-G, `strand export`), `--start-at`, hoặc đổi cột khóa sang `COLLATE "C"`. Đổi collation là đổi schema của ứng dụng, vì thứ tự `ORDER BY` của ứng dụng cũng đổi.
- Không có rủi ro đúng đắn, không tốn công.

### 2. Chia chunk theo thứ tự byte thay cho collation của cột (khuyến nghị cho collation deterministic)

Ý chính: snapshot không bắt buộc phải dùng collation của cột. Mọi query của snapshot trên source có thể dùng thứ tự byte:

```sql
WHERE (a, b COLLATE "C") > ($1, $2) ORDER BY a, b COLLATE "C"
```

Thứ tự này trùng mã hóa `PKBytes` đã có ở mức 1. Với collation deterministic, phép bằng của cột cũng là phép bằng byte, nên:

- Gán chunk: so byte trong Go = `COLLATE "C"` trong SQL. Đúng.
- Danh tính khóa (buffer, writeset, unique): byte. Đúng.
- Flush ở target: xóa theo range cũng phải dùng `COLLATE "C"`. Target có cùng collation (deterministic), nên tập khóa trong range là như nhau ở hai phía.

Vấn đề là hiệu năng. Index PK có collation của cột, nên planner không dùng nó cho `ORDER BY … COLLATE "C"`. Mỗi chunk sẽ thành seq scan cộng sort, tức O(n²) trên cả bảng. Muốn chạy được cần index biểu thức:

- **Source:** DBA tạo `CREATE INDEX CONCURRENTLY strand_k ON app.t (a, b COLLATE "C")`. Đây là một lần ghi lên source, trái với "Strand không ghi source". Strand không tự tạo. Thiếu index thì từ chối, kèm lệnh cần chạy, như cách đang làm với `publish_via_partition_root`.
- **Target:** Strand tạo index tương tự trước snapshot và xóa sau khi bảng xong, vì target thuộc quyền Strand. Nếu không, mỗi lần flush xóa range sẽ quét cả bảng. Một cách khác: chỉ xóa theo danh sách khóa. Nhưng range cần xóa cả những khóa *vắng* ở source, nên vẫn cần điều kiện range.
- Verify full theo khóa (`merkle_keys.go`) dùng cùng điều kiện, nên cũng cần các index này.

Chưa kiểm thực nghiệm:

- Planner PG 16–18 có dùng index `(a, b COLLATE "C")` cho tuple comparison `(a, b COLLATE "C") > ($1, $2)` không.
- `bpchar` (bỏ khoảng trắng cuối) kết hợp `COLLATE "C"` có giữ đúng ngữ nghĩa `PKTrimBytes` không.

Ước lượng: 2–3 ngày, gồm SQL tuple có collation, cổng kiểm index, index tạm ở target và test (thứ tự trên en_US và ICU, e2e PG→PG dưới tải, kill -9).

### 3. Hỏi source cho từng phép so

Gán chunk bằng cách gửi khóa của event tới source theo lô và để PG so với các biên, ví dụ `SELECT k, count(*) FILTER (WHERE b <= k) FROM unnest($bounds) …`. Đây là cùng ý với mức 2: chỉ hỏi khi bảng đang snapshot.

- Đúng cho mọi collation deterministic. Không cần index biểu thức, vì keyset vẫn dùng index PK.
- Mọi event của bảng đang snapshot phải chờ một round-trip tới source.
- Biên phải so bằng chính collation của source. Target có thể khác phiên bản libc/ICU (khác máy), khi đó flush xóa range ở target theo thứ tự target có thể lệch thứ tự source. Phải kiểm `pg_collation_actual_version` hai phía bằng nhau, nếu khác thì từ chối.
- Phức tạp hơn hướng 2 nhiều, vì mọi chỗ so trong range-state phải thành bất đồng bộ.

### 4. Collation nondeterministic

Không hướng nào ở trên giải được, vì phép bằng không phải byte và PG không có hàm chuẩn hóa. **Đề xuất giữ từ chối.** Ca này hiếm làm PK.

## Đề nghị

1. Owner chọn giữa hướng 1 (giữ nguyên) và hướng 2 (thêm điều kiện index biểu thức do DBA tạo trên source).
2. Nếu chọn hướng 2: chạy thử planner trước (mục chưa kiểm thực nghiệm), rồi mới triển khai.
3. Nondeterministic: giữ từ chối trong mọi trường hợp.

Liên quan: MySQL→PG với khóa chuỗi đang bị từ chối vì một lý do khác (thứ tự MySQL khác `"C"` của PG, cộng PAD SPACE). Hướng 2 không giải ca đó; cần câu hỏi riêng nếu muốn mở.
