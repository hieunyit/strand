# Strand

Strand đồng bộ dữ liệu **một chiều** từ CSDL nguồn sang CSDL đích bằng CDC (đọc binlog MySQL hoặc logical replication PostgreSQL), rồi cho phép cutover (promote) ứng dụng sang đích.

| Nguồn → Đích | |
| --- | --- |
| MySQL 5.5 / 5.6 / 5.7 / 8.x → MySQL 8.x | Có |
| MySQL 5.5 / 5.6 / 5.7 / 8.x → PostgreSQL 16–18 | Có |
| PostgreSQL → PostgreSQL | Có |

## Build

Cần Go 1.25 trở lên.

```bash
git clone https://github.com/hieunyit/strand.git && cd strand
CGO_ENABLED=0 go build -trimpath -o strand ./cmd/strand
./strand version
sudo install -m 0755 strand /usr/local/bin/strand
```

Gói `.deb` / `.rpm` + tarball: `VERSION=1.0.0 scripts/package.sh` (kết quả ở `dist/`).

## Bắt đầu

- **Chuyển MySQL 5.x lên 8.x / PostgreSQL:** [docs/MYSQL-5X-MIGRATION.md](docs/MYSQL-5X-MIGRATION.md). Mục 0 (build) và mục A (chạy thử từ đầu đến cutover, các lệnh đã chạy thật).
- **Tham chiếu đầy đủ** (mọi khóa cấu hình, mọi lệnh, metric, exit code, PG→PG): [docs/USAGE.md](docs/USAGE.md).
- `strand --help`, `strand <lệnh> --help`.

Luồng cơ bản:

```bash
export SRC_PW=... DST_PW=...
strand validate -c pipeline.yaml   # kiểm nguồn/đích/cấu hình
strand plan     -c pipeline.yaml   # ước lượng thời gian snapshot
strand sync     -c pipeline.yaml   # snapshot + stream (chạy thường trực, Ctrl-C dừng an toàn)
strand top      -c pipeline.yaml   # trạng thái, lag
strand verify   -c pipeline.yaml --mode full --table app.t
strand promote  ...                # cutover
```
