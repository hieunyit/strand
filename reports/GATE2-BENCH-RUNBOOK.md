# Gate 2 (G2-12) — runbook đo chính thức 3 governor × 3 lần

Công cụ: `tools/g2bench`. Giao thức và các amendment: `reports/GATE2-BENCH-PROTOCOL.md` (đọc mục Amendment 2026-11-13). Chưa có số đo chính thức nào.

## Yêu cầu phần cứng (staging)

- Máy dành riêng, không chạy việc khác; source + target + Strand + client (có thể cùng máy; nếu client ở máy khác thì NTP lệch < 100 ms).
- Đĩa NVMe ≥ 100 GB trống (fixture tar ~15 GB, hai volume dữ liệu, log thô vài trăm MB), RAM ≥ 32 GB, ≥ 8 core. Docker, image `strand-sysbench-toolchain:20261108`, `mysql:8.4` / `postgres:18`.
- Dữ liệu: bảng `bench.big` ≥ H × 16 KiB × 1500 s (H = 600 → ~67M row, ~14,7 GB); app: 1 bảng sysbench 100.000 row / pgbench scale 1.
- Thời gian: dựng fixture 0,5–1 h; mỗi lần chạy ≈ restore 10 min + manifest 10 min + warmup 2 min + snapshot 25–40 min + stream 2 min + so khớp 10 min ≈ 1–1,2 h; 9 lần ≈ 10–11 h (+1 lần bù có thể) mỗi engine.

## 1. Dựng sysbench đã ghim (MySQL)

```bash
export PATH=$PATH:/usr/local/go/bin
tools/g2bench/toolchain/build-sysbench.sh /path/upstream.tar /opt/g2/sb   # sha256 upstream.tar ghim trong script
sha256sum /opt/g2/sb/sysbench-patched            # → dùng làm --client-sha256
go build -o /opt/g2/g2bench ./tools/g2bench && go build -o /opt/g2/strand ./cmd/strand
```

## 2. Container (ví dụ MySQL; PG tương tự với `postgres:18`, wal_level=logical, mount `/var/lib/postgresql`)

```bash
OUT=/data/g2/out
for n in src dst; do docker volume create g2-my-$n; docker run -d --name g2-my-$n -e MYSQL_ROOT_PASSWORD=bench \
  -p 127.0.0.1:$([ $n = src ] && echo 33801 || echo 33802):3306 -v g2-my-$n:/var/lib/mysql mysql:8.4 \
  --server-id=$([ $n = src ] && echo 11 || echo 12) --gtid-mode=ON --enforce-gtid-consistency=ON --binlog-format=ROW \
  --binlog-row-image=FULL --binlog-row-metadata=FULL --max-connections=300; done
docker run -d --name g2-client --network host -v $OUT:$OUT -v /opt/g2/sb:/opt/g2/sb:ro \
  -v /opt/g2/sb/lua:/usr/local/share/sysbench:ro strand-sysbench-toolchain:20261108 sleep infinity
# PG: client = postgres:18 image, binary /usr/lib/postgresql/18/bin/pgbench, mount $OUT cùng đường dẫn
```

## 3. Fixture (một lần; bất biến)

```bash
COMMON="--engine mysql --source root:bench@tcp(127.0.0.1:33801)/ --target root:bench@tcp(127.0.0.1:33802)/ \
 --source-container g2-my-src --target-container g2-my-dst --helper-image strand-sysbench-toolchain:20261108 \
 --fixture-dir /data/g2/fixture --client-container g2-client --client-binary /opt/g2/sb/sysbench-patched"
/opt/g2/g2bench fixture-create $COMMON --big-rows 67000000 --app-rows 100000
```

PG: `--engine postgres --source 'postgres://strand:dev@127.0.0.1:33811/strand?sslmode=disable' ... --client-binary /usr/lib/postgresql/18/bin/pgbench --pg-scale 1`.

## 4. Pilot overhead và calibrate (trước lượt chính thức; commit kết quả)

```bash
/opt/g2/g2bench overhead $COMMON --pristine-binary /opt/g2/sb/sysbench-pristine --patched-binary /opt/g2/sb/sysbench-patched \
  --out $OUT/overhead.json --duration 2m --rounds 3      # phải pass (≥0,98 / ≤1,05)
/opt/g2/g2bench calibrate $COMMON --out $OUT/calibration.json --duration 3m   # low/high = 12,5% / 50% bão hoà
git add reports/... && git commit   # chốt low/high, soft/hard trước khi chạy
```

## 5. Lượt chính thức

```bash
/opt/g2/g2bench run $COMMON --strand /opt/g2/strand --client-sha256 $(sha256sum /opt/g2/sb/sysbench-patched|cut -d' ' -f1) \
  --low <từ calibration> --high <từ calibration> --soft-iops 400 --hard-iops 600 \
  --seed 20261113 --out $OUT/mysql --official --attest-dedicated
```

Lệnh từ chối chạy nếu thiếu điều kiện OFFICIAL. Kết quả trong `$OUT/mysql`: `plan.json` (ghi trước khi đo), `results.json` (mỗi lần chạy, tổng hợp, `verdict.status`), `NN-gov-R/{run.json,telemetry.csv,timeseries.csv,chart.svg,raw/,strand.log}`. Tính lại từ dữ liệu thô: `g2bench analyze --out $OUT/mysql`.

`verdict.status`: `grace-v2-default` | `onoff-default` | `incomplete` (không có kết quả). Chỉ `official: true` trong results.json mới dùng làm bằng chứng Gate 2.

## Blocker còn lại

Máy staging; chạy pilot overhead và calibrate; chốt S/H/biên độ; chạy PG (cần Strand PG→PG ổn định, xem docs/GOVERNOR-WIRING.md).
