# G2-12 PostgreSQL — số chốt trước lượt chính thức (2026-11-16 07:50:53)
- PostgreSQL 18.6 (postgres:18) source/target, wal_level=logical
- low/high = 51 / 205 txn/s (calibration.json); spike 2x high 10 s mỗi 3 phút; chu kỳ 10 phút
- soft/hard IOPS = 400 / 600; seed 20261113; fixture bench.big 40.000.000 row (Amendment 2026-11-16), pgbench scale 1
- client pgbench 18.6 upstream sha256 ffc2c2e3fbc810c4491362954c40791132d99ae4f9804b907b95a5abae93014b (không vá; không có pilot overhead — Amendment 2026-11-16 §2)
