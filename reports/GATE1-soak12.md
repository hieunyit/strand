# Soak 12h (soak12) — kết quả cuối (cứu sau khi driver bị OOM-kill)
- Driver soak bị kernel OOM-kill 2026-11-04 00:41:43 (anon-rss 21 GB) sau ~12h05m, trong pha cuối (compareFinalState nạp toàn bộ bảng vào RAM). Không có report.json.
- Mốc cuối trong log (11h50m): ops=10,650,498, restarts=103, faults kill9=45 sigterm=43 sigstop=54, strand rss 29–33 MB, lag_gtids=0.
- 06:36: chạy lại `strand sync` cùng binary/cấu hình: checkpoint target = source gtid_executed = 77f670cc-…:1-4452513 (đã bắt kịp, không có GTID thiếu). Dừng bằng SIGTERM.
- Verify full (Merkle, strand build be155d5):
  - app.accounts: 163 chunks, 0 mismatched (1,608,724 rows; count+crc32 sum khớp)
  - app.orders: 306 chunks, 0 mismatched (3,038,213 rows)
  - app.events: 6,514 chunks, 0 mismatched (65,113,907 rows), 1843 s, RSS 30 MB
  - app.ignored: source 20,000 / target 0 (đúng bộ lọc exclude)
