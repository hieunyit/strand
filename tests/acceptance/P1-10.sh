#!/usr/bin/env bash
set -uo pipefail

echo "=== Kiểm tra P1-10: Snapshot chunk + range-state (MySQL) ==="
FAIL=0

echo "--- AC1: TestRS1 snapshot add dưới tải cập nhật dày, 0 lệch ---"
if go test -race -v -run ^TestRS1_SnapshotAdd_HeavyConcurrentUpdates$ ./internal/snapshot; then
  echo "  [PASS] AC1: TestRS1 snapshot add 0 lệch"
else
  echo "  [FAIL] AC1: TestRS1 snapshot add 0 lệch"
  FAIL=1
fi

echo "--- AC2: SELECT > 5 s thì chia đôi chunk, chunk 1 row thì không áp timeout ---"
if go test -race -v -run ^TestRS_ChunkSplitOnTimeout_SingleRowNoTimeout$ ./internal/snapshot; then
  echo "  [PASS] AC2: chia đôi chunk khi timeout, chunk 1 row bỏ qua timeout"
else
  echo "  [FAIL] AC2: chia đôi chunk khi timeout, chunk 1 row bỏ qua timeout"
  FAIL=1
fi

echo "--- AC3: row lớn hơn max_chunk_bytes không gây vòng lặp ---"
if go test -race -v -run ^TestRS_LargeRowExceedsMaxChunkBytes_NoInfiniteLoop$ ./internal/snapshot; then
  echo "  [PASS] AC3: row vượt max_chunk_bytes không lặp vô hạn"
else
  echo "  [FAIL] AC3: row vượt max_chunk_bytes không lặp vô hạn"
  FAIL=1
fi

echo "--- AC4: chunk đuôi xử lý đúng ---"
if go test -race -v -run ^TestRS_TailChunk_HandledCorrectly$ ./internal/snapshot; then
  echo "  [PASS] AC4: chunk đuôi [b_last, +inf) xử lý đúng"
else
  echo "  [FAIL] AC4: chunk đuôi [b_last, +inf) xử lý đúng"
  FAIL=1
fi

if [ $FAIL -eq 0 ]; then
  echo "=== KẾT QUẢ: TẤT CẢ AC ĐÃ PASS ==="
  exit 0
else
  echo "=== KẾT QUẢ: CÓ AC THẤT BẠI ==="
  exit 1
fi
