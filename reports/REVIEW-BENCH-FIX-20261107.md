# Sửa review R06 — benchmark fail-open

Nhánh `agent/review-bench-fix-20261107`, nền `c2fab665`; worktree riêng `/home/hellc/strand-bench-review`. Không merge/push. Không chạy benchmark thực tế.

Decision cũ chấp nhận thiếu telemetry, NaN p2p, không đủ governor/repeat hoặc workload lỗi. Sửa: JSON ghi số mẫu valid/invalid trong snapshot; run và decision kiểm coverage, p99, workload errors, cửa sổ p2p hữu hạn; đủ đúng ba repeat độc lập mỗi governor; threshold p2p áp lên maximum của mọi cửa sổ. Giữ ngưỡng spec/04-testing §5, không sửa spec. Giao thức được làm rõ trước lượt đo chính thức.

File sửa: `tools/bench/main.go`, `main_test.go`, `review_test.go`; cập nhật protocol và HANDOFF. Không chạm đường dữ liệu/CP/RS/SD/CO.

Xác nhận hành vi cũ: 9 tình huống thiếu bằng chứng đều vẫn trả “làm mặc định” khi tạm revert logic decision. Giữ hai trường dữ liệu mới trong struct khi revert để test biên dịch được. Một lượt revert nguyên file trước đó chỉ lỗi compile vì thiếu các trường này; không tính là bằng chứng regression. Một lần gọi script thiếu PATH không chạy được Go, đã sửa môi trường và chạy lại; không tính là kết quả test.

Bản sửa: `go test ./tools/bench -count=1` qua, 20 test/subtest pass, 0 fail, 0 skip trong JSON; `-race` cũng 20 test/subtest qua; build và vet toàn repo của nhánh qua. Log: `/tmp/strand-fix-evidence-20261107/{bench-tests.json,revert-bench-behavior.log,bench-build.log,bench-vet.log}`.

Chưa có số đo governor hay kết luận Gate 2. Cần tiếp tục kiểm công cụ tải client/sysbench/pgbench, staging, restore và protocol trước khi đo thật. Không dùng smoke ngắn để chọn mặc định.

Bổ sung kiểm sampler bằng HTTP server riêng: chỉ có R, thiếu lag đều phải invalid; total pressure = 0 là dữ liệu hợp lệ, không được thay bằng signal replica_lag khác. Ba ca này đỏ khi tạm revert sampler về c254d59, xanh trên bản sửa. JSON test cuối: `bench-tests-final.json`, `bench-race-final.json`; log đỏ `revert-bench-sampler.log`. Không chạy tải benchmark.
