# Strand CDC — 06 Hướng dẫn giao việc cho Gemini

Oct 31, 2026 · @hieuny

## 1. Mục đích, vai trò, giả định

Gemini viết code theo đúng đặc tả đã chốt. Gemini không thiết kế lại và không tự tuyên bố xong việc. Một task chỉ "xong" khi CI chạy trên commit của PR đạt, và người review kiểm lại được theo mục 10. Lời báo cáo của agent không phải bằng chứng.

| Vai trò | Ai | Được làm | Không được làm |
| --- | --- | --- | --- |
| Owner | Hieuny | Sửa đặc tả, duyệt thay đổi thiết kế, merge PR, quyết định gate | — |
| Implementer | Gemini | Viết code và test trong phạm vi một task card, mở PR, đề xuất thay đổi qua ADR | Sửa đặc tả, merge, đổi task card, chạm production |
| Trọng tài | CI local (hook + ci/run.sh) | Chạy test, sinh bằng chứng, chặn merge | — |

**Giả định** (bạn sửa nếu khác):

- Chạy toàn bộ trên WSL, chỉ dùng git local, không dùng GitHub. Phần hạ tầng nằm ở mục 1.1; bộ script dựng sẵn là `strand-local-kit`.
- Gemini CLI chạy dưới user Linux riêng `strand-agent` và tự nạp `GEMINI.md` ở gốc repo (mục 12).
- Giao việc theo từng task card nhỏ, mỗi task một nhánh `agent/<ID>`. Không giao cả phase một lần.
- Các chỗ trong tài liệu nhắc tới "PR" hay "CI" được hiểu theo bảng ánh xạ ở mục 1.1.

### 1.1 Hạ tầng chạy local trên WSL

**Cập nhật: dùng Antigravity thay cho Gemini CLI.** Antigravity chạy dưới chính user của bạn, nên không còn user `strand-agent`. Ranh giới chuyển sang mật khẩu sudo. Push nhánh `agent/<ID>` đi qua wrapper `push-agent`, không cần mật khẩu. Cập nhật `main` đi qua `push-owner`, bắt buộc mật khẩu. Antigravity chỉ mở workspace `~/strand-work`; repo của bạn là `~/strand`. Antigravity cũng nạp `GEMINI.md` ở gốc workspace. Chi tiết cài đặt nằm trong README của `strand-local-kit`; bảng dưới đây mô tả mô hình ban đầu.

Nguyên tắc: thứ dùng để kiểm Gemini phải chạy dưới user mà Gemini không trở thành được. Git hook phía client không đủ, vì agent bỏ qua được bằng `--no-verify`. Vì vậy hook nằm ở phía nhận (inbox) và chạy dưới user khác.

| User Linux | Vai trò | Quyền |
| --- | --- | --- |
| Owner (bạn) | Repo chính `~/strand` (chmod 700), review, merge | sudo đầy đủ |
| `strand-agent` | Chạy Gemini CLI, clone từ inbox | Không sudo, trừ đúng một lệnh push |
| `strand-gate` | Sở hữu `/srv/strand/inbox.git`, hook, `ci-runs/` | Chỉ được chuyển sang `strand-ci` để chạy CI |
| `strand-ci` | Thực thi code của agent trong CI, Docker rootless | Không ghi được hook hay `ci-runs/` |

| Trên GitHub | Chạy local |
| --- | --- |
| Push | `sudo -u strand-gate git-receive-pack`, cấu hình sẵn trong remote. Hook chạy dưới `strand-gate` |
| Branch protection, CODEOWNERS | `pre-receive` (gate.py): chỉ nhận `agent/<ID>`; từ chối vi phạm R2, R3, R4, R5, R9, R13, R15, R17. Task card và `deps.allow` luôn đọc từ `main` của inbox |
| CI | `post-receive` chạy `ci/run.sh`. File được bảo vệ luôn lấy từ `main`; code của agent chạy dưới `strand-ci` |
| Link CI | `/srv/strand/ci-runs/<sha>/summary.json` kèm log. Agent đọc được, không ghi được |
| PR và mô tả PR | Nhánh `agent/<ID>` + file `reports/<ID>.md`, gate kiểm định dạng |
| Merge | `owner/review.sh <ID>`, rồi `owner/merge.sh <ID>`: chỉ merge khi CI tại đúng SHA cuối là PASS |
| Issue `spec-question` | File `questions/<ID>.md` trong nhánh, rồi agent dừng |

**Bắt buộc dùng distro WSL riêng, tắt interop và automount.** Nếu không, tiến trình của agent gọi được `wsl.exe -u root` hoặc chạm ổ C:, và mọi phân quyền ở trên mất tác dụng. Không bao giờ cho `strand-agent` hay `strand-ci` vào group `docker`, vì group này tương đương quyền root; dùng Docker rootless.

**Bộ tài liệu liên quan:** [thiết kế rev 91](https://claude.ai/artifact/GKQWmLMMgJ6M1MMHcmsJ1V) · [01 Plan](https://claude.ai/artifact/5pYVP92PJHbNAqtmHLkbW3) · [02 Specs](https://claude.ai/artifact/8B8SNKoRdPLyeA9Z7b2HyU) · [03 Deploy](https://claude.ai/artifact/3e6SLJJVBQi1fLq7qVjSg1) · [04 Testing](https://claude.ai/artifact/KWb5Rh4qX7V3ae3g35X1Ei) · [05 Ops](https://claude.ai/artifact/QE1sfX6Qfgp3aoPa5Tak9u)

## 2. Nguồn sự thật và thứ tự ưu tiên

Gemini không đọc được Claude Doc. Vì vậy bạn export toàn bộ bộ tài liệu ra Markdown và commit vào thư mục `spec/` của repo. Đó là bản duy nhất agent được dùng. Khi cập nhật đặc tả, bạn export lại và commit bằng tay; agent không bao giờ tự sửa `spec/`.

| File trong repo | Nguồn | Vai trò |
| --- | --- | --- |
| `spec/02-specs.md` | Tài liệu 02 | Hợp đồng kỹ thuật: bất biến, interface, schema, CLI, metric, mã lỗi |
| `spec/design.md` | Thiết kế rev 91 | Lý do và chi tiết thuật toán |
| `spec/01-plan.md` | Tài liệu 01 | Phase, gate, ma trận phiên bản |
| `spec/03-deploy.md`, `spec/04-testing.md`, `spec/05-ops.md` | Tài liệu 03–05 | Triển khai, test, vận hành |
| `tasks/<ID>.md` | Bạn viết theo mẫu ở mục 4 | Phạm vi của một lần giao việc |
| `spec.lock` | Script sinh ra | SHA-256 của mọi file trong `spec/`; CI kiểm tra (mục 6) |

**Thứ tự ưu tiên khi các nguồn mâu thuẫn:**

1. `GEMINI.md`, phần quy tắc cứng.
2. `spec/02-specs.md`.
3. `spec/design.md`.
4. Task card đang làm.
5. `spec/01`, `spec/03`, `spec/04`, `spec/05`.
6. Code đã có trong repo. Code hiện có không phải là đặc tả.

**Khi thấy mâu thuẫn, hoặc đặc tả không nói rõ:** agent dừng task, mở issue gắn nhãn `spec-question`, trích đúng đoạn văn và đường dẫn file kèm số dòng, nêu các cách hiểu có thể. Agent không tự chọn một cách hiểu rồi code tiếp.

## 3. Quy tắc cứng cho agent

Mỗi quy tắc có mã riêng để người review trích dẫn khi từ chối PR. Vi phạm bất kỳ quy tắc nào thì PR bị đóng, không sửa tiếp trên PR đó. Các quy tắc đánh dấu (CI) có kiểm tra tự động ở mục 6.

**Phạm vi**

- **R1.** Chỉ làm đúng task card được giao. Không refactor, không "tiện tay sửa", không thêm tính năng ngoài task.
- **R2.** Chỉ sửa file nằm trong danh sách `Files allowed` của task card. (CI)
- **R3.** Một task là một PR, trên nhánh `agent/<task-id>`. Không push vào `main`.
- **R4.** Không thêm dependency ngoài `deps.allow`. Cần thêm thì mở issue `dep-request`. (CI)

**Đặc tả**

- **R5.** Không sửa `spec/`, `spec.lock`, `GEMINI.md`, `tests/required.yaml`, `.github/`. (CI + CODEOWNERS)
- **R6.** Không đổi tên hoặc chữ ký của interface, bảng metadata, lệnh CLI, flag, exit code, mã lỗi, tên metric đã có trong `spec/02-specs.md`.
- **R7.** Không đổi giá trị mặc định hay ngưỡng trong đặc tả (ví dụ 4.096 holes, 64 KB delta, 50.000 khóa, 5 s timeout).
- **R8.** Muốn thay đổi thiết kế thì viết ADR trong `adr/proposed/` và dừng task. Chỉ khi owner chuyển ADR sang `adr/accepted/` và cập nhật `spec/` thì mới được code theo.

**Test**

- **R9.** Không xóa, skip, đổi tên hay nới ngưỡng bất kỳ test nào có trong `tests/required.yaml`. Cấm `t.Skip`, `//go:build ignore`, và cấm sửa assert cho khớp với output sai. (CI)
- **R10.** Test integration và e2e phải chạy trên MySQL/PG thật (testcontainers hoặc compose). Không mock DB ở các tầng này.
- **R11.** Test cho một bất biến phải ghi ID bất biến đó trong tên test, ví dụ `TestCP1_KillDuringCompaction`. (CI)
- **R12.** Không hard-code kết quả mong đợi chỉ để test qua. Không viết code nhận biết đang chạy trong test để đổi hành vi.

**Báo cáo trung thực**

- **R13.** Không bao giờ viết rằng một lệnh đã chạy hoặc đã pass nếu không dán output thật của lần chạy đó. Không tự viết output, không tóm tắt thay output.
- **R14.** Số liệu hiệu năng chỉ lấy từ artifact do CI sinh ra, không ước lượng hay làm tròn đẹp.
- **R15.** Việc chưa xong thì ghi `NOT DONE` kèm lý do. Không dùng các cụm như "should work", "hoàn thành về cơ bản", "đã test kỹ".
- **R16.** Mọi khẳng định về thư viện hay hành vi của DB phải kèm nguồn kiểm chứng được: file và dòng code, link tài liệu chính thức, hoặc output của test. Không có nguồn thì ghi `UNVERIFIED`.

**An toàn**

- **R17.** Không truy cập DB production, không dùng dữ liệu thật, không commit credential hoặc dump. (CI: secret scan)
- **R18.** Không chạy lệnh phá hủy ngoài môi trường compose của chính task: `DROP`, `rm -rf` ngoài thư mục tạm, `git push --force`, `git reset` trên nhánh chung.
- **R19.** Không log giá trị row, chỉ log PK đã hash (02, mục 12).
- **R20.** Không chắc chắn thì dừng và hỏi. Dừng đúng lúc luôn được đánh giá cao hơn hoàn thành sai.

## 4. Quy trình một task

Bạn viết task card, Gemini làm theo 6 bước cố định. Điểm mấu chốt là test phải được commit trước và **đỏ trên CI** trước khi có code. Nếu không, không có bằng chứng là test thật sự kiểm tra được điều gì.

**Mẫu task card** (`tasks/P1-07.md`):

```markdown
# P1-07: Ghi row delta có điều kiện epoch
Phase: P1 · Bất biến: CP-1, SG-1 · Phụ thuộc: P1-05, P1-06

## Đọc trước
- spec/02-specs.md mục 2 (CP-1, SG-1), mục 4 (quy tắc ghi 3, 6)

## Phạm vi
Writer.ApplyBatch ghi UPDATE _strand_worker_delta ... WHERE epoch = ? trong cùng
transaction dữ liệu; ảnh hưởng != 1 row thì rollback và trả lỗi mã `fenced`.

## Ngoài phạm vi
Compaction, lease etcd, metric.

## Files allowed
internal/writer/**, internal/checkpoint/**, tests/integration/writer_*_test.go

## Tiêu chí nghiệm thu (mỗi dòng = ít nhất 1 test)
- AC1 TestSG1_StaleEpochRollsBack: epoch cũ -> rollback, 0 row dữ liệu bị ghi, lỗi `fenced`
- AC2 TestCP1_DeltaSameTxn: kill sau commit -> delta chứa Txn vừa ghi
- AC3 MySQL không bật CLIENT_FOUND_ROWS vẫn đếm đúng (nhờ batch_no)

## Bằng chứng bắt buộc
- Link CI run cho commit test (đỏ) và commit code (xanh)
- Artifact: test-report.xml, coverage cho package đã sửa
```

**Sáu bước của agent:**

1. **Đọc và diễn giải lại.** Trong mô tả PR, viết lại task bằng lời của mình và liệt kê bất biến bị ảnh hưởng. Hiểu khác task card thì dừng và hỏi.
2. **Lập kế hoạch:** danh sách file sẽ sửa và test sẽ viết. Có file ngoài `Files allowed` thì dừng.
3. **Commit test trước** (`test(P1-07): ...`), push lên, để CI chạy và báo đỏ đúng chỗ mong đợi.
4. **Commit code** (`feat(P1-07): ...`), vừa đủ để test xanh.
5. **Mở PR** theo mẫu báo cáo ở mục 5, dán link cả hai lần chạy CI.
6. **Sửa theo review** bằng commit mới. Sau khi review đã bắt đầu thì không force-push, để lịch sử còn nguyên.

**Kích cỡ task:** mỗi task nên dưới khoảng 400 dòng thay đổi, không tính test. Task lớn hơn thì chia nhỏ. Agent làm sai dễ nhất ở các task lớn và mơ hồ.

## 5. Mẫu báo cáo bắt buộc

Mô tả PR phải theo đúng mẫu này, lưu ở `.github/pull_request_template.md`. Trạng thái của mỗi tiêu chí chỉ được là `PASS`, `FAIL`, `NOT DONE` hoặc `BLOCKED`. `PASS` phải kèm tên test và link CI; thiếu link thì người review coi là `NOT DONE`.

```markdown
## Task
P1-07 — <tiêu đề>

## Tôi hiểu task là
<2–4 câu, bằng lời của agent; liệt kê bất biến bị ảnh hưởng>

## Tiêu chí nghiệm thu
| AC | Trạng thái | Test | Bằng chứng |
|----|-----------|------|-----------|
| AC1 | PASS | TestSG1_StaleEpochRollsBack | <link CI run, job, bước> |
| AC2 | NOT DONE | — | <lý do> |

## CI
- Commit test (kỳ vọng đỏ): <SHA> — <link run>
- Commit code (kỳ vọng xanh): <SHA> — <link run>

## Lệch khỏi đặc tả
Không có. (hoặc: link ADR trong adr/proposed/ — PR này phải để Draft)

## Khẳng định chưa kiểm chứng
<mỗi dòng ghi UNVERIFIED: ...; để trống nếu không có>

## Việc còn lại / rủi ro biết trước
<liệt kê; không được để trống nếu có AC khác PASS>

## Tự kiểm (đánh dấu thật, không đánh dấu hết cho đẹp)
- [ ] Chỉ sửa file trong Files allowed
- [ ] Không sửa spec/, tests/required.yaml, .github/
- [ ] Không thêm t.Skip, không nới ngưỡng test
- [ ] Mọi số liệu trong PR đều lấy từ artifact của CI
```

**Cụm từ bị cấm trong báo cáo** (CI quét mô tả PR và gắn cờ nếu gặp): "should work", "should pass", "đã test kỹ", "hoàn thành về cơ bản", "production-ready", "tương tự như", "all tests pass" khi không có link CI đi kèm.

## 6. Cơ chế chống thay đổi tự ý và báo cáo ảo

Không tin vào lời hứa của agent; tin vào những thứ agent không sửa được. Quy tắc trong `GEMINI.md` chỉ giảm số lần vi phạm; cơ chế kỹ thuật dưới đây mới chặn được vi phạm. Nguyên tắc quan trọng nhất là **tách người viết test nghiệm thu khỏi người viết code**: test trong `tests/required.yaml` do bạn viết, hoặc do một agent khác viết và bạn duyệt. Gemini không tự viết bài kiểm tra cho chính mình.

| # | Cơ chế | Chặn được | Cách làm |
| --- | --- | --- | --- |
| G1 | Branch protection trên `main` | Push thẳng, tự merge, force-push | Bắt buộc PR, bắt buộc status check, bắt buộc review của CODEOWNERS, bỏ approval cũ khi có commit mới. Token của agent không có quyền admin hay bypass |
| G2 | CODEOWNERS | Sửa đặc tả, sửa test bắt buộc, sửa CI | `spec/`, `spec.lock`, `tests/required.yaml`, `tests/acceptance/`, `GEMINI.md`, `.github/`, `deps.allow`, `adr/accepted/` → chỉ owner duyệt |
| G3 | `spec.lock` | Sửa đặc tả lặng lẽ | CI tính SHA-256 của `spec/**` và so với `spec.lock`; lệch thì fail |
| G4 | `tests/required.yaml` | Xóa, skip, đổi tên test | CI chạy `go test -json`, đối chiếu: mọi test trong danh sách phải có mặt, đã chạy và PASS. Thiếu hoặc bị skip là fail |
| G5 | Cấm skip | Vô hiệu hóa test | CI tìm `t.Skip`, `testing.Short()`, build tag `ignore` trong `*_test.go`; có thì fail |
| G6 | Files allowed | Làm ngoài phạm vi | CI đọc task card theo tên nhánh `agent/<ID>`, so `git diff --name-only` với glob trong task card |
| G7 | Đỏ trước, xanh sau | Test không kiểm tra gì | Job `red-first` checkout commit `test(<ID>)`, chạy test mới và **phải thấy fail**. Test pass khi chưa có code thì bị gắn cờ |
| G8 | Checker tự kiểm | Checker giả vờ báo 0 lệch | Build Strand với failpoint cài lỗi (bỏ một Txn, apply trùng, đảo thứ tự). Checker **phải báo lỗi** với cả ba bản; không báo thì CI fail (04, mục 3) |
| G9 | Mutation testing (nightly) | Test yếu, chỉ chạy qua code | Chạy công cụ mutation cho Go trên `internal/checkpoint`, `internal/sched`, `internal/snapshot`. Điểm mutation không được giảm so với `main` |
| G10 | Số liệu chỉ từ CI | Bịa số benchmark | Benchmark chỉ chạy trong job CI, xuất JSON làm artifact. Người review đọc số từ artifact, không đọc từ mô tả PR |
| G11 | Quét mô tả PR | Báo cáo mơ hồ | Action kiểm tra đủ các mục của mẫu (mục 5), `PASS` phải có link CI, và không chứa cụm từ bị cấm |
| G12 | `deps.allow` | Thêm thư viện lạ | CI so `go.mod` với danh sách cho phép |
| G13 | Secret scan | Lộ credential | gitleaks trên mọi PR |
| G14 | Coverage không giảm | Code mới không có test | Coverage của package bị sửa không được thấp hơn `main` |
| G15 | Kiểm lại ngẫu nhiên | Thứ CI không bắt được | Cứ khoảng 1/5 PR, bạn tự chạy lại `make e2e SEED=<seed trong log CI>` trên máy mình |

**Những gì các cơ chế này không chặn được:** code đúng với test nhưng sai với ý đồ thiết kế, hoặc lỗi chỉ lộ ra ở tải thật. Phần này vẫn cần review của người (mục 10) và các gate.

## 7. Quy chuẩn code

Các quy chuẩn này được kiểm bằng `golangci-lint` và CI ở mức có thể. Phần còn lại nằm trong checklist review.

**Ngôn ngữ và công cụ**

- Go bản stable mới nhất, ghim bằng dòng `toolchain` trong `go.mod`. Build tĩnh với `CGO_ENABLED=0` cho linux/amd64 và linux/arm64.
- Linter bật: `errcheck`, `govet`, `staticcheck`, `gosec`, `revive`, `errorlint`, `contextcheck`, `depguard` (theo `deps.allow`), `forbidigo`.
- `forbidigo` cấm `fmt.Print*` ngoài `cmd/`, `time.Sleep` trong test (dùng clock giả), `log.Fatal`, và `panic` ngoài code khởi tạo.

**Cấu trúc và interface**

- Bố cục package đúng theo 02, mục 3. Package mới cần ADR.
- Mọi hàm có I/O nhận `context.Context` làm tham số đầu. Không dùng biến toàn cục có thể thay đổi.
- Mỗi goroutine có chủ rõ ràng và dừng được qua context. Khi pipeline dừng, không còn goroutine nào rò ra; test kiểm bằng `goleak`.
- Thời gian lấy qua interface `Clock` tiêm vào, để test không phụ thuộc giờ thật.
- Cấu hình decode YAML ở chế độ strict: key lạ là lỗi, exit code 2.

**Lỗi và log**

- Lỗi bọc bằng `%w`. Lỗi ra ngoài phải mang mã trong 02, mục 12, qua kiểu `*strand.Error{Code, Exit}`. Không tự đặt mã mới.
- Log bằng `log/slog` dạng JSON. Trường cố định: `pipeline`, `component`, `code`. Không log giá trị row (R19).
- Code cài đặt một quy tắc trong đặc tả phải có comment trỏ ngược, ví dụ `// spec: 02 §4 rule 3` hoặc `// invariant: CP-1`, để người review truy được.

**Test**

- Unit test dạng table-driven; property test dùng `rapid`; integration dùng testcontainers-go.
- Tên test cho bất biến có tiền tố là ID bất biến (R11).
- Mọi test ngẫu nhiên in seed ra log và nhận `SEED` từ biến môi trường để chạy lại đúng kịch bản.

**Commit:** Conventional Commits kèm ID task, ví dụ `feat(P1-07): fence writer by epoch`.

**`deps.allow` ban đầu** (owner duyệt; mỗi thư viện phải qua audit P0-03 trước khi dùng thật):

| Mục đích | Thư viện |
| --- | --- |
| Binlog MySQL | `github.com/go-mysql-org/go-mysql` (hoặc bản fork nếu P0-03 quyết định) |
| pgoutput, driver PG | `github.com/jackc/pglogrepl`, `github.com/jackc/pgx/v5` |
| Driver MySQL | `github.com/go-sql-driver/mysql` |
| Spool | `github.com/cockroachdb/pebble` |
| CLI, TUI | `github.com/spf13/cobra`, `github.com/charmbracelet/bubbletea` |
| Hash, nén | `github.com/zeebo/xxh3`, `github.com/klauspost/compress` |
| Metric | `github.com/prometheus/client_golang` |
| YAML | `gopkg.in/yaml.v3` |
| Test | `testcontainers-go`, `pgregory.net/rapid`, `go.uber.org/goleak`, `github.com/pingcap/failpoint` |

## 8. Cấu trúc repo, Makefile, CI

Phần khung này (task **P0-00**) do bạn tự dựng, hoặc nhờ một agent khác dựng rồi bạn duyệt, **trước khi** giao task đầu tiên cho Gemini. Lý do: nó chứa chính các cơ chế kiểm soát Gemini. Agent tự dựng hàng rào cho mình thì hàng rào đó không có giá trị.

**Khi chạy local (mục 1.1):** không có `.github/`. Thay vào đó là `/srv/strand/gate/` (gate.py, hook, `ci/run.sh`) do `strand-gate` sở hữu. Danh sách test bắt buộc dùng `tests/required.json`. Bộ `strand-local-kit` đã dựng sẵn toàn bộ phần này, kèm task card và test nghiệm thu cho P0-01.

```text
strand/
├── GEMINI.md                 # quy tắc cho agent (mục 12), CODEOWNERS
├── spec/                     # bản export của tài liệu, chỉ owner sửa
├── spec.lock                 # SHA-256 của spec/**
├── tasks/                    # task card, owner viết
├── adr/{proposed,accepted}/  # đề xuất thay đổi thiết kế
├── deps.allow
├── cmd/strand/
├── internal/                 # theo 02, mục 3
├── spec/tla/                 # mô hình TLA+ (owner duyệt)
├── tests/
│   ├── required.yaml         # danh sách test bắt buộc, chỉ owner sửa
│   ├── acceptance/           # test nghiệm thu gate, không do Gemini viết
│   ├── integration/  e2e/  fault/
│   └── checker/              # checker độc lập (04, mục 3)
├── dev/                      # compose, init SQL, toxiproxy.json
├── scripts/guards/           # G3–G6, G11, G12
└── .github/{workflows,CODEOWNERS,pull_request_template.md}
```

**Makefile:**

| Target | Làm gì |
| --- | --- |
| `make build` | Build binary cho amd64 và arm64, kiểm binary < 50 MB |
| `make lint` | `golangci-lint` + các guard script |
| `make test` | Unit + property, có `-race` |
| `make it` | Integration với testcontainers |
| `make up` / `make down` | Bật/tắt môi trường compose |
| `make e2e SEED=` | E2E + checker |
| `make fault SEED=` | Bộ fault injection (04, mục 4) |
| `make tla` | Chạy TLC trên `spec/tla/` |
| `make check` | `lint` + `test` + `it`; agent chạy trước khi mở PR |
| `make spec-lock` | Sinh lại `spec.lock`; chỉ owner chạy |

**Workflow CI:**

| File | Chạy khi | Job |
| --- | --- | --- |
| `pr.yml` | Mọi PR | lint, test, it (MySQL 8.4), G3–G7, G11–G14 |
| `nightly.yml` | 02:00 mỗi ngày | Ma trận phiên bản (04, mục 6), e2e, fault, fuzz 1 giờ, G8 checker tự kiểm, G9 mutation |
| `gate.yml` | Chạy tay trước gate | Toàn bộ `tests/acceptance/` + benchmark, xuất artifact JSON |
| `tla.yml` | `spec/tla/**` đổi | TLC với biên ở 02, mục 12 |

## 9. Backlog

P0 được chia sẵn thành task, ghi rõ ai làm. P1 mới ở mức danh sách task; task card chi tiết viết sau Gate 0 (tài liệu 01, mục 5). Cột "Ai làm" tuân theo nguyên tắc tách quyền ở mục 6: việc tạo ra cơ chế kiểm soát, việc cần dữ liệu thật, và việc ra quyết định đều không giao Gemini.

**Phase 0:**

| ID | Task | Ai làm | Tiêu chí nghiệm thu chính | Bằng chứng |
| --- | --- | --- | --- | --- |
| P0-00 | Khung repo, CI, guard G1–G15, mẫu PR | Owner (hoặc agent khác, owner duyệt) | Một PR cố ý vi phạm từng guard phải bị CI chặn | Link CI của các PR thử |
| P0-01 | Môi trường compose (03, mục 3) | Gemini | `make up` xanh dưới 5 phút trên runner; user và quyền đúng 03, mục 2 | Log CI có thời gian |
| P0-02 | Lấy binlog production, đo peak | Owner | Có phê duyệt; có số peak event/s, MB/s | File số liệu |
| P0-03a | Audit go-mysql: tagged GTID 8.4, event 9.7, `COM_BINLOG_DUMP_GTID` | Gemini viết báo cáo + test; owner quyết định fork | Mọi khẳng định trỏ tới file:dòng tại một SHA cố định; test parse chạy trên MySQL 8.4 và 9.7 thật | `docs/audit/go-mysql.md` + link CI |
| P0-03b | Audit pglogrepl: pgoutput v1–v4, PG 16–18 | Gemini; owner quyết định | Như trên | Như trên |
| P0-04 | Kiểu `Txn`, `Pos`, JSON Schema cho `seed.json` | Gemini soạn; owner đưa vào `spec/` | Khớp 02, mục 3 và 9; không có trường `seq` khi serialize | Test serialize + link CI |
| P0-05 | Prototype writer theo writeset + công cụ replay | Gemini (code); owner chạy trên staging | Replay binlog tổng hợp trong CI, checker 0 lệch; xuất JSON throughput, độ rộng DAG | Artifact CI; số staging do owner chạy |
| P0-06 | Thí nghiệm watermark RS-2 | Gemini viết harness; owner viết assert trong task card | 10.000 vòng trên 8.4 và 9.7, 0 vi phạm | Artifact CI có số vòng thật |
| P0-07 | TLA+: Scheduler, Checkpoint, RangeState, Handover | Gemini soạn; owner duyệt ánh xạ bất biến | TLC xanh ở biên 02, mục 12; mỗi module có một biến thể cài lỗi và TLC **phải** tìm ra phản ví dụ | Log TLC trong CI cho cả hai bản |
| P0-08 | Đo RAM/CPU | Owner chạy `gate.yml` | p50/p99 RSS, CPU ở peak | Artifact |
| P0-09 | GRACE sidecar (stretch) | Gemini, nếu còn thời gian | Chạy được với Debezium trên compose | Artifact |
| P0-10 | Báo cáo Gate 0 | Owner | Theo 01, mục 4 | — |

**Phase 1 — danh sách task (card chi tiết viết sau Gate 0):**

| ID | Task | Bất biến | Ai làm |
| --- | --- | --- | --- |
| P1-01 | Nạp cấu hình strict, khung `validate`, `config schema` | — | Gemini |
| P1-02 | Reader MySQL: GTID, tagged GTID, resume bằng GTID set | CP-2 | Gemini |
| P1-03 | Txn Assembler | — | Gemini |
| P1-04 | Spool Pebble, tầng local | — | Gemini |
| P1-05 | Bảng `_strand_*`, khóa pipeline, tăng `epoch` | SG-1 | Gemini |
| P1-06 | Checkpoint toàn cục, compaction, trần delta | CP-1, CP-4 | Gemini |
| P1-07 | Writer ghi delta có điều kiện `epoch` (mẫu ở mục 4) | CP-1, SG-1 | Gemini |
| P1-08 | Scheduler writeset phân cấp, `writeset_budget` | CO-1, CO-2 | Gemini |
| P1-09 | Writer MySQL: multi-row upsert, group commit | CP-5 | Gemini |
| P1-10 | Chunking + range-state machine cho MySQL | RS-1, RS-2 | Gemini |
| P1-11 | UPDATE đổi PK qua hai range | RS-1 | Gemini |
| P1-12 | `seed plan`/`apply`: XtraBackup | SD-1..SD-4 | Gemini |
| P1-13 | `seed plan`/`apply`: mydumper, CLONE | SD-1..SD-4 | Gemini |
| P1-14 | Archive catch-up, prefetch, spool tràn sang bucket | SD-2 | Gemini |
| P1-15 | `verify sample`/`delta` | — | Gemini |
| P1-16 | Governor `onoff` + breaker theo tín hiệu DB | — | Gemini |
| P1-17 | `promote` chạy lại được | PR-1 | Gemini |
| P1-18 | Admin socket, `top`, `pause`, `throttle` | — | Gemini |
| P1-19 | Metric, `/healthz` | — | Gemini |
| P1-20 | Unit systemd, manifest FKE | — | Gemini |
| P1-21 | Checker độc lập + bộ fault (04, mục 3–4) | Tất cả | **Không phải Gemini** |
| P1-22 | Test nghiệm thu Gate 1 trong `tests/acceptance/` | Tất cả | **Không phải Gemini** |

## 10. Checklist nghiệm thu của người review

Mỗi PR mất khoảng 10–20 phút nếu đi đúng thứ tự dưới đây. Đọc bằng chứng trước, đọc code sau; đọc mô tả của agent cuối cùng.

**Mỗi PR:**

- [ ] Mọi check của `pr.yml` xanh, và chạy trên đúng SHA cuối của PR, không phải một commit cũ.
- [ ] Job `red-first` cho thấy test mới đỏ trên commit `test(<ID>)`.
- [ ] Mở link CI trong bảng AC. Tên test trong log khớp với tên ghi trong PR.
- [ ] Đọc phần thân test trước phần code: assert có kiểm đúng điều AC nói không, hay chỉ kiểm "không lỗi"?
- [ ] Tìm trong diff: số cứng trùng với kết quả mong đợi; nhánh `if` chỉ chạy trong test; `recover()` nuốt lỗi; `_ = err`.
- [ ] Comment `// spec:` / `// invariant:` trỏ đúng mục, và code làm đúng điều mục đó viết.
- [ ] Mục "Lệch khỏi đặc tả" ghi "Không có", và diff thật sự không đổi tên, ngưỡng hay hành vi nào trong 02.
- [ ] Cứ khoảng 1/5 PR: chạy lại trên máy mình với cùng `SEED` (G15).

**Dấu hiệu báo cáo ảo:** gặp một trong số này thì tự chạy lại toàn bộ trước khi tin.

- Số liệu quá tròn, hoặc quá đẹp so với lần trước.
- Output dán trong PR không có timestamp, hoặc không khớp định dạng log thật của CI.
- PR nói "đã chạy trên MySQL 9.7" nhưng CI của PR chỉ có job 8.4.
- AC nào cũng `PASS` và mục rủi ro để trống, trong khi task khó.
- Agent trả lời câu hỏi review bằng lời giải thích thay vì bằng link log mới.

**Trước mỗi gate:**

- [ ] Chạy `gate.yml` bằng tay. Đọc số trực tiếp từ artifact JSON.
- [ ] Kiểm lại job checker tự kiểm (G8) ở nightly gần nhất: cả ba bản cài lỗi đều bị bắt.
- [ ] Đọc lại toàn bộ `adr/accepted/` kể từ gate trước, đối chiếu với `spec/` đã cập nhật.
- [ ] Báo cáo gate do bạn viết, không dán lại tóm tắt của agent.

## 11. Những việc không giao cho agent

Có ba nhóm việc không giao cho Gemini: việc tạo ra cơ chế kiểm soát chính agent, việc chạm dữ liệu hoặc hệ thống thật, và việc ra quyết định.

| Việc | Ai làm | Lý do |
| --- | --- | --- |
| Sửa `spec/`, duyệt ADR | Owner | Đặc tả là thước đo; người bị đo không được sửa thước |
| CI, guard script, CODEOWNERS, `tests/required.yaml` | Owner, hoặc agent khác và owner duyệt | Như trên |
| Checker độc lập, test nghiệm thu gate | Owner, hoặc agent khác và owner duyệt | Tách người ra đề khỏi người làm bài |
| Quyết định fork thư viện, qua hay không qua gate, governor mặc định | Owner | Quyết định có hệ quả dài hạn |
| Truy cập DB production, binlog thật, staging có dữ liệu thật | Owner | R17; còn vướng phê duyệt compliance |
| Chạy `promote`, `slot drop`, `checkpoint rewind` ngoài compose | Owner | Thao tác không hoàn tác được |
| Release, ký binary, chọn license | Owner | Trách nhiệm pháp lý và bảo mật |
| Báo cáo gate | Owner | Không dựa vào tóm tắt của agent |

"Agent khác" có thể là Claude Code hoặc một phiên Gemini riêng, miễn là nó không thấy code đang được kiểm tra trong lúc viết test.

## 12. GEMINI.md đặt ở gốc repo

Gemini CLI tự nạp file này vào ngữ cảnh mỗi phiên. File viết bằng tiếng Anh vì agent làm theo quy tắc tiếng Anh ổn định hơn; riêng mô tả PR thì yêu cầu viết tiếng Việt. Mã quy tắc R1–R20 khớp với mục 3, để khi từ chối PR bạn chỉ cần ghi mã. File đầy đủ được gửi kèm trong cuộc trò chuyện; phần dưới là nội dung chính.

**Bản dùng thật là bản local**, nằm trong `strand-local-kit/template/GEMINI.md`. Bản này thay PR bằng `reports/<ID>.md`, thay link CI bằng `ci-runs/<sha>`, và câu hỏi được viết vào `questions/<ID>.md`. Trích đoạn dưới đây là bản GitHub, giữ lại để dùng khi chuyển lên GitHub sau này.

```markdown
# GEMINI.md — Rules for the Strand CDC implementer agent

You are the IMPLEMENTER. You write code and tests for ONE task card at a time.
You do NOT design, you do NOT change the specification, and you do NOT decide
that work is done. Work is done only when CI passes on the PR head commit and
the owner merges it.

## 0. Before every task
Read tasks/<TASK-ID>.md and every spec section it lists. If anything is
ambiguous or two sources conflict: STOP, open an issue labelled spec-question,
quote file + line numbers, list interpretations. Do not pick one.

## 1. Priority: GEMINI.md > spec/02-specs.md > spec/design.md > task card
> spec/01,03,04,05 > existing code (existing code is NOT the spec)

## 2. Hard rules R1–R20 (violation closes the PR)
Scope R1–R4 · Specification R5–R8 · Tests R9–R12 ·
Honest reporting R13–R16 · Safety R17–R20   (full text in the file)

## 3. Workflow
restate task -> plan files/tests -> commit tests first (CI must be RED)
-> commit code (CI GREEN) -> PR with both CI links -> review via new commits

## 4. Code standards
context first, no mutable globals, goleak, injected Clock, %w + spec error
codes, slog JSON, // spec: / // invariant: comments, seeded randomness

## 5. Report
PR in Vietnamese, exact template, AC status PASS|FAIL|NOT DONE|BLOCKED,
PASS requires test name + CI link.
```

**Cách giao một task cho Gemini** (câu lệnh mẫu):

```text
Làm task tasks/P0-01.md theo đúng GEMINI.md. Không làm gì ngoài task card.
Nếu gặp điều không rõ hoặc mâu thuẫn, dừng lại và mở issue spec-question.
Khi xong, mở PR theo mẫu và dán link cả hai lần chạy CI (đỏ rồi xanh).
```
