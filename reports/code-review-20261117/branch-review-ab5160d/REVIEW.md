> Kết quả sửa sau review: [branch-repair-ab5160d/STATUS.md](../branch-repair-ab5160d/STATUS.md). Nội dung dưới đây giữ nguyên làm bằng chứng lịch sử của ab5160d.

# Review nhánh fix/03-pg-profiling-review

**Kết luận: chưa nên merge.** Các bản sửa mới còn đường ghi/xóa sai identity, kiểm constraint sai, lộ secret, quota chưa đầy đủ và công cụ chẩn đoán có thao tác phá dữ liệu ngoài tài nguyên do nó tạo. Build, vet và test hiện hữu đạt không khép kín các counterexample dưới đây.

Review tại commit `ab5160d24a89d6fbdaa5980da7118186a36f9a2c`, so với `5273009`. Không sửa mã sản phẩm, không commit/push. Các lượt agy cũ đã dừng trước review; mọi repro mới dùng Go overlay ngoài source. Report và evidence là phần bổ sung duy nhất.

## Phạm vi và phân công

| Reviewer | Model | Phạm vi |
|---|---|---|
| Data | GPT-6.1 Terra / high | Archive/sidecar, prefetch/seed, unique constraint và đường replay |
| Surface/storage | GPT-6.1 Sol / high | Scalar masking/unmasking, inventory/sidecar quota, CLI import |
| Tools/evidence | GPT-6 Luna / medium | Diagnostic runners, browser, breaker, MinIO, crash gates và tính đúng của kết luận |
| Điều phối | Model chính | Profiling lifecycle, kiểm chứng counterexample, phần nối module và tổng hợp |

106 file code/test/docs ngoài reports thay đổi so với base. **62 file có SHA256 giống hệt bản đã review ở lượt sửa trước**, nên dùng lại review đó; 44 file mới/khác được phân công đọc trong lượt này. Manifest: [coverage.csv](coverage.csv), [scope.json](scope.json). Đây là phạm vi đọc, không phải 100% runtime coverage hay chứng nhận toàn repository. Các test được đọc theo nhánh/assertion liên quan, không coi việc có test là đã chứng minh tính đúng.

## Findings cần sửa

### B01 — P1: seed prefetch bình thường đọc nhầm SQL schema snapshot thành JSON sidecar

- Vị trí: `cmd/strand/prefetch.go:106–116`; wiring `cmd/strand/seed.go:210–216`.
- Khi không có ArchiveSchema, code tự dùng SchemaSnapshot và luôn gọi LoadSchemaSidecar, **kể cả không dùng archive**. Manifest mysqldump/mydumper/clone hiện có thể trỏ trường này tới SQL dump/metadata/schema.sql.
- `seed apply --prefetch` hợp lệ vì vậy lỗi parse JSON/open file trước khi đọc nguồn; thêm một tính năng archive làm hỏng đường seed hiện hữu.
- **Đã tái hiện bằng overlay không mạng:** đưa SQL `CREATE ...` vào SchemaSnapshot, không có archive, nhận `load archive schema sidecar ... invalid character 'C'`.
- Sửa: chỉ dùng sidecar khi được khai báo rõ định dạng/field; không nạp sidecar cho source-only prefetch. Thêm regression manifest thật của các seed producer.

### B02 — P1: identity archive không được bind với key thực sự của pipeline tiêu thụ

- Vị trí: `internal/reader/archive/schema.go:210–230`; `cmd/strand/prefetch.go:247–256`.
- VerifyRowChangeIdentity chỉ so **số phần tử**, không so tên key/mode/thứ tự với key writer đã resolve. Bảng có hai UNIQUE một cột hoặc cùng key ghép đảo thứ tự đều vượt kiểm tra.
- Spool giữ values theo sidecar, writer dùng tên cột của identity khác; DELETE/UPDATE có thể nhắm sai row và checkpoint transaction đó. Ví dụ UNIQUE(a), UNIQUE(b), hai row (a=1,b=11)/(a=11,b=1): key [1] theo a dùng vào WHERE b=1 sẽ xóa row còn lại.
- **Đã tái hiện acceptance trong unit overlay;** đường replay/writer đã trace, chưa chạy DB end-to-end làm mất row.
- Sửa: persist identity mode + ordered column names/fingerprint trong durable format, kiểm với metadata consumer trước apply; hoặc rekey losslessly từ FULL before/after image. Chưa làm được phải từ chối, không dùng length-only làm bằng chứng.

### B03 — P1: quoted qualified DDL vượt hàng rào schema sidecar

- Vị trí: `internal/reader/archive/reader.go:400–419`, đối chiếu `:435–484`.
- `ALTER TABLE \`app\`.\`accounts\` ...` bị token hóa thành các phần rời. Nếu default schema của QUERY khác `app`, detector không tìm ra bảng sidecar để invalidate.
- Với metadata MINIMAL, đổi thứ tự cột cùng count/type có thể tiếp tục qua verifyAgainstIdentity; tên/key cũ sau đó gán nhầm lên row image mới và GTID được covered.
- **Overlay chứng minh detector bỏ lọt đúng SQL này.** Tác động row mapping theo trace code, chưa dùng binlog do server tạo.
- Sửa: parse SQL identifier đúng hoặc reject bảo thủ khi không xác định chắc phạm vi DDL; bind generation sidecar với log interval, không xác nhận schema từ count/type tùy chọn.

### B04 — P1: hai functional UNIQUE khác biểu thức bị coi là cùng constraint

- Vị trí: `internal/validate/constraint_compat.go:164–173,282–294,495–497`.
- Descriptor/catalog bỏ expression SQL, chỉ giữ HasExpr và ordinary columns. UNIQUE(lower(email)) và UNIQUE(lower(phone)) có thể cùng HasExpr=true, Columns rỗng và được chấp nhận tương thích.
- Với cùng PRIMARY KEY(id) ở hai bên, source vẫn có thể cho hai row hợp lệ theo email nhưng target đụng lower(phone); MySQL ON DUPLICATE KEY UPDATE có thể cập nhật đè row khác.
- **Mock-catalog overlay đã tái hiện unsafe acceptance;** chưa chạy MySQL thật với hai expression indexes.
- Sửa: thu expression semantics có thẩm quyền, xét collation/type/predicate cần thiết; không chứng minh tương đương được thì báo inconclusive/incompatible. Không so tương đương chỉ bằng cờ expression.

### B05 — P2: không đọc PG UNIQUE NULLS NOT DISTINCT

- Vị trí: `internal/validate/constraint_compat.go:549–559,630–639`.
- Catalog query không lấy `pg_index.indnullsnotdistinct`; field tương ứng luôn false. Source nullable UNIQUE thông thường vì vậy được coi tương thích với target UNIQUE NULLS NOT DISTINCT.
- Source cho nhiều NULL, target chỉ cho một; validator báo qua rồi apply gặp duplicate-key. Đây là lỗi tương thích gây dừng, không tự kết luận PG đã ghi đè mất row.
- **Mock-catalog overlay đã chứng minh acceptance sai.** Chưa replay PG thật.
- Sửa: đọc flag theo server version, xét nullable/equality thật; thêm PG 15+ fixture và đường phiên bản cũ không có catalog field.

### B06 — P1: multiline DSN trả password plaintext ra Web

- Vị trí: `internal/web/masktext.go:402–409`; caller configView và các masked diff.
- Khi non-secret-key scalar (`dsn`) không có single-line span, collectSecrets bỏ qua mà không báo lỗi. Folded/block hoặc multiline quoted DSN vẫn có password trong decoded value.
- Ví dụ `source.dsn: >-` chứa `postgres://u:canary@h/db`: MaskConfigText trả nguyên password và nil error. Requires quyền xem config; không phải bypass đăng nhập.
- **Safety tests fail ở HEAD; cùng repro pass trên base 5273009**, chứng minh hồi quy.
- Sửa: tìm secret trong decoded scalar trước; hỗ trợ thay an toàn hoặc fail-closed khi không map được raw span. Không được silently skip.

### B07 — P2: đổi quote YAML làm đổi password DSN dù unmask báo thành công

- Vị trí: `internal/web/masktext.go:637–644`.
- Code chép raw escape của DSN gốc vào syntax scalar mới, không xác nhận expectedVal. Đổi double-quoted DSN có một backslash semantic sang single-quoted placeholder có thể thành hai backslash semantic.
- Credential khác làm kết nối sai sau save; parse YAML thành công không có nghĩa giữ đúng password.
- **Đã tái hiện bằng overlay round-trip có so semantic value.**
- Sửa: khôi phục decoded components, encode lại cho quote/context người dùng sửa; parse kết quả và assert semantic secret đúng, không chỉ assert placeholder biến mất.

### B08 — P2: import inventory cũ ghi đè size của sidecar đang có reference

- Vị trí: `internal/spool/inventory.go:415–422,462–466`.
- Sequence hợp lệ: scan orphan 100B → file tăng thành 200B → Append giữ reference và index size=200 → import report cũ StatusNew. Import vẫn ghi size=100 trong khi refs=1.
- Quota đang chạy bị thiếu 100B; reopen lỗi `changed size while referenced`.
- **Safety overlay dùng filesystem/Pebble thật đã tái hiện.**
- Sửa: revalidate filesystem + current index dưới import lock; không cho stale report thay size/live entry. Scan/import API phải chịu được thời gian trôi giữa hai bước.

### B09 — P2: crash orphan trong thư mục spill chuẩn không được tính sau lần migration đầu

- Vị trí: `internal/spool/sidecar.go:53–59` và migrateSidecars; source spill dir tại `internal/pipeline/pipeline.go:799–801`.
- sidecar_version đã có thì không scan thư mục mặc định nữa. File được assembler tạo rồi crash trước Append/index tồn tại trên disk nhưng vĩnh viễn không được charge khi reopen thông thường.
- **Repro thật:** spool đã init, thêm orphan 4096B trong spill chuẩn, reopen hard limit1024 → Usage=0 và append mới thành công.
- Sửa: reconcile thư mục spill chuẩn đã biết ở mỗi startup, merge unknown paths atomically giữ refcounts; chỉ gate migration DB schema bằng version. Không nhầm với orphan ngoài cây chuẩn cần explicit inventory.

### B10 — P2: CPU profile ghi lỗi nhưng Stop/sync vẫn có thể báo thành công

- Vị trí: `cmd/strand/profile.go:95–99,143–150`.
- Chỉ đưa *os.File cho pprof.StartCPUProfile rồi kiểm Close không thu được error của writer CPU profile.
- **Repro không DB:** chọn `/dev/full` với overwrite, start thành công, Stop trả nil mặc dù mọi write đều lỗi. Việc join lỗi Stop trong sync chưa giải quyết đường này.
- Sửa: wrapper io.Writer ghi nhớ write error một cách thread-safe; sau StopCPUProfile join error đó với close/block errors và trả về RunE.

### B11 — P2: CPU/block ghi chung file mới qua alias relative/absolute

- Vị trí: `cmd/strand/profile.go:38–56`.
- Clean không chuẩn hóa absolute; EvalSymlinks/Stat cùng thất bại nếu file chưa tồn tại. Với overwrite, một relative path và absolute path cùng target được mở hai lần, O_TRUNC và hai định dạng ghi chồng.
- **Overlay đã xác nhận os.SameFile(cpuFile,blockFile)=true dù start không lỗi.**
- Sửa: canonicalize absolute/parent symlink, reserve/open cả hai trước mutation; so inode handle trước truncate/start. Test file chưa tồn tại, không chỉ alias của file đã có.

### B12 — P1: diagnostic artifact làm lộ DSN; overwrite profile không siết quyền

- Vị trí chính: `tools/diagnostics/pg-strict/main.go:248–278`; liên quan `cmd/strand/profile.go:79–88`.
- Với `-auto-docker=false -source/-target` có credentials, harness ghi DSN nguyên văn vào strand.yaml mode0644, outdir0755. Người dùng local khác có thể đọc password và artifact dễ bị đưa vào report/backups.
- Profile mode0600 chỉ có tác dụng lúc tạo mới; overwrite file0644 vẫn giữ0644. **Phần overwrite đã tái hiện bằng overlay**; đường DSN leak dựa trực tiếp trên code, không dùng credentials thật.
- Sửa: private output directory/config0600, redaction cho artifact chia sẻ; chmod/atomic replacement khi overwrite. Không lưu credential-bearing config vào report mặc định.

### B13 — P1: harness xóa/dừng container không do lần chạy đó tạo

- Vị trí: `tools/diagnostics/pg-strict/main.go:131–140`; `tools/diagnostics/browser-repro/run.sh:13,19–20`.
- PG harness dùng tên cố định rồi docker rm -f trước startup và lúc cleanup; browser script tương tự docker stop tên cố định.
- Hai lần chạy đồng thời hoặc container có trước cùng tên khiến một lần phá fixture/dịch vụ của lần khác; PG rm có thể làm mất writable layer. Vi phạm yêu cầu cleanup đúng tài nguyên sở hữu.
- **Bằng chứng source, không chạy lệnh phá container để thử.**
- Sửa: tên/run ID duy nhất, giữ container ID thực sự tạo, cleanup chỉ ID đó; port động/ownership labels. Không dọn tên người dùng có sẵn để giành chỗ.

### B14 — P1: profiler dùng external DSN tự truncate dữ liệu có sẵn

- Vị trí: `tools/diagnostics/pg-strict/main.go:216–218,608–629`.
- Khi tắt auto-docker và chỉ DB có sẵn, setupDatabases vẫn TRUNCATE bench.accounts ở cả nguồn/đích và drop/recreate publication tên cố định. Không có ownership marker hay explicit destructive guard.
- Vì vậy đường diagnostic external-DSN có thể phá bảng/publication đã có của người chạy; không phải collector chỉ đọc.
- **Bằng chứng call path/SQL, không chạy trên DB người dùng.**
- Sửa: chỉ dùng database/schema mới do harness tạo và có ownership proof; mặc định từ chối object có sẵn. Nếu hỗ trợ destructive existing DB phải có opt-in tách biệt và mô tả rõ, không suy từ việc nhập DSN.

### B15 — P2: avg_batch_size là sai đại lượng

- Vị trí: `tools/diagnostics/pg-strict/main.go:408–410,475–476`.
- appliedRows/appliedTxns là rows/source transaction, không phải source txns/target commit batch. Workload một row/txn luôn ra1 kể cả writer gộp nhiều txn/commit.
- Dùng con số đó để giải thích giới hạn strict commit/round-trip sẽ dẫn đến kết luận sai. Trace trong report chính nó có nhiều txn/batch.
- **Bằng chứng công thức và attached txntrace;** không chạy lại benchmark.
- Sửa: đo số target batches/commits thực tế hoặc dùng trace batch IDs; rename metric cũ cho đúng đơn vị. Không lấy raw single-connection TPS làm trần apply TPS nếu batching khác nhau.

### B16 — P2: crash gate không assert state/plan thật sự tồn tại

- Vị trí: `tests/e2e/toast_completion_crash_test.go:117–129`.
- foundState chỉ được log, false không fail. SnapshotStarted cũng có fallback sleep. Kịch bản restart sạch rồi hội tụ vẫn có thể pass trong khi chưa chứng minh resume từ bounds plan mới.
- Báo cáo 09 nói đã kiểm preservation vượt bằng chứng assertion hiện có.
- Sửa: chờ/đọc plan bền đúng table với bound_snapshot, chứng minh pending/reread tại mốc kill và assert file/state sau kill, sau restart; phân biệt clean-bootstrap với resume. Không coi tên file JSON bất kỳ là đủ.

### B17 — P2: currentPhase bị truy cập không đồng bộ trong diagnostic runner

- Vị trí: `tools/diagnostics/pg-strict/main.go:381,415,437`.
- Poller goroutine đọc string trong khi main đổi warmup→steady, không mutex/atomic/channel. Đây là data race trên phase label của timeseries.
- **Bằng chứng phân tích truy cập đồng thời; chưa chạy race với benchmark DB thật.** Aggregate TPS dùng counter boundary riêng nên không kết luận mọi số TPS sai vì race này.
- Sửa: truyền phase qua atomic/mutex hoặc suy từ timestamp so với immutable boundary, join sampler trước khi xuất kết quả. Thêm race-enabled seam không đòi DB nặng.

## Kết luận report/test cần hiệu chỉnh riêng

- **Q01 — Root cause 280–300 TPS chưa được chứng minh.** Report task03 gọi fsync/strict single writer là root cause chính xác, nhưng artifact baseline local khoảng2669.8TPS và `reproduces_280_to_300=false`; không đo fsync trên môi trường lỗi gốc. Đây là giả thuyết có điều kiện, không phải kết luận đã sửa vấn đề của người dùng.
- **Q02 — Gh-ost không có binary thì test log NOT RUN rồi return PASS.** `tests/e2e/remaining_repro_test.go:95–116`; dùng t.Skip hoặc gate riêng bắt buộc binary nếu nghiệm thu real-tool. Report06 có ghi NOT RUN; không chuyển thành “real gh-ost đã qua”.
- **Q03 — Browser stale-response scenario chưa đi qua app.** `tools/diagnostics/browser-repro/suite.mjs:425–451` parse HTML bằng DOMParser rồi tự tính pathname mismatch; không feed delayed response qua refresh/router thực tế. Pass không chứng minh app drop response cũ, nên task07 còn thiếu regression theo plan.
- Task09 stress15 và fullsuite vẫn opt-in/chưa chạy trong artifact đang có; bounded stage không thay thế các gate đó. Kết luận “complete verification all9” trong commit không đủ bằng chứng để coi mọi triệu chứng gốc đã khép kín.

## Kiểm chứng của lượt review

- Build `./cmd/strand ./cmd/replay`, `go vet ./...`: đạt; [build](evidence/build.log), [vet](evidence/vet.log).
- Existing targeted suites ở cmd/web/validate/archive/spool đều đạt: [log](evidence/existing-tests.log).
- **Counterexample ngoài source:** [chạy lại](reproduce.py), [manifest](repro-cases.json), [kết quả](evidence/repro-results.json). Web/spool/profile là safety assertions kỳ vọng fail trên HEAD còn lỗi; constraints/archive/prefetch là observational assertions pass khi chứng minh acceptance/exception sai. Không được diễn giải những PASS đó là đã an toàn.
- Test overlay không dùng DB/customer credentials; filesystem quota/Pebble và YAML/profiler có tái hiện thực. Các metadata/DDL/identity cases chưa phải full live-DB/binlog replay.
- Không chạy harness có cleanup/TRUNCATE nguy hiểm, không chạy fullsuite/stress/browser/benchmark DB trong lượt này. Source branch giữ nguyên, chỉ thêm report.

Chi tiết reviewer: [data](evidence/data-review.md), [surface/storage](evidence/surface-storage-review.md), [tools](evidence/tools-review.md). Hướng ưu tiên: B01–B04/B06 trước; sau đó B08–B09 và B12–B14; sửa regression gate trước khi dùng để tuyên bố hoàn tất.
