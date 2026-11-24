# Ma trận kiểm thử lỗi CDC

Bản rà soát sử dụng `05256ab` trên nhánh `fix/cascade-foreign-key-preflight`. Khi đối chiếu, `origin/main` ở `e49e853`. Sáu agent model nhẹ rà độc lập các nhánh MySQL/MariaDB, PostgreSQL, MongoDB, schema, Web/CLI và checkpoint. Ma trận có 80 kịch bản theo vòng đời pipeline và dùng làm danh sách kiểm thử hồi quy. Mỗi hàng là một điểm kiểm soát, có thể cần nhiều phiên bản engine và nhiều lần chèn lỗi để đạt độ bao phủ thực tế.

| Ký hiệu | Bằng chứng |
| --- | --- |
| M | Unit test hoặc mô phỏng bằng mock đã chạy trong lượt này và đạt |
| E | Kiểm thử có máy chủ DB thật đã đạt. Cột bằng chứng ghi rõ lượt này hay lượt trước |
| L | Phát hiện từ luồng code. Phần phát hiện ghi rõ nếu đã tái hiện |
| C | Có ca hồi quy trong repo hoặc đã đọc code, cần chạy lại kịch bản này |
| T | Thiếu bằng chứng kiểm thử trực tiếp cho tổ hợp này |

## Khởi tạo, cấu hình và điều phối

| ID | Tình huống | Tiêu chí kiểm tra | Bằng chứng |
| --- | --- | --- | --- |
| O01 | YAML sai, cấu hình không hợp lệ | Validate thất bại và UI hiển thị đúng trạng thái | M, TestNewPipelineValidateFailureNotOK |
| O02 | DSN nhập qua Web, lưu và khởi chạy | Lấy đúng bí mật khi chạy và giữ bí mật ngoài argv | M, TestNewPipelineCreation, TestVerifyJobsArgvAndEnv |
| O03 | SSH tunnel dùng cho kết nối nguồn hoặc đích | Chuyển tiếp đúng cổng và dọn tiến trình khi dừng | M, TestNewPipelineSSHTunnelConfig, TestWebTunnelOpensAndStopsVerifiedLoopbackForward |
| O04 | Gateway đổi host key, tunnel đứt giữa lần đọc hoặc commit | Dừng hoặc kết nối lại đúng đích và tiếp tục đúng checkpoint | T |
| O05 | Start, Stop, Restart của tiến trình nền | Mỗi thao tác định tuyến đúng lệnh | M, TestOpsBackgroundLifecycle |
| O06 | Web khởi động lại trong khi job còn chạy | Runner nhận lại đúng tiến trình và lưu kết quả | M, TestRunner.* |
| O07 | PID được tái sử dụng, hai Web instance cùng can thiệp | Xác minh danh tính trước khi gửi tín hiệu | C, kiểm tra guard của job runner với hai instance |
| O08 | Validate thành công nhưng đích đã có dữ liệu hoặc scope thiếu FK liên quan | Validate báo điều kiện sẽ cản Start, Start vẫn kiểm lại ngay trước khi ghi | L, F04 |
| O09 | Add table rồi reload config và restart | Scope lưu bền vững và trạng thái job đồng nhất | E, TestSnapshotAddPersist_E2E_PGSurvivesRestart ở lượt trước |
| O10 | Config bị sửa đồng thời với Start, đổi bảng hoặc promote | Mỗi tiến trình sử dụng một phiên bản cấu hình nhất quán | T |
| O11 | Người dùng Pause, Resume khi spool đang đầy hoặc process đã thoát | UI phân biệt trạng thái và khôi phục đúng điểm | C, TestE2E_LagGrowsWhilePausedUnderSourceLoad |
| O12 | ACL, DB grant hoặc chứng chỉ hết hạn giữa bootstrap và stream | Lỗi đúng bước, có thông tin chẩn đoán và giữ checkpoint an toàn | T |

## MySQL và MariaDB

| ID | Tình huống | Tiêu chí kiểm tra | Bằng chứng |
| --- | --- | --- | --- |
| Y01 | Binlog OFF, định dạng không phải ROW, row image không đầy đủ hoặc thay đổi theo session | Validate/Start chặn cấu hình không thể phát lại chính xác | C, TestBinlogPos_E2E_Rejections |
| Y02 | Statement DML, XA, GTID/BEGIN ở giữa giao dịch | Từ chối dạng chưa hỗ trợ trước khi emit một phần giao dịch | M, TestR09_StatementDMLInsideBeginIsError, TestG2_XA*, TestCP1_GTIDInsideOpenTxnIsError |
| Y03 | Rollback, xoay file, giao dịch vắt qua hai file, start ở event đầu | Ranh giới binlog position đúng, từ chối giao dịch lỗi | M, TestBinlogPos* trong assemble |
| Y04 | Binlog đã purge, seed mất thời hạn retention hoặc nguồn đổi | Từ chối điểm bắt đầu không còn khả dụng | M, bộ test auto-seed/binlog position trong pipeline |
| Y05 | Tagged GTID, GTID set có lỗ, replica hoặc thay nguồn | Replay đúng giao dịch thiếu và xác minh danh tính nguồn | C, tests/audit/mysql và internal/checkpoint |
| Y06 | MariaDB thiếu quyền RELOAD với đích trống | Lấy tọa độ cùng snapshot, tạo schema/khóa ngoại và nạp nhóm | E, TestMDB_E2E_AutoSeedCascadesWithoutReload ở lượt trước |
| Y07 | DML ghi liên tục khi copy nhóm FK | Seed nhất quán và CDC đuổi kịp từ đúng điểm | E, TestMDB_E2E_AutoSeedGroup ở lượt trước |
| Y08 | DELETE/UPDATE CASCADE, SET NULL, RESTRICT, FK nhiều cột và FK vào unique key | Khớp hành động, thứ tự cột và bảng sau rename | M, TestParseMySQLForeignKeys, TestMySQLForeignKeyDDL, TestCO1_ForeignKeyToUniqueIndexOrdersChildAfterParent |
| Y09 | Cascade vòng, tự tham chiếu, nhiều đường, trùng tên sau rename | Phân biệt trường hợp có thể giữ nguyên thứ tự với trường hợp phải chặn | M, TestCheckCascadeOrder, TestCascadeSignatureKeepsUnpinnedFormat, test rename |
| Y10 | Đích có schema nửa chừng, thiếu FK, FK sai hành động, hoặc FK thêm ngoài scope | Tạo phần còn thiếu khi an toàn hoặc từ chối trước khi ghi dữ liệu | M, bộ test cascade/auto-seed pipeline |
| Y11 | Cascade từ/đến bảng bị exclude hoặc FK liên schema | Kiểm tra tập liên kết sau khi áp scope và rename | M, bộ test cascade và rename pipeline |
| Y12 | Tài khoản đích bị ẩn metadata FK dù có quyền DML | Phân biệt thiếu quyền đọc metadata với FK thực sự vắng | L, F05 |
| Y13 | Đích PostgreSQL đã có bảng nhưng thiếu FK cascade của nguồn MySQL | Phát hiện lệch trước CDC và giữ dữ liệu cha-con nhất quán | L, F01 |
| Y14 | Bảng keyless có dòng trùng, NULL và UPDATE/DELETE một trong nhiều dòng giống nhau | Duy trì chính xác số lượng từng giá trị, kể cả sau restart | C, các test keyless và keyless_commit_restart |
| Y15 | Keyless hoặc cascade group thay schema sau seed nhưng trước replay | Chặn phát lại lịch sử theo metadata không còn hợp lệ | E, TestMDB_E2E_CascadeDDLBlocksHistoricalReplay ở lượt trước |
| Y16 | MySQL 5.7 metadata cũ, table map thiếu PK hoặc số cột lệch | Giữ identity đã đăng ký và từ chối decode sai | M, test M57 trong reader mysql và assemble |
| Y17 | Charset latin1, utf8/utf8mb3, collation unique và composite | Decode đúng ký tự, so khóa như engine đích | M, TestDecodeMySQLLatin1Basics, TestCO1_CaseInsensitiveUniqueKeysConflict và test cascade alias |
| Y18 | DDL schema khác có cùng tên bảng | Chỉ drain hoặc đối chiếu bảng thật sự chịu thay đổi | L, F06 |
| Y19 | AUTO_INCREMENT, generated column, kiểu số unsigned, giá trị zero date, JSON, binary | DML và DDL giữ giá trị hoặc chặn trường hợp không biểu diễn được | M với mapping đã chọn, T với toàn bộ tổ hợp engine và giá trị |
| Y20 | Bảng non-InnoDB, trigger, event, routine, khóa metadata đang bị giữ | Chặn engine không an toàn, báo đối tượng ngoài phạm vi CDC | C, Validate và object summary |
| Y21 | Kill giữa lúc tạo schema, copy nhóm, commit đích hoặc ghi seed marker | Restart dùng lại trạng thái hợp lệ và tránh nạp trùng | E, TestMDB_E2E_AutoSeedGroup ở lượt trước với SIGKILL sau khi copy một bảng trước COMMIT. T với các điểm kill khác |
| Y22 | Nhóm FK hàng trăm GB đến TB, binlog vẫn tăng hoặc một bảng nóng | Đo tài nguyên, thời hạn binlog, thời gian hồi phục và tính nhất quán | T |

## PostgreSQL

| ID | Tình huống | Tiêu chí kiểm tra | Bằng chứng |
| --- | --- | --- | --- |
| P01 | wal_level, publication, slot hoặc REPLICA IDENTITY thiếu/sai | Báo lỗi cấu hình hoặc từ chối kết nối trước khi thay đổi dữ liệu | C, pg_validate_cdc_test |
| P02 | Slot mất WAL, retention pressure, timeline đổi hoặc replica promote | Báo mất khả năng resume, không tiến checkpoint vượt vùng bị mất | C, kiểm thử tín hiệu WAL và failover |
| P03 | Snapshot visibility, transaction XID wraparound và giới hạn coverage | Chỉ bỏ qua giao dịch thật sự đã có trong snapshot | M, nhóm unit PGSnapshot của agent |
| P04 | Bảng keyless chạy với REPLICA IDENTITY FULL | Nạp và phát lại UPDATE/DELETE đúng multiset | E, TestKL_E2E_PGToPG_KeylessReplicaIdentityFull ở lượt trước |
| P05 | PK UPDATE có FK CASCADE, DELETE RESTRICT, cột TOAST chưa đổi và SIGKILL | Giữ quan hệ cha-con và giá trị TOAST qua restart | E, TestP2_E2E_PGForeignKeyPKUpdateCascadeResume ở lượt trước |
| P06 | TOAST partial update và unique key hoán đổi | Không ghi đè cột vắng mặt hoặc vi phạm thứ tự unique | E, TestPGWriter_AC1_ToastPartialUpdate, TestPGWriter_UniqueSwapOrdered ở lượt trước |
| P07 | Partition root/leaf, attach/detach khi đang phát lại | Chuyển sự kiện đúng bảng và báo lỗi khi identity thay đổi | C, PG partition replay regressions |
| P08 | Inheritance, bảng con chung key và TRUNCATE nhiều bảng | Snapshot/UPDATE/DELETE giữ scope của từng bảng | C, cần chạy inherited-table và truncate trên nhiều phiên bản |
| P09 | NUMERIC giá trị bằng nhau khác scale, NULLS NOT DISTINCT và collation | Relaxed scheduler tạo cùng khóa xung đột khi database xem là bằng | C, numeric hash và writeset regressions |
| P10 | Add/drop/rename cột giữa snapshot, DDL tự động hoặc cần phê duyệt | Dừng và khôi phục đúng phiên bản schema, giữ giao dịch trước/sau DDL | E, TestP3_E2E_SchemaEvolution_PG_AddColumnAuto trên PostgreSQL 18 ở cả strict và relaxed. C với drop/rename |
| P11 | LSN đã đọc nhưng target commit chưa bền vững | Feedback LSN giữ WAL đủ để replay sau crash | C, f11_boundary_restart_test |
| P12 | MySQL sang PostgreSQL, bảng cha chưa snapshot khi có ghi bảng con | Không làm rơi dữ liệu hoặc áp bảng con quá sớm | C, TestN10_E2E_MySQLToPGFKChildBeforeParentSnapshot |
| P13 | PostgreSQL table đã tồn tại với kiểu, FK/trigger hoặc default khác nguồn | Phát hiện mismatch ảnh hưởng DML và xác minh sau cutover | T |
| P14 | Snapshot có hàng rất rộng, kiểu đặc biệt, khóa lớn, deadlock hoặc serialization failure | Hạn chế bộ nhớ và retry đúng transaction | C, TestReview_E2E_PGWideKeySnapshot và test writer |
| P15 | LSN giữ lâu, load tăng cao, đĩa WAL đích hoặc nguồn đầy | Báo áp lực đủ sớm và khôi phục có kiểm soát | T |

## MongoDB

| ID | Tình huống | Tiêu chí kiểm tra | Bằng chứng |
| --- | --- | --- | --- |
| G01 | Standalone, sharded, cùng deployment, quyền change stream thiếu | Chặn topology không hỗ trợ và quyền thiếu | C, TestCheckTopologyRefusesShardedAndStandalone, TestContainer_AuthTLSLeastPrivilege |
| G02 | Concurrent load lúc snapshot và resume, mixed _id | Giữ chính xác BSON và vị trí snapshot | E, TestContainer_SnapshotUnderConcurrentLoad ở lượt trước. C với mixed _id |
| G03 | Delta update, nested fields, numeric _id, event reorder | Giữ thứ tự và kiểu ID khi dùng delta/lookup | M, TestBatch*, TestResolve*; C với live lookup |
| G04 | Transaction nhiều collection dưới giới hạn | Đồng thời áp tài khoản và sổ cái, không thấy trạng thái trung gian | E, TestContainer_TransactionsAtomicOnTarget ở lượt này với 35 giao dịch, 197 lần quan sát đích và 0 trạng thái trung gian |
| G05 | Transaction vượt giới hạn nội bộ số event hoặc byte | Bảo toàn atomicity hoặc từ chối trước khi ghi giao dịch | L, F02 |
| G06 | Oplog rollover, token quá cũ, invalidate hoặc primary stepdown | Phân biệt mất lịch sử với lỗi có thể retry, resume đúng token | E, TestContainer_OplogRolloverFailsClosed ở lượt này. C với primary stepdown ba node |
| G07 | Hai instance tranh lease, instance cũ commit sau takeover | Writer cũ bị fence và không phát sinh dữ liệu ngoài epoch | E, TestCDCLeaseTakeoverFencesStaleTransaction, TestContainer_FencedAfterLeaseTakeover ở lượt trước |
| G08 | Mất lease giữa DDL, index hoặc validator đang áp | DDL không được phép tiếp tục sau takeover | C, TestCDCLeaseBlocksTakeoverDuringUncertainDDL |
| G09 | Chạy liên tục, cursor không idle | Checkpoint vẫn cập nhật theo thời hạn khi an toàn với ranh giới giao dịch | T |
| G10 | Tạo collection rỗng có validator/index sau snapshot | Collection và metadata xuất hiện ở đích, kể cả chưa từng insert | L, F03 |
| G11 | Rename, drop, drop database, rename cắt qua filter scope | Áp đúng namespace và không chạm collection ngoài scope | E, TestContainer_DropRenameDropDatabase ở lượt trước. T với toàn bộ filter |
| G12 | Index unique, TTL, partial, hidden và index do operator tạo | Áp metadata hợp lệ và giữ index riêng của đích | C, TestContainer_ReplicateIndexesAndOptions |
| G13 | Đích đã chứa collection hoặc dữ liệu trước bootstrap | Từ chối trước khi ghi khi target không đủ điều kiện | E, TestContainer_RefusesExistingTarget ở lượt này |
| G14 | Kill -9 trong snapshot, stream, index reconcile hoặc recopy | Tiếp tục đúng tiến độ và đối chiếu toàn bộ BSON | C, các test Kill9 và Recopy, T với kill giữa DDL |
| G15 | Document sát giới hạn BSON và nguồn ghi nhanh hơn đích | Bộ nhớ và batch có giới hạn rõ, báo lag và phục hồi | C, TestContainer_LargeDocuments. T với tải kéo dài |

## Checkpoint, lưu trữ, xác minh và cutover

| ID | Tình huống | Tiêu chí kiểm tra | Bằng chứng |
| --- | --- | --- | --- |
| H01 | Giao dịch lớn phải spill, hết quota hoặc lỗi flush | Giữ đủ cột và writeset, dọn file lỗi, báo quota chính xác | M, TestCP1_SpilledTxnKeepsAllColumns, TestSpillByteBudget, TestSpillFlushFailureRemovesFile |
| H02 | Spool sidecar missing, quota hoặc crash khi truncate | Giữ tham chiếu còn dùng và dừng khi file đã cam kết bị mất | M, TestSpoolTruncateCrashNeverLeavesDanglingBucketRefs, nhóm TestSidecar |
| H03 | Object storage đã Put thành công nhưng crash trước ghi tham chiếu local | Dọn object mồ côi và chạy tiếp nhất quán sau retry | T |
| H04 | Prefetch payload sai hoặc format qua nâng cấp | Đọc format cũ hợp lệ, từ chối contract hỏng | M, TestPrefetchCodecRefusesMissingOrMalformedContracts. C với nâng cấp trực tiếp |
| H05 | Tác vụ xung đột, lỗi worker hoặc queue đầy | Không release giao dịch phụ thuộc khi apply thất bại | M, TestCO1_DoneWithErrorDoesNotReleaseDependents, TestCO1_DoneDoesNotDeadlockWhenReadyQueueFull |
| H06 | Giao dịch đã spill xung đột với giao dịch in-memory | Giữ thứ tự theo bảng, hàng và các quan hệ liên quan | M, TestCO1_SpilledTxnOrdersAgainstConflictingTxns |
| H07 | Relaxed ACK có lỗ, giao dịch cỡ lớn vượt queue | Chặn độ lệch quá mức và tránh deadlock | M, TestCP4_SchedulerStopsOnMaxHoles, TestN02_OversizedLaneTxnNoDeadlock |
| H08 | Crash giữa checkpoint compaction, prune và cập nhật epoch | Tránh mất dấu giao dịch đã apply và tránh ACK bởi epoch cũ | C, compaction_test và fencing tests |
| H09 | Target nhận COMMIT nhưng kết nối đứt trước khi trả lời | Khi resume, xác định commit từ checkpoint thay vì áp trùng | T, cần fault injection vào đúng thời điểm |
| H10 | Đứt mạng nguồn/đích, reconnect hoặc timeout ngẫu nhiên | Replay đúng điểm, không phải can thiệp khi lỗi tạm thời | C, TestG1_E2E_NetworkFaultsSourceAndTarget |
| H11 | Hai tiến trình tranh lock khi writer hoặc snapshot đang bị block | Chỉ instance giữ epoch còn hiệu lực được ghi | C, TestSG1_PipelineLockSecondInstance và fencing |
| H12 | Verify/repair khi nguồn còn ghi, keyless checksum, filter hoặc rename | So sánh đúng scope tại mốc nhất quán và phân biệt drift với lag | C, verification_scope_test và keyless tests |
| H13 | Promote đủ dữ liệu, retry, abort, failover và mất nguồn | Web lưu đúng chuyển trạng thái và Core báo mốc hoặc dữ liệu có thể mất | M với Web wizard. C với TestP1_E2E_FailoverPromoteShowsLossAndContinues |
| H14 | Health ping nguồn lỗi khi promote, fence bên ngoài đã xác nhận, thiếu DSN nguồn | Chỉ promote khi chứng minh điều kiện phù hợp với chế độ đã chọn | C, rà đường thực thi promote/failover |
| H15 | Spool đầy rồi hạ tải, tiến trình bị kill trong lúc backpressure | Lag và trạng thái phục hồi phản ánh phần dữ liệu thực sự bền vững | T |
| H16 | Lỗi đồng thời do nâng cấp, mất mạng, đĩa đầy hoặc failover liên vùng | Phân biệt rollback an toàn với mất mốc cần xử lý thủ công | T |

## Phát hiện cần xử lý

| ID | Tác động | Bằng chứng và ca cần khóa bằng hồi quy |
| --- | --- | --- |
| F01 | **P1**. MySQL → PostgreSQL có thể bỏ sót DELETE/UPDATE cascade khi bảng đích đã tồn tại và thiếu FK tương ứng. | `internal/pipeline/mysql_cascade.go:296` chỉ đối chiếu FK khi hai đầu là MySQL. `internal/schemagen/plan.go` bỏ qua bảng đích đã có. Đã tái hiện với MySQL 8.4 → PostgreSQL 18. Tạo `app.parent` và `app.child` ở hai đầu, chỉ nguồn có `FOREIGN KEY (parent_id) REFERENCES app.parent(id) ON DELETE CASCADE`. Validate đạt và snapshot chép đủ dữ liệu. Xóa cha 42 tại nguồn, đợi đích bắt kịp GTID. Nguồn còn 0 dòng con, đích còn 1 dòng con dù dòng cha đã bị xóa. |
| F02 | **P1**. MongoDB có thể công bố một phần giao dịch nguồn ở đích trước khi phần còn lại hoàn tất. | `internal/mongocdc/stream.go:440` chủ động flush trong cùng giao dịch vượt giới hạn (`20 * stream_batch` hoặc `8 * streamBatchBytes`) và ghi cảnh báo. Đã tái hiện trên MongoDB 8.0. Với `stream_batch: 1`, commit một giao dịch 25 lệnh insert ở nguồn. Đích nhận đủ 25 dòng và Strand ghi cảnh báo đã chia thành nhiều giao dịch đích. |
| F03 | **P2**. MongoDB có thể bỏ sót collection rỗng được tạo sau khi snapshot xong. | Nhánh `create` ở `internal/mongocdc/stream.go:633` chỉ kích hoạt reconcile. `internal/mongocdc/indexes.go` bỏ qua collection chưa có ở đích. Đã tái hiện trên MongoDB 8.0 với tùy chọn replicate index bật. Tạo collection rỗng có validator sau khi Streaming. Đích nhận đủ một giao dịch phát sinh sau đó, nhưng `listCollections` vẫn thiếu collection rỗng sau nhiều chu kỳ reconcile. |
| F04 | **P2**. Validate có thể báo đạt rồi Start từ chối cùng cấu hình auto-seed. | `cmd/strand/validate.go` kiểm tra schema và keys. Start kiểm thêm scope và đích trống trong `internal/pipeline/auto_seed.go` trước khi sửa schema. Đã tái hiện cả hai ca trên MariaDB 10.6.28 → MySQL 8.4. Với FK từ `app.child` đến `app.parent`, Validate trả 0 nhưng Start trả 3 khi chỉ include bảng con. Khi include cả hai bảng và chèn trước dữ liệu đích, Validate vẫn trả 0 và Start trả 3 với thông báo đích đã có dữ liệu. |
| F05 | **Cần xác minh**. Metadata FK đích có thể thiếu do quyền đọc catalog. | `internal/pipeline/mysql_cascade.go` có fallback `SHOW CREATE TABLE` khi đọc FK nguồn MariaDB, phía đích dựa vào catalog. Cần tái hiện với role đích có quyền DML thực tế. |
| F06 | **P2**. DDL FK ở schema ngoài phạm vi có thể chặn checkpoint của bảng khác trùng tên. | `internal/pipeline/ddl.go:182` so tên trần. Đã tái hiện trên MariaDB 10.6.28 → MySQL 8.4. Pipeline chỉ include `app.same_name`. Chạy `ALTER TABLE other.same_name ADD CONSTRAINT ... FOREIGN KEY ... ON DELETE CASCADE` ở nguồn, pipeline báo `foreign-key DDL at ... checkpoint cannot pass this event`. Trường hợp include cả hai schema chưa chạy. |

F01, F02, F03, F04 và F06 đã tái hiện trên máy chủ thật. F05 còn là giả thuyết từ code. Các ca tái hiện dùng fixture tạm, phần kiểm thử sản phẩm trong repo vẫn giữ kỳ vọng hiện hành.

## Chạy lại bộ test tập trung

Lần chạy đầu có bài test cần container bị chặn vì Docker VFS hết chỗ. Sau khi dọn container kiểm thử đã dừng, các nhóm E2E được chạy tuần tự. Phạm vi bằng chứng còn giới hạn ở các phiên bản và dữ liệu đã ghi trong ma trận.

Chạy với Go theo phiên bản trong `go.mod`. Nhóm E2E yêu cầu Docker và đủ dung lượng cho các instance nguồn, đích và tiến trình phụ trợ. Chạy tuần tự theo engine khi dùng Docker VFS.

```sh
go test ./internal/assemble ./internal/spool ./internal/sched \
  -run '^(TestBinlogPos.*|TestSpillByteBudget|TestSpoolTruncateCrashNeverLeavesDanglingBucketRefs|TestSidecar.*|TestCO1_.*|TestCP4_SchedulerStopsOnMaxHoles)$' -count=1
go test ./internal/pipeline -run 'Test.*(Cascade|AutoSeed|BinlogPos|Rename)' -count=1
go test ./internal/web \
  -run '^(TestNewPipelineCreation|TestNewPipelineValidateFailureNotOK|TestOpsBackgroundLifecycle|TestPromoteWizardNormalFlow|TestPromoteWizardNeedsDecisionAndFailover|TestPromoteWizardFailureRetryAndAbort|TestNewPipelineSSHTunnelConfig|TestVerifyJobsArgvAndEnv|TestRunner.*|TestWebTunnelOpensAndStopsVerifiedLoopbackForward)$' -count=1
go test ./tests/e2e \
  -run '^(TestMDB_E2E_CascadeDDLBlocksHistoricalReplay|TestMDB_E2E_AutoSeedGroup|TestMDB_E2E_AutoSeedCascadesWithoutReload)$' -count=1
go test ./tests/e2e \
  -run '^(TestP2_E2E_PGForeignKeyPKUpdateCascadeResume|TestP3_E2E_SchemaEvolution_PG_AddColumnAuto)$' -count=1
go test ./internal/mongocdc \
  -run '^(TestContainer_TransactionsAtomicOnTarget|TestContainer_OplogRolloverFailsClosed|TestContainer_RefusesExistingTarget)$' -count=1
```

## Lượt xác minh tiếp theo và sửa hồi quy

Các kết quả dưới đây bổ sung cho ma trận gốc. “Một phần” ghi rõ kịch bản đã quan sát được và điểm còn thiếu trong cùng một hàng.

| Ca | Bằng chứng của lượt sau | Kết quả |
| --- | --- | --- |
| O04 | `TestRejectsChangedHostKeyAtSameAddress` và test cắt kết nối SSH của forward đang mở | Host key mới bị từ chối. Listener và kết nối local đang chạy đều đóng. Phục hồi checkpoint qua đường hầm chưa chạy riêng |
| O07 | `TestJobSignalRejectsUnrelatedLaunchWithMatchingBinary` | Mô phỏng đạt. Còn tranh chấp giữa hai Web instance và tái sử dụng PID thật |
| O10 | `TestMaterializeCannotOverwriteConfigSaveInFlight` mô phỏng hai Web instance tranh ghi config; CAS trong metadata store | Sửa lỗi ghi mất config. File và DB giữ bản đã validate; phiên lưu cũ nhận conflict. Chưa chạy hai Web host thật cùng lúc promote |
| O12 | MongoDB 8.0, thu hồi quyền INSERT/UPDATE ở đích khi pipeline streaming rồi cấp lại | Từ chối thao tác, giữ 1 dòng đã ghi. Sau restart đích có đúng 2 dòng. Còn ca hết hạn chứng chỉ giữa luồng |
| O11 | Test pause/cancel trong pipeline và `TestE2E_LagGrowsWhilePausedUnderSourceLoad` | Đạt lag tăng khi pause và pipeline bắt kịp khi resume. Còn SIGKILL lúc spool đầy |
| Y01 | `TestBinlogPos_E2E_Rejections` với MySQL 5.7 và 8.4 | Đạt. Test các session binlog được ghi riêng |
| Y05 | `TestAC1_TaggedGTID_MySQL84` và `TestAC3_Resume_Holes_GTID` | Đạt tagged GTID và chỉ replay đúng GNO bị thiếu. Chưa thay nguồn trong ca này |
| Y12/F05 | Tài khoản chỉ SELECT, và tài khoản SELECT/INSERT/UPDATE/DELETE trên MariaDB 10.6.3 và MySQL 8.4. Cả bốn lần đều thấy FK trong catalog lẫn SHOW CREATE | Không tái hiện giả thuyết với hai cấu hình quyền này |
| Y14 | `TestKL_E2E_MariaDBToMySQL_Keyless` với 4.493 thao tác MariaDB 10.6.28 → MySQL 8.4 | Đạt |
| Y20 | `TestR26_TargetEngineLiveMySQL` | Đạt kiểm tra InnoDB ở đích. Chưa chặn khóa metadata bằng fault injection |
| Y22 | `TestAutoSeedBinlogGuardPropagatesRetentionFailure` | Mô phỏng mất retention được truyền ra trước khi công bố checkpoint. Chưa chạy cỡ hàng trăm GB đến TB |
| P01 | `TestPGValidateChecksCDCPrerequisites` với PostgreSQL 18 | Đạt với publication, slot và thiếu quyền replication |
| P02 | `TestAC5_FailoverSlot` và đọc mô phỏng WAL pressure | PostgreSQL 18 tạo và stream được qua failover slot. Còn promote trên standby thật và ép mất WAL |
| P07 | `TestR02_PGPartitionLeafRowsAreNeverSilentlyDropped` | PostgreSQL 18 từ chối publication sai lúc start và dừng nếu publication đổi giữa stream. Còn attach/detach giữa tải |
| P08 | `TestPGInheritanceScopeInSnapshotAndCDC` có PostgreSQL thật | Đạt với giới hạn của inheritance cho snapshot và CDC |
| P09 | Unit test scheduler writeset NUMERIC cùng giá trị khác scale và regression UNIQUE NULLS NOT DISTINCT | Mô phỏng đạt. Còn ma trận collation thực tế |
| P11 | `TestF11Sim_SnapshotAddWhileStreaming`, `TestF11CombinedSim_PipelineRelaxedSnapshotAdd` | Mô phỏng và `TestF11_E2E_WatermarkBoundaryCrashRestart` trên PostgreSQL 18 đều đạt |
| P12 | `TestN10_E2E_MySQLToPGFKChildBeforeParentSnapshot` | Đạt ghi bảng con trong khi cha đang snapshot, verify full cả hai bảng đạt |
| P13 | `TestRegressionPGColumnCatalogFailureIsNotIgnored` | Tìm và sửa thêm một lỗi. Validate trả lỗi khi truy vấn metadata cột PostgreSQL thất bại. Còn đối chiếu đầy đủ type/default/trigger/FK trong schema có sẵn |
| P14 | `TestReview_E2E_PGWideKeySnapshot` trên PostgreSQL 18 | Đạt với khóa rộng và snapshot. Còn deadlock/serialization failure dưới tải |
| P15 | Unit model governor với WAL pressure, slot mất, standby và lỗi đọc | Mô phỏng đạt. Còn đĩa WAL đầy và tải kéo dài |
| G01 | `TestContainer_AuthTLSLeastPrivilege` chạy với mongod 8.0.38 trên hai replica set cục bộ, cùng logic fixture | Đạt X.509 và SCRAM/TLS với tài khoản giới hạn sau khi chuyển từ Docker host network sang tiến trình local |
| G06 | `TestContainer_FailoverThreeNode` với hai replica set cục bộ, mỗi set ba tiến trình MongoDB 8.0.38 | Đạt step-down, kill primary và restart khi snapshot/stream đang ghi. 33.095 document trùng từng byte sau 2.907 thao tác đồng thời |
| G08 | `TestCDCLeaseBlocksTakeoverDuringUncertainDDL` trên MongoDB 8.0 | Đạt |
| G09 | `TestBusyStreamFlushesAtCheckpointDeadline` | Mô phỏng cursor bận liên tục đạt |
| G12 | `TestContainer_ReplicateIndexesAndOptions` | Đạt thay đổi index, validator và giữ index riêng ở đích |
| G14 | `TestContainer_Kill9SnapshotAndStream` trên MongoDB 8.0 | Đạt 3 lần SIGKILL. Còn SIGKILL giữa index DDL |
| G15 | `TestContainer_LargeDocuments` trên MongoDB 8.0.38 với WiredTiger cache 1 GiB | Đạt snapshot, insert, update và replace BSON khoảng 15 MiB. Còn chênh lệch tải kéo dài nhiều ngày |
| H03 | `TestSegment_CrashSafetyBeforeBatchCommit`, `TestSegment_FailedFlushRetryUsesDistinctObjects` | Sửa dọn đúng object của lần upload dở bằng intent cục bộ. Reopen dọn orphan, giữ object khác chung prefix và đọc giao dịch một lần |
| H04 | Unit test prefetch codec lỗi, format cũ | Mô phỏng đạt. Còn nâng cấp tiến trình với spool đang có dữ liệu |
| H08 | `TestCP1_KillDuringCompaction`, `TestCP4_DeltaSyncCompaction`, `TestCP4_LargeDeltaIsCompactedNotRejected` | Đạt các ranh giới compaction đã chọn. Còn tổ hợp stale-epoch ACK với từng thời điểm crash |
| H09 | `TestMySQLResumeAfterLostCommitAcknowledgment` trên MySQL 8.4, driver trả EOF sau khi server COMMIT | Restart sang epoch mới thấy đúng một dòng keyless và checkpoint chứa GTID đã commit |
| H10 | `TestG1_E2E_NetworkFaultsSourceAndTarget` với MySQL và Toxiproxy | Đạt 6 lượt chèn latency, ngắt và reset ở cả nguồn/đích, 6 lượt tự restart, đối chiếu 0 dòng lệch |
| H11 | `TestSG1_PipelineLockSecondInstance` | Đạt tranh lock cơ bản. Còn takeover đúng lúc writer/snapshot đang bị block |
| H12 | `TestPostSeedVerificationScopeAtCatchUp`, verify keyless khi nguồn còn thay đổi và phục hồi touched keys | Đạt scope keyed/mixed/keyless, giữ bằng chứng sau SIGKILL và báo đúng drift. Còn đổi tên đồng thời với tải |
| H14 | Test promote khi nguồn không tới được và fence lỗi | Đạt phần đã mô phỏng. Còn tổ hợp xác nhận fence ngoài hệ thống và thiếu DSN thực tế |
| H15 | Unit test ngưỡng spool và hồi quy Pebble crash/reopen | Đạt từng thành phần. Còn SIGKILL đúng lúc bị backpressure trong một kịch bản tích hợp |
| H16 | Mô phỏng format spool cũ/lỗi lưu và E2E đứt mạng được chạy riêng | Còn tiêm đồng thời lỗi nâng cấp, đĩa đầy và failover liên vùng |

### Kiểm thử bản sửa

| Phát hiện | Hồi quy |
| --- | --- |
| F01 | `TestF01_E2E_MySQLToPGCascadeForeignKeyPreflight` kiểm đích có FK sai, đích có hai FK đúng trên cùng bảng con và đích trống được tạo schema. Cả hai đích hợp lệ áp được DELETE CASCADE. Khi nguồn tắt `foreign_key_checks` cho một DELETE, đích giữ nguyên cả dòng cha lẫn dòng con. |
| F02 | MongoDB 8.0.38 với `stream_batch=1`, 25 sự kiện trên nhiều getMore. Đích ghi 0 dòng và token giữ nguyên khi dừng. Restart ở batch 50 phát lại đủ 25 dòng. Tách thêm ca DDL trong giao dịch, cũng giữ nguyên token. |
| F03 | MongoDB 8.0.38 tạo collection rỗng khi streaming. Đích có collection, validator và index, số document vẫn bằng 0. |
| F04 | MariaDB → MySQL. Validate và Start cùng chặn scope thiếu cha và đích đã có dữ liệu. Validate chặn cả FK sai action. Sau khi auto-seed commit, Validate đạt và restart tiếp tục từ checkpoint hiện có. |
| F06 | MariaDB → MySQL. FK DDL ở schema ngoài scope có tên bảng trùng. Pipeline còn chạy và áp được INSERT tiếp theo đúng bảng trong scope. |
| P13 | Fake PostgreSQL catalog lỗi. Truy vấn metadata bị lỗi trả về từ Validate với thông tin bảng. |
| H03 | Pebble + bộ nhớ giả lập bucket. Xác minh orphan sau Put, giữ object cùng prefix của chủ khác, thử lại bằng object key mới và replay đúng một lần. |
| O10 | Hai Web instance chia sẻ thư mục cấu hình. Đọc materialize đang chờ phiên lưu đang giữ khóa và không thể ghi đè bản đã lưu. Store dùng CAS để chỉ cập nhật YAML nếu phiên bản cũ còn đúng. Contract trực tiếp đạt trên PostgreSQL 17 và Mem, gồm cả pipeline phát hiện từ file cục bộ. |

Bootstrap cỡ TB, đĩa WAL thực sự đầy và lỗi đồng thời khi failover liên vùng vẫn cần hạ tầng riêng. Các test tái hiện với server thật sử dụng dữ liệu fixture cục bộ.
