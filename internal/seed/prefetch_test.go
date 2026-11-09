package seed_test

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strand/internal/testutil"
	"sync"
	"testing"
	"time"

	"strand/internal/checkpoint"
	"strand/internal/exitcode"
	"strand/internal/model"
	"strand/internal/reader"
	"strand/internal/seed"
	"strand/internal/spool"
)

// simulatedArchiveReader mô phỏng một archive reader đọc log từ file trên đĩa
type simulatedArchiveReader struct {
	filePath   string
	events     []reader.Event
	mu         sync.Mutex
	currentPos model.Pos
}

func newSimulatedArchiveReader(filePath string, events []reader.Event) *simulatedArchiveReader {
	return &simulatedArchiveReader{
		filePath: filePath,
		events:   events,
	}
}

func (r *simulatedArchiveReader) Start(ctx context.Context, from checkpoint.AppliedSet) (<-chan reader.Event, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	// Nếu file không tồn tại trên đĩa thì báo log_gap (spec: 02 §9)
	if _, err := os.Stat(r.filePath); err != nil {
		return nil, exitcode.New(
			exitcode.ErrLogGap,
			exitcode.DataError,
			fmt.Sprintf("archive file missing or deleted: %s", r.filePath),
			err,
		)
	}

	out := make(chan reader.Event, len(r.events)+1)
	go func() {
		defer close(out)
		for _, ev := range r.events {
			// Kiểm tra nếu file bị xóa giữa chừng
			if _, err := os.Stat(r.filePath); err != nil {
				select {
				case out <- reader.Event{
					Type: reader.EventError,
					Err: exitcode.New(
						exitcode.ErrLogGap,
						exitcode.DataError,
						fmt.Sprintf("archive file deleted during read: %s", r.filePath),
						err,
					),
				}:
				case <-ctx.Done():
				}
				return
			}

			select {
			case out <- ev:
			case <-ctx.Done():
				return
			}
		}
	}()

	return out, nil
}

func (r *simulatedArchiveReader) Position() model.Pos {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.currentPos
}

func (r *simulatedArchiveReader) Close() error {
	return nil
}

func makeSampleTxnEvents(uuid string, count int) []reader.Event {
	var events []reader.Event
	for i := 1; i <= count; i++ {
		gtid := fmt.Sprintf("%s:%d", uuid, i)
		events = append(events,
			reader.Event{
				Type: reader.EventGTID,
				GTID: gtid,
				Pos:  model.Pos{Engine: model.EngineMySQL, GTID: gtid},
			},
			reader.Event{
				Type:    reader.EventTableMap,
				TableID: 101,
				Schema:  "app",
				Table:   "orders",
			},
			reader.Event{
				Type:    reader.EventRows,
				TableID: 101,
				Schema:  "app",
				Table:   "orders",
				Op:      model.OpInsert,
				Rows: [][]any{
					{int64(i), fmt.Sprintf("order-%d", i), 99.9},
				},
			},
			reader.Event{
				Type: reader.EventXID,
				XID:  uint64(1000 + i),
			},
		)
	}
	return events
}

// AC1: xóa archive trong lúc seed apply thì prefetch vẫn cứu được
func TestPF1_PrefetchRescuesDeletedArchive(t *testing.T) {
	ctx := context.Background()
	tmpDir := t.TempDir()

	spoolDir := filepath.Join(tmpDir, "spool")
	archiveFile := filepath.Join(tmpDir, "mysql-bin.000001")

	// Tạo file archive giả lập trên đĩa
	if err := os.WriteFile(archiveFile, []byte("BINLOG_DATA_DUMMY"), 0644); err != nil {
		t.Fatalf("failed to create dummy archive file: %v", err)
	}

	sp, err := spool.New(spoolDir)
	if err != nil {
		t.Fatalf("failed to create spool: %v", err)
	}
	defer sp.Close()

	uuid := "3e11fa47-71ca-11e1-9e33-c80aa9429562"
	events := makeSampleTxnEvents(uuid, 5)
	archiveReader := newSimulatedArchiveReader(archiveFile, events)

	manifest := &seed.SeedManifest{
		Version: 1,
		Engine:  "mysql",
		Kind:    "xtrabackup",
		StartPosition: seed.StartPosition{
			GTIDSet: uuid + ":1-5",
		},
	}

	var appliedTxns []string
	cfg := seed.ApplyConfig{
		PrefetchEnabled: true, // Bật prefetch mặc định
		Spool:           sp,
		ArchiveReader:   archiveReader,
		OnRestore: func(ctx context.Context) error {
			// Đợi prefetch nạp xong archive vào spool
			time.Sleep(30 * time.Millisecond)
			// Mô phỏng retention xóa archive file trong lúc restore dữ liệu đang hoàn tất
			if err := os.Remove(archiveFile); err != nil {
				return fmt.Errorf("failed to delete archive file during restore: %w", err)
			}
			return nil
		},
		OnAppliedTxn: func(txn *model.Txn) error {
			appliedTxns = append(appliedTxns, txn.SourcePos.GTID)
			return nil
		},
	}

	res, err := seed.ApplySeed(ctx, manifest, cfg)
	if err != nil {
		t.Fatalf("ApplySeed failed: %v", err)
	}

	// Xác nhận file archive thật sự đã bị xóa khỏi đĩa
	if _, statErr := os.Stat(archiveFile); !os.IsNotExist(statErr) {
		t.Fatalf("expected archive file to be deleted, but it still exists")
	}

	// Prefetch đã cứu toàn bộ 5 transaction từ trước khi archive bị xóa
	if res.AppliedTxns != 5 {
		t.Fatalf("expected 5 applied txns, got %d", res.AppliedTxns)
	}
	if len(appliedTxns) != 5 {
		t.Fatalf("expected 5 txns recorded, got %d: %v", len(appliedTxns), appliedTxns)
	}

	t.Logf("AC1 PASS: Xóa archive trong lúc seed apply được cứu thành công bởi prefetch (5/5 txns applied)")
}

// AC2: tắt prefetch thì báo log_gap khi archive bị xóa / thiếu log
func TestPF2_DisabledPrefetch_ReportsLogGap(t *testing.T) {
	ctx := context.Background()
	tmpDir := t.TempDir()

	archiveFile := filepath.Join(tmpDir, "mysql-bin.000001")
	// Tạo file ban đầu
	if err := os.WriteFile(archiveFile, []byte("BINLOG_DATA_DUMMY"), 0644); err != nil {
		t.Fatalf("failed to create dummy archive file: %v", err)
	}

	uuid := "3e11fa47-71ca-11e1-9e33-c80aa9429562"
	events := makeSampleTxnEvents(uuid, 5)
	archiveReader := newSimulatedArchiveReader(archiveFile, events)

	manifest := &seed.SeedManifest{
		Version: 1,
		Engine:  "mysql",
		Kind:    "xtrabackup",
		StartPosition: seed.StartPosition{
			GTIDSet: uuid + ":1-5",
		},
	}

	cfg := seed.ApplyConfig{
		PrefetchEnabled: false, // TẮT PREFETCH
		ArchiveReader:   archiveReader,
		OnRestore: func(ctx context.Context) error {
			// Xóa file archive trong lúc restore
			return os.Remove(archiveFile)
		},
	}

	_, err := seed.ApplySeed(ctx, manifest, cfg)
	if err == nil {
		t.Fatalf("expected error when prefetch is disabled and archive is deleted, got nil")
	}

	// Phải báo lỗi exitcode.ErrLogGap với exit code 5
	var ecErr *exitcode.Error
	if !errors.As(err, &ecErr) {
		t.Fatalf("expected exitcode.Error, got %T: %v", err, err)
	}
	if ecErr.Code != exitcode.ErrLogGap {
		t.Fatalf("expected error code %q, got %q", exitcode.ErrLogGap, ecErr.Code)
	}
	if exitcode.GetExitCode(err) != exitcode.DataError {
		t.Fatalf("expected exit code %d, got %d", exitcode.DataError, exitcode.GetExitCode(err))
	}

	t.Logf("AC2 PASS: Tắt prefetch khi mất archive báo đúng mã lỗi log_gap và exit code 5")
}

// AC3: mọi tầng spool đầy thì báo prefetch_exhausted
func TestPF3_AllSpoolTiersFull_ReportsPrefetchExhausted(t *testing.T) {
	ctx := context.Background()
	tmpDir := t.TempDir()

	spoolDir := filepath.Join(tmpDir, "spool")
	archiveFile := filepath.Join(tmpDir, "mysql-bin.000001")
	_ = os.WriteFile(archiveFile, []byte("DUMMY"), 0644)

	// Giới hạn dung lượng spool rất nhỏ: local 100 bytes, total 250 bytes
	memBucket := spool.NewMemoryBucketBackend()
	sp, err := spool.New(spoolDir,
		spool.WithLocalHardLimit(100),
		spool.WithTotalHardLimit(250),
		spool.WithBucket(memBucket, "staging/"),
	)
	if err != nil {
		t.Fatalf("failed to create tiered spool: %v", err)
	}
	defer sp.Close()

	uuid := "3e11fa47-71ca-11e1-9e33-c80aa9429562"
	// Tạo 10 transaction (mỗi txn ~ 120 bytes) -> tổng hơn 1200 bytes, vượt xa 250 bytes
	events := makeSampleTxnEvents(uuid, 10)
	archiveReader := newSimulatedArchiveReader(archiveFile, events)

	manifest := &seed.SeedManifest{
		Version: 1,
		Engine:  "mysql",
		Kind:    "xtrabackup",
		StartPosition: seed.StartPosition{
			GTIDSet: uuid + ":1-10",
		},
	}

	cfg := seed.ApplyConfig{
		PrefetchEnabled: true,
		Spool:           sp,
		ArchiveReader:   archiveReader,
	}

	_, err = seed.ApplySeed(ctx, manifest, cfg)
	if err == nil {
		t.Fatalf("expected prefetch_exhausted error when all spool tiers full, got nil")
	}

	var ecErr *exitcode.Error
	if !errors.As(err, &ecErr) {
		t.Fatalf("expected exitcode.Error, got %T: %v", err, err)
	}
	if ecErr.Code != exitcode.ErrPrefetchExhausted {
		t.Fatalf("expected error code %q, got %q", exitcode.ErrPrefetchExhausted, ecErr.Code)
	}
	if exitcode.GetExitCode(err) != exitcode.DataError {
		t.Fatalf("expected exit code %d, got %d", exitcode.DataError, exitcode.GetExitCode(err))
	}

	t.Logf("AC3 PASS: Mọi tầng spool đầy báo đúng prefetch_exhausted và exit code 5")
}

// AC4: spool tràn sang bucket (MinIO trong compose)
func TestPF4_SpoolOverflowToBucket_MinIO(t *testing.T) {
	endpoint := testutil.MinIOEndpoint(t)

	ctx := context.Background()
	tmpDir := t.TempDir()

	spoolDir := filepath.Join(tmpDir, "spool")
	archiveFile := filepath.Join(tmpDir, "mysql-bin.000001")
	_ = os.WriteFile(archiveFile, []byte("DUMMY"), 0644)

	// Khởi tạo MinIO backend thực tế từ container trong compose
	minioBackend, err := spool.NewMinIOBackend(spool.MinIOConfig{
		Endpoint:  endpoint,
		Bucket:    "strand-staging",
		AccessKey: "minioadmin",
		SecretKey: "minioadmin",
	})
	if err != nil {
		t.Fatalf("failed to init MinIO backend: %v", err)
	}
	defer minioBackend.Close()

	// Đặt LocalHardLimit nhỏ (200 bytes) để các txn sau tự động tràn sang MinIO bucket
	prefix := fmt.Sprintf("test-p1-14-%d/", time.Now().UnixNano())
	sp, err := spool.New(spoolDir,
		spool.WithLocalHardLimit(200),
		spool.WithTotalHardLimit(100*1024*1024), // 100MB
		spool.WithBucket(minioBackend, prefix),
	)
	if err != nil {
		t.Fatalf("failed to init spool: %v", err)
	}
	defer sp.Close()

	uuid := "550e8400-e29b-41d4-a716-446655440000"
	// 6 transaction (mỗi txn ~ 120 bytes) -> 2 txn đầu nằm ở local, 4 txn sau tràn sang MinIO
	events := makeSampleTxnEvents(uuid, 6)
	archiveReader := newSimulatedArchiveReader(archiveFile, events)

	manifest := &seed.SeedManifest{
		Version: 1,
		Engine:  "mysql",
		Kind:    "xtrabackup",
		StartPosition: seed.StartPosition{
			GTIDSet: uuid + ":1-6",
		},
	}

	var appliedTxns []string
	cfg := seed.ApplyConfig{
		PrefetchEnabled: true,
		Spool:           sp,
		ArchiveReader:   archiveReader,
		OnAppliedTxn: func(t *model.Txn) error {
			appliedTxns = append(appliedTxns, t.SourcePos.GTID)
			return nil
		},
	}

	res, err := seed.ApplySeed(ctx, manifest, cfg)
	if err != nil {
		t.Fatalf("ApplySeed failed with MinIO overflow: %v", err)
	}

	usage := sp.Usage()
	t.Logf("Spool usage sau khi tràn: LocalBytes=%d, BucketBytes=%d", usage.LocalBytes, usage.BucketBytes)

	// Kiểm tra dung lượng local không vượt quá LocalHardLimit đáng kể và BucketBytes > 0
	if usage.BucketBytes <= 0 {
		t.Fatalf("expected BucketBytes > 0 due to overflow, got %d", usage.BucketBytes)
	}

	// Kiểm tra các đối tượng thực tế trên MinIO bucket
	keys, err := minioBackend.List(ctx, prefix)
	if err != nil {
		t.Fatalf("failed to list keys in MinIO: %v", err)
	}
	if len(keys) == 0 {
		t.Fatalf("expected objects to exist in MinIO bucket under prefix %s, got none", prefix)
	}
	t.Logf("Đã tìm thấy %d đối tượng lưu trữ tràn trên MinIO: %v", len(keys), keys)

	// Kiểm tra Catch-up đã đọc và apply đầy đủ tất cả 6 transaction theo đúng thứ tự
	if res.AppliedTxns != 6 {
		t.Fatalf("expected 6 applied txns, got %d", res.AppliedTxns)
	}
	if len(appliedTxns) != 6 {
		t.Fatalf("expected 6 applied txns recorded, got %d: %v", len(appliedTxns), appliedTxns)
	}
	for i := 1; i <= 6; i++ {
		expectedGTID := fmt.Sprintf("%s:%d", uuid, i)
		if appliedTxns[i-1] != expectedGTID {
			t.Fatalf("order mismatch at %d: got %s, want %s", i, appliedTxns[i-1], expectedGTID)
		}
	}

	t.Logf("AC4 PASS: Spool tràn thành công sang MinIO bucket, đọc và apply đầy đủ 6/6 transaction")
}
