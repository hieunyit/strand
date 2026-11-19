package spool

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"strand/internal/exitcode"
	"strand/internal/model"
)

func writeNamedFile(t *testing.T, dir, name string, size int) string {
	t.Helper()
	p := filepath.Join(dir, name)
	if err := os.WriteFile(p, []byte(strings.Repeat("a", size)), 0600); err != nil {
		t.Fatal(err)
	}
	return p
}

// 1. Outside-root orphan: sidecar file nằm ngoài <spool-dir>/spill được phát hiện và tính charge chính xác.
func TestInventory_OutsideRootOrphan(t *testing.T) {
	temp := t.TempDir()
	dbDir := filepath.Join(temp, "db")
	outsideDir := filepath.Join(temp, "outside_trusted_spill")
	if err := os.MkdirAll(outsideDir, 0700); err != nil {
		t.Fatal(err)
	}

	orphanPath := writeNamedFile(t, outsideDir, "orphan_01.bin", 2048)

	s, err := New(dbDir)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	if s.Usage().LocalBytes != 0 {
		t.Fatalf("usage before inventory: %d, want 0", s.Usage().LocalBytes)
	}

	// Quét ở chế độ read-only mặc định
	report, err := s.ScanInventory([]string{outsideDir})
	if err != nil {
		t.Fatalf("ScanInventory failed: %v", err)
	}

	if report.DiscoveredFiles != 1 || report.DiscoveredBytes != 2048 {
		t.Fatalf("unexpected report: files=%d bytes=%d", report.DiscoveredFiles, report.DiscoveredBytes)
	}
	if report.NewFiles != 1 || report.NewBytes != 2048 {
		t.Fatalf("unexpected new counts: new_files=%d new_bytes=%d", report.NewFiles, report.NewBytes)
	}
	if len(report.Entries) != 1 || report.Entries[0].Status != StatusNew {
		t.Fatalf("unexpected entries: %+v", report.Entries)
	}

	// Read-only check: usage không đổi
	if s.Usage().LocalBytes != 0 {
		t.Fatalf("usage after read-only scan: %d, want 0", s.Usage().LocalBytes)
	}

	// Cập nhật accounting rõ ràng (explicit import action)
	res, err := s.ImportInventory(report)
	if err != nil {
		t.Fatalf("ImportInventory failed: %v", err)
	}
	if res.ImportedFiles != 1 || res.ImportedBytes != 2048 {
		t.Fatalf("unexpected import result: %+v", res)
	}

	// Usage phải phản ánh đúng 2048 bytes
	if got := s.Usage().LocalBytes; got != 2048 {
		t.Fatalf("usage after import: %d, want 2048", got)
	}

	// File sidecar có trong s.sidecars với refs == 0
	entry := s.sidecars[orphanPath]
	if entry == nil || entry.size != 2048 || entry.refs != 0 {
		t.Fatalf("sidecar entry invalid: %+v", entry)
	}
}

// 2. Duplicate roots/path: các root trùng nhau hoặc lồng nhau (overlap) được khử trùng và không bị double-count.
func TestInventory_DuplicateRootsAndPath(t *testing.T) {
	temp := t.TempDir()
	dbDir := filepath.Join(temp, "db")
	parentDir := filepath.Join(temp, "parent_spill")
	subDir := filepath.Join(parentDir, "nested_sub")
	if err := os.MkdirAll(subDir, 0700); err != nil {
		t.Fatal(err)
	}

	file1 := writeNamedFile(t, parentDir, "file1.bin", 1000)
	file2 := writeNamedFile(t, subDir, "file2.bin", 2000)

	s, err := New(dbDir)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	// Truyền duplicate root và nested root: [parentDir, parentDir, subDir]
	roots := []string{parentDir, parentDir, subDir}
	report, err := s.ScanInventory(roots)
	if err != nil {
		t.Fatalf("ScanInventory with duplicates: %v", err)
	}

	// Cả 2 file chỉ được phát hiện 1 lần
	if report.DiscoveredFiles != 2 || report.DiscoveredBytes != 3000 {
		t.Fatalf("discovered files=%d bytes=%d, want 2 and 3000", report.DiscoveredFiles, report.DiscoveredBytes)
	}
	if len(report.Warnings) == 0 {
		t.Fatalf("expected warnings about duplicate/overlapping roots, got none")
	}

	// Import và kiểm tra charge
	res, err := s.ImportInventory(report)
	if err != nil {
		t.Fatalf("ImportInventory failed: %v", err)
	}
	if res.ImportedFiles != 2 || res.ImportedBytes != 3000 {
		t.Fatalf("imported result: %+v", res)
	}
	if s.Usage().LocalBytes != 3000 {
		t.Fatalf("local bytes: %d, want 3000", s.Usage().LocalBytes)
	}

	_ = file1
	_ = file2
}

// 3. Symlink escape: symlink trỏ ra ngoài trusted root phải bị chặn và báo lỗi ErrSymlinkEscape.
func TestInventory_SymlinkEscape(t *testing.T) {
	temp := t.TempDir()
	dbDir := filepath.Join(temp, "db")
	trustedDir := filepath.Join(temp, "trusted_spill")
	outsideDir := filepath.Join(temp, "outside_forbidden")
	if err := os.MkdirAll(trustedDir, 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(outsideDir, 0700); err != nil {
		t.Fatal(err)
	}

	secretFile := writeNamedFile(t, outsideDir, "sensitive.data", 4096)

	// Tạo symlink bên trong trustedDir trỏ ra outsideDir
	symlinkPath := filepath.Join(trustedDir, "escape_link.bin")
	if err := os.Symlink(secretFile, symlinkPath); err != nil {
		t.Fatal(err)
	}

	s, err := New(dbDir)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	// ScanInventory phải fail-fast với ErrSymlinkEscape
	_, err = s.ScanInventory([]string{trustedDir})
	if err == nil {
		t.Fatal("expected ErrSymlinkEscape, got nil")
	}
	if !errors.Is(err, ErrSymlinkEscape) {
		t.Fatalf("expected ErrSymlinkEscape, got: %v", err)
	}

	// Không có file nào được charge
	if s.Usage().LocalBytes != 0 {
		t.Fatalf("escaped symlink caused charge: %d", s.Usage().LocalBytes)
	}
}

// 4. Symlink within root: symlink trỏ tới file hợp lệ trong cùng trusted root được cho phép và khử trùng (deduplicate).
func TestInventory_SymlinkWithinRoot(t *testing.T) {
	temp := t.TempDir()
	dbDir := filepath.Join(temp, "db")
	trustedDir := filepath.Join(temp, "trusted_spill")
	if err := os.MkdirAll(trustedDir, 0700); err != nil {
		t.Fatal(err)
	}

	realFile := writeNamedFile(t, trustedDir, "target.bin", 1500)
	symlinkPath := filepath.Join(trustedDir, "link_to_target.bin")
	if err := os.Symlink(realFile, symlinkPath); err != nil {
		t.Fatal(err)
	}

	s, err := New(dbDir)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	report, err := s.ScanInventory([]string{trustedDir})
	if err != nil {
		t.Fatalf("unexpected scan error: %v", err)
	}

	// Do cả realFile và symlinkPath cùng trỏ về 1 file canonical, chỉ phát hiện 1 file
	if report.DiscoveredFiles != 1 || report.DiscoveredBytes != 1500 {
		t.Fatalf("discovered files=%d bytes=%d, want 1 and 1500", report.DiscoveredFiles, report.DiscoveredBytes)
	}
}

// 5. Missing / permission error: root không tồn tại hoặc không có quyền đọc trả về lỗi thích hợp.
func TestInventory_MissingAndPermissionError(t *testing.T) {
	temp := t.TempDir()
	dbDir := filepath.Join(temp, "db")
	s, err := New(dbDir)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	// Missing root
	missingRoot := filepath.Join(temp, "non_existent_dir")
	_, err = s.ScanInventory([]string{missingRoot})
	if err == nil || !errors.Is(err, ErrMissingPath) {
		t.Fatalf("expected ErrMissingPath for missing root, got: %v", err)
	}

	// Permission error: thư mục không có quyền truy cập (mode 0000)
	permDir := filepath.Join(temp, "forbidden_dir")
	if err := os.MkdirAll(permDir, 0700); err != nil {
		t.Fatal(err)
	}
	_ = writeNamedFile(t, permDir, "locked.bin", 100)
	if err := os.Chmod(permDir, 0000); err != nil {
		t.Fatal(err)
	}
	defer os.Chmod(permDir, 0700) // dọn dẹp sau test

	// Nếu đang chạy dưới quyền không phải root (user thường), sẽ gặp permission error
	if os.Geteuid() != 0 {
		_, err = s.ScanInventory([]string{permDir})
		if err == nil || !errors.Is(err, ErrPermission) {
			t.Fatalf("expected ErrPermission, got: %v", err)
		}
	}
}

// 6. Reopen: sau khi import orphan sidecar, đóng spool và mở lại giữ nguyên charge bền vững trong Pebble.
func TestInventory_Reopen(t *testing.T) {
	temp := t.TempDir()
	dbDir := filepath.Join(temp, "db")
	trustedDir := filepath.Join(temp, "trusted_spill")
	if err := os.MkdirAll(trustedDir, 0700); err != nil {
		t.Fatal(err)
	}

	path := writeNamedFile(t, trustedDir, "persistent_orphan.bin", 3500)

	s, err := New(dbDir)
	if err != nil {
		t.Fatal(err)
	}

	_, err = s.ImportTrustedRoots([]string{trustedDir})
	if err != nil {
		t.Fatal(err)
	}
	if s.Usage().LocalBytes != 3500 {
		t.Fatalf("usage before close: %d, want 3500", s.Usage().LocalBytes)
	}

	if err = s.Close(); err != nil {
		t.Fatal(err)
	}

	// Mở lại KHÔNG truyền trusted root: PebbleSpool tự động phục hồi các sidecar đã import
	s2, err := New(dbDir)
	if err != nil {
		t.Fatal(err)
	}
	defer s2.Close()

	if s2.Usage().LocalBytes != 3500 {
		t.Fatalf("usage after reopen: %d, want 3500", s2.Usage().LocalBytes)
	}
	if s2.sidecars[path] == nil || s2.sidecars[path].size != 3500 {
		t.Fatalf("sidecar entry missing or corrupted after reopen: %+v", s2.sidecars[path])
	}
}

// 7. Partial failure & resumable import: import gặp lỗi giữa chừng không làm hỏng dữ liệu; thử lại thành công và idempotent.
func TestInventory_PartialFailureAndResumable(t *testing.T) {
	temp := t.TempDir()
	dbDir := filepath.Join(temp, "db")
	trustedDir := filepath.Join(temp, "trusted_spill")
	if err := os.MkdirAll(trustedDir, 0700); err != nil {
		t.Fatal(err)
	}

	_ = writeNamedFile(t, trustedDir, "file_a.bin", 1000)
	_ = writeNamedFile(t, trustedDir, "file_b.bin", 2000)

	s, err := New(dbDir)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	report, err := s.ScanInventory([]string{trustedDir})
	if err != nil {
		t.Fatal(err)
	}

	// Giả lập lỗi tại commit hook
	injectErr := errors.New("simulated commit crash")
	_, err = s.ImportInventory(report, WithBeforeInventoryCommit(func() error {
		return injectErr
	}))
	if err == nil || !errors.Is(err, injectErr) {
		t.Fatalf("expected injected error, got: %v", err)
	}

	// Do batch bị rollback, không file nào được import
	if s.Usage().LocalBytes != 0 {
		t.Fatalf("usage after failed import: %d, want 0", s.Usage().LocalBytes)
	}

	// Resume import: chạy lại bình thường
	res, err := s.ImportInventory(report)
	if err != nil {
		t.Fatalf("resumed import failed: %v", err)
	}
	if res.ImportedFiles != 2 || res.ImportedBytes != 3000 {
		t.Fatalf("resumed import result: %+v", res)
	}
	if s.Usage().LocalBytes != 3000 {
		t.Fatalf("usage after resumed import: %d, want 3000", s.Usage().LocalBytes)
	}

	// Chạy import lần nữa: phải hoàn toàn idempotent (không ghi đè hay nhân đôi charge)
	res2, err := s.ImportInventory(report)
	if err != nil {
		t.Fatalf("idempotent import failed: %v", err)
	}
	if res2.ImportedFiles != 0 || res2.ImportedBytes != 0 {
		t.Fatalf("re-import should not import duplicate files: %+v", res2)
	}
	if s.Usage().LocalBytes != 3000 {
		t.Fatalf("usage after duplicate import: %d, want 3000", s.Usage().LocalBytes)
	}
}

// 8. Quota after import: file orphan đã import được tính vào local hard limit; append vượt hạn mức bị từ chối với prefetch_exhausted.
func TestInventory_QuotaAfterImport(t *testing.T) {
	temp := t.TempDir()
	dbDir := filepath.Join(temp, "db")
	trustedDir := filepath.Join(temp, "trusted_spill")
	if err := os.MkdirAll(trustedDir, 0700); err != nil {
		t.Fatal(err)
	}

	// Tạo file orphan 1200 bytes
	_ = writeNamedFile(t, trustedDir, "large_orphan.bin", 1200)

	// Đặt limit 1500 bytes
	s, err := New(dbDir, WithLocalHardLimit(1500))
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	// Import file orphan
	_, err = s.ImportTrustedRoots([]string{trustedDir})
	if err != nil {
		t.Fatal(err)
	}
	if s.Usage().LocalBytes != 1200 {
		t.Fatalf("usage: %d, want 1200", s.Usage().LocalBytes)
	}

	ctx := context.Background()

	// Append một txn nhỏ (100 bytes payload) -> tổng 1200 + ~100 <= 1500 -> thành công
	okTxn := &model.Txn{
		SourcePos: model.Pos{Engine: model.EngineMySQL, GTID: spillSID + ":1"},
		RowCount:  1,
	}
	if err = s.Append(ctx, okTxn); err != nil {
		t.Fatalf("small append within quota failed: %v", err)
	}

	// Tạo thêm 1 sidecar mới 400 bytes. 1200 + 100 + 400 > 1500 -> phải vượt hard limit
	anotherSidecar := writeNamedFile(t, trustedDir, "exceed.bin", 400)
	failTxn := spillTxn(anotherSidecar, "2")
	err = s.Append(ctx, failTxn)
	if err == nil {
		t.Fatal("expected quota rejection, got nil")
	}
	if exitcode.GetExitCode(err) != exitcode.DataError {
		t.Fatalf("expected DataError exitcode, got: %v (code %v)", err, exitcode.GetExitCode(err))
	}
}

// 9. Never deleting borrowed files: Spool KHÔNG BAO GIỜ tự xóa các file sidecar mượn.
// Chỉ khi chủ sở hữu xóa vật lý file trên đĩa và không còn live ref thì charge mới được giải phóng.
func TestInventory_NeverDeletesBorrowedFiles(t *testing.T) {
	temp := t.TempDir()
	dbDir := filepath.Join(temp, "db")
	trustedDir := filepath.Join(temp, "trusted_spill")
	if err := os.MkdirAll(trustedDir, 0700); err != nil {
		t.Fatal(err)
	}

	sidecarPath := writeNamedFile(t, trustedDir, "borrowed.bin", 2000)

	s, err := New(dbDir)
	if err != nil {
		t.Fatal(err)
	}

	// 1. Import orphan file
	_, err = s.ImportTrustedRoots([]string{trustedDir})
	if err != nil {
		t.Fatal(err)
	}

	// 2. Append txn tham chiếu file này (refs: 0 -> 1)
	ctx := context.Background()
	if err = s.Append(ctx, spillTxn(sidecarPath, "1")); err != nil {
		t.Fatal(err)
	}

	// 3. Truncate txn (refs: 1 -> 0)
	if err = s.Truncate(ctx, appliedGTIDs(t, "1")); err != nil {
		t.Fatal(err)
	}

	// 4. Kiểm tra file trên đĩa: tuyệt đối không bị xóa!
	if _, err = os.Stat(sidecarPath); err != nil {
		t.Fatalf("borrowed file was deleted by spool truncate: %v", err)
	}

	// 5. Khởi động lại spool
	if err = s.Close(); err != nil {
		t.Fatal(err)
	}
	s, err = New(dbDir)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	if _, err = os.Stat(sidecarPath); err != nil {
		t.Fatalf("borrowed file was deleted by spool restart: %v", err)
	}
	if s.Usage().LocalBytes != 2000 {
		t.Fatalf("usage: %d, want 2000", s.Usage().LocalBytes)
	}

	// 6. Chủ sở hữu xóa file vật lý
	if err = os.Remove(sidecarPath); err != nil {
		t.Fatal(err)
	}

	// Append một txn mới để kích hoạt refreshSidecars
	if err = s.Append(ctx, &model.Txn{SourcePos: model.Pos{Engine: model.EngineMySQL, GTID: spillSID + ":2"}}); err != nil {
		t.Fatal(err)
	}

	// Charge được giải phóng sau khi xóa vật lý và 0 refs
	if s.Usage().LocalBytes >= 2000 {
		t.Fatalf("deleted file charge was not released: %d", s.Usage().LocalBytes)
	}
}

// 10. Preserves live refcounts: Import inventory không được ghi đè live refcount của sidecar đang có txn tham chiếu.
func TestInventory_PreservesLiveRefcounts(t *testing.T) {
	temp := t.TempDir()
	dbDir := filepath.Join(temp, "db")
	trustedDir := filepath.Join(temp, "trusted_spill")
	if err := os.MkdirAll(trustedDir, 0700); err != nil {
		t.Fatal(err)
	}

	sidecarPath := writeNamedFile(t, trustedDir, "referenced.bin", 1234)

	s, err := New(dbDir)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	// Append txn tham chiếu sidecarPath
	ctx := context.Background()
	if err = s.Append(ctx, spillTxn(sidecarPath, "1")); err != nil {
		t.Fatal(err)
	}

	if s.sidecars[sidecarPath].refs != 1 {
		t.Fatalf("refs before import=%d, want 1", s.sidecars[sidecarPath].refs)
	}

	// Quét và import inventory chứa sidecarPath
	report, err := s.ScanInventory([]string{trustedDir})
	if err != nil {
		t.Fatal(err)
	}
	if report.ExistingFiles != 1 {
		t.Fatalf("expected 1 existing file, got %d", report.ExistingFiles)
	}

	_, err = s.ImportInventory(report)
	if err != nil {
		t.Fatal(err)
	}

	// Live refcount vẫn phải là 1, không bị reset về 0!
	if s.sidecars[sidecarPath].refs != 1 {
		t.Fatalf("refs after import=%d, want 1", s.sidecars[sidecarPath].refs)
	}
}

// 11. Offline scan & import: kiểm tra ScanInventoryOffline (read-only) và ImportInventoryOffline.
func TestInventory_OfflineScanAndImport(t *testing.T) {
	temp := t.TempDir()
	dbDir := filepath.Join(temp, "db")
	trustedDir := filepath.Join(temp, "trusted_spill")
	if err := os.MkdirAll(trustedDir, 0700); err != nil {
		t.Fatal(err)
	}

	orphan := writeNamedFile(t, trustedDir, "offline_orphan.bin", 1800)

	// Khởi tạo db rồi đóng lại
	s, err := New(dbDir)
	if err != nil {
		t.Fatal(err)
	}
	if err = s.Close(); err != nil {
		t.Fatal(err)
	}

	// 1. Quét offline (read-only)
	report, err := ScanInventoryOffline(dbDir, []string{trustedDir})
	if err != nil {
		t.Fatalf("ScanInventoryOffline failed: %v", err)
	}
	if report.NewFiles != 1 || report.NewBytes != 1800 {
		t.Fatalf("unexpected offline report: %+v", report)
	}

	// 2. Import offline
	res, err := ImportInventoryOffline(dbDir, []string{trustedDir})
	if err != nil {
		t.Fatalf("ImportInventoryOffline failed: %v", err)
	}
	if res.ImportedFiles != 1 || res.ImportedBytes != 1800 {
		t.Fatalf("unexpected offline import result: %+v", res)
	}

	// 3. Mở lại spool kiểm tra accounting
	s2, err := New(dbDir)
	if err != nil {
		t.Fatal(err)
	}
	defer s2.Close()

	if s2.Usage().LocalBytes != 1800 {
		t.Fatalf("usage: %d, want 1800", s2.Usage().LocalBytes)
	}
	if s2.sidecars[orphan] == nil || s2.sidecars[orphan].size != 1800 {
		t.Fatalf("missing sidecar entry: %+v", s2.sidecars[orphan])
	}
}
