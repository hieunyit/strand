package spool

import (
	"encoding/base64"
	"encoding/binary"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"github.com/cockroachdb/pebble"
)

var (
	// ErrSymlinkEscape báo lỗi khi phát hiện liên kết mềm (symlink) trỏ ra ngoài thư mục trusted root.
	ErrSymlinkEscape = errors.New("spool inventory: symlink escapes trusted root")

	// ErrInvalidTrustedRoot báo lỗi khi trusted root không hợp lệ hoặc không phải thư mục.
	ErrInvalidTrustedRoot = errors.New("spool inventory: invalid trusted root")

	// ErrNonRegularFile báo lỗi khi file sidecar không phải là regular file.
	ErrNonRegularFile = errors.New("spool inventory: not a regular file")

	// ErrMissingPath báo lỗi khi đường dẫn không tồn tại trên filesystem.
	ErrMissingPath = errors.New("spool inventory: path does not exist")

	// ErrPermission báo lỗi khi không có quyền truy cập filesystem.
	ErrPermission = errors.New("spool inventory: permission denied")
)

// EntryStatus biểu thị trạng thái của file sidecar trong inventory.
type EntryStatus string

const (
	StatusNew            EntryStatus = "new"
	StatusAlreadyIndexed EntryStatus = "already_indexed"
	StatusSymlinkEscape  EntryStatus = "symlink_escape"
	StatusError          EntryStatus = "error"
)

// InventoryEntry chứa thông tin của một file sidecar được phát hiện trong trusted root.
type InventoryEntry struct {
	Path   string      `json:"path"`            // đường dẫn tuyệt đối chuẩn hóa (canonical)
	Size   int64       `json:"size"`            // kích thước byte
	Status EntryStatus `json:"status"`          // new, already_indexed, symlink_escape, error
	Refs   int         `json:"refs"`            // số lượng reference hiện có (0 nếu là orphan)
	Root   string      `json:"root"`            // trusted root chứa file
	Error  string      `json:"error,omitempty"` // thông báo lỗi nếu có
}

// InventoryReport chứa kết quả quét inventory tại các trusted roots.
type InventoryReport struct {
	TrustedRoots    []string         `json:"trusted_roots"`
	Entries         []InventoryEntry `json:"entries"`
	DiscoveredFiles int              `json:"discovered_files"`
	DiscoveredBytes int64            `json:"discovered_bytes"`
	NewFiles        int              `json:"new_files"`
	NewBytes        int64            `json:"new_bytes"`
	ExistingFiles   int              `json:"existing_files"`
	ExistingBytes   int64            `json:"existing_bytes"`
	Warnings        []string         `json:"warnings,omitempty"`
	Errors          []string         `json:"errors,omitempty"`
}

// ImportResult chứa kết quả cập nhật hạch toán sidecar vào Pebble database.
type ImportResult struct {
	ImportedFiles int   `json:"imported_files"`
	ImportedBytes int64 `json:"imported_bytes"`
	TotalSidecars int   `json:"total_sidecars"`
	TotalBytes    int64 `json:"total_bytes"`
}

type inventoryOptions struct {
	batchSize        int
	beforeCommitHook func() error
	failOnEscape     bool
}

// InventoryOption cấu hình quá trình quét và import inventory.
type InventoryOption func(*inventoryOptions)

// WithInventoryBatchSize cấu hình kích thước batch tối đa cho mỗi transaction commit (mặc định 1000).
func WithInventoryBatchSize(n int) InventoryOption {
	return func(o *inventoryOptions) {
		if n > 0 {
			o.batchSize = n
		}
	}
}

// WithBeforeInventoryCommit đặt hook kiểm thử chạy ngay trước khi commit batch vào Pebble.
func WithBeforeInventoryCommit(fn func() error) InventoryOption {
	return func(o *inventoryOptions) {
		o.beforeCommitHook = fn
	}
}

// WithFailFastSymlinkEscape cấu hình dừng ngay lập tức khi gặp symlink trỏ ra ngoài root.
func WithFailFastSymlinkEscape(failFast bool) InventoryOption {
	return func(o *inventoryOptions) {
		o.failOnEscape = failFast
	}
}

func defaultInventoryOptions() *inventoryOptions {
	return &inventoryOptions{
		batchSize:    1000,
		failOnEscape: true,
	}
}

// CleanAndValidateTrustedRoots chuẩn hóa, kiểm tra hợp lệ, phát hiện trùng lặp và xung đột lồng nhau (overlap) giữa các roots.
func CleanAndValidateTrustedRoots(roots []string) ([]string, []string, error) {
	if len(roots) == 0 {
		return nil, nil, fmt.Errorf("%w: at least one trusted root must be provided", ErrInvalidTrustedRoot)
	}

	type rootCandidate struct {
		original  string
		canonical string
	}

	var candidates []rootCandidate
	seen := make(map[string]bool)
	var warnings []string

	for _, raw := range roots {
		trimmed := strings.TrimSpace(raw)
		if trimmed == "" {
			continue
		}

		cleaned := filepath.Clean(trimmed)
		abs, err := filepath.Abs(cleaned)
		if err != nil {
			return nil, nil, fmt.Errorf("%w: cannot resolve absolute path for %q: %v", ErrInvalidTrustedRoot, raw, err)
		}

		st, err := os.Stat(abs)
		if err != nil {
			if os.IsNotExist(err) {
				return nil, nil, fmt.Errorf("%w: %s", ErrMissingPath, abs)
			}
			if os.IsPermission(err) {
				return nil, nil, fmt.Errorf("%w: %s: %v", ErrPermission, abs, err)
			}
			return nil, nil, fmt.Errorf("%w: %s: %v", ErrInvalidTrustedRoot, abs, err)
		}
		if !st.IsDir() {
			return nil, nil, fmt.Errorf("%w: %s is not a directory", ErrInvalidTrustedRoot, abs)
		}

		canonical, err := filepath.EvalSymlinks(abs)
		if err != nil {
			return nil, nil, fmt.Errorf("%w: eval symlinks for root %s: %v", ErrInvalidTrustedRoot, abs, err)
		}
		canonical = filepath.Clean(canonical)

		if seen[canonical] {
			warnings = append(warnings, fmt.Sprintf("duplicate trusted root ignored: %q (canonical: %q)", raw, canonical))
			continue
		}
		seen[canonical] = true
		candidates = append(candidates, rootCandidate{original: raw, canonical: canonical})
	}

	if len(candidates) == 0 {
		return nil, nil, fmt.Errorf("%w: no valid trusted roots after validation", ErrInvalidTrustedRoot)
	}

	// Sắp xếp theo độ dài canonical tăng dần để phát hiện thư mục cha chứa thư mục con
	sort.Slice(candidates, func(i, j int) bool {
		return len(candidates[i].canonical) < len(candidates[j].canonical)
	})

	var resultRoots []string
	for _, c := range candidates {
		isNested := false
		for _, kept := range resultRoots {
			rel, err := filepath.Rel(kept, c.canonical)
			if err == nil && rel != "." && !strings.HasPrefix(rel, ".."+string(filepath.Separator)) {
				isNested = true
				warnings = append(warnings, fmt.Sprintf("overlapping root %q is a subdirectory of %q; skipping nested walk", c.canonical, kept))
				break
			}
		}
		if !isNested {
			resultRoots = append(resultRoots, c.canonical)
		}
	}

	return resultRoots, warnings, nil
}

// scanRoots performs the actual filesystem walk across validated roots with strict symlink escape enforcement.
func scanRoots(
	cleanedRoots []string,
	warnings []string,
	opts *inventoryOptions,
	knownChecker func(path string) (isKnown bool, size int64, refs int),
) (*InventoryReport, error) {
	report := &InventoryReport{
		TrustedRoots: cleanedRoots,
		Warnings:     append([]string(nil), warnings...),
	}

	seenFiles := make(map[string]bool)

	for _, root := range cleanedRoots {
		walkErr := filepath.WalkDir(root, func(path string, d fs.DirEntry, err error) error {
			if err != nil {
				if os.IsPermission(err) {
					return fmt.Errorf("%w: %s: %v", ErrPermission, path, err)
				}
				if os.IsNotExist(err) {
					return fmt.Errorf("%w: %s", ErrMissingPath, path)
				}
				return err
			}

			if path == root {
				return nil
			}

			// Symlink policy checks
			if d.Type()&fs.ModeSymlink != 0 {
				linkDest, readErr := os.Readlink(path)
				if readErr != nil {
					if os.IsPermission(readErr) {
						return fmt.Errorf("%w: readlink %s: %v", ErrPermission, path, readErr)
					}
					return fmt.Errorf("%w: readlink %s: %v", ErrMissingPath, path, readErr)
				}
				if !filepath.IsAbs(linkDest) {
					linkDest = filepath.Join(filepath.Dir(path), linkDest)
				}
				linkDest = filepath.Clean(linkDest)

				// Kiểm tra xem đích trực tiếp có thoát ra ngoài root không
				relDest, relErr := filepath.Rel(root, linkDest)
				if relErr != nil || relDest == ".." || strings.HasPrefix(relDest, ".."+string(filepath.Separator)) {
					escapeErr := fmt.Errorf("%w: symlink %s targets %s outside root %s", ErrSymlinkEscape, path, linkDest, root)
					if opts.failOnEscape {
						return escapeErr
					}
					report.Errors = append(report.Errors, escapeErr.Error())
					report.Entries = append(report.Entries, InventoryEntry{
						Path:   path,
						Status: StatusSymlinkEscape,
						Root:   root,
						Error:  escapeErr.Error(),
					})
					return nil
				}

				// Đánh giá toàn bộ chuỗi symlink
				evalTarget, evalErr := filepath.EvalSymlinks(path)
				if evalErr != nil {
					if os.IsNotExist(evalErr) {
						return fmt.Errorf("%w: broken symlink %s: %v", ErrMissingPath, path, evalErr)
					}
					if os.IsPermission(evalErr) {
						return fmt.Errorf("%w: symlink %s: %v", ErrPermission, path, evalErr)
					}
					return fmt.Errorf("%w: eval symlink %s: %v", ErrInvalidTrustedRoot, path, evalErr)
				}
				evalTarget = filepath.Clean(evalTarget)
				relEval, relErr := filepath.Rel(root, evalTarget)
				if relErr != nil || relEval == ".." || strings.HasPrefix(relEval, ".."+string(filepath.Separator)) {
					escapeErr := fmt.Errorf("%w: symlink %s resolves to %s outside root %s", ErrSymlinkEscape, path, evalTarget, root)
					if opts.failOnEscape {
						return escapeErr
					}
					report.Errors = append(report.Errors, escapeErr.Error())
					report.Entries = append(report.Entries, InventoryEntry{
						Path:   path,
						Status: StatusSymlinkEscape,
						Root:   root,
						Error:  escapeErr.Error(),
					})
					return nil
				}

				targetStat, statErr := os.Stat(evalTarget)
				if statErr != nil {
					if os.IsPermission(statErr) {
						return fmt.Errorf("%w: stat symlink target %s: %v", ErrPermission, evalTarget, statErr)
					}
					return fmt.Errorf("%w: stat symlink target %s: %v", ErrMissingPath, evalTarget, statErr)
				}
				if targetStat.IsDir() {
					report.Warnings = append(report.Warnings, fmt.Sprintf("skipping directory symlink: %s -> %s", path, evalTarget))
					return nil
				}
				if !targetStat.Mode().IsRegular() {
					report.Warnings = append(report.Warnings, fmt.Sprintf("skipping non-regular symlink target: %s (%s)", evalTarget, targetStat.Mode().String()))
					return nil
				}

				path = evalTarget
			}

			if d.IsDir() {
				return nil
			}

			// Regular file check
			info, infoErr := os.Stat(path)
			if infoErr != nil {
				if os.IsPermission(infoErr) {
					return fmt.Errorf("%w: stat %s: %v", ErrPermission, path, infoErr)
				}
				if os.IsNotExist(infoErr) {
					return fmt.Errorf("%w: stat %s: %v", ErrMissingPath, path, infoErr)
				}
				return infoErr
			}
			if !info.Mode().IsRegular() {
				report.Warnings = append(report.Warnings, fmt.Sprintf("skipping non-regular file: %s (mode: %s)", path, info.Mode().String()))
				return nil
			}

			normPath, err := normalizeSidecar(path)
			if err != nil {
				return err
			}

			if seenFiles[normPath] {
				return nil
			}
			seenFiles[normPath] = true

			size := info.Size()
			report.DiscoveredFiles++
			report.DiscoveredBytes += size

			isKnown, knownSize, refs := knownChecker(normPath)
			if isKnown {
				if knownSize != size && refs > 0 {
					return fmt.Errorf("spool sidecar %s changed size while referenced (indexed: %d, disk: %d)", normPath, knownSize, size)
				}
				report.ExistingFiles++
				report.ExistingBytes += size
				report.Entries = append(report.Entries, InventoryEntry{
					Path:   normPath,
					Size:   size,
					Status: StatusAlreadyIndexed,
					Refs:   refs,
					Root:   root,
				})
			} else {
				report.NewFiles++
				report.NewBytes += size
				report.Entries = append(report.Entries, InventoryEntry{
					Path:   normPath,
					Size:   size,
					Status: StatusNew,
					Refs:   0,
					Root:   root,
				})
			}
			return nil
		})
		if walkErr != nil {
			return nil, walkErr
		}
	}

	return report, nil
}

// ScanInventory thực hiện quét kiểm tra read-only các trusted spill roots mà không làm thay đổi accounting hoặc Pebble DB.
func (s *PebbleSpool) ScanInventory(trustedRoots []string, opts ...InventoryOption) (*InventoryReport, error) {
	cfg := defaultInventoryOptions()
	for _, opt := range opts {
		opt(cfg)
	}

	cleanedRoots, warnings, err := CleanAndValidateTrustedRoots(trustedRoots)
	if err != nil {
		return nil, err
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	return scanRoots(cleanedRoots, warnings, cfg, func(path string) (bool, int64, int) {
		if entry, ok := s.sidecars[path]; ok {
			return true, entry.size, entry.refs
		}
		return false, 0, 0
	})
}

// ImportInventory áp dụng các sidecar phát hiện được vào chỉ mục hạch toán bền vững của PebbleSpool.
// Thao tác này là idempotent: không ghi đè refcount đang hoạt động (live refcounts),
// duy trì charge cho tới khi chủ sở hữu xóa vật lý file, và hỗ trợ resumable/atomic.
func (s *PebbleSpool) ImportInventory(report *InventoryReport, opts ...InventoryOption) (*ImportResult, error) {
	if report == nil {
		return nil, errors.New("spool import: inventory report cannot be nil")
	}

	cfg := defaultInventoryOptions()
	for _, opt := range opts {
		opt(cfg)
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	var toImport []InventoryEntry
	for _, entry := range report.Entries {
		if entry.Status != StatusNew {
			continue
		}
		if existing, ok := s.sidecars[entry.Path]; ok && existing.size == entry.Size {
			continue
		}
		toImport = append(toImport, entry)
	}

	result := &ImportResult{
		TotalSidecars: len(s.sidecars),
		TotalBytes:    s.sideBytes,
	}

	if len(toImport) == 0 {
		return result, nil
	}

	for i := 0; i < len(toImport); i += cfg.batchSize {
		end := i + cfg.batchSize
		if end > len(toImport) {
			end = len(toImport)
		}
		chunk := toImport[i:end]

		b := s.db.NewBatch()
		for _, entry := range chunk {
			if err := b.Set(sidecarKey(entry.Path), sidecarValue(entry.Size), nil); err != nil {
				_ = b.Close()
				return nil, fmt.Errorf("spool import batch set %s: %w", entry.Path, err)
			}
		}

		if cfg.beforeCommitHook != nil {
			if err := cfg.beforeCommitHook(); err != nil {
				_ = b.Close()
				return nil, fmt.Errorf("spool import hook failure: %w", err)
			}
		}

		if err := b.Commit(pebble.Sync); err != nil {
			_ = b.Close()
			return nil, fmt.Errorf("spool import commit: %w", err)
		}
		_ = b.Close()

		for _, entry := range chunk {
			if existing, ok := s.sidecars[entry.Path]; ok {
				delta := entry.Size - existing.size
				s.sideBytes += delta
				existing.size = entry.Size
			} else {
				s.sidecars[entry.Path] = &sidecar{
					size: entry.Size,
					refs: 0,
				}
				s.sideBytes += entry.Size
				result.ImportedFiles++
				result.ImportedBytes += entry.Size
			}
		}
	}

	result.TotalSidecars = len(s.sidecars)
	result.TotalBytes = s.sideBytes
	return result, nil
}

// ImportTrustedRoots là hàm tiện ích thực hiện cả ScanInventory và ImportInventory trong một lần gọi.
func (s *PebbleSpool) ImportTrustedRoots(trustedRoots []string, opts ...InventoryOption) (*ImportResult, error) {
	report, err := s.ScanInventory(trustedRoots, opts...)
	if err != nil {
		return nil, err
	}
	return s.ImportInventory(report, opts...)
}

// ScanInventoryOffline mở database Pebble ở chế độ read-only và thực hiện quét kiểm tra inventory offline.
func ScanInventoryOffline(spoolDir string, trustedRoots []string, opts ...InventoryOption) (*InventoryReport, error) {
	cfg := defaultInventoryOptions()
	for _, opt := range opts {
		opt(cfg)
	}

	cleanedRoots, warnings, err := CleanAndValidateTrustedRoots(trustedRoots)
	if err != nil {
		return nil, err
	}

	db, err := pebble.Open(spoolDir, &pebble.Options{ReadOnly: true})
	if err != nil {
		return nil, fmt.Errorf("open spool db offline (read-only): %w", err)
	}
	defer db.Close()

	knownSizes := make(map[string]int64)
	knownRefs := make(map[string]int)

	it, err := db.NewIter(&pebble.IterOptions{
		LowerBound: []byte("spill/path/"),
		UpperBound: []byte("spill/path0"),
	})
	if err != nil {
		return nil, fmt.Errorf("scan spill/path offline: %w", err)
	}
	for ok := it.First(); ok; ok = it.Next() {
		raw, e := base64.RawURLEncoding.DecodeString(string(it.Key()[len("spill/path/"):]))
		if e == nil && len(it.Value()) == 8 {
			size := int64(binary.BigEndian.Uint64(it.Value()))
			if size >= 0 {
				knownSizes[string(raw)] = size
			}
		}
	}
	_ = it.Close()

	itRef, err := db.NewIter(&pebble.IterOptions{
		LowerBound: []byte("spill/ref/"),
		UpperBound: []byte("spill/ref0"),
	})
	if err == nil {
		for ok := itRef.First(); ok; ok = itRef.Next() {
			path := string(itRef.Value())
			knownRefs[path]++
		}
		_ = itRef.Close()
	}

	return scanRoots(cleanedRoots, warnings, cfg, func(path string) (bool, int64, int) {
		if size, ok := knownSizes[path]; ok {
			return true, size, knownRefs[path]
		}
		return false, 0, 0
	})
}

// ImportInventoryOffline mở spool, áp dụng các trusted roots vào accounting và đóng spool.
func ImportInventoryOffline(spoolDir string, trustedRoots []string, opts ...InventoryOption) (*ImportResult, error) {
	sp, err := New(spoolDir)
	if err != nil {
		return nil, fmt.Errorf("open spool db for import: %w", err)
	}
	defer sp.Close()

	return sp.ImportTrustedRoots(trustedRoots, opts...)
}
