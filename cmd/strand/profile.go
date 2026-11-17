package main

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"runtime/pprof"
	"sync"
)

// profileConfig chứa các tùy chọn cấu hình cho việc ghi profile ra file.
type profileConfig struct {
	cpuPath        string
	blockPath      string
	blockRate      int
	allowOverwrite bool
}

// profiler quản lý vòng đời lấy mẫu CPU và block profile.
type profiler struct {
	mu        sync.Mutex
	once      sync.Once
	cpuFile   *os.File
	blockFile *os.File
	cpuOn     bool
	blockOn   bool
	stopErr   error
}

// checkProfileConflict kiểm tra đường dẫn hoặc inode của CPU và Block profile có xung đột nhau không.
func checkProfileConflict(cpuPath, blockPath string) error {
	if cpuPath == "" || blockPath == "" {
		return nil
	}

	cleanCPU := filepath.Clean(cpuPath)
	cleanBlock := filepath.Clean(blockPath)
	if cleanCPU == cleanBlock {
		return fmt.Errorf("cpu profile and block profile cannot target the same path: %s", cleanCPU)
	}

	// Kiểm tra nếu cả hai đường dẫn cùng trỏ tới một file đích qua symlink
	evalCPU, errCPU := filepath.EvalSymlinks(cleanCPU)
	evalBlock, errBlock := filepath.EvalSymlinks(cleanBlock)
	if errCPU == nil && errBlock == nil && evalCPU == evalBlock {
		return fmt.Errorf("cpu profile and block profile resolve to the same target: %s", evalCPU)
	}

	// Kiểm tra nếu cả hai file đã tồn tại và cùng một inode (hardlink hoặc file giống nhau)
	fiCPU, statErrCPU := os.Stat(cleanCPU)
	fiBlock, statErrBlock := os.Stat(cleanBlock)
	if statErrCPU == nil && statErrBlock == nil && os.SameFile(fiCPU, fiBlock) {
		return fmt.Errorf("cpu profile and block profile point to the same inode: %s", cleanCPU)
	}

	return nil
}

// startProfiling kích hoạt file CPU profile và block profile theo cấu hình.
// Nếu cả hai đường dẫn đều rỗng, hàm trả về (nil, nil) với chi phí tối thiểu (negligible disabled overhead).
// Nếu file đích đã tồn tại và không bật allowOverwrite, hàm trả về lỗi tránh vô tình ghi đè.
// Profile artifact được tạo với quyền hạn an toàn 0600.
func startProfiling(cfg profileConfig) (*profiler, error) {
	if cfg.cpuPath == "" && cfg.blockPath == "" {
		return nil, nil
	}

	if err := checkProfileConflict(cfg.cpuPath, cfg.blockPath); err != nil {
		return nil, err
	}

	p := &profiler{}
	cleanup := func() {
		_ = p.Stop()
	}

	openFlag := os.O_WRONLY | os.O_CREATE
	if cfg.allowOverwrite {
		openFlag |= os.O_TRUNC
	} else {
		openFlag |= os.O_EXCL
	}

	// Khởi tạo CPU profile nếu có đường dẫn (mode 0600)
	if cfg.cpuPath != "" {
		f, err := os.OpenFile(cfg.cpuPath, openFlag, 0o600)
		if err != nil {
			if errors.Is(err, os.ErrExist) {
				return nil, fmt.Errorf("cpu profile file already exists (use --profile-overwrite to replace): %s", cfg.cpuPath)
			}
			return nil, fmt.Errorf("create cpu profile file %q: %w", cfg.cpuPath, err)
		}
		p.cpuFile = f
		if err := pprof.StartCPUProfile(f); err != nil {
			_ = f.Close()
			p.cpuFile = nil
			return nil, fmt.Errorf("start cpu profile: %w", err)
		}
		p.cpuOn = true
	}

	// Khởi tạo Block profile nếu có đường dẫn (mode 0600)
	if cfg.blockPath != "" {
		f, err := os.OpenFile(cfg.blockPath, openFlag, 0o600)
		if err != nil {
			cleanup()
			if errors.Is(err, os.ErrExist) {
				return nil, fmt.Errorf("block profile file already exists (use --profile-overwrite to replace): %s", cfg.blockPath)
			}
			return nil, fmt.Errorf("create block profile file %q: %w", cfg.blockPath, err)
		}
		p.blockFile = f
		rate := cfg.blockRate
		if rate <= 0 {
			rate = 1 // mặc định lấy mẫu mọi block event nếu chỉ định block profile
		}
		runtime.SetBlockProfileRate(rate)
		p.blockOn = true
	}

	return p, nil
}

// Stop dừng việc ghi profile, xả dữ liệu ra file và đóng file an toàn.
// Phương thức này an toàn khi gọi nhiều lần (idempotent) và ghi nhớ lỗi đầu tiên xảy ra.
func (p *profiler) Stop() error {
	if p == nil {
		return nil
	}

	p.once.Do(func() {
		p.mu.Lock()
		defer p.mu.Unlock()

		var errs []error

		if p.cpuOn {
			pprof.StopCPUProfile()
			p.cpuOn = false
		}
		if p.cpuFile != nil {
			if err := p.cpuFile.Close(); err != nil {
				errs = append(errs, fmt.Errorf("close cpu profile file: %w", err))
			}
			p.cpuFile = nil
		}

		if p.blockOn {
			// Ghi dữ liệu block profile
			if p.blockFile != nil {
				if bp := pprof.Lookup("block"); bp != nil {
					if err := bp.WriteTo(p.blockFile, 0); err != nil {
						errs = append(errs, fmt.Errorf("write block profile: %w", err))
					}
				}
			}
			// Đặt lại tỷ lệ block profile về 0 để tắt
			runtime.SetBlockProfileRate(0)
			p.blockOn = false
		}
		if p.blockFile != nil {
			if err := p.blockFile.Close(); err != nil {
				errs = append(errs, fmt.Errorf("close block profile file: %w", err))
			}
			p.blockFile = nil
		}

		p.stopErr = errors.Join(errs...)
	})

	return p.stopErr
}
