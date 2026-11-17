// Package main provides an offline diagnostic tool for inspecting and importing
// trusted spill sidecars into Strand's Pebble spool index.
package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"strings"

	"strand/internal/spool"
)

type stringSlice []string

func (s *stringSlice) String() string {
	return strings.Join(*s, ",")
}

func (s *stringSlice) Set(val string) error {
	parts := strings.Split(val, ",")
	for _, p := range parts {
		trimmed := strings.TrimSpace(p)
		if trimmed != "" {
			*s = append(*s, trimmed)
		}
	}
	return nil
}

func main() {
	var (
		spoolDir     string
		trustedRoots stringSlice
		apply        bool
		dryRun       bool
		jsonOutput   bool
	)

	flag.StringVar(&spoolDir, "spool-dir", "", "Đường dẫn thư mục Pebble spool database (bắt buộc)")
	flag.Var(&trustedRoots, "trusted-root", "Thư mục spill đáng tin cậy do operator cung cấp (có thể gọi nhiều lần hoặc phân tách bằng dấu phẩy)")
	flag.BoolVar(&apply, "apply", false, "Áp dụng cập nhật hạch toán vào Pebble DB (mặc định false - chỉ kiểm tra read-only)")
	flag.BoolVar(&dryRun, "dry-run", false, "Chế độ chạy thử chỉ đọc (mặc định true)")
	flag.BoolVar(&jsonOutput, "json", false, "Xuất kết quả dưới định dạng JSON")

	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, "Usage: spool-inventory -spool-dir <path> -trusted-root <path> [-apply] [-json]\n\n")
		flag.PrintDefaults()
	}

	flag.Parse()

	if spoolDir == "" {
		fmt.Fprintf(os.Stderr, "Lỗi: -spool-dir là bắt buộc\n")
		os.Exit(1)
	}
	if len(trustedRoots) == 0 {
		fmt.Fprintf(os.Stderr, "Lỗi: ít nhất một -trusted-root phải được cung cấp\n")
		os.Exit(1)
	}

	isApply := apply && !dryRun

	if !isApply {
		report, err := spool.ScanInventoryOffline(spoolDir, trustedRoots)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Lỗi quét inventory: %v\n", err)
			os.Exit(2)
		}

		if jsonOutput {
			enc := json.NewEncoder(os.Stdout)
			enc.SetIndent("", "  ")
			if err := enc.Encode(report); err != nil {
				fmt.Fprintf(os.Stderr, "Lỗi mã hóa JSON: %v\n", err)
				os.Exit(1)
			}
			return
		}

		fmt.Printf("=== SPOOL INVENTORY REPORT (READ-ONLY) ===\n")
		fmt.Printf("Spool Dir:        %s\n", spoolDir)
		fmt.Printf("Trusted Roots:    %v\n", report.TrustedRoots)
		fmt.Printf("Discovered Files: %d (%d bytes)\n", report.DiscoveredFiles, report.DiscoveredBytes)
		fmt.Printf("New Sidecars:     %d (%d bytes)\n", report.NewFiles, report.NewBytes)
		fmt.Printf("Already Indexed:  %d (%d bytes)\n", report.ExistingFiles, report.ExistingBytes)
		if len(report.Warnings) > 0 {
			fmt.Printf("Cảnh báo (%d):\n", len(report.Warnings))
			for _, w := range report.Warnings {
				fmt.Printf("  - %s\n", w)
			}
		}
		if len(report.Errors) > 0 {
			fmt.Printf("Lỗi (%d):\n", len(report.Errors))
			for _, e := range report.Errors {
				fmt.Printf("  - %s\n", e)
			}
		}
		fmt.Printf("Ghi chú: Đây là chế độ kiểm tra không ghi. Dùng cờ -apply để ghi nhận vào Pebble DB.\n")
		return
	}

	result, err := spool.ImportInventoryOffline(spoolDir, trustedRoots)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Lỗi cập nhật hạch toán: %v\n", err)
		os.Exit(3)
	}

	if jsonOutput {
		enc := json.NewEncoder(os.Stdout)
		enc.SetIndent("", "  ")
		if err := enc.Encode(result); err != nil {
			fmt.Fprintf(os.Stderr, "Lỗi mã hóa JSON: %v\n", err)
			os.Exit(1)
		}
		return
	}

	fmt.Printf("=== SPOOL INVENTORY IMPORT SUCCESS ===\n")
	fmt.Printf("Spool Dir:      %s\n", spoolDir)
	fmt.Printf("Imported Files: %d (%d bytes)\n", result.ImportedFiles, result.ImportedBytes)
	fmt.Printf("Total Sidecars: %d (%d bytes)\n", result.TotalSidecars, result.TotalBytes)
	fmt.Printf("Trạng thái: Hoàn tất hạch toán vào chỉ mục Pebble bền vững.\n")
}
