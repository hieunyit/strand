package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"

	"github.com/spf13/cobra"

	"strand/internal/spool"
)

var (
	spoolInspectDir          string
	spoolInspectTrustedRoots []string
	spoolInspectApply        bool
	spoolInspectDryRun       bool
	spoolInspectJSON         bool
)

func init() {
	rootCmd.AddCommand(newSpoolInspectCmd())
}

func newSpoolInspectCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:     "spool-inspect",
		Aliases: []string{"spool-inventory"},
		Short:   "Kiểm tra và hạch toán inventory các spill sidecars trong trusted roots do operator cung cấp",
		Long: `spool-inspect cho phép operator kiểm tra (read-only) hoặc cập nhật (--apply)
chỉ mục hạch toán các file spill sidecar nằm ngoài cấu trúc mặc định của spool.
Thao tác kiểm tra là read-only mặc định, bảo vệ chống symlink escape, khử trùng lặp và xung đột.`,
		RunE: func(cmd *cobra.Command, args []string) error {
			if spoolInspectDir == "" {
				return errors.New("flag --spool-dir là bắt buộc")
			}
			if len(spoolInspectTrustedRoots) == 0 {
				return errors.New("ít nhất một flag --trusted-root phải được cung cấp")
			}

			// Mặc định là dry-run nếu không bật --apply hoặc nếu người dùng bật --dry-run
			isApply := spoolInspectApply && !spoolInspectDryRun

			if !isApply {
				// Read-only dry run
				report, err := spool.ScanInventoryOffline(spoolInspectDir, spoolInspectTrustedRoots)
				if err != nil {
					return fmt.Errorf("scan spool inventory: %w", err)
				}

				if spoolInspectJSON {
					enc := json.NewEncoder(os.Stdout)
					enc.SetIndent("", "  ")
					return enc.Encode(report)
				}

				fmt.Printf("=== SPOOL INVENTORY (DRY RUN / READ-ONLY) ===\n")
				fmt.Printf("Spool Dir:        %s\n", spoolInspectDir)
				fmt.Printf("Trusted Roots:    %v\n", report.TrustedRoots)
				fmt.Printf("Discovered Files: %d (%d bytes)\n", report.DiscoveredFiles, report.DiscoveredBytes)
				fmt.Printf("New Sidecars:     %d (%d bytes)\n", report.NewFiles, report.NewBytes)
				fmt.Printf("Already Indexed:  %d (%d bytes)\n", report.ExistingFiles, report.ExistingBytes)
				if len(report.Warnings) > 0 {
					fmt.Printf("Warnings (%d):\n", len(report.Warnings))
					for _, w := range report.Warnings {
						fmt.Printf("  - %s\n", w)
					}
				}
				if len(report.Errors) > 0 {
					fmt.Printf("Errors (%d):\n", len(report.Errors))
					for _, e := range report.Errors {
						fmt.Printf("  - %s\n", e)
					}
				}
				fmt.Printf("Hành động: Không có thay đổi nào được ghi vào cơ sở dữ liệu. Sử dụng --apply để cập nhật accounting.\n")
				return nil
			}

			// Apply changes
			result, err := spool.ImportInventoryOffline(spoolInspectDir, spoolInspectTrustedRoots)
			if err != nil {
				return fmt.Errorf("import spool inventory: %w", err)
			}

			if spoolInspectJSON {
				enc := json.NewEncoder(os.Stdout)
				enc.SetIndent("", "  ")
				return enc.Encode(result)
			}

			fmt.Printf("=== SPOOL INVENTORY IMPORT APPLIED ===\n")
			fmt.Printf("Spool Dir:      %s\n", spoolInspectDir)
			fmt.Printf("Imported Files: %d (%d bytes)\n", result.ImportedFiles, result.ImportedBytes)
			fmt.Printf("Total Sidecars: %d (%d bytes)\n", result.TotalSidecars, result.TotalBytes)
			fmt.Printf("Trạng thái: Hoàn tất hạch toán vào chỉ mục Pebble bền vững.\n")
			return nil
		},
	}

	cmd.Flags().StringVar(&spoolInspectDir, "spool-dir", "", "Đường dẫn thư mục Pebble spool database")
	cmd.Flags().StringSliceVar(&spoolInspectTrustedRoots, "trusted-root", nil, "Thư mục spill đáng tin cậy do operator cung cấp (có thể truyền nhiều lần hoặc phân tách bằng dấu phẩy)")
	cmd.Flags().BoolVar(&spoolInspectApply, "apply", false, "Áp dụng cập nhật hạch toán vào Pebble DB (mặc định false - chỉ kiểm tra)")
	cmd.Flags().BoolVar(&spoolInspectDryRun, "dry-run", false, "Chế độ chạy thử chỉ đọc (mặc định true)")
	cmd.Flags().BoolVar(&spoolInspectJSON, "json", false, "Xuất kết quả dưới định dạng JSON")

	return cmd
}
