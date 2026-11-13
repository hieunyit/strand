package main

import (
	"fmt"
	"time"

	"github.com/spf13/cobra"

	"strand/internal/admin"
	"strand/internal/config"
)

var adminSocketPath string

func newTopCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "top",
		Short: "TUI hiển thị lag, throughput, zone, tiến độ, ETA (spec/02 §10)",
		RunE: func(cmd *cobra.Command, args []string) error {
			cli := admin.NewClient(getAdminSocketPath())
			top, err := cli.Top(cmd.Context())
			if err != nil {
				return fmt.Errorf("strand top failed: %w", err)
			}

			printTop(top)
			return nil
		},
	}
	cmd.Flags().StringVar(&adminSocketPath, "socket", "", "Đường dẫn admin socket UNIX")
	return cmd
}

func getAdminSocketPath() string {
	if adminSocketPath != "" {
		return adminSocketPath
	}
	if cfgPath != "" {
		// Chỉ cần đường dẫn socket: không đòi biến môi trường mật khẩu (trước đây phải thêm --socket).
		if cfg, err := config.LoadFileForPaths(cfgPath); err == nil && cfg.Observability.AdminSocket != "" {
			return cfg.Observability.AdminSocket
		}
	}
	return "/run/strand/default.sock"
}

func printTop(top *admin.TopInfo) {
	fmt.Printf("Pipeline:   %s\n", top.PipelineID)
	fmt.Printf("Trạng thái: %s\n", top.State)
	fmt.Printf("Lag:        %.3f s\n", top.LagSeconds)
	fmt.Printf("Tốc độ:     %.1f rows/s (stream)\n", top.Throughput)
	fmt.Printf("Vùng (Zone):%s\n", top.Zone)
	if top.BreakerOpen {
		fmt.Printf("Breaker:    MỞ (snapshot tạm dừng để bảo vệ nguồn): %s\n", top.BreakerReason)
	}
	if top.TablesTotal > 0 {
		fmt.Printf("Snapshot:   %d/%d bảng", top.TablesDone, top.TablesTotal)
		if top.SnapshotTable != "" {
			fmt.Printf(", đang chạy %s", top.SnapshotTable)
		}
		fmt.Println()
		fmt.Printf("Đã chép:    %d / ~%d rows, %.0f rows/s\n", top.SnapshotRows, top.SnapshotRowsTotal, top.SnapshotRate)
	}
	fmt.Printf("Tiến độ:    %.1f %%\n", top.Progress)
	fmt.Printf("ETA:        %s\n", fmtETA(top.ETASeconds))
}

func fmtETA(s float64) string {
	if s <= 0 {
		return "-"
	}
	d := time.Duration(s * float64(time.Second)).Round(time.Second)
	return d.String()
}
