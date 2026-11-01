package main

import (
	"fmt"

	"github.com/spf13/cobra"
)

var (
	cfgPath string
	dryRun  bool
)

var rootCmd = &cobra.Command{
	Use:   "strand",
	Short: "Strand CDC — Công cụ đồng bộ dữ liệu CDC phân tán",
	Long: `Strand là hệ thống CDC đồng bộ dữ liệu giữa MySQL và PostgreSQL
với các bất biến cốt lõi về thứ tự commit và snapshot nhất quán.`,
	SilenceErrors: true,
	SilenceUsage:  true,
}

func init() {
	rootCmd.PersistentFlags().StringVarP(&cfgPath, "config", "c", "", "Đường dẫn tệp cấu hình pipeline (YAML)")
	rootCmd.PersistentFlags().BoolVar(&dryRun, "dry-run", false, "Chế độ chạy thử không tác động đích")

	rootCmd.AddCommand(newConfigCmd())
	rootCmd.AddCommand(newValidateCmd())
	rootCmd.AddCommand(newPlanCmd())
	rootCmd.AddCommand(newSyncCmd())
	rootCmd.AddCommand(newTopCmd())
}

func newPlanCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "plan",
		Short: "Ước lượng ETA; từ chối chế độ A khi ETA > 50% retention",
		RunE: func(cmd *cobra.Command, args []string) error {
			return fmt.Errorf("plan command not yet implemented")
		},
	}
}

func newSyncCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "sync",
		Short: "Chạy pipeline đồng bộ CDC",
		RunE: func(cmd *cobra.Command, args []string) error {
			return fmt.Errorf("sync command not yet implemented")
		},
	}
}

func newTopCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "top",
		Short: "TUI hiển thị lag, throughput, zone, tiến độ, ETA",
		RunE: func(cmd *cobra.Command, args []string) error {
			return fmt.Errorf("top command not yet implemented")
		},
	}
}
