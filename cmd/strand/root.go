package main

import (
	"github.com/spf13/cobra"

	"strand/internal/config"
	"strand/internal/exitcode"
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
	rootCmd.AddCommand(newSchemaCmd())
	rootCmd.AddCommand(newValidateCmd())
	rootCmd.AddCommand(newPlanCmd())
	rootCmd.AddCommand(newSyncCmd())
	rootCmd.AddCommand(newStopCmd())
	rootCmd.AddCommand(newStatusCmd())
	rootCmd.AddCommand(newTopCmd())
	rootCmd.AddCommand(newPauseCmd())
	rootCmd.AddCommand(newResumeCmd())
	rootCmd.AddCommand(newThrottleCmd())
	rootCmd.AddCommand(newCheckpointCmd())
	rootCmd.AddCommand(newPromoteCmd())
	rootCmd.AddCommand(newSnapshotCmd())
	rootCmd.AddCommand(newSeedCmd())
	rootCmd.AddCommand(newExportCmd())
	rootCmd.AddCommand(newVerifyCmd())
	rootCmd.AddCommand(newVersionCmd())
	rootCmd.AddCommand(newArchiveCmd())
	rootCmd.AddCommand(newObjectsCmd())
	rootCmd.Version = versionString()
	rootCmd.SetVersionTemplate("strand {{.Version}}\n")
}

// normalizeDSNFlags đổi các cờ DSN dạng mysql://... sang DSN của driver (cùng cú pháp với file cấu hình).
func normalizeDSNFlags(ptrs ...*string) error {
	for _, p := range ptrs {
		if p == nil || *p == "" {
			continue
		}
		v, err := config.NormalizeMySQLDSN(*p)
		if err != nil {
			return exitcode.New("invalid_connection", exitcode.InvalidSyntax, err.Error(), err)
		}
		*p = v
	}
	return nil
}
