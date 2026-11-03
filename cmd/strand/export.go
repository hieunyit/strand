package main

import (
	"fmt"
	"strings"

	"github.com/spf13/cobra"

	"strand/internal/exitcode"
	"strand/internal/seed"
)

// newExportCmd tạo lệnh `strand export` theo spec/02 §10 và spec/design §line 398.
func newExportCmd() *cobra.Command {
	var (
		sourceDSN   string
		tables      []string
		outDir      string
		slotName    string
		jobs        int
		format      string
		pgDumpPath  string
		manifestOut string
	)

	cmd := &cobra.Command{
		Use:   "export",
		Short: "Dump logic PostgreSQL gắn với snapshot của replication slot (spec/02 §10)",
		RunE: func(cmd *cobra.Command, args []string) error {
			if sourceDSN == "" {
				return exitcode.New("missing_source", exitcode.InvalidSyntax, "missing --source", nil)
			}
			if len(tables) == 0 {
				return exitcode.New("missing_tables", exitcode.InvalidSyntax, "missing --tables", nil)
			}
			if outDir == "" {
				return exitcode.New("missing_out", exitcode.InvalidSyntax, "missing --out", nil)
			}

			// Tách các bảng nếu người dùng truyền dạng phân cách bởi dấu phẩy
			var parsedTables []string
			for _, t := range tables {
				for _, sub := range strings.Split(t, ",") {
					sub = strings.TrimSpace(sub)
					if sub != "" {
						parsedTables = append(parsedTables, sub)
					}
				}
			}

			if dryRun {
				fmt.Printf("strand export --source %s --tables %s --out %s (dry-run)\n",
					sourceDSN, strings.Join(parsedTables, ","), outDir)
				return nil
			}

			cfg := seed.ExportConfig{
				SourceDSN:   sourceDSN,
				Tables:      parsedTables,
				Out:         outDir,
				SlotName:    slotName,
				Jobs:        jobs,
				Format:      format,
				PgDumpPath:  pgDumpPath,
				ManifestOut: manifestOut,
			}

			res, err := seed.RunExport(cmd.Context(), cfg)
			if err != nil {
				return err
			}

			fmt.Printf("export OK: P = %s (consistent_point of slot %s), manifest %s\n",
				res.Manifest.StartPosition.LSN, res.SlotName, manifestOut)
			return nil
		},
	}

	cmd.Flags().StringVar(&sourceDSN, "source", "", "DSN PostgreSQL source")
	cmd.Flags().StringSliceVar(&tables, "tables", nil, "Danh sách bảng cần dump (cách nhau bởi dấu phẩy hoặc lặp lại cờ)")
	cmd.Flags().StringVar(&outDir, "out", "", "Thư mục (-F d) hoặc file (-F c/t/p) đích của pg_dump")
	cmd.Flags().StringVar(&slotName, "slot", "", "Tên replication slot tạo trên source (tùy chọn)")
	cmd.Flags().IntVarP(&jobs, "jobs", "j", 4, "Số luồng chạy song song cho pg_dump")
	cmd.Flags().StringVarP(&format, "format", "F", "d", "Định dạng pg_dump (d: directory, c: custom, t: tar, p: plain)")
	cmd.Flags().StringVar(&pgDumpPath, "pg-dump", "pg_dump", "Đường dẫn binary pg_dump")
	cmd.Flags().StringVar(&manifestOut, "manifest", "seed.json", "Đường dẫn file seed.json")

	return cmd
}
