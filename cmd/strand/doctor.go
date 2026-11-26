package main

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"strings"
	"unicode"

	"github.com/spf13/cobra"

	"strand/internal/config"
	"strand/internal/doctor"
	"strand/internal/exitcode"
	"strand/internal/findings"
	"strand/internal/mongocdc"
)

func newDoctorCmd() *cobra.Command {
	var asJSON bool
	cmd := &cobra.Command{
		Use:   "doctor",
		Short: "Preflight đầy đủ: liệt kê MỌI vấn đề cùng lúc, kèm cách sửa (chỉ đọc)",
		Long: `doctor chạy mọi kiểm tra của validate và của bước khởi động sync (binlog, quyền, khóa hàng, FK,
Group Replication, bảng đích có dữ liệu, trùng tên constraint...) mà không dừng ở lỗi đầu tiên.
Mỗi vấn đề có mức độ (error/warning/info), mã, đối tượng, thông điệp và cách sửa.
Exit 0 khi không có vấn đề mức error, exit 3 khi có. Chỉ đọc: không DDL, không ghi, không khóa bảng.`,
		Args: cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			if cfgPath == "" {
				return exitcode.New("missing_config_path", exitcode.InvalidSyntax, "missing config file path (use -c or --config)", nil)
			}
			return runDoctor(cmd.Context(), cmd.OutOrStdout(), cfgPath, asJSON)
		},
	}
	cmd.Flags().BoolVar(&asJSON, "json", false, "In kết quả dạng JSON (pipeline, ok, summary, findings[])")
	return cmd
}

// runDoctor nạp cấu hình path, chạy doctor và in kết quả ra out. Lỗi trả về mang exit 3 khi có vấn đề mức error.
func runDoctor(ctx context.Context, out io.Writer, path string, asJSON bool) error {
	c := &findings.Collector{}
	name := path
	cfg, loadErr := config.LoadFile(path)
	if loadErr != nil {
		c.Err(findings.Error, "config_invalid", path, loadErr, "fix the pipeline YAML (and the environment variables / secret files it references)")
	} else {
		name = cfg.Name
		runDoctorChecks(ctx, cfg, c)
	}
	r := findings.NewReport(name, c)
	if asJSON {
		if err := findings.WriteJSON(out, r); err != nil {
			return err
		}
	} else {
		findings.WriteHuman(out, r)
	}
	if loadErr != nil {
		return loadErr
	}
	if !r.OK {
		return exitcode.New("doctor_failed", exitcode.Rejected, fmt.Sprintf("%d blocking problem(s) found", r.Summary.Errors), nil)
	}
	return nil
}

func runDoctorChecks(ctx context.Context, cfg *config.Config, c *findings.Collector) {
	if cfg.IsMongoDB() {
		doctorMongo(ctx, cfg, c)
		return
	}
	closeTunnels, err := config.OpenSSHTunnels(ctx, cfg)
	if err != nil {
		c.Err(findings.Error, "ssh_tunnel_failed", "ssh_tunnel", err, "check the ssh_tunnel settings (host, user, key) of the pipeline")
		return
	}
	defer closeTunnels()
	doctor.Run(ctx, cfg, c)
}

// doctorMongo: mongocdc.Preflight đã gom mọi kiểm tra; mỗi kiểm tra fail/warn thành một vấn đề.
func doctorMongo(ctx context.Context, cfg *config.Config, c *findings.Collector) {
	for _, k := range cfg.MongoDBIgnoredKeys() {
		c.Warn("config_key_ignored", k, k+" has no effect on a MongoDB pipeline", "remove "+k+" from the config")
	}
	cfgs, closeTunnels, _ := mongoDatabaseConfigs(ctx, cfg, slog.New(slog.NewTextHandler(io.Discard, nil)))
	defer closeTunnels()
	checks, _, err := mongoPreflight(ctx, cfgs)
	if err != nil {
		c.Err(findings.Error, "mongo_preflight", "mongodb", err, "check source/target uri and network")
		return
	}
	for _, ch := range checks {
		sev := findings.Info
		switch ch.Status {
		case mongocdc.CheckFail:
			sev = findings.Error
		case mongocdc.CheckWarn:
			sev = findings.Warning
		default:
			continue
		}
		c.Add(findings.Finding{Severity: sev, Code: "mongo_" + snake(ch.Name), Object: "mongodb", Message: ch.Name + ": " + ch.Detail})
	}
}

// snake: "oplog window" → "oplog_window".
func snake(s string) string {
	var b strings.Builder
	for _, r := range strings.ToLower(s) {
		if unicode.IsLetter(r) || unicode.IsDigit(r) {
			b.WriteRune(r)
		} else if b.Len() > 0 && !strings.HasSuffix(b.String(), "_") {
			b.WriteByte('_')
		}
	}
	return strings.TrimSuffix(b.String(), "_")
}
