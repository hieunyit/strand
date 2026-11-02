package main

import (
	"fmt"

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

			fmt.Printf("Pipeline:   %s\n", top.PipelineID)
			fmt.Printf("Trạng thái: %s\n", top.State)
			fmt.Printf("Lag:        %.3f s\n", top.LagSeconds)
			fmt.Printf("Tốc độ:     %.1f rows/s\n", top.Throughput)
			fmt.Printf("Vùng (Zone):%s\n", top.Zone)
			fmt.Printf("Tiến độ:    %.1f %%\n", top.Progress)
			fmt.Printf("ETA:        %.1f s\n", top.ETASeconds)
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
		if cfg, err := config.LoadFile(cfgPath); err == nil && cfg.Observability.AdminSocket != "" {
			return cfg.Observability.AdminSocket
		}
	}
	return "/run/strand/default.sock"
}
