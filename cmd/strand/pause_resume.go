package main

import (
	"fmt"

	"github.com/spf13/cobra"

	"strand/internal/admin"
)

var (
	pauseSnapshot bool
	snapshotOnly  bool
)

func newPauseCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "pause",
		Short: "Tạm dừng pipeline đồng bộ qua admin socket (spec/02 §10)",
		RunE: func(cmd *cobra.Command, args []string) error {
			cli := admin.NewClient(getAdminSocketPath())
			if pauseSnapshot {
				res, err := cli.PauseSnapshot(cmd.Context())
				if err != nil {
					return fmt.Errorf("strand pause failed: %w", err)
				}
				fmt.Printf("Snapshot đã tạm dừng; stream vẫn chạy. Trạng thái: %s\n", res.State)
				return nil
			}
			res, err := cli.Pause(cmd.Context())
			if err != nil {
				return fmt.Errorf("strand pause failed: %w", err)
			}
			fmt.Printf("Pipeline đã tạm dừng. Trạng thái: %s\n", res.State)
			return nil
		},
	}
	cmd.Flags().StringVar(&adminSocketPath, "socket", "", "Đường dẫn admin socket UNIX")
	cmd.Flags().BoolVar(&pauseSnapshot, "snapshot", false, "Chỉ tạm dừng phần snapshot logic, giữ stream CDC chạy")
	return cmd
}

func newResumeCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "resume",
		Short: "Chạy tiếp pipeline đồng bộ qua admin socket (spec/02 §10)",
		RunE: func(cmd *cobra.Command, args []string) error {
			cli := admin.NewClient(getAdminSocketPath())
			res, err := cli.Resume(cmd.Context(), snapshotOnly)
			if err != nil {
				return fmt.Errorf("strand resume failed: %w", err)
			}
			if res.SnapshotOnly {
				fmt.Printf("Pipeline đã chạy tiếp ở chế độ snapshot-only. Trạng thái: %s\n", res.State)
			} else {
				fmt.Printf("Pipeline đã chạy tiếp bình thường. Trạng thái: %s\n", res.State)
			}
			return nil
		},
	}
	cmd.Flags().StringVar(&adminSocketPath, "socket", "", "Đường dẫn admin socket UNIX")
	cmd.Flags().BoolVar(&snapshotOnly, "snapshot-only", false, "Chỉ chạy phần snapshot, hoãn stream")
	return cmd
}
