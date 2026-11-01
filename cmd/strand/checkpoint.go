package main

import (
	"fmt"

	"github.com/spf13/cobra"

	"strand/internal/admin"
)

var checkpointRewindTo string

func newCheckpointCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "checkpoint",
		Short: "Xem hoặc tua lại vị trí checkpoint qua admin socket (spec/02 §10)",
	}

	showCmd := &cobra.Command{
		Use:   "show",
		Short: "Xem vị trí checkpoint hiện tại (spec/02 §10)",
		RunE: func(cmd *cobra.Command, args []string) error {
			cli := admin.NewClient(getAdminSocketPath())
			res, err := cli.CheckpointShow(cmd.Context())
			if err != nil {
				return fmt.Errorf("strand checkpoint show failed: %w", err)
			}
			fmt.Printf("Pipeline:   %s\n", res.PipelineID)
			fmt.Printf("Epoch:      %d\n", res.Epoch)
			fmt.Printf("Applied:    %s\n", res.Applied)
			fmt.Printf("Updated At: %s\n", res.UpdatedAt.Format("2006-01-02 15:04:05"))
			return nil
		},
	}

	rewindCmd := &cobra.Command{
		Use:   "rewind",
		Short: "Tua lại vị trí checkpoint, có kiểm tra gtid_purged / restart_lsn (spec/02 §10)",
		RunE: func(cmd *cobra.Command, args []string) error {
			if checkpointRewindTo == "" {
				return fmt.Errorf("missing required flag --to <gtid|lsn>")
			}
			cli := admin.NewClient(getAdminSocketPath())
			res, err := cli.CheckpointRewind(cmd.Context(), checkpointRewindTo)
			if err != nil {
				return fmt.Errorf("strand checkpoint rewind failed: %w", err)
			}
			fmt.Printf("Đã tua lại checkpoint thành công tới: %s\n", res.RewoundTo)
			fmt.Printf("Epoch mới:  %d\n", res.Epoch)
			fmt.Printf("Applied:    %s\n", res.Applied)
			return nil
		},
	}

	showCmd.Flags().StringVar(&adminSocketPath, "socket", "", "Đường dẫn admin socket UNIX")
	rewindCmd.Flags().StringVar(&adminSocketPath, "socket", "", "Đường dẫn admin socket UNIX")
	rewindCmd.Flags().StringVar(&checkpointRewindTo, "to", "", "Vị trí tua lại (GTID set hoặc LSN)")

	cmd.AddCommand(showCmd)
	cmd.AddCommand(rewindCmd)
	return cmd
}
