package main

import (
	"fmt"

	"github.com/spf13/cobra"

	"strand/internal/admin"
)

var (
	throttleSourceIOPS int
	throttleTargetIOPS int
	throttleSoft       bool
)

func newThrottleCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "throttle",
		Short: "Điều chỉnh giới hạn tài nguyên qua admin socket (spec/02 §10)",
	}

	setCmd := &cobra.Command{
		Use:   "set",
		Short: "Đổi limit nóng qua admin socket (spec/02 §10)",
		RunE: func(cmd *cobra.Command, args []string) error {
			cli := admin.NewClient(getAdminSocketPath())
			res, err := cli.ThrottleSet(cmd.Context(), throttleSourceIOPS, throttleTargetIOPS, throttleSoft)
			if err != nil {
				return fmt.Errorf("strand throttle set failed: %w", err)
			}
			fmt.Printf("Giới hạn tài nguyên đã được cập nhật thành công:\n")
			fmt.Printf("  Source IOPS: soft=%d, hard=%d\n", res.SourceIOPSSoft, res.SourceIOPSHard)
			fmt.Printf("  Target IOPS: soft=%d, hard=%d\n", res.TargetIOPSSoft, res.TargetIOPSHard)
			return nil
		},
	}

	setCmd.Flags().StringVar(&adminSocketPath, "socket", "", "Đường dẫn admin socket UNIX")
	setCmd.Flags().IntVar(&throttleSourceIOPS, "source-iops", 0, "Giới hạn IOPS source")
	setCmd.Flags().IntVar(&throttleTargetIOPS, "target-iops", 0, "Giới hạn IOPS target")
	setCmd.Flags().BoolVar(&throttleSoft, "soft", false, "Áp dụng cho soft limit")

	cmd.AddCommand(setCmd)
	return cmd
}
