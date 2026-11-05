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
			fmt.Printf("Đã đổi soft limit IOPS source: soft=%d (hard=%d, chỉ đổi qua cấu hình + restart)\n", res.SourceIOPSSoft, res.SourceIOPSHard)
			if !throttleSoft {
				fmt.Printf("  Lưu ý: hard limit không đổi được khi đang chạy; giá trị đã áp cho soft limit.\n")
			}
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
