package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strings"

	"strand/internal/exitcode"
)

func main() {
	ctx := context.Background()
	if err := rootCmd.ExecuteContext(ctx); err != nil {
		var se *exitcode.Error
		if errors.As(err, &se) {
			fmt.Fprintf(os.Stderr, "Error [%s]: %s\n", se.Code, se.Message)
			os.Exit(se.ExitCode)
		}
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		// Lỗi cú pháp lệnh của Cobra là exit 2; mọi lỗi khác chưa phân loại là exit 1 (02 §10),
		// để systemd vẫn restart được khi lỗi tạm thời (RestartPreventExitStatus=2 3 4 5 10).
		if strings.HasPrefix(err.Error(), "unknown command") || strings.HasPrefix(err.Error(), "unknown flag") ||
			strings.HasPrefix(err.Error(), "unknown shorthand flag") || strings.Contains(err.Error(), "flag needs an argument") ||
			strings.HasPrefix(err.Error(), "invalid argument") {
			os.Exit(exitcode.InvalidSyntax)
		}
		os.Exit(exitcode.Unclassified)
	}
}
