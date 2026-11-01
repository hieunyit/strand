package main

import (
	"context"
	"errors"
	"fmt"
	"os"

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
		// Lỗi cú pháp Cobra hoặc lỗi flag
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(exitcode.InvalidSyntax)
	}
}
