package main

import (
	"fmt"
	"runtime"

	"github.com/spf13/cobra"
)

// version, commit được gắn lúc build: go build -ldflags "-X main.version=1.2.3 -X main.commit=abc123"
// (scripts/package.sh). Build tay không gắn thì là "dev".
var (
	version = "dev"
	commit  = ""
)

func versionString() string {
	s := version
	if commit != "" {
		s += " (" + commit + ")"
	}
	return fmt.Sprintf("%s %s/%s %s", s, runtime.GOOS, runtime.GOARCH, runtime.Version())
}

func newVersionCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "version",
		Short: "In phiên bản của strand",
		Args:  cobra.NoArgs,
		Run: func(cmd *cobra.Command, args []string) {
			fmt.Fprintln(cmd.OutOrStdout(), "strand "+versionString())
		},
	}
}
