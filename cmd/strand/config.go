package main

import (
	"fmt"

	"github.com/spf13/cobra"
	"strand/internal/config"
)

func newConfigCmd() *cobra.Command {
	configCmd := &cobra.Command{
		Use:   "config",
		Short: "Thao tác với cấu hình Strand",
	}

	schemaCmd := &cobra.Command{
		Use:   "schema",
		Short: "In JSON Schema của file cấu hình Strand theo spec/02 §10",
		RunE: func(cmd *cobra.Command, args []string) error {
			fmt.Println(config.JSONSchema)
			return nil
		},
	}

	configCmd.AddCommand(schemaCmd)
	return configCmd
}
