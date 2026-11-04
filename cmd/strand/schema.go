package main

import (
	"database/sql"
	"fmt"

	_ "github.com/go-sql-driver/mysql"
	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/spf13/cobra"
	"strand/internal/config"
	"strand/internal/exitcode"
	"strand/internal/schemagen"
)

func schemaRequest(cfg *config.Config) (schemagen.Request, error) {
	r := schemagen.Request{SourceKind: cfg.Source.Kind, TargetKind: cfg.Target.Kind, Rename: cfg.Tables.Rename}
	driver := func(kind string) string {
		if kind == "postgres" {
			return "pgx"
		}
		return "mysql"
	}
	var err error
	r.Source, err = sql.Open(driver(r.SourceKind), cfg.Source.DSN)
	if err != nil {
		return r, err
	}
	r.Target, err = sql.Open(driver(r.TargetKind), cfg.Target.DSN)
	if err != nil {
		r.Source.Close()
		return r, err
	}
	return r, nil
}
func newSchemaCmd() *cobra.Command {
	root := &cobra.Command{Use: "schema", Short: "Quản lý schema ở target"}
	root.AddCommand(&cobra.Command{Use: "init", Short: "Tạo schema/bảng còn thiếu từ source; --dry-run chỉ in DDL", Args: cobra.NoArgs, RunE: func(cmd *cobra.Command, args []string) error {
		if cfgPath == "" {
			return exitcode.New("missing_config_path", exitcode.InvalidSyntax, "missing config file path (use -c or --config)", nil)
		}
		cfg, err := config.LoadFile(cfgPath)
		if err != nil {
			return err
		}
		r, err := schemaRequest(cfg)
		if err != nil {
			return err
		}
		defer r.Source.Close()
		defer r.Target.Close()
		tables, warnings, err := schemagen.Discover(cmd.Context(), r.Source, r.SourceKind, cfg.Tables.Include, cfg.Tables.Exclude)
		if err != nil {
			return err
		}
		r.Tables = tables
		p, err := schemagen.Build(cmd.Context(), r)
		if err != nil {
			return err
		}
		for _, w := range append(warnings, p.Warnings...) {
			fmt.Fprintln(cmd.ErrOrStderr(), "warning: "+w)
		}
		if dryRun {
			for _, q := range p.Statements {
				fmt.Fprintln(cmd.OutOrStdout(), q+";")
			}
			return nil
		}
		if err := p.Apply(cmd.Context(), r.Target); err != nil {
			return err
		}
		fmt.Fprintf(cmd.OutOrStdout(), "Schema initialized: %d table(s) created\n", len(p.Tables))
		return nil
	}})
	return root
}
