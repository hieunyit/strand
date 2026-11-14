package main

import (
	"database/sql"
	"fmt"
	"strconv"
	"strings"

	_ "github.com/go-sql-driver/mysql"
	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/spf13/cobra"
	"strand/internal/config"
	"strand/internal/exitcode"
	"strand/internal/schemachange"
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
	root.AddCommand(newSchemaDiffCmd(), newSchemaApproveCmd())
	return root
}

// openSchemaStore mở kết nối target của cấu hình và Store các thay đổi schema đang chờ (P3-01).
func openSchemaStore() (*config.Config, *sql.DB, *schemachange.Store, error) {
	if cfgPath == "" {
		return nil, nil, nil, exitcode.New("missing_config_path", exitcode.InvalidSyntax, "missing config file path (use -c or --config)", nil)
	}
	cfg, err := config.LoadFile(cfgPath)
	if err != nil {
		return nil, nil, nil, err
	}
	driver := "mysql"
	if cfg.Target.Kind == "postgres" {
		driver = "pgx"
	}
	db, err := sql.Open(driver, cfg.Target.DSN)
	if err != nil {
		return nil, nil, nil, err
	}
	return cfg, db, schemachange.NewStore(db, cfg.Target.Kind == "postgres"), nil
}

func printSchemaChange(cmd *cobra.Command, c schemachange.Change) {
	out := cmd.OutOrStdout()
	fmt.Fprintf(out, "#%d  [%s]  %s  at %s  (%s)\n", c.ID, c.Status, c.Classification, c.SourcePos, c.CreatedAt.Format("2006-01-02 15:04:05"))
	fmt.Fprintf(out, "  source DDL: %s\n", strings.TrimSpace(c.SourceDDL))
	fmt.Fprintf(out, "  changes:    %s\n", c.Reason)
	fmt.Fprintln(out, "  proposed target DDL:")
	for _, st := range c.Statements {
		if strings.HasPrefix(st, schemagen.ManualPrefix) {
			fmt.Fprintf(out, "    %s\n", st)
			continue
		}
		fmt.Fprintf(out, "    %s;\n", st)
	}
}

func newSchemaDiffCmd() *cobra.Command {
	var all bool
	c := &cobra.Command{Use: "diff", Short: "Liệt kê thay đổi schema đang chờ duyệt cùng DDL đề xuất cho target", Args: cobra.NoArgs, RunE: func(cmd *cobra.Command, args []string) error {
		cfg, db, store, err := openSchemaStore()
		if err != nil {
			return err
		}
		defer db.Close()
		changes, err := store.List(cmd.Context(), cfg.Name, all)
		if err != nil {
			return err
		}
		if len(changes) == 0 {
			fmt.Fprintln(cmd.OutOrStdout(), "No pending schema changes.")
			return nil
		}
		for _, c := range changes {
			printSchemaChange(cmd, c)
		}
		fmt.Fprintf(cmd.OutOrStdout(), "Apply with: strand schema approve <id> -c %s\n", cfgPath)
		return nil
	}}
	c.Flags().BoolVar(&all, "all", false, "Gồm cả thay đổi đã áp")
	return c
}

func newSchemaApproveCmd() *cobra.Command {
	return &cobra.Command{Use: "approve <id>", Short: "Áp DDL đề xuất của một thay đổi schema lên target (--dry-run chỉ in DDL)", Args: cobra.ExactArgs(1), RunE: func(cmd *cobra.Command, args []string) error {
		id, err := strconv.ParseInt(args[0], 10, 64)
		if err != nil {
			return exitcode.New("invalid_syntax", exitcode.InvalidSyntax, fmt.Sprintf("invalid schema change id %q", args[0]), err)
		}
		cfg, db, store, err := openSchemaStore()
		if err != nil {
			return err
		}
		defer db.Close()
		c, applied, err := store.Approve(cmd.Context(), cfg.Name, id, dryRun)
		if err != nil {
			return err
		}
		switch {
		case dryRun:
			printSchemaChange(cmd, c)
		case applied:
			fmt.Fprintf(cmd.OutOrStdout(), "Schema change #%d applied (%d statement(s)). Run strand sync to continue.\n", id, len(c.Statements))
		default:
			fmt.Fprintf(cmd.OutOrStdout(), "Schema change #%d is already applied; nothing to do.\n", id)
		}
		return nil
	}}
}
