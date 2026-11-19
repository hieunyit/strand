package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/spf13/cobra"
	"log/slog"

	"strand/internal/backup"
	"strand/internal/mcp"
	"strand/internal/web"
)

// newMcpCmd exposes the durable backup catalog over MCP stdio. stdout is reserved
// for JSON-RPC; diagnostics and startup failures use stderr through Cobra/main.
func newMcpCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "mcp",
		Short: "Expose the read-only backup catalog over MCP stdio",
		Long:  "Run a read-only MCP server over stdin/stdout using the Web metadata database. Configure it with --config web.yaml or STRAND_WEB_DATABASE_DSN.",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			ctx, stop := signal.NotifyContext(cmd.Context(), syscall.SIGTERM, os.Interrupt)
			defer stop()

			logger := slog.New(slog.NewTextHandler(cmd.ErrOrStderr(), &slog.HandlerOptions{Level: slog.LevelWarn}))
			st, err := web.CLIStore(ctx, cfgPath, logger)
			if err != nil {
				return fmt.Errorf("open backup metadata store: %w", err)
			}
			if st == nil {
				return fmt.Errorf("backup metadata database is required; provide --config web.yaml with database.dsn or set STRAND_WEB_DATABASE_DSN")
			}
			defer st.Close()

			catalog, err := backup.NewCatalog(st)
			if err != nil {
				return fmt.Errorf("initialize backup catalog: %w", err)
			}
			service := catalogMCPService{catalog: catalog}
			if err := mcp.Serve(ctx, cmd.InOrStdin(), cmd.OutOrStdout(), service); err != nil && ctx.Err() == nil {
				return fmt.Errorf("MCP stdio server: %w", err)
			}
			return nil
		},
	}
}

// catalogMCPService deliberately omits bucket/object/manifest locations from
// tool responses. The MCP surface exposes lifecycle and integrity metadata only.
type catalogMCPService struct {
	catalog *backup.Catalog
}

func (s catalogMCPService) ListBackups(ctx context.Context, filters map[string]any) ([]mcp.Record, error) {
	f := backup.Filter{}
	if source, ok := filters["source"].(string); ok {
		f.Source = source
	}
	if engine, ok := filters["engine"].(string); ok {
		f.Engine = engine
	}
	if status, ok := filters["status"].(string); ok {
		f.Status = backup.Status(status)
	}
	if limit, ok := filters["limit"].(int); ok {
		f.Limit = limit
	}
	rows, err := s.catalog.List(ctx, f)
	if err != nil {
		return nil, err
	}
	result := make([]mcp.Record, 0, len(rows))
	for _, row := range rows {
		result = append(result, safeBackupRecord(row))
	}
	return result, nil
}

func (s catalogMCPService) GetBackup(ctx context.Context, backupID string) (mcp.Record, error) {
	row, ok, err := s.catalog.Get(ctx, backupID)
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, fmt.Errorf("backup not found")
	}
	return safeBackupRecord(row), nil
}

func (s catalogMCPService) BackupStatus(ctx context.Context, backupID string) (mcp.Record, error) {
	row, ok, err := s.catalog.Get(ctx, backupID)
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, fmt.Errorf("backup not found")
	}
	return mcp.Record{
		"id": row.ID, "status": row.Status, "created_at": row.CreatedAt,
		"verified_at": row.VerifiedAt, "expires_at": row.ExpiresAt, "legal_hold": row.LegalHold,
	}, nil
}

func safeBackupRecord(row backup.Record) mcp.Record {
	return mcp.Record{
		"id": row.ID, "source": row.Source, "engine": row.Engine,
		"tool": row.Tool, "tool_version": row.ToolVersion, "bytes": row.Bytes,
		"sha256": row.SHA256, "position": row.Position, "status": row.Status,
		"created_at": row.CreatedAt, "verified_at": row.VerifiedAt,
		"expires_at": row.ExpiresAt, "legal_hold": row.LegalHold,
	}
}
