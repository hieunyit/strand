package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/spf13/cobra"
	"log/slog"

	"strand/internal/backup"
	"strand/internal/mcp"
	"strand/internal/web"
	"strand/internal/web/store"
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
			auditor := mcpAuditor{st: st}
			if err := mcp.Serve(ctx, cmd.InOrStdin(), cmd.OutOrStdout(), service, mcp.WithAuditor(auditor)); err != nil && ctx.Err() == nil {
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
		return nil, mcpErr(err)
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
		return nil, mcpErr(err)
	}
	if !ok {
		return nil, mcp.ErrNotFound
	}
	return safeBackupRecord(row), nil
}

func (s catalogMCPService) BackupStatus(ctx context.Context, backupID string) (mcp.Record, error) {
	row, ok, err := s.catalog.Get(ctx, backupID)
	if err != nil {
		return nil, mcpErr(err)
	}
	if !ok {
		return nil, mcp.ErrNotFound
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

// mcpErr exposes validation messages (they contain only the caller's own input) and hides the rest.
func mcpErr(err error) error {
	if errors.Is(err, backup.ErrInvalid) {
		return &mcp.InvalidArgumentError{Msg: err.Error()}
	}
	return err
}

// mcpAuditor writes one Web audit row per MCP tool call (action "mcp.<tool>"), so MCP access shows up
// in the same audit trail as the Web UI and the backup CLI.
type mcpAuditor struct{ st store.Store }

func (a mcpAuditor) Audit(ctx context.Context, tool string, args map[string]any, result string) error {
	known, other := map[string]any{}, 0
	for k, v := range args {
		switch k {
		case "source", "engine", "status", "limit", "backup_id":
			if s, ok := v.(string); ok && len(s) > 256 {
				v = s[:256]
			}
			known[k] = v
		default:
			other++
		}
	}
	detail := map[string]any{"arguments": known, "transport": "stdio"}
	if other > 0 {
		detail["unrecognised_arguments"] = other
	}
	return auditBackup(ctx, a.st, "mcp."+tool, result, detail)
}
