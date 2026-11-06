package seed

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/url"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"

	"strand/internal/checkpoint"
	"strand/internal/exitcode"
	"strand/internal/reader/pg"
)

// spec: 02 §9 (PG 16–18: `strand export`, P = consistent_point của slot), §10; design "`strand export`"

// ExportConfig là cấu hình của `strand export`.
type ExportConfig struct {
	SourceDSN   string   // DSN PostgreSQL source (user cần quyền REPLICATION)
	Tables      []string // bảng cần dump (pattern -t của pg_dump)
	Out         string   // thư mục (-Fd) hoặc file (-Fc/-Ft/-Fp) đích của pg_dump
	SlotName    string   // tên slot logical tạo trên source; rỗng = tự sinh
	Jobs        int      // số worker pg_dump (chỉ với -Fd); <=0 = 4
	Format      string   // d | c | t | p; rỗng = d
	PgDumpPath  string   // đường dẫn binary pg_dump; rỗng = "pg_dump"
	ManifestOut string   // đường dẫn seed.json; rỗng = "seed.json"
}

// ExportResult là kết quả của RunExport.
type ExportResult struct {
	Manifest        *SeedManifest
	ConsistentPoint uint64
	SnapshotName    string
	SlotName        string
}

// RunExport chạy `strand export`:
//  1. CREATE_REPLICATION_SLOT <slot> LOGICAL pgoutput (SNAPSHOT 'export') qua reader/pg → P = consistent_point,
//     snapshot S; IDENTIFY_SYSTEM trên cùng kết nối replication → systemid (source_identity).
//  2. pg_dump --snapshot=S cho các bảng đã chọn; kết nối replication giữ mở tới khi pg_dump xong
//     (đóng sớm thì snapshot S hết hiệu lực).
//  3. Ghi seed.json: start_position.lsn = P (X/Y), start_position.slot = slot.
//
// Dump chứa đúng các Txn có commit LSN < P; slot stream mọi Txn có commit LSN ≥ P. Lỗi sau khi đã tạo
// slot thì slot bị xóa để không giữ WAL trên source.
func RunExport(ctx context.Context, cfg ExportConfig) (res *ExportResult, err error) {
	if cfg.SourceDSN == "" {
		return nil, exitcode.New("missing_source", exitcode.InvalidSyntax, "source DSN is required", nil)
	}
	if len(cfg.Tables) == 0 {
		return nil, exitcode.New("missing_tables", exitcode.InvalidSyntax, "at least one table must be specified", nil)
	}
	if cfg.Out == "" {
		return nil, exitcode.New("missing_out", exitcode.InvalidSyntax, "output path is required", nil)
	}
	if strings.Contains(cfg.Out, "://") {
		return nil, exitcode.New("invalid_out", exitcode.InvalidSyntax,
			"remote --out is not supported yet; dump to a local path and upload it", nil)
	}
	if cfg.Jobs <= 0 {
		cfg.Jobs = 4
	}
	if cfg.Format == "" {
		cfg.Format = "d"
	}
	switch cfg.Format {
	case "d", "c", "t", "p":
	default:
		return nil, exitcode.New("invalid_format", exitcode.InvalidSyntax, "--format must be d, c, t or p", nil)
	}
	if cfg.PgDumpPath == "" {
		cfg.PgDumpPath = "pg_dump"
	}
	if cfg.ManifestOut == "" {
		cfg.ManifestOut = "seed.json"
	}
	slot := cfg.SlotName
	if slot == "" {
		slot = fmt.Sprintf("strand_export_%d", time.Now().Unix())
	}

	toolVersion, err := pgDumpVersion(ctx, cfg.PgDumpPath)
	if err != nil {
		return nil, err
	}
	serverVersion, err := pgServerVersion(ctx, cfg.SourceDSN)
	if err != nil {
		return nil, err
	}

	// 1. Tạo slot + export snapshot; reader giữ kết nối replication mở.
	r, err := pg.New(pg.Config{DSN: cfg.SourceDSN, Slot: slot, CreateSlot: true, ExportSnapshot: true})
	if err != nil {
		return nil, fmt.Errorf("create slot %s with exported snapshot: %w", slot, err)
	}
	defer func() {
		_ = r.Close()
		if err != nil {
			// Không để lại slot giữ WAL khi export thất bại.
			if dErr := dropSlot(cfg.SourceDSN, slot); dErr != nil {
				slog.Warn("export thất bại, không xóa được slot", "slot", slot, "error", dErr)
			}
		}
	}()
	p := r.ConsistentPoint().LSN
	snap := r.SnapshotName()
	sysID := r.SystemID()
	if p == 0 || snap == "" || sysID == "" {
		return nil, fmt.Errorf("create slot %s: missing consistent_point/snapshot/systemid", slot)
	}
	slog.Info("strand export: slot đã tạo", "slot", slot, "consistent_point", checkpoint.FormatLSN(p), "snapshot", snap)

	// 2. pg_dump --snapshot (kết nối replication vẫn mở).
	dsn, password := splitPassword(cfg.SourceDSN)
	args := []string{"--snapshot=" + snap, "--dbname=" + dsn, "-F", cfg.Format, "-f", cfg.Out}
	if cfg.Format == "d" && cfg.Jobs > 1 {
		args = append(args, "-j", strconv.Itoa(cfg.Jobs))
	}
	for _, t := range cfg.Tables {
		args = append(args, "-t", t)
	}
	cmd := exec.CommandContext(ctx, cfg.PgDumpPath, args...)
	cmd.Env = os.Environ()
	if password != "" {
		cmd.Env = append(cmd.Env, "PGPASSWORD="+password) // không đưa mật khẩu lên dòng lệnh
	}
	cmd.Stdout, cmd.Stderr = os.Stderr, os.Stderr
	if err = cmd.Run(); err != nil {
		return nil, fmt.Errorf("pg_dump --snapshot=%s: %w", snap, err)
	}
	// Dump xong mới giải phóng snapshot (xmin trên source).
	if err = r.ReleaseSnapshot(); err != nil {
		slog.Warn("đóng kết nối replication giữ snapshot", "error", err)
		err = nil
	}

	// 3. seed.json.
	m := &SeedManifest{
		Version:        1,
		Engine:         "pg",
		Kind:           "strand-export",
		ToolVersion:    toolVersion,
		ToolPrerelease: IsPrereleaseToolVersion(toolVersion),
		SourceIdentity: sysID,
		BackupRef:      cfg.Out,
		StartPosition:  StartPosition{LSN: checkpoint.FormatLSN(p), Slot: slot},
		SchemaSnapshot: cfg.Out, // SD-3: schema nằm trong chính bản dump (cùng snapshot)
		Tables:         exportManifestTables(cfg.Tables),
		Integrity:      &IntegrityInfo{PrepareOK: true},
		// Dump logic: nạp được vào mọi major version / engine đích (SD-4 không ràng buộc dòng version).
		Target:    TargetInfo{Version: "any (logical dump from PostgreSQL " + serverVersion + ")", Compatible: true},
		CreatedAt: time.Now().UTC().Format(time.RFC3339),
	}
	raw, err := json.MarshalIndent(m, "", "  ")
	if err != nil {
		return nil, err
	}
	if err = os.WriteFile(cfg.ManifestOut, raw, 0o640); err != nil {
		return nil, fmt.Errorf("write %s: %w", cfg.ManifestOut, err)
	}
	return &ExportResult{Manifest: m, ConsistentPoint: p, SnapshotName: snap, SlotName: slot}, nil
}

// exportManifestTables ghi các pattern `pg_dump -t` vào "tables" của seed.json. Riêng "*" đứng một mình:
// với pg_dump nó chỉ khớp bảng thấy được qua search_path, còn trong seed.json "*" nghĩa là MỌI bảng
// (SeedCovers) → ghi thành "public.*" để sync không coi bảng của schema khác là đã có (review N06).
func exportManifestTables(patterns []string) []string {
	out := make([]string, 0, len(patterns))
	for _, t := range patterns {
		if strings.TrimSpace(t) == "*" {
			t = "public.*"
		}
		out = append(out, t)
	}
	return out
}

// pgDumpVersion chạy `pg_dump --version` (vd "pg_dump (PostgreSQL) 18.1" → "18.1").
func pgDumpVersion(ctx context.Context, bin string) (string, error) {
	out, err := exec.CommandContext(ctx, bin, "--version").Output()
	if err != nil {
		return "", exitcode.New(exitcode.ErrBinlogConfigInvalid, exitcode.Rejected,
			fmt.Sprintf("cannot run %s --version: %v", bin, err), err)
	}
	f := strings.Fields(strings.TrimSpace(string(out)))
	if len(f) == 0 {
		return "", fmt.Errorf("unexpected %s --version output %q", bin, out)
	}
	return f[len(f)-1], nil
}

// pgServerVersion đọc server_version của source.
func pgServerVersion(ctx context.Context, dsn string) (string, error) {
	conn, err := pgx.Connect(ctx, dsn)
	if err != nil {
		return "", fmt.Errorf("connect source: %w", err)
	}
	defer func() { _ = conn.Close(context.Background()) }()
	var v string
	if err := conn.QueryRow(ctx, "SELECT current_setting('server_version')").Scan(&v); err != nil {
		return "", fmt.Errorf("read source server_version: %w", err)
	}
	return v, nil
}

// dropSlot xóa slot (dùng khi export thất bại).
func dropSlot(dsn, slot string) error {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	conn, err := pgx.Connect(ctx, dsn)
	if err != nil {
		return err
	}
	defer func() { _ = conn.Close(context.Background()) }()
	_, err = conn.Exec(ctx, "SELECT pg_drop_replication_slot($1)", slot)
	return err
}

// splitPassword tách mật khẩu khỏi DSN dạng URL để truyền qua PGPASSWORD; DSN dạng key=value giữ nguyên.
func splitPassword(dsn string) (string, string) {
	u, err := url.Parse(dsn)
	if err != nil || (u.Scheme != "postgres" && u.Scheme != "postgresql") || u.User == nil {
		return dsn, ""
	}
	pw, ok := u.User.Password()
	if !ok {
		return dsn, ""
	}
	u.User = url.User(u.User.Username())
	return u.String(), pw
}
