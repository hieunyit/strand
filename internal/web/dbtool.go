package web

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"text/tabwriter"
	"time"

	"gopkg.in/yaml.v3"

	"strand/internal/web/store"
)

// `strand web db migrate` (file -> DB, có kiểm chứng) và `strand web db export` (DB -> file, để sao lưu hoặc
// quay lại chế độ file).

// MigrateOptions cho MigrateFiles.
type MigrateOptions struct {
	ConfigPath string // web.yaml của bản cài chế độ file (bắt buộc)
	DSN        string // rỗng: STRAND_WEB_DATABASE_DSN, rồi database.dsn của web.yaml
	SecretKey  string // rỗng: STRAND_WEB_SECRET_KEY
	DryRun     bool
	Out        io.Writer
	Log        *slog.Logger
}

// MigrateReport là kết quả của MigrateFiles.
type MigrateReport struct {
	Items    []fileItem     `json:"items"`
	Summary  *ImportSummary `json:"summary,omitempty"`
	Verified bool           `json:"verified"`
	Problems []string       `json:"problems,omitempty"`
}

func resolveDSN(flag string, c *Config) string {
	if flag != "" {
		return flag
	}
	if v := strings.TrimSpace(os.Getenv(EnvDatabaseDSN)); v != "" {
		return v
	}
	return c.Database.DSN
}

func boxFor(secret string) (*store.Box, error) {
	if secret == "" {
		secret = strings.TrimSpace(os.Getenv(EnvSecretKey))
	}
	if secret == "" {
		return nil, nil
	}
	return store.NewBox(secret)
}

// MigrateFiles nhập trạng thái chế độ file vào PostgreSQL: in kế hoạch (số lượng theo loại, xung đột), nhập
// (idempotent, chạy lại an toàn), rồi kiểm chứng (mọi thứ "new" đã có trong DB với cùng nội dung/sha256) và in
// báo cáo. DryRun: chỉ in kế hoạch, không ghi gì (kể cả lược đồ).
func MigrateFiles(ctx context.Context, o MigrateOptions) (*MigrateReport, error) {
	if o.Out == nil {
		o.Out = io.Discard
	}
	if o.Log == nil {
		o.Log = slog.New(slog.NewTextHandler(io.Discard, nil))
	}
	if o.ConfigPath == "" {
		return nil, errors.New("--config <web.yaml> of the file-mode installation is required")
	}
	cfg, err := LoadConfig(o.ConfigPath)
	if err != nil {
		return nil, err
	}
	dsn := resolveDSN(o.DSN, cfg)
	if dsn == "" {
		return nil, errors.New("no database: pass --dsn, set " + EnvDatabaseDSN + " or database.dsn in web.yaml")
	}
	box, err := boxFor(o.SecretKey)
	if err != nil {
		return nil, err
	}
	pg, err := store.Open(ctx, dsn, store.OpenOptions{Logger: o.Log, NoMigrate: o.DryRun, MaxWait: 15 * time.Second})
	if err != nil {
		return nil, err
	}
	defer pg.Close()
	return migrateWith(ctx, pg, cfg, box, o, func() (bool, string, error) {
		applied, latest, err := store.SchemaStatus(ctx, pg.DB())
		if err != nil {
			return false, "", err
		}
		if applied < latest {
			return true, fmt.Sprintf("database schema at version %d of %d (created on the first real run): every item is compared as new", applied, latest), nil
		}
		return false, "", nil
	})
}

// MigrateFilesFromStore runs the same verified migration against the Server's already-open metadata store.
// The HTTP handler must not open a second connection to the same metadata DB while the web server is active.
func MigrateFilesFromStore(ctx context.Context, st store.Store, o MigrateOptions) (*MigrateReport, error) {
	if st == nil {
		return nil, errors.New("metadata store is unavailable")
	}
	if o.ConfigPath == "" {
		return nil, errors.New("--config <web.yaml> of the file-mode installation is required")
	}
	cfg, err := LoadConfig(o.ConfigPath)
	if err != nil {
		return nil, err
	}
	box, err := boxFor(o.SecretKey)
	if err != nil {
		return nil, err
	}
	return migrateWith(ctx, st, cfg, box, o, nil)
}

func migrateWith(ctx context.Context, st store.Store, cfg *Config, box *store.Box, o MigrateOptions, schemaCheck func() (bool, string, error)) (*MigrateReport, error) {
	if o.Out == nil {
		o.Out = io.Discard
	}
	if o.Log == nil {
		o.Log = slog.New(slog.NewTextHandler(io.Discard, nil))
	}
	im := &importer{st: st, cfg: cfg, box: box, log: o.Log}
	dbEmpty := false
	if o.DryRun && schemaCheck != nil {
		e, note, err := schemaCheck()
		if err != nil {
			return nil, err
		}
		dbEmpty = e
		if note != "" {
			fmt.Fprintln(o.Out, "note:", note)
		}
	}
	items, warns, err := im.plan(ctx, dbEmpty)
	if err != nil {
		return nil, err
	}
	rep := &MigrateReport{Items: items}
	fmt.Fprintf(o.Out, "file state: web config %s, state_dir %s\n", cfg.SourcePath, cfg.StateDir)
	for _, w := range warns {
		fmt.Fprintln(o.Out, "warning:", w)
	}
	if box == nil {
		fmt.Fprintln(o.Out, "note: "+EnvSecretKey+" is not set: pipeline .env files are NOT copied into the database (they stay on disk)")
	}
	printPlan(o.Out, "plan", items)
	if o.DryRun {
		fmt.Fprintln(o.Out, "dry run: nothing was written")
		return rep, nil
	}
	err = st.Locked(ctx, "import", func(ctx context.Context) error {
		sum, err := im.apply(ctx)
		if err != nil {
			return err
		}
		rep.Summary = sum
		b, _ := json.Marshal(sum)
		return st.MetaSet(ctx, importedMetaKey, time.Now().UTC().Format(time.RFC3339)+" "+string(b)+" (strand web db migrate)")
	})
	if err != nil {
		return rep, fmt.Errorf("import: %w (safe to re-run: completed parts are kept, the rest resumes)", err)
	}
	// Kiểm chứng: lập lại kế hoạch trên DB vừa ghi.
	after, _, err := im.plan(ctx, false)
	if err != nil {
		return rep, fmt.Errorf("verify: %w", err)
	}
	before := map[string]string{}
	for _, it := range items {
		before[it.Kind+"/"+it.Name] = it.Status
	}
	for _, it := range after {
		if before[it.Kind+"/"+it.Name] == "new" && it.Status != "present" {
			rep.Problems = append(rep.Problems, fmt.Sprintf("%s %s: still %s after import %s", it.Kind, it.Name, it.Status, it.Note))
		}
	}
	rep.Items = after
	rep.Verified = len(rep.Problems) == 0
	printPlan(o.Out, "result", after)
	if rep.Verified {
		fmt.Fprintln(o.Out, "verified: every new item is in the database with the same content (sha256 of configs, ids of jobs/wizards, audit line count)")
		fmt.Fprintln(o.Out, "next: start the web with "+EnvDatabaseDSN+" (or database.dsn in web.yaml); the files are left untouched for a rollback")
		return rep, nil
	}
	for _, p := range rep.Problems {
		fmt.Fprintln(o.Out, "PROBLEM:", p)
	}
	return rep, errors.New("verification failed: see the problems above")
}

func printPlan(w io.Writer, title string, items []fileItem) {
	counts := map[string]map[string]int{}
	var kinds []string
	for _, it := range items {
		if counts[it.Kind] == nil {
			counts[it.Kind] = map[string]int{}
			kinds = append(kinds, it.Kind)
		}
		counts[it.Kind][it.Status]++
	}
	fmt.Fprintf(w, "\n%s:\n", title)
	tw := tabwriter.NewWriter(w, 2, 4, 2, ' ', 0)
	fmt.Fprintln(tw, "  KIND\tNEW\tPRESENT\tCONFLICT\tSKIPPED")
	for _, k := range kinds {
		c := counts[k]
		fmt.Fprintf(tw, "  %s\t%d\t%d\t%d\t%d\n", k, c["new"], c["present"], c["conflict"], c["skipped"])
	}
	_ = tw.Flush()
	for _, it := range items {
		if it.Status == "conflict" || it.Status == "skipped" || it.Kind == "audit" {
			fmt.Fprintf(w, "  %s %s: %s %s\n", it.Kind, it.Name, it.Status, it.Note)
		}
	}
}

// ---- export ----

// ExportOptions cho ExportDB.
type ExportOptions struct {
	Dir        string // phải rỗng hoặc chưa có
	DSN        string
	ConfigPath string // web.yaml hiện tại (tùy chọn): giữ listen/TLS/state_dir... trong web.yaml xuất ra
	SecretKey  string
	Out        io.Writer
	Log        *slog.Logger
}

// ExportDB ghi toàn bộ trạng thái DB ra cây file của chế độ file:
//
//	<dir>/web.yaml                     password_hash, api_token_hash, cài đặt từ trang Settings; không có database
//	<dir>/state/pipelines.json         pipeline tạo từ web (đường dẫn gốc)
//	<dir>/state/jobs/<id>.json|.log    lịch sử và output job
//	<dir>/state/wizards/*.json         trạng thái wizard
//	<dir>/state/audit.log              audit log (JSON lines, thứ tự gốc)
//	<dir>/pipelines/<name>.yaml|.env   bản cấu hình trong DB (.env chỉ khi có khóa), MANIFEST.json = đường dẫn gốc
func ExportDB(ctx context.Context, o ExportOptions) error {
	if o.Out == nil {
		o.Out = io.Discard
	}
	if o.Dir == "" {
		return errors.New("--dir is required")
	}
	if ents, err := os.ReadDir(o.Dir); err == nil && len(ents) > 0 {
		return fmt.Errorf("%s is not empty", o.Dir)
	}
	cfg := &Config{}
	if o.ConfigPath != "" {
		c, err := LoadConfig(o.ConfigPath)
		if err != nil {
			return err
		}
		cfg = c
	}
	dsn := resolveDSN(o.DSN, cfg)
	if dsn == "" {
		return errors.New("no database: pass --dsn, set " + EnvDatabaseDSN + " or database.dsn in web.yaml")
	}
	box, err := boxFor(o.SecretKey)
	if err != nil {
		return err
	}
	pg, err := store.Open(ctx, dsn, store.OpenOptions{Logger: o.Log, MaxWait: 15 * time.Second})
	if err != nil {
		return err
	}
	defer pg.Close()
	return exportWith(ctx, pg, cfg, box, o)
}

// ExportDBFromStore exports from the already-open server metadata store, avoiding a second DB pool from HTTP.
func ExportDBFromStore(ctx context.Context, st store.Store, o ExportOptions) error {
	if st == nil {
		return errors.New("metadata store is unavailable")
	}
	if o.Dir == "" {
		return errors.New("--dir is required")
	}
	cfg := &Config{}
	if o.ConfigPath != "" {
		loaded, err := LoadConfig(o.ConfigPath)
		if err != nil {
			return err
		}
		cfg = loaded
	}
	box, err := boxFor(o.SecretKey)
	if err != nil {
		return err
	}
	return exportWith(ctx, st, cfg, box, o)
}

func exportWith(ctx context.Context, st store.Store, cfg *Config, box *store.Box, o ExportOptions) error {
	if o.Out == nil {
		o.Out = io.Discard
	}
	if ents, err := os.ReadDir(o.Dir); err == nil && len(ents) > 0 {
		return fmt.Errorf("%s is not empty", o.Dir)
	}
	stateDir := filepath.Join(o.Dir, "state")
	for _, d := range []string{filepath.Join(stateDir, "jobs"), filepath.Join(stateDir, "wizards"), filepath.Join(o.Dir, "pipelines")} {
		if err := os.MkdirAll(d, 0o700); err != nil {
			return err
		}
	}
	write := func(p string, b []byte) error { return os.WriteFile(p, b, 0o600) }

	// web.yaml
	doc := map[string]any{}
	if cfg.SourcePath != "" {
		if b, err := os.ReadFile(cfg.SourcePath); err == nil {
			_ = yaml.Unmarshal(b, &doc)
		}
	}
	if doc == nil {
		doc = map[string]any{}
	}
	delete(doc, "database")
	adm, _, err := st.Admin(ctx)
	if err != nil {
		return err
	}
	if adm.PasswordHash != "" {
		doc["password_hash"] = adm.PasswordHash
	}
	if adm.APITokenHash != "" {
		doc["api_token_hash"] = adm.APITokenHash
	}
	sets, err := st.Settings(ctx)
	if err != nil {
		return err
	}
	for k, v := range sets {
		var x any
		if json.Unmarshal([]byte(v), &x) == nil && settingDefOf(k) != nil {
			doc[k] = x
		}
	}
	yb, err := yaml.Marshal(doc)
	if err != nil {
		return err
	}
	if err := write(filepath.Join(o.Dir, "web.yaml"), yb); err != nil {
		return err
	}

	// pipelines
	rows, err := st.ListPipelines(ctx)
	if err != nil {
		return err
	}
	var created []PipelineEntry
	manifest := map[string]any{}
	envSkipped := 0
	for _, r := range rows {
		m := map[string]any{"config_path": r.ConfigPath, "env_path": r.EnvPath, "origin": r.Origin, "systemd_unit": r.SystemdUnit}
		if r.HasConfig {
			if err := write(filepath.Join(o.Dir, "pipelines", r.Name+".yaml"), []byte(r.ConfigYAML)); err != nil {
				return err
			}
		}
		if len(r.EnvEnc) > 0 {
			if box == nil {
				envSkipped++
				m["env"] = "sealed in the database; not exported (" + EnvSecretKey + " not set)"
			} else if pt, err := box.Open(r.EnvEnc, r.Name); err != nil {
				return fmt.Errorf("pipeline %s: %w", r.Name, err)
			} else if err := write(filepath.Join(o.Dir, "pipelines", r.Name+".env"), pt); err != nil {
				return err
			}
		}
		manifest[r.Name] = m
		if r.Origin == store.OriginCreated || r.Origin == "" {
			created = append(created, PipelineEntry{Name: r.Name, Config: r.ConfigPath, EnvFile: r.EnvPath, SystemdUnit: r.SystemdUnit, LogFile: r.LogFile, PidFile: r.PidFile})
		}
	}
	mb, _ := json.MarshalIndent(manifest, "", "  ")
	if err := write(filepath.Join(o.Dir, "pipelines", "MANIFEST.json"), mb); err != nil {
		return err
	}
	cb, _ := json.MarshalIndent(createdFileDoc{Version: 1, Pipelines: created}, "", "  ")
	if err := write(filepath.Join(stateDir, CreatedFile), append(cb, '\n')); err != nil {
		return err
	}

	// jobs
	jobs, err := st.ListJobs(ctx)
	if err != nil {
		return err
	}
	for _, j := range jobs {
		if !JobIDRE.MatchString(j.ID) {
			continue
		}
		if err := write(filepath.Join(stateDir, "jobs", j.ID+".json"), j.Data); err != nil {
			return err
		}
		f, err := os.OpenFile(filepath.Join(stateDir, "jobs", j.ID+".log"), os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o600)
		if err != nil {
			return err
		}
		var off int64
		for {
			b, err := st.ReadJobOutput(ctx, j.ID, off, 1<<20)
			if err != nil {
				f.Close()
				return err
			}
			if len(b) == 0 {
				break
			}
			if _, err := f.Write(b); err != nil {
				f.Close()
				return err
			}
			off += int64(len(b))
		}
		if err := f.Close(); err != nil {
			return err
		}
	}

	// wizards
	wz, err := st.ListWizards(ctx)
	if err != nil {
		return err
	}
	for _, w := range wz {
		if !ValidName(w.Kind) || !ValidName(w.Pipeline) {
			continue
		}
		if err := write(filepath.Join(stateDir, "wizards", w.Kind+"-"+w.Pipeline+".json"), w.Data); err != nil {
			return err
		}
	}

	// audit
	af, err := os.OpenFile(filepath.Join(stateDir, "audit.log"), os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o600)
	if err != nil {
		return err
	}
	var last int64
	nAudit := 0
	for {
		rs, err := st.AuditAfter(ctx, last, 5000)
		if err != nil {
			af.Close()
			return err
		}
		if len(rs) == 0 {
			break
		}
		for _, r := range rs {
			var compact json.RawMessage = r.Data
			if _, err := af.Write(append(compactJSON(compact), '\n')); err != nil {
				af.Close()
				return err
			}
			last = r.ID
			nAudit++
		}
	}
	if err := af.Close(); err != nil {
		return err
	}
	names := make([]string, 0, len(rows))
	for _, r := range rows {
		names = append(names, r.Name)
	}
	sort.Strings(names)
	fmt.Fprintf(o.Out, "exported to %s: %d pipelines (%s), %d jobs, %d wizards, %d audit rows, web.yaml with the admin hashes and saved settings\n",
		o.Dir, len(rows), strings.Join(names, ", "), len(jobs), len(wz), nAudit)
	if envSkipped > 0 {
		fmt.Fprintf(o.Out, "warning: %d pipeline .env copies are sealed and were not exported (set %s); the .env files on disk are not affected\n", envSkipped, EnvSecretKey)
	}
	fmt.Fprintln(o.Out, "rollback to an older strand build (file mode; this build needs PostgreSQL): stop the web; copy state/* into the state_dir; copy pipelines/<name>.yaml|.env to the paths in pipelines/MANIFEST.json if they differ; use the exported web.yaml (no database section) and unset "+EnvDatabaseDSN+"; start the web")
	return nil
}

// compactJSON: jsonb trả về có khoảng trắng; audit.log là một dòng mỗi bản ghi.
func compactJSON(b []byte) []byte {
	var out bytes.Buffer
	if json.Compact(&out, b) != nil {
		return b
	}
	return out.Bytes()
}
