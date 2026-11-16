package web

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"log/slog"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"strand/internal/web/store"
)

// DatabaseConfig là mục `database:` của web.yaml. Không có DSN (cả trong file lẫn STRAND_WEB_DATABASE_DSN)
// thì Web UI chạy chế độ file như trước.
type DatabaseConfig struct {
	// DSN: postgres://user:pass@host:5432/db. Biến môi trường STRAND_WEB_DATABASE_DSN thắng giá trị này.
	DSN string `yaml:"dsn,omitempty"`
	// Audit: nơi ghi audit log: db | file | both (mặc định both: DB là nguồn chính, file là bản sao
	// để còn dấu vết khi mất DB).
	Audit string `yaml:"audit,omitempty"`
}

// Biến môi trường.
const (
	EnvDatabaseDSN = "STRAND_WEB_DATABASE_DSN"
	EnvSecretKey   = "STRAND_WEB_SECRET_KEY"
)

// Giá trị database.audit.
const (
	AuditDB   = "db"
	AuditFile = "file"
	AuditBoth = "both"
)

// DBEnabled: có DSN nên dùng PostgreSQL.
func (c *Config) DBEnabled() bool { return c.Database.DSN != "" }

// metaDB là kho metadata PostgreSQL (hoặc Mem trong test) gắn với một Server.
type metaDB struct {
	st       store.Store
	box      *store.Box // nil: không có STRAND_WEB_SECRET_KEY, .env chỉ nằm trên đĩa
	log      *slog.Logger
	secret   string
	previous string // STRAND_WEB_SECRET_KEY_PREVIOUS (xoay khóa)

	mu      sync.Mutex
	admin   store.Admin
	adminAt time.Time

	warnedKey sync.Once

	metrics metricsBuf // mẫu số liệu chờ ghi theo lô
}

const adminCacheTTL = 3 * time.Second

func dbCtx() (context.Context, context.CancelFunc) {
	return context.WithTimeout(context.Background(), 10*time.Second)
}

// OpenStore mở kho theo cấu hình (thử lại với backoff, áp migration). Lỗi đã che mật khẩu.
func OpenStore(ctx context.Context, c *Config, log *slog.Logger) (store.Store, error) {
	if !c.DBEnabled() {
		return nil, errors.New("no database configured")
	}
	return store.Open(ctx, c.Database.DSN, store.OpenOptions{Logger: log})
}

func newMetaDB(st store.Store, secret string, log *slog.Logger) (*metaDB, error) {
	m := &metaDB{st: st, log: log, secret: secret}
	if secret != "" {
		b, err := store.NewBox(secret)
		if err != nil {
			return nil, err
		}
		m.box = b
	}
	return m, nil
}

// adminHashes trả (password_hash, api_token_hash) từ DB, cache vài giây (đổi mật khẩu bằng CLI có hiệu lực
// ngay mà không cần khởi động lại). Lỗi DB: dùng giá trị cache cuối.
func (m *metaDB) adminHashes() (string, string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if time.Since(m.adminAt) < adminCacheTTL {
		return m.admin.PasswordHash, m.admin.APITokenHash
	}
	ctx, cancel := dbCtx()
	defer cancel()
	a, _, err := m.st.Admin(ctx)
	if err != nil {
		m.log.Warn("read admin credentials failed", "err", err)
		return m.admin.PasswordHash, m.admin.APITokenHash
	}
	m.admin, m.adminAt = a, time.Now()
	return a.PasswordHash, a.APITokenHash
}

func (s *Server) passwordHash() string {
	if s.db != nil {
		p, _ := s.db.adminHashes()
		return p
	}
	return s.cfg.PasswordHash
}

func (s *Server) apiTokenHash() string {
	if s.db != nil {
		_, t := s.db.adminHashes()
		return t
	}
	return s.cfg.APITokenHash
}

// SetStorePassword đặt mật khẩu admin trong DB và hủy MỌI session đang có. Chế độ file đổi mật khẩu cần khởi
// động lại web (session nằm trong RAM nên tự mất); chế độ DB session sống qua khởi động lại, nên phải xóa
// tường minh: người giữ cookie cũ (có thể là lý do đổi mật khẩu) không được dùng tiếp tới 12 giờ.
func SetStorePassword(ctx context.Context, st store.Store, hash string) error {
	if err := st.SetPasswordHash(ctx, hash); err != nil {
		return err
	}
	all := time.Now().Add(100 * 365 * 24 * time.Hour)
	return st.PurgeSessions(ctx, all, all)
}

// ---- khóa bí mật: kiểm tra và xoay khóa ----

const (
	keyCheckMetaKey = "secret_key_check"
	keyCheckPlain   = "strand-web secret key check"
	keyCheckAAD     = "\x00key-check"
	// EnvSecretKeyPrevious: khóa cũ khi xoay khóa (một lần khởi động: giải mã bằng khóa cũ, mã hóa lại bằng khóa mới).
	EnvSecretKeyPrevious = "STRAND_WEB_SECRET_KEY_PREVIOUS"
)

// checkSecretKey đảm bảo STRAND_WEB_SECRET_KEY là đúng khóa đã mã hóa các .env trong DB. Khóa sai thì lỗi rõ
// ràng khi khởi động (trước đây: mỗi lần start chỉ có cảnh báo "decrypt failed" và .env trong DB lặng lẽ bị
// mã hóa lại bằng khóa khác khi sửa). Có STRAND_WEB_SECRET_KEY_PREVIOUS: mã hóa lại mọi .env sang khóa mới.
func (m *metaDB) checkSecretKey(ctx context.Context, previous string) error {
	if m.box == nil {
		return nil
	}
	return m.st.Locked(ctx, "secret-key", func(ctx context.Context) error {
		var old *store.Box
		if previous != "" {
			b, err := store.NewBox(previous)
			if err != nil {
				return fmt.Errorf("%s: %w", EnvSecretKeyPrevious, err)
			}
			old = b
		}
		opens := func(b *store.Box, sealed []byte, aad string) bool {
			_, err := b.Open(sealed, aad)
			return err == nil
		}
		v, have, err := m.st.MetaGet(ctx, keyCheckMetaKey)
		if err != nil {
			return err
		}
		rows, err := m.st.ListPipelines(ctx)
		if err != nil {
			return err
		}
		currentOK := true
		if have {
			currentOK = opens(m.box, []byte(v), keyCheckAAD)
		} else {
			// DB của bản cũ chưa có dấu kiểm: thử với các .env đã lưu.
			for _, r := range rows {
				if len(r.EnvEnc) > 0 && !opens(m.box, r.EnvEnc, r.Name) {
					currentOK = false
					break
				}
			}
		}
		if !currentOK && old == nil {
			return fmt.Errorf("%s does not match the key the pipeline .env copies in the database were encrypted with: "+
				"start with the previous key, or to rotate set %s=<previous key> together with the new %s (all copies are re-encrypted on start)",
				EnvSecretKey, EnvSecretKeyPrevious, EnvSecretKey)
		}
		if old != nil {
			n := 0
			for _, r := range rows {
				if len(r.EnvEnc) == 0 || opens(m.box, r.EnvEnc, r.Name) {
					continue
				}
				pt, err := old.Open(r.EnvEnc, r.Name)
				if err != nil {
					return fmt.Errorf("pipeline %s: .env copy opens with neither %s nor %s", r.Name, EnvSecretKey, EnvSecretKeyPrevious)
				}
				if r.EnvEnc, err = m.box.Seal(pt, r.Name); err != nil {
					return err
				}
				if err := m.st.UpsertPipeline(ctx, r); err != nil {
					return err
				}
				n++
			}
			if n > 0 {
				m.log.Info("re-encrypted pipeline .env copies with the new secret key; remove "+EnvSecretKeyPrevious+" now", "pipelines", n)
			}
		}
		if have && currentOK {
			return nil
		}
		sealed, err := m.box.Seal([]byte(keyCheckPlain), keyCheckAAD)
		if err != nil {
			return err
		}
		return m.st.MetaSet(ctx, keyCheckMetaKey, string(sealed))
	})
}

// ---- audit sink ----

func (m *metaDB) appendAudit(e AuditEntry) error {
	b, err := json.Marshal(e)
	if err != nil {
		return err
	}
	ctx, cancel := dbCtx()
	defer cancel()
	return m.st.AppendAudit(ctx, store.AuditRow{Time: e.Time, IP: e.IP, Action: e.Action, Pipeline: e.Pipeline, Result: e.Result, Data: b})
}

// ---- pipeline files: DB là nguồn sự thật của pipeline tạo từ web ----

// materialize ghi lại file cấu hình (0640) và .env (0600, chỉ khi có khóa giải mã) của pipeline do web tạo từ
// DB ra config_path/env_path nếu thiếu hoặc khác. Gọi trước validate/start vì CLI strand chỉ đọc file.
func (s *Server) materialize(name string) { s.materializeOpt(name, false) }

// materializeOpt: force = dựng lại cả pipeline đang bị đánh dấu missing (thao tác Restore file).
func (s *Server) materializeOpt(name string, force bool) {
	if s.db == nil {
		return
	}
	ctx, cancel := dbCtx()
	defer cancel()
	row, ok, err := s.db.st.GetPipeline(ctx, name)
	if err != nil {
		s.log.Warn("materialize: read pipeline from database failed", "pipeline", name, "err", err)
		return
	}
	if !ok || !row.HasConfig || row.ConfigPath == "" {
		return
	}
	if !row.MissingSince.IsZero() && !force {
		return // file/entry đã bị gỡ: không tự dựng lại, chờ người dùng chọn Restore hoặc Remove
	}
	var conflicts []DiskConflict
	if aside, err := s.db.writeIfDiffers(row.ConfigPath, []byte(row.ConfigYAML), 0o640, false, s.now()); err != nil {
		s.log.Warn("materialize config failed", "pipeline", name, "path", row.ConfigPath, "err", err)
	} else if aside != "" {
		conflicts = append(conflicts, DiskConflict{File: "config", Path: aside, At: s.now().UTC().Format(time.RFC3339)})
	}
	defer func() {
		if len(conflicts) == 0 {
			return
		}
		all := append(parseConflicts(row.Conflicts), conflicts...)
		b, _ := json.Marshal(all)
		if err := s.db.st.SetPipelineConflicts(ctx, name, string(b)); err != nil {
			s.log.Warn("record disk conflict failed", "pipeline", name, "err", err)
		}
		for _, c := range conflicts {
			_ = s.audit.Log(AuditEntry{Action: "config.conflict", Pipeline: name, Result: "ok",
				Detail: map[string]any{"file": c.File, "aside": c.Path, "note": "file on disk differed from the database copy; the database copy was written, the disk version set aside"}})
		}
	}()
	if len(row.EnvEnc) > 0 && row.EnvPath != "" {
		if s.db.box == nil {
			s.db.warnedKey.Do(func() {
				s.log.Warn("pipeline .env is stored encrypted in the database but " + EnvSecretKey + " is not set: leaving .env files on disk as they are")
			})
			return
		}
		pt, err := s.db.box.Open(row.EnvEnc, name)
		if err != nil {
			s.log.Warn("materialize env failed", "pipeline", name, "err", err)
			return
		}
		if aside, err := s.db.writeIfDiffers(row.EnvPath, pt, 0o600, true, s.now()); err != nil {
			s.log.Warn("materialize env failed", "pipeline", name, "path", row.EnvPath, "err", err)
		} else if aside != "" {
			conflicts = append(conflicts, DiskConflict{File: "env", Path: aside, At: s.now().UTC().Format(time.RFC3339)})
		}
	}
}

// writeIfDiffers ghi data vào path nếu thiếu hoặc khác. Khác: bản trên đĩa được cất sang
// <path>.conflict-<thời gian> (cùng quyền, không bao giờ rộng hơn) trước khi ghi, trả đường dẫn bản cất.
func (m *metaDB) writeIfDiffers(path string, data []byte, newPerm os.FileMode, secret bool, now time.Time) (string, error) {
	cur, err := os.ReadFile(path)
	aside := ""
	if err == nil {
		if bytes.Equal(cur, data) {
			return "", nil
		}
		perm := os.FileMode(0o600)
		if fi, serr := os.Stat(path); serr == nil {
			perm = fi.Mode().Perm() & secretPermMask
		}
		aside = path + ".conflict-" + now.UTC().Format("20060102T150405Z")
		if werr := writeBackup(aside, cur, path, perm); werr != nil {
			return "", fmt.Errorf("set aside the on-disk version before restoring the database copy: %w", werr)
		}
		m.log.Warn("file differs from the database copy; the database is the source of truth: the disk version was set aside", "path", path, "aside", aside)
	} else if !errors.Is(err, fs.ErrNotExist) {
		return "", err
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o750); err != nil {
		return "", err
	}
	existed := err == nil
	if werr := writeFilePreserving(path, data, secret, false); werr != nil {
		return "", werr
	}
	if !existed {
		return aside, os.Chmod(path, newPerm)
	}
	return aside, nil
}

// materializeAll chạy lúc khởi động: khôi phục file của mọi pipeline do web tạo (ví dụ volume cấu hình mới).
func (s *Server) materializeAll() {
	if s.db == nil {
		return
	}
	ctx, cancel := dbCtx()
	defer cancel()
	rows, err := s.db.st.ListPipelines(ctx)
	if err != nil {
		s.log.Warn("materialize: list pipelines failed", "err", err)
		return
	}
	for _, r := range rows {
		s.materialize(r.Name)
	}
}

// persistPipelineFiles đọc lại file cấu hình/.env của pipeline do web tạo và lưu vào DB (sau khi tạo hoặc sửa).
// Chỉ áp dụng khi pipeline đã có dòng trong DB. .env chỉ vào DB khi có STRAND_WEB_SECRET_KEY.
func (s *Server) persistPipelineFiles(p *Pipeline) error {
	if s.db == nil || p == nil {
		return nil
	}
	ctx, cancel := dbCtx()
	defer cancel()
	row, ok, err := s.db.st.GetPipeline(ctx, p.Name)
	if err != nil {
		return err
	}
	if !ok {
		return nil
	}
	cfgB, err := os.ReadFile(row.ConfigPath)
	if err != nil {
		return err
	}
	row.ConfigYAML, row.HasConfig = string(cfgB), true
	if row.EnvPath == "" && p.EnvFile != "" {
		row.EnvPath = p.EnvFile // .env tạo sau (tab Config) cho pipeline chưa có
	}
	if row.EnvPath != "" {
		if envB, err := os.ReadFile(row.EnvPath); err == nil {
			if s.db.box != nil {
				if row.EnvEnc, err = s.db.box.Seal(envB, p.Name); err != nil {
					return err
				}
			} else {
				// Không có khóa: bản mã hóa cũ (nếu có, từ lúc còn khóa) đã lỗi thời; giữ lại thì khi đặt khóa
				// trở lại nó sẽ ghi đè .env mới hơn trên đĩa.
				row.EnvEnc = nil
				s.db.warnedNoKey(s.log)
			}
		}
	}
	return s.db.st.UpsertPipeline(ctx, row)
}

var noKeyOnce sync.Once

func (m *metaDB) warnedNoKey(log *slog.Logger) {
	noKeyOnce.Do(func() {
		log.Info(EnvSecretKey + " is not set: pipeline .env files stay on disk only (not stored in the database); set it to keep them encrypted in the database")
	})
}

// ---- desired state, autostart ----

func (s *Server) setDesired(p *Pipeline, desired string) {
	if s.db == nil || p.Mode() != ModeBackground {
		return
	}
	ctx, cancel := dbCtx()
	defer cancel()
	if err := s.db.st.SetDesired(ctx, p.Name, desired); err != nil {
		s.log.Warn("record desired state failed", "pipeline", p.Name, "desired", desired, "err", err)
	}
}

// ResumeDesired khởi động lại các pipeline chế độ nền có trạng thái mong muốn "running" mà đang không chạy
// (container/web khởi động lại). Trả tên các pipeline đã thử khởi động.
func (s *Server) ResumeDesired(ctx context.Context) []string {
	if s.db == nil {
		return nil
	}
	dctx, cancel := dbCtx()
	desired, err := s.db.st.Desired(dctx)
	cancel()
	if err != nil {
		s.log.Warn("autostart: read desired state failed", "err", err)
		return nil
	}
	var started []string
	for _, p := range s.reg.List() {
		if desired[p.Name] != store.DesiredRunning || p.Mode() != ModeBackground {
			continue
		}
		if rt := s.reg.Runtime(ctx, p); rt.Running {
			continue
		}
		_ = s.audit.Log(AuditEntry{Action: "autostart", Pipeline: p.Name, Result: "started"})
		_, err := s.runLifecycle(ctx, p, ActStart)
		e := AuditEntry{Action: "autostart", Pipeline: p.Name, Result: "ok"}
		if err != nil {
			e.Result, e.Detail = "error", map[string]any{"error": err.Error()}
			s.log.Warn("autostart failed", "pipeline", p.Name, "err", err)
		} else {
			s.log.Info("autostarted pipeline (desired state: running)", "pipeline", p.Name)
		}
		_ = s.audit.Log(e)
		started = append(started, p.Name)
	}
	return started
}

// stopForShutdown dừng êm các pipeline nền đang chạy khi web thoát (shutdown_stop_pipelines), KHÔNG đổi
// trạng thái mong muốn: lần khởi động sau ResumeDesired chạy lại chúng.
func (s *Server) stopForShutdown() {
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Second)
	defer cancel()
	var wg sync.WaitGroup
	for _, p := range s.reg.List() {
		if p.Mode() != ModeBackground || p.CfgErr != "" || !s.reg.Runtime(ctx, p).Running {
			continue
		}
		wg.Add(1)
		go func(p *Pipeline) {
			defer wg.Done()
			if _, err := s.bgStop(ctx, p); err != nil {
				s.log.Warn("stop pipeline on shutdown failed", "pipeline", p.Name, "err", err)
			}
		}(p)
	}
	wg.Wait()
}

// ---- import file state -> DB (một lần) ----

const importedMetaKey = "files_imported"

// ImportSummary mô tả những gì đã nhập.
type ImportSummary struct {
	Admin     bool `json:"admin"`
	Token     bool `json:"api_token"`
	Pipelines int  `json:"pipelines"`
	Jobs      int  `json:"jobs"`
	Wizards   int  `json:"wizards"`
	Audit     int  `json:"audit_lines"`
}

// importFiles nhập trạng thái file hiện có (web.yaml hash, pipelines.json, jobs/, wizards/, audit.log) vào DB
// đúng một lần: khóa advisory + cờ files_imported trong web_meta. Chạy lại là no-op; không xóa file gốc.
func (s *Server) importFiles(ctx context.Context) (*ImportSummary, error) {
	st := s.db.st
	var sum *ImportSummary
	err := st.Locked(ctx, "import", func(ctx context.Context) error {
		if _, done, err := st.MetaGet(ctx, importedMetaKey); err != nil || done {
			return err
		}
		sum = &ImportSummary{}
		adm, have, err := st.Admin(ctx)
		if err != nil {
			return err
		}
		if (!have || adm.PasswordHash == "") && s.cfg.PasswordHash != "" {
			if err := st.SetPasswordHash(ctx, s.cfg.PasswordHash); err != nil {
				return err
			}
			sum.Admin = true
		}
		if adm.APITokenHash == "" && s.cfg.APITokenHash != "" {
			if err := st.SetAPITokenHash(ctx, s.cfg.APITokenHash); err != nil {
				return err
			}
			sum.Token = true
		}
		// pipelines.json
		reg := NewRegistry(s.cfg, nil)
		created, cerr := reg.loadCreatedFile()
		if cerr != nil {
			s.log.Warn("import: pipelines.json skipped", "err", cerr)
		}
		for _, e := range created {
			if !ValidName(e.Name) || e.Config == "" {
				continue
			}
			if _, ok, _ := st.GetPipeline(ctx, e.Name); ok {
				continue
			}
			row := store.Pipeline{Name: e.Name, ConfigPath: e.Config, EnvPath: e.EnvFile, SystemdUnit: e.SystemdUnit, LogFile: e.LogFile, PidFile: e.PidFile}
			if b, err := os.ReadFile(e.Config); err == nil {
				row.ConfigYAML, row.HasConfig = string(b), true
			}
			if e.EnvFile != "" && s.db.box != nil {
				if b, err := os.ReadFile(e.EnvFile); err == nil {
					if row.EnvEnc, err = s.db.box.Seal(b, e.Name); err != nil {
						return err
					}
				}
			}
			if err := st.UpsertPipeline(ctx, row); err != nil {
				return err
			}
			sum.Pipelines++
		}
		// jobs/*.json
		have2 := map[string]bool{}
		if js, err := st.ListJobs(ctx); err == nil {
			for _, j := range js {
				have2[j.ID] = true
			}
		}
		jdir := filepath.Join(s.cfg.StateDir, "jobs")
		if ents, err := os.ReadDir(jdir); err == nil {
			for _, e := range ents {
				if !strings.HasSuffix(e.Name(), ".json") {
					continue
				}
				b, err := os.ReadFile(filepath.Join(jdir, e.Name()))
				if err != nil {
					continue
				}
				var js JobStatus
				if json.Unmarshal(b, &js) != nil || !JobIDRE.MatchString(js.ID) || e.Name() != js.ID+".json" || have2[js.ID] {
					continue
				}
				if err := st.UpsertJob(ctx, store.Job{ID: js.ID, Pipeline: js.Pipeline, Started: js.Started, Running: js.Running, Data: b}); err != nil {
					return err
				}
				sum.Jobs++
			}
		}
		// wizards/<kind>-<pipeline>.json
		wdir := filepath.Join(s.cfg.StateDir, "wizards")
		if ents, err := os.ReadDir(wdir); err == nil {
			for _, e := range ents {
				if !strings.HasSuffix(e.Name(), ".json") {
					continue
				}
				b, err := os.ReadFile(filepath.Join(wdir, e.Name()))
				if err != nil {
					continue
				}
				var w wizardState
				if json.Unmarshal(b, &w) != nil || w.Kind == "" || w.Pipeline == "" {
					continue
				}
				if _, ok, _ := st.GetWizard(ctx, w.Kind, w.Pipeline); ok {
					continue
				}
				if err := st.PutWizard(ctx, w.Kind, w.Pipeline, b); err != nil {
					return err
				}
				sum.Wizards++
			}
		}
		// audit.log
		n, err := s.importAuditFile(ctx, filepath.Join(s.cfg.StateDir, "audit.log"))
		if err != nil {
			return err
		}
		sum.Audit = n
		b, _ := json.Marshal(sum)
		return st.MetaSet(ctx, importedMetaKey, time.Now().UTC().Format(time.RFC3339)+" "+string(b))
	})
	return sum, err
}

const auditImportProgressKey = "audit_import_lines"

func (s *Server) importAuditFile(ctx context.Context, path string) (int, error) {
	f, err := os.Open(path)
	if err != nil {
		return 0, nil
	}
	defer f.Close()
	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 1<<20), 8<<20)
	// Tiến độ (số dòng hợp lệ đã nhập) ghi cùng transaction với mỗi lô: lần nhập bị ngắt giữa chừng (mất DB,
	// container bị dừng) tiếp tục đúng chỗ, không nhân đôi dòng (audit_log chỉ-thêm, không xóa được).
	skip := 0
	if v, ok, err := s.db.st.MetaGet(ctx, auditImportProgressKey); err != nil {
		return 0, err
	} else if ok {
		skip, _ = strconv.Atoi(v)
	}
	var batch []store.AuditRow
	total, seen := 0, 0
	flush := func() error {
		if len(batch) == 0 {
			return nil
		}
		if err := s.db.st.AppendAuditBatch(ctx, batch, &store.MetaKV{Key: auditImportProgressKey, Value: strconv.Itoa(seen)}); err != nil {
			return err
		}
		total += len(batch)
		batch = batch[:0]
		return nil
	}
	for sc.Scan() {
		line := bytes.TrimSpace(sc.Bytes())
		if len(line) == 0 {
			continue
		}
		var e AuditEntry
		if json.Unmarshal(line, &e) != nil || e.Action == "" {
			continue
		}
		seen++
		if seen <= skip {
			continue
		}
		batch = append(batch, store.AuditRow{Time: e.Time, IP: e.IP, Action: e.Action, Pipeline: e.Pipeline, Result: e.Result, Data: append([]byte(nil), line...)})
		if len(batch) >= 1000 {
			if err := flush(); err != nil {
				return total, err
			}
		}
	}
	if err := flush(); err != nil {
		return total, err
	}
	return total, sc.Err()
}

// CLIStore dùng cho `strand web passwd|token|init`: nếu có DSN (STRAND_WEB_DATABASE_DSN hoặc database.dsn trong
// web.yaml đã tồn tại) thì mở kho (thử lại, migrate) và trả về; không thì (nil, nil) = chế độ file.
func CLIStore(ctx context.Context, cfgPath string, log *slog.Logger) (store.Store, error) {
	c := &Config{}
	if cfgPath != "" {
		if _, err := os.Stat(cfgPath); err == nil {
			lc, err := LoadConfig(cfgPath)
			if err != nil {
				return nil, err
			}
			c = lc
		}
	}
	if v := strings.TrimSpace(os.Getenv(EnvDatabaseDSN)); v != "" {
		c.Database.DSN = v
	}
	if !c.DBEnabled() {
		return nil, nil
	}
	return OpenStore(ctx, c, log)
}

// ---- /healthz ----

func (s *Server) handleHealthz(w http.ResponseWriter, r *http.Request) {
	out := map[string]any{"status": "ok", "version": s.version}
	code := http.StatusOK
	if s.db == nil {
		out["database"] = map[string]any{"mode": "file"}
	} else {
		ctx, cancel := context.WithTimeout(r.Context(), 3*time.Second)
		defer cancel()
		if err := s.db.st.Ping(ctx); err != nil {
			// /healthz không cần đăng nhập: chi tiết lỗi (host, cổng, user, tên DB) chỉ vào log của web.
			s.log.Warn("healthz: metadata database unreachable", "err", store.Redact(err.Error()))
			out["status"] = "degraded"
			out["database"] = map[string]any{"mode": "postgres", "ok": false, "error": "metadata database unreachable (details in the web log)"}
			code = http.StatusServiceUnavailable
		} else {
			out["database"] = map[string]any{"mode": "postgres", "ok": true}
		}
	}
	w.Header().Set("Cache-Control", "no-store")
	writeJSON(w, code, out)
}
