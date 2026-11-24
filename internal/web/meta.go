package web

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"log/slog"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"strand/internal/sshprofile"
	"strand/internal/web/store"
)

// DatabaseConfig là mục `database:` của web.yaml. DSN là BẮT BUỘC (trong file hoặc STRAND_WEB_DATABASE_DSN):
// Web UI chỉ chạy với kho metadata PostgreSQL (không còn chế độ file).
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

// Lỗi khởi động khi thiếu điều kiện bắt buộc của chế độ PostgreSQL-only.
const (
	ErrNoDatabase = "strand web needs a PostgreSQL metadata database: set database.dsn in web.yaml or " + EnvDatabaseDSN +
		" (new install: `strand web init --database-dsn postgres://...`; an install that ran without a database (file mode): " +
		"run `strand web db migrate --config <web.yaml> --dsn postgres://...` first to import its pipelines, jobs and audit log)"
	ErrNoSecretKey = EnvSecretKey + " is required: pipeline credentials are encrypted in the metadata database. " +
		"Generate one with `openssl rand -hex 32`, keep it safe (it cannot be recovered), and set it in the service environment " +
		"(/etc/strand/web.env for the systemd unit, .env for docker compose)"
)

// DBEnabled: có DSN.
func (c *Config) DBEnabled() bool { return c.Database.DSN != "" }

// metaDB là kho metadata PostgreSQL (hoặc Mem trong test) gắn với một Server.
type metaDB struct {
	st       store.Store
	box      *store.Box // STRAND_WEB_SECRET_KEY (bắt buộc): mã hóa thông tin đăng nhập của pipeline trong DB
	log      *slog.Logger
	secret   string
	previous string // STRAND_WEB_SECRET_KEY_PREVIOUS (xoay khóa)

	mu      sync.Mutex
	admin   store.Admin
	adminAt time.Time

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
	b, err := store.NewBox(secret)
	if err != nil {
		return nil, err
	}
	return &metaDB{st: st, log: log, secret: secret, box: b}, nil
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

func (m *metaDB) invalidateAdminCache() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.adminAt = time.Time{}
}

func (s *Server) passwordHash() string {
	p, _ := s.db.adminHashes()
	return p
}

func (s *Server) apiTokenHash() string {
	_, t := s.db.adminHashes()
	return t
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
			currentOK = opens(m.box, b64dec(v), keyCheckAAD)
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
			conns, err := m.st.ListBackupConnections(ctx)
			if err != nil {
				return err
			}
			for _, c := range conns {
				changed := false
				reseal := func(b []byte) ([]byte, error) {
					if len(b) == 0 || opens(m.box, b, c.ID) {
						return b, nil
					}
					pt, err := old.Open(b, c.ID)
					if err != nil {
						return nil, fmt.Errorf("backup connection %s: stored secret opens with neither %s nor %s", c.Name, EnvSecretKey, EnvSecretKeyPrevious)
					}
					changed = true
					return m.box.Seal(pt, c.ID)
				}
				if c.PasswordSealed, err = reseal(c.PasswordSealed); err != nil {
					return err
				}
				if c.URISealed, err = reseal(c.URISealed); err != nil {
					return err
				}
				if changed {
					if _, err := m.st.UpdateBackupConnection(ctx, c); err != nil {
						return err
					}
					n++
				}
			}
			rotate := func(sealed *[]byte, aad string) (bool, error) {
				if len(*sealed) == 0 || opens(m.box, *sealed, aad) {
					return false, nil
				}
				plain, err := old.Open(*sealed, aad)
				if err != nil {
					return false, errors.New("stored secret cannot be decrypted with either Web secret key")
				}
				defer clear(plain)
				*sealed, err = m.box.Seal(plain, aad)
				return err == nil, err
			}
			repos, err := m.st.ListBackupRepos(ctx)
			if err != nil {
				return err
			}
			for _, repo := range repos {
				changed := false
				for _, secret := range []*[]byte{&repo.AccessKeySealed, &repo.SecretKeySealed} {
					rotated, err := rotate(secret, repo.ID)
					if err != nil {
						return fmt.Errorf("backup repository %s: %w", repo.ID, err)
					}
					changed = changed || rotated
				}
				if changed {
					if _, err := m.st.UpdateBackupRepo(ctx, repo); err != nil {
						return err
					}
				}
			}
			channels, err := m.st.ListNotifyChannels(ctx)
			if err != nil {
				return err
			}
			for _, channel := range channels {
				changed, err := rotate(&channel.SecretSealed, channel.ID)
				if err != nil {
					return fmt.Errorf("notification channel %s: %w", channel.ID, err)
				}
				if changed {
					if _, err := m.st.UpdateNotifyChannel(ctx, channel); err != nil {
						return err
					}
				}
			}
			profiles, err := sshprofile.List(ctx, m.st)
			if err != nil {
				return err
			}
			profilesChanged := false
			for i, profile := range profiles {
				if len(profile.SealedKey) == 0 {
					continue
				}
				if _, err := profile.OpenKey(m.box); err == nil {
					continue
				}
				key, err := profile.OpenKey(old)
				if err != nil {
					return fmt.Errorf("SSH tunnel profile %s: cannot decrypt the stored key with either secret key", profile.ID)
				}
				profiles[i].SealedKey, err = profile.SealKey(m.box, key)
				if err != nil {
					return err
				}
				profilesChanged = true
			}
			if profilesChanged {
				if err := sshprofile.Save(ctx, m.st, profiles); err != nil {
					return err
				}
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
		return m.st.MetaSet(ctx, keyCheckMetaKey, base64.StdEncoding.EncodeToString(sealed))
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
	// Serialize the database read and file rewrite with config saves. The lock is
	// process-shared so another Web instance cannot restore a stale row over a
	// just-saved config.
	ctx, cancel := dbCtx()
	row, ok, err := s.db.st.GetPipeline(ctx, name)
	cancel()
	if err != nil {
		s.log.Warn("materialize: read pipeline from database failed", "pipeline", name, "err", err)
		return
	}
	if !ok || !row.HasConfig || row.ConfigPath == "" {
		return
	}
	lockedPath := row.ConfigPath
	unlock, err := lockPipelineFiles(lockedPath)
	if err != nil {
		s.log.Warn("materialize: lock pipeline files failed", "pipeline", name, "err", err)
		return
	}
	defer unlock()
	// The row could have changed while waiting for the file lock.
	ctx, cancel = dbCtx()
	defer cancel()
	row, ok, err = s.db.st.GetPipeline(ctx, name)
	if err != nil {
		s.log.Warn("materialize: read pipeline from database failed", "pipeline", name, "err", err)
		return
	}
	if !ok || !row.HasConfig || row.ConfigPath == "" {
		return
	}
	if row.ConfigPath != lockedPath {
		s.log.Warn("materialize: config path changed while acquiring file lock", "pipeline", name)
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

// pipelineEnv reads the newest stored credentials without writing a plaintext file.
func (m *metaDB) pipelineEnv(name string) ([]string, error) {
	ctx, cancel := dbCtx()
	defer cancel()
	row, found, err := m.st.GetPipeline(ctx, name)
	if err != nil {
		return nil, fmt.Errorf("read pipeline credentials: %w", err)
	}
	if !found || len(row.EnvEnc) == 0 {
		return nil, errors.New("pipeline credentials are missing from the metadata database")
	}
	plain, err := m.box.Open(row.EnvEnc, name)
	if err != nil {
		return nil, errors.New("cannot decrypt pipeline credentials")
	}
	defer clear(plain)
	return ParseEnvData(bytes.NewReader(plain))
}

// persistPipelineFiles đọc lại file cấu hình/.env của pipeline do web tạo và lưu vào DB (sau khi tạo hoặc sửa).
// Chỉ áp dụng khi pipeline đã có dòng trong DB. .env chỉ vào DB khi có STRAND_WEB_SECRET_KEY.
func (s *Server) persistPipelineFiles(p *Pipeline) error {
	if p == nil {
		return nil
	}
	ctx, cancel := dbCtx()
	defer cancel()
	row, ok, err := s.db.st.GetPipeline(ctx, p.Name)
	if err != nil {
		return err
	}
	now := s.now().UTC()
	if !ok {
		row = store.Pipeline{
			Name:        p.Name,
			ConfigPath:  p.ConfigPath,
			EnvPath:     p.EnvFile,
			SystemdUnit: p.SystemdUnit,
			LogFile:     p.LogFile,
			PidFile:     p.PidFile,
			Origin:      storeOrigin(p.Origin),
			Created:     now,
			Updated:     now,
		}
	}
	if row.ConfigPath == "" && p.ConfigPath != "" {
		row.ConfigPath = p.ConfigPath
	}
	cfgB, err := os.ReadFile(row.ConfigPath)
	if err != nil {
		return err
	}
	row.ConfigYAML, row.HasConfig = string(cfgB), true
	row.Updated = now
	if p.EnvFile != "" {
		row.EnvPath = p.EnvFile
	}
	if row.EnvPath != "" {
		if envB, err := os.ReadFile(row.EnvPath); err == nil {
			if row.EnvEnc, err = s.db.box.Seal(envB, p.Name); err != nil {
				return err
			}
		}
	}
	return s.db.st.UpsertPipeline(ctx, row)
}

// persistPipelineEnv stores the .env file only. It never rewrites config_yaml
// from the local disk, which can hold an older config than the database when
// another Web instance saved a config meanwhile.
func (s *Server) persistPipelineEnv(p *Pipeline, envPath string) error {
	if p == nil {
		return nil
	}
	envB, err := os.ReadFile(envPath)
	if err != nil {
		return err
	}
	sealed, err := s.db.box.Seal(envB, p.Name)
	if err != nil {
		return err
	}
	ctx, cancel := dbCtx()
	defer cancel()
	updated, err := s.db.st.UpdatePipelineEnv(ctx, p.Name, envPath, sealed)
	if err != nil {
		return err
	}
	if !updated {
		// No metadata row yet: create it from the files once.
		withEnv := *p
		withEnv.EnvFile = envPath
		return s.persistPipelineFiles(&withEnv)
	}
	return nil
}

// persistPipelineConfigSnapshot stores exactly the bytes that the caller wrote
// to disk. expected is the config version read before validation; when another
// Web instance saved meanwhile, fail instead of replacing its newer version.
func (s *Server) persistPipelineConfigSnapshot(p *Pipeline, expected, config []byte) error {
	ctx, cancel := dbCtx()
	defer cancel()
	row, ok, err := s.db.st.GetPipeline(ctx, p.Name)
	if err != nil {
		return err
	}
	if !ok {
		return errors.New("pipeline metadata row is missing")
	}
	if err := checkPipelineConfigVersion(row, expected); err != nil {
		return err
	}
	updated, err := s.db.st.UpdatePipelineConfigCompareAndSwap(ctx, p.Name, row.HasConfig, row.ConfigYAML, string(config))
	if err != nil {
		return err
	}
	if !updated {
		return Conflict("pipeline configuration changed while you were editing it; reload Config and try again")
	}
	return nil
}

func checkPipelineConfigVersion(row store.Pipeline, expected []byte) error {
	if row.HasConfig && !bytes.Equal([]byte(row.ConfigYAML), expected) {
		return Conflict("pipeline configuration changed while you were editing it; reload Config and try again")
	}
	return nil
}

func (s *Server) checkPipelineConfigSnapshot(p *Pipeline, expected []byte) error {
	ctx, cancel := dbCtx()
	defer cancel()
	row, ok, err := s.db.st.GetPipeline(ctx, p.Name)
	if err != nil {
		return err
	}
	if !ok {
		return errors.New("pipeline metadata row is missing")
	}
	return checkPipelineConfigVersion(row, expected)
}

// ---- desired state, autostart ----

func (s *Server) setDesired(p *Pipeline, desired string) {
	if p.Mode() != ModeBackground {
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
	if !s.isActive() {
		return nil // standby: instance active làm việc này (tiếp quản thì chạy lại)
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
		rt := s.reg.Runtime(ctx, p)
		if rt.Running {
			continue
		}
		if s.observeStop(p, rt) {
			continue // dừng êm ngoài web (strand stop) khi web không chạy: không tự khởi động lại
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

// CLIStore dùng cho `strand web passwd|token|init`: nếu có DSN (STRAND_WEB_DATABASE_DSN hoặc database.dsn trong
// web.yaml đã tồn tại) thì mở kho (thử lại, migrate) và trả về; không có DSN thì lỗi ErrNoDatabase.
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
		return nil, errors.New(ErrNoDatabase)
	}
	return OpenStore(ctx, c, log)
}

// ---- /healthz ----

func (s *Server) handleHealthz(w http.ResponseWriter, r *http.Request) {
	out := map[string]any{"status": "ok", "version": s.version}
	if lv := s.leaseInfo(); lv != nil {
		out["role"] = lv.Role
		out["lease"] = lv
	}
	code := http.StatusOK
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
	w.Header().Set("Cache-Control", "no-store")
	writeJSON(w, code, out)
}

// b64dec: giá trị web_meta là text (UTF-8), nên dữ liệu nhị phân lưu dạng base64.
func b64dec(s string) []byte {
	b, err := base64.StdEncoding.DecodeString(s)
	if err != nil {
		return nil
	}
	return b
}

// storeOrigin: Pipeline.Origin -> cột origin.
func storeOrigin(o string) string {
	switch o {
	case "web.yaml":
		return store.OriginFile
	case "scan":
		return store.OriginScan
	}
	return store.OriginCreated
}
