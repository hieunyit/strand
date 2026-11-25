package web

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"sync"
	"unicode"
)

// Cài đặt web chỉnh được trên trang Settings (chế độ DB). Thứ tự ưu tiên (giá trị hiệu lực):
//
//	biến môi trường (nếu có cho khóa đó và khác rỗng) > database (đã sửa trên trang Settings) > web.yaml > mặc định
//
// Lý do: env là cấu hình của người triển khai (compose/systemd) nên thắng và khóa ô sửa; thay đổi trên UI là ý
// định mới nhất nên thắng web.yaml; "Reset" xóa dòng DB để trở về web.yaml. Cài đặt khởi động (listen, TLS,
// database, state_dir, allow_plain_http, strand_binary, mật khẩu/token) chỉ nằm ở web.yaml/env.

// Nguồn của giá trị hiệu lực.
const (
	SrcEnv      = "env"
	SrcDatabase = "database"
	SrcFile     = "web.yaml"
	SrcDefault  = "default"
)

type settingDef struct {
	Key     string
	Label   string
	Help    string
	Kind    string // bool | int | string | list
	Restart bool   // chỉ có hiệu lực sau khi khởi động lại web
	Env     string // biến môi trường ghi đè (rỗng: không có)
	// Min, Max: khoảng hợp lệ của khóa int (Max 0: 1..300).
	Min, Max int
	// Section groups the setting on the Settings page; Choices are the allowed values of a "choice" setting.
	Section string
	Choices []string
	file    func(c *Config) any
	def     any
}

var settingDefs = []settingDef{
	{Key: "environment_name", Label: "Environment name", Kind: "text", Section: "general",
		Help: "Shown as a chip in the top bar on every page (for example production, staging), so nobody mistakes one console for another. Empty: the host name.",
		file: func(c *Config) any { return c.EnvironmentName }, def: ""},
	{Key: "environment_tone", Label: "Environment colour", Kind: "choice", Section: "general", Choices: []string{"neutral", "info", "success", "warning", "danger"},
		Help: "Colour of the environment chip: danger (red) is a common choice for production.",
		file: func(c *Config) any { return c.EnvironmentTone }, def: "neutral"},
	{Key: "default_language", Label: "Default language", Kind: "choice", Section: "general", Choices: []string{"en", "vi"},
		Help: "Default language for new visitors and pages before sign-in (English or Tiếng Việt). Users can choose their own language in My account.",
		file: func(c *Config) any { return "en" }, def: "en"},
	{Key: "scan", Section: "general", Label: "Scan globs", Kind: "list",
		Help: "Pipeline config files discovered automatically, one absolute glob per line (for example /etc/strand/*.yaml). Applies at the next refresh (5 s).",
		file: func(c *Config) any { return append([]string{}, c.Scan...) }, def: []string{}},
	{Key: "config_dir", Section: "general", Label: "Directory for new pipelines", Kind: "string",
		Help: "Where the New pipeline form writes <name>.yaml and <name>.env. Empty: /etc/strand if writable, else <state_dir>/pipelines.",
		file: func(c *Config) any { return c.ConfigDir }, def: ""},
	{Key: "secure_cookies", Section: "security", Label: "Secure cookies", Kind: "bool", Env: "STRAND_WEB_SECURE_COOKIES",
		Help: "Mark cookies Secure (behind a TLS reverse proxy). Always on when the web serves TLS itself. Applies to new cookies.",
		file: func(c *Config) any { return c.SecureCookies }, def: false},
	{Key: "trust_forwarded_for", Section: "security", Label: "Trust X-Forwarded-For", Kind: "bool", Env: "STRAND_WEB_TRUST_FORWARDED_FOR",
		Help: "Take the client IP (audit log, login rate limit) from X-Forwarded-For. Only behind a trusted proxy.",
		file: func(c *Config) any { return c.TrustForwardedFor }, def: false},
	{Key: "shutdown_stop_pipelines", Section: "general", Label: "Stop pipelines on web shutdown", Kind: "bool",
		Help: "When the web exits (container stop), gracefully stop the background pipelines it runs; they are restarted from their desired state on the next start.",
		file: func(c *Config) any { return c.ShutdownStopPipelines }, def: false},
	{Key: "sample_interval_sec", Section: "general", Label: "Sample interval (seconds)", Kind: "int", Restart: true,
		Help: "How often pipelines are sampled for the dashboard and metrics history (1 to 300). Needs a web restart.",
		file: func(c *Config) any { return c.SampleIntervalSec }, def: 5},
	{Key: "audit_retention_days", Section: "audit", Label: "Audit retention (days)", Kind: "int", Min: 0, Max: 36500,
		Help: "PostgreSQL mode: audit rows older than this are archived (exported to a verified, compressed JSONL file with a sha256 manifest, then removed; an audit.archive entry records it) by `strand web db audit-archive`, or automatically when enabled below. 0 = keep forever.",
		file: func(c *Config) any { return c.AuditRetentionDays }, def: 365},
	{Key: "audit_archive_auto", Section: "audit", Label: "Archive old audit rows automatically", Kind: "bool",
		Help: "PostgreSQL mode: once a day the active web instance archives the audit rows older than the retention into the archive directory. Off: run `strand web db audit-archive` yourself.",
		file: func(c *Config) any { return c.AuditArchiveAuto }, def: false},
	{Key: "audit_archive_dir", Section: "audit", Label: "Audit archive directory", Kind: "string",
		Help: "Where archive files (.jsonl.gz, .sha256, .manifest.json) are written. Empty: <state_dir>/audit-archive. Files are never deleted by Strand.",
		file: func(c *Config) any { return c.AuditArchiveDir }, def: ""},
	{Key: "audit_rotate_mb", Section: "audit", Label: "Rotate audit.log at (MiB)", Kind: "int", Min: 0, Max: 102400,
		Help: "The audit.log file copy (database.audit: file or both) is rotated to audit-<time>.log.gz (with a .sha256; the new file starts with an audit.rotate line naming it) when it reaches this size. 0 = never.",
		file: func(c *Config) any { return c.AuditRotateMB }, def: 100},
	{Key: "audit_rotate_days", Section: "audit", Label: "Rotate audit.log after (days)", Kind: "int", Min: 0, Max: 36500,
		Help: "Also rotate audit.log when its first line is older than this (checked daily). 0 = never.",
		file: func(c *Config) any { return c.AuditRotateDays }, def: 0},
}

func settingDefOf(key string) *settingDef {
	for i := range settingDefs {
		if settingDefs[i].Key == key {
			return &settingDefs[i]
		}
	}
	return nil
}

// liveSettings là giá trị hiệu lực đang dùng (đổi được khi đang chạy).
type liveSettings struct {
	mu    sync.RWMutex
	vals  map[string]any
	srcs  map[string]string
	start map[string]any // giá trị lúc khởi động (khóa cần khởi động lại dùng giá trị này tới lần sau)
}

func (l *liveSettings) get(key string) any {
	l.mu.RLock()
	defer l.mu.RUnlock()
	return l.vals[key]
}

func (s *Server) settingBool(key string) bool  { b, _ := s.live.get(key).(bool); return b }
func (s *Server) settingStr(key string) string { v, _ := s.live.get(key).(string); return v }
func (s *Server) settingList(key string) []string {
	v, _ := s.live.get(key).([]string)
	return append([]string(nil), v...)
}

// parseSetting đổi giá trị (JSON đã giải mã, hoặc chuỗi từ form) thành kiểu của khóa và kiểm hợp lệ.
func parseSetting(d *settingDef, raw any) (any, error) {
	switch d.Kind {
	case "bool":
		switch v := raw.(type) {
		case bool:
			return v, nil
		case string:
			switch strings.ToLower(strings.TrimSpace(v)) {
			case "1", "true", "yes", "on":
				return true, nil
			case "0", "false", "no", "off", "":
				return false, nil
			}
		}
		return nil, fmt.Errorf("%s: expected true or false", d.Key)
	case "int":
		var n int
		switch v := raw.(type) {
		case float64:
			n = int(v)
			if float64(n) != v {
				return nil, fmt.Errorf("%s: expected a whole number", d.Key)
			}
		case int:
			n = v
		case string:
			x, err := strconv.Atoi(strings.TrimSpace(v))
			if err != nil {
				return nil, fmt.Errorf("%s: expected a whole number", d.Key)
			}
			n = x
		default:
			return nil, fmt.Errorf("%s: expected a whole number", d.Key)
		}
		lo, hi := 1, 300
		if d.Max != 0 {
			lo, hi = d.Min, d.Max
		}
		if n < lo || n > hi {
			return nil, fmt.Errorf("%s: must be between %d and %d", d.Key, lo, hi)
		}
		return n, nil
	case "string":
		v, ok := raw.(string)
		if !ok {
			return nil, fmt.Errorf("%s: expected a string", d.Key)
		}
		v = strings.TrimSpace(v)
		if v != "" && (!filepath.IsAbs(v) || filepath.Clean(v) != v) {
			return nil, fmt.Errorf("%s: must be an absolute, clean path (got %q)", d.Key, v)
		}
		return v, nil
	case "text":
		v, ok := raw.(string)
		if !ok {
			return nil, fmt.Errorf("%s: expected text", d.Key)
		}
		v = strings.TrimSpace(v)
		if len([]rune(v)) > 40 {
			return nil, fmt.Errorf("%s: at most 40 characters", d.Key)
		}
		for _, r := range v {
			if !unicode.IsPrint(r) {
				return nil, fmt.Errorf("%s: contains a control character", d.Key)
			}
		}
		return v, nil
	case "choice":
		v, _ := raw.(string)
		v = strings.TrimSpace(v)
		for _, c := range d.Choices {
			if v == c {
				return v, nil
			}
		}
		return nil, fmt.Errorf("%s: must be one of %s", d.Key, strings.Join(d.Choices, ", "))
	case "list":
		var items []string
		switch v := raw.(type) {
		case string:
			items = strings.Split(v, "\n")
		case []any:
			for _, x := range v {
				sx, ok := x.(string)
				if !ok {
					return nil, fmt.Errorf("%s: expected a list of strings", d.Key)
				}
				items = append(items, sx)
			}
		case []string:
			items = v
		default:
			return nil, fmt.Errorf("%s: expected a list of strings", d.Key)
		}
		out := []string{}
		for _, it := range items {
			it = strings.TrimSpace(it)
			if it == "" {
				continue
			}
			if !filepath.IsAbs(it) {
				return nil, fmt.Errorf("%s: %q is not an absolute path", d.Key, it)
			}
			if _, err := filepath.Match(it, ""); err != nil {
				return nil, fmt.Errorf("%s: %q is not a valid glob: %v", d.Key, it, err)
			}
			out = append(out, it)
		}
		if len(out) > 32 {
			return nil, fmt.Errorf("%s: at most 32 globs", d.Key)
		}
		return out, nil
	}
	return nil, errors.New("unknown setting kind")
}

// fileSetSetting: web.yaml có đặt khóa này không (Config nạp từ file biết chính xác; Config dựng trong code
// thì coi giá trị khác zero là "đặt").
func fileSetSetting(c *Config, d *settingDef) bool {
	if c.FileKeys != nil {
		return c.FileKeys[d.Key]
	}
	v := reflect.ValueOf(d.file(c))
	if v.Kind() == reflect.Slice {
		return v.Len() > 0
	}
	return !v.IsZero()
}

// resolveSettings tính giá trị hiệu lực và nguồn của mọi khóa.
func resolveSettings(c *Config, db map[string]string) (map[string]any, map[string]string) {
	vals, srcs := map[string]any{}, map[string]string{}
	for i := range settingDefs {
		d := &settingDefs[i]
		v, src := d.def, SrcDefault
		if fileSetSetting(c, d) {
			v, src = d.file(c), SrcFile
		}
		if raw, ok := db[d.Key]; ok {
			var x any
			if json.Unmarshal([]byte(raw), &x) == nil {
				if pv, err := parseSetting(d, x); err == nil {
					v, src = pv, SrcDatabase
				}
			}
		}
		if d.Env != "" {
			if ev := strings.TrimSpace(os.Getenv(d.Env)); ev != "" {
				if pv, err := parseSetting(d, ev); err == nil {
					v, src = pv, SrcEnv
				}
			}
		}
		if d.Key == "sample_interval_sec" && v.(int) <= 0 {
			v = 5
		}
		vals[d.Key], srcs[d.Key] = v, src
	}
	return vals, srcs
}

// loadSettings đọc cài đặt từ DB (chế độ DB) rồi đặt giá trị hiệu lực. initial: lúc khởi động (mọi khóa,
// kể cả khóa cần khởi động lại, đều được áp dụng).
func (s *Server) loadSettings(initial bool) error {
	db := map[string]string{}
	ctx, cancel := dbCtx()
	m, err := s.db.st.Settings(ctx)
	cancel()
	if err != nil {
		return err
	}
	db = m
	vals, srcs := resolveSettings(s.cfg, db)
	s.live.mu.Lock()
	defer s.live.mu.Unlock()
	if initial || s.live.start == nil {
		s.live.start = vals
	}
	s.live.vals, s.live.srcs = vals, srcs
	return nil
}

// SettingView là một dòng của trang/API Settings.
type SettingView struct {
	Key            string   `json:"key"`
	Label          string   `json:"label"`
	Help           string   `json:"help"`
	Kind           string   `json:"kind"`
	Value          any      `json:"value"`
	Source         string   `json:"source"`
	FileValue      any      `json:"file_value"`
	Env            string   `json:"env,omitempty"`
	Editable       bool     `json:"editable"`
	RestartNeeded  bool     `json:"restart_required"`
	PendingRestart bool     `json:"pending_restart,omitempty"`
	Text           string   `json:"-"` // giá trị dạng text cho form
	Min            int      `json:"min,omitempty"`
	Max            int      `json:"max,omitempty"`
	Section        string   `json:"section,omitempty"`
	Choices        []string `json:"choices,omitempty"`
}

type settingsBody struct {
	Items        []SettingView
	Message      string
	Error        string
	Bootstrap    []SettingView
	Section      string
	SectionLabel string
}

func (s *Server) settingsView() []SettingView {
	s.live.mu.RLock()
	defer s.live.mu.RUnlock()
	var out []SettingView
	for i := range settingDefs {
		d := &settingDefs[i]
		pending := d.Restart && !reflect.DeepEqual(s.live.vals[d.Key], s.live.start[d.Key])
		v := SettingView{Key: d.Key, Label: d.Label, Help: d.Help, Kind: d.Kind, Value: s.live.vals[d.Key], Source: s.live.srcs[d.Key],
			FileValue: d.file(s.cfg), Env: d.Env, RestartNeeded: d.Restart, PendingRestart: pending}
		v.Editable = v.Source != SrcEnv
		v.Text = settingText(v.Value)
		v.Section, v.Choices = d.Section, d.Choices
		if d.Kind == "int" {
			v.Min, v.Max = 1, 300
			if d.Max != 0 {
				v.Min, v.Max = d.Min, d.Max
			}
		}
		out = append(out, v)
	}
	return out
}

func settingText(v any) string {
	switch x := v.(type) {
	case []string:
		return strings.Join(x, "\n")
	case bool:
		return strconv.FormatBool(x)
	case int:
		return strconv.Itoa(x)
	case string:
		return x
	}
	return fmt.Sprint(v)
}

// applySettings lưu các thay đổi (giá trị nil = reset về web.yaml/mặc định). Trả các khóa đã đổi.
func (s *Server) applySettings(changes map[string]any, ip string) ([]string, error) {
	keys := make([]string, 0, len(changes))
	for k := range changes {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	type op struct {
		key, js string
		reset   bool
		val     any
	}
	var ops []op
	cur := map[string]SettingView{}
	for _, v := range s.settingsView() {
		cur[v.Key] = v
	}
	for _, k := range keys {
		d := settingDefOf(k)
		if d == nil {
			return nil, BadRequest("unknown setting %q (editable: scan, config_dir, secure_cookies, trust_forwarded_for, shutdown_stop_pipelines, sample_interval_sec)", k)
		}
		if cur[k].Source == SrcEnv {
			return nil, &ActionError{Status: http.StatusConflict, Msg: fmt.Sprintf("%s is set by the environment variable %s, which wins over the database; change it there", k, d.Env)}
		}
		if changes[k] == nil {
			ops = append(ops, op{key: k, reset: true})
			continue
		}
		pv, err := parseSetting(d, changes[k])
		if err != nil {
			return nil, BadRequest("%s", err.Error())
		}
		b, _ := json.Marshal(pv)
		ops = append(ops, op{key: k, js: string(b), val: pv})
	}
	ctx, cancel := dbCtx()
	defer cancel()
	var changed []string
	for _, o := range ops {
		old := cur[o.key]
		if o.reset && old.Source != SrcDatabase {
			continue
		}
		if !o.reset && old.Source == SrcDatabase && reflect.DeepEqual(old.Value, o.val) {
			continue
		}
		e := AuditEntry{IP: ip, Action: "settings.edit", Detail: map[string]any{"key": o.key, "old": old.Value, "old_source": old.Source}}
		if o.reset {
			e.Detail["reset"] = true
		} else {
			e.Detail["new"] = o.val
		}
		// Audit trước: không ghi được audit thì không đổi.
		e.Result = "started"
		if err := s.audit.Log(e); err != nil {
			return changed, &ActionError{Status: http.StatusInternalServerError, Msg: "audit log unavailable; refusing to change settings"}
		}
		var err error
		if o.reset {
			err = s.db.st.DeleteSetting(ctx, o.key)
		} else {
			err = s.db.st.PutSetting(ctx, o.key, o.js)
		}
		e.Result = "ok"
		if err != nil {
			e.Result, e.Detail["error"] = "error", err.Error()
			_ = s.audit.Log(e)
			return changed, &ActionError{Status: http.StatusServiceUnavailable, Msg: "metadata database: " + err.Error()}
		}
		_ = s.audit.Log(e)
		changed = append(changed, o.key)
	}
	if len(changed) > 0 {
		if err := s.loadSettings(false); err != nil {
			return changed, err
		}
		s.reg.Refresh(ctx) // scan đổi: áp dụng ngay
	}
	return changed, nil
}

func (s *Server) setSetting(ctx context.Context, key string, val string) error {
	if s.db == nil || s.db.st == nil {
		return nil
	}
	b, _ := json.Marshal(val)
	if err := s.db.st.PutSetting(ctx, key, string(b)); err != nil {
		return err
	}
	return s.loadSettings(false)
}

// ---- HTTP ----

func (s *Server) pageSettings(w http.ResponseWriter, r *http.Request, sess *Session) {
	b := settingsBody{Section: r.URL.Query().Get("section")}
	switch b.Section {
	case "security":
		b.SectionLabel = "Security"
	case "audit":
		b.SectionLabel = "Audit"
	case "about":
		b.Bootstrap = s.bootstrapSettings()
	default:
		b.Section, b.SectionLabel = "general", "General"
	}
	sectionItems := func() []SettingView {
		var items []SettingView
		for _, it := range s.settingsView() {
			if it.Section == b.Section {
				items = append(items, it)
			}
		}
		return items
	}
	b.Items = sectionItems()
	if r.Method == http.MethodPost {
		r.Body = http.MaxBytesReader(w, r.Body, 64<<10)
		_ = r.ParseForm()
		changes := map[string]any{}
		items := b.Items
		if r.URL.Query().Get("section") == "" {
			items = s.settingsView() // Forms rendered before section URLs were introduced.
		}
		for _, it := range items {
			if !it.Editable {
				continue
			}
			if r.PostForm.Get("reset_"+it.Key) == "1" {
				changes[it.Key] = nil
				continue
			}
			if _, posted := r.PostForm[it.Key]; !posted {
				continue // không có trong form (form cũ, client script): giữ nguyên
			}
			var nv any
			switch it.Kind {
			case "bool":
				nv = r.PostForm.Get(it.Key) == "true"
			default:
				nv = r.PostForm.Get(it.Key)
			}
			d := settingDefOf(it.Key)
			pv, err := parseSetting(d, nv)
			if err != nil || !reflect.DeepEqual(pv, it.Value) {
				changes[it.Key] = nv
			}
		}
		changed, err := s.applySettings(changes, s.clientIP(r))
		if err != nil {
			b.Error = err.Error()
		} else if len(changed) == 0 {
			b.Message = "No changes."
		} else {
			b.Message = "Saved: " + strings.Join(changed, ", ") + "."
		}
		b.Items = sectionItems()
	}
	s.render(w, "settings", sess, pageData{Title: "Settings", Active: "settings", Body: b})
}

// bootstrapSettings: các giá trị chỉ đọc (web.yaml/env), hiện để tham khảo.
func (s *Server) bootstrapSettings() []SettingView {
	dbv := "PostgreSQL (" + EnvDatabaseDSN + " or database.dsn)"
	tlsv := "off"
	if s.cfg.TLSCert != "" {
		tlsv = s.cfg.TLSCert
	}
	return []SettingView{
		{Key: "listen", Label: "Listen", Text: s.cfg.Listen},
		{Key: "tls", Label: "TLS certificate", Text: tlsv},
		{Key: "allow_plain_http", Label: "Allow plain HTTP", Text: strconv.FormatBool(s.cfg.AllowPlainHTTP)},
		{Key: "state_dir", Label: "State directory", Text: s.cfg.StateDir},
		{Key: "database", Label: "Metadata store", Text: dbv},
	}
}

func (s *Server) apiSettingsGet(w http.ResponseWriter, r *http.Request, _ *Session) {
	writeJSON(w, http.StatusOK, map[string]any{"database_mode": true, "settings": s.settingsView()})
}

func (s *Server) apiSettingsSave(w http.ResponseWriter, r *http.Request, _ *Session) {
	r.Body = http.MaxBytesReader(w, r.Body, 64<<10)
	var req map[string]any
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeErr(w, http.StatusBadRequest, `expected a JSON object {"key": value, ...} (null resets a key)`)
		return
	}
	changed, err := s.applySettings(req, s.clientIP(r))
	if err != nil {
		status := http.StatusInternalServerError
		var ae *ActionError
		if errors.As(err, &ae) {
			status = ae.Status
		}
		writeErr(w, status, err.Error())
		return
	}
	if changed == nil {
		changed = []string{}
	}
	writeJSON(w, http.StatusOK, map[string]any{"ok": true, "changed": changed, "settings": s.settingsView()})
}
