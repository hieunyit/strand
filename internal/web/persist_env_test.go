package web

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func newOpts(name, srcPw, dstPw string) NewPipelineOpts {
	return NewPipelineOpts{Name: name, From: "mysql", To: "postgres", SourceHost: "10.0.0.11", SourcePort: 3306,
		TargetHost: "10.0.0.21", TargetPort: 5432, SourceUser: "u", TargetUser: "u",
		SourcePassword: srcPw, TargetPassword: dstPw, Database: "store", CommitOrder: "relaxed"}
}

// Pipeline tạo từ web phải còn sau khi strand-web khởi động lại, kể cả khi thư mục chứa web.yaml không ghi được
// (unit systemd ProtectSystem=strict: chỉ state_dir ghi được). Thư mục không tồn tại => không ghi được với mọi uid.
func TestCreatedPipelineSurvivesRestartReadOnlyWebYAML(t *testing.T) {
	webYAML := filepath.Join(t.TempDir(), "no-such-dir", "web.yaml")
	cfgDir := t.TempDir()

	var stateDir string
	e := opsEnv(t, false, func(c *Config) {
		c.ConfigDir = cfgDir
		c.SourcePath = webYAML
		stateDir = c.StateDir
	})
	c, csrf := e.loginOK()
	e.setExit(0)
	code, _, _, out := createNP(t, e, c, csrf, newOpts("persist1", "a", "b"))
	if code != 200 {
		t.Fatalf("create: %d %s", code, out)
	}
	fi, err := os.Stat(filepath.Join(stateDir, CreatedFile))
	if err != nil {
		t.Fatalf("registry file missing: %v", err)
	}
	if fi.Mode().Perm() != 0o600 {
		t.Errorf("registry mode = %v, want 0600", fi.Mode().Perm())
	}

	// "Khởi động lại": server mới từ cấu hình nạp lại (không mang theo thay đổi trong bộ nhớ).
	cfg2 := &Config{StrandBinary: e.bin, Listen: "127.0.0.1:0", PasswordHash: e.srv.cfg.PasswordHash,
		StateDir: stateDir, SourcePath: webYAML, ConfigDir: cfgDir,
		Pipelines: []PipelineEntry{e.srv.cfg.Pipelines[0]}}
	srv2, err := New(Options{Config: cfg2, Version: "test", Commander: e.cmd, Now: time.Now})
	if err != nil {
		t.Fatal(err)
	}
	p := srv2.reg.Get("persist1")
	if p == nil {
		t.Fatal("web-created pipeline vanished after restart")
	}
	if p.Origin != "created" || p.CfgErr != "" || p.EnvFile == "" {
		t.Errorf("unexpected pipeline after restart: %+v", p)
	}
}

func TestCreatedRegistryPrecedenceAndMissingConfig(t *testing.T) {
	state := t.TempDir()
	reg := []PipelineEntry{
		{Name: "dup", Config: "/created/dup.yaml"},
		{Name: "gone", Config: filepath.Join(state, "missing.yaml")},
	}
	b, _ := json.Marshal(createdFileDoc{Version: 1, Pipelines: reg})
	if err := os.WriteFile(filepath.Join(state, CreatedFile), b, 0o600); err != nil {
		t.Fatal(err)
	}
	cfg := &Config{StateDir: state, Pipelines: []PipelineEntry{{Name: "dup", Config: "/explicit/dup.yaml"}}}
	r := NewRegistry(cfg, &fakeCmd{out: map[string]string{}})
	r.Refresh(t.Context())
	if p := r.Get("dup"); p == nil || p.ConfigPath != "/explicit/dup.yaml" || p.Origin != "web.yaml" {
		t.Errorf("explicit web.yaml entry must win: %+v", p)
	}
	if p := r.Get("gone"); p == nil || p.CfgErr == "" {
		t.Errorf("missing config must show as error, not vanish/crash: %+v", p)
	}
	found := false
	for _, w := range r.Warnings() {
		if strings.Contains(w, `"dup"`) {
			found = true
		}
	}
	if !found {
		t.Errorf("expected conflict warning, got %v", r.Warnings())
	}
}

var trickyPasswords = []string{
	"simple", "with space", `has"quote`, "it's", "dollar$HOME and ${X}", `back\slash`, `trailing\`, "hash # not comment",
	"back`tick`", `"`, `'`, `\"`, `$`, " lead and trail ", "a=b=c", "ünï©ode✓", "#start", "x'y\"z$w\\v`u", `'quoted'`, `"dq"`, "tab\there",
}

func TestEnvFormatRoundTrip(t *testing.T) {
	for _, pw := range trickyPasswords {
		line, err := formatEnvLine("K", pw)
		if err != nil {
			t.Fatalf("%q: %v", pw, err)
		}
		if strings.HasPrefix(line, "export ") || strings.Count(line, "\n") != 1 {
			t.Errorf("%q: bad line %q", pw, line)
		}
		f := filepath.Join(t.TempDir(), "x.env")
		if err := os.WriteFile(f, []byte(line), 0o600); err != nil {
			t.Fatal(err)
		}
		kv, err := ReadEnvFile(f)
		if err != nil || len(kv) != 1 || kv[0] != "K="+pw {
			t.Errorf("round-trip %q via %q: got %v err=%v", pw, line, kv, err)
		}
	}
	for _, bad := range []string{"a\nb", "a\rb", "a\x00b"} {
		if _, err := formatEnvLine("K", bad); err == nil {
			t.Errorf("%q should be rejected", bad)
		}
	}
	if l, _ := formatEnvLine("K", "abc-1.2/x"); l != "K=abc-1.2/x\n" {
		t.Errorf("bare: %q", l)
	}
}

// Các file cũ dạng `export KEY="..."` vẫn đọc được.
func TestReadEnvFileLegacyExport(t *testing.T) {
	f := filepath.Join(t.TempDir(), "old.env")
	_ = os.WriteFile(f, []byte("# c\nexport A=\"x y\"\nexport B='q'\nC=plain\n"), 0o600)
	kv, err := ReadEnvFile(f)
	if err != nil || strings.Join(kv, "|") != "A=x y|B=q|C=plain" {
		t.Fatalf("%v %v", kv, err)
	}
}

func TestCreatePipelineEnvFileTrickyAndNewline(t *testing.T) {
	e, c, csrf := npEnv(t)
	pw := "p@ss \"w'o$rd\\#1`"
	code, res, _, out := createNP(t, e, c, csrf, newOpts("envt", pw, "plain"))
	if code != 200 {
		t.Fatalf("%d %s", code, out)
	}
	envPath := e.srv.reg.Get("envt").EnvFile
	raw, _ := os.ReadFile(envPath)
	if strings.Contains(string(raw), "export ") {
		t.Errorf("env must not use export: %s", raw)
	}
	kv, err := ReadEnvFile(envPath)
	if err != nil || len(kv) != 2 || kv[0] != "SRC_PW="+pw || kv[1] != "DST_PW=plain" {
		t.Fatalf("read back %v %v (%+v)", kv, err, res)
	}
	// Đường sửa .env: giữ nguyên giá trị bí mật khi gửi '********', giá trị mới ghi đúng định dạng.
	p := e.srv.reg.Get("envt")
	if _, err := e.srv.saveEnvContent(t.Context(), p, map[string]string{"SRC_PW": maskStr, "EXTRA": "n w$"}, "ip"); err != nil {
		t.Fatal(err)
	}
	kv, _ = ReadEnvFile(envPath)
	if strings.Join(kv, "|") != "DST_PW=plain|EXTRA=n w$|SRC_PW="+pw {
		t.Fatalf("after save: %v", kv)
	}
	code, _, _, out = createNP(t, e, c, csrf, newOpts("envnl", "a\nb", "x"))
	if code != 400 || !strings.Contains(out, "newline") {
		t.Errorf("newline password: %d %s", code, out)
	}
	if e.srv.reg.Get("envnl") != nil {
		t.Error("pipeline must not be registered after rejected password")
	}
}
