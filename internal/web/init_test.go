package web

import (
	"bytes"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"golang.org/x/crypto/bcrypt"
)

func initOpts(t *testing.T) (InitOptions, *bytes.Buffer, *bytes.Buffer) {
	t.Helper()
	d := t.TempDir()
	var so, se bytes.Buffer
	return InitOptions{
		ConfigPath: filepath.Join(d, "web.yaml"),
		Listen:     "127.0.0.1:8080",
		StateDir:   filepath.Join(d, "state"),
		Scan:       []string{"/etc/strand/*.yaml"},
		Password:   "correct-horse-1",
		Stdout:     &so, Stderr: &se,
		Runner: func(string, ...string) error { t.Fatal("unexpected command"); return nil },
	}, &so, &se
}

func TestInitCreatesWithDefaultsAndPerms(t *testing.T) {
	o, so, _ := initOpts(t)
	res, err := RunInit(o)
	if err != nil || !res.Created {
		t.Fatalf("res=%+v err=%v", res, err)
	}
	fi, _ := os.Stat(o.ConfigPath)
	if fi.Mode().Perm() != 0o600 {
		t.Fatalf("mode %v", fi.Mode().Perm())
	}
	if st, err := os.Stat(o.StateDir); err != nil || st.Mode().Perm() != 0o750 {
		t.Fatalf("state dir: %v %v", st, err)
	}
	c, err := LoadConfig(o.ConfigPath)
	if err != nil {
		t.Fatal(err)
	}
	if err := c.Validate(); err != nil {
		t.Fatalf("config must pass the same validation as `strand web`: %v", err)
	}
	if c.Listen != "127.0.0.1:8080" || c.StateDir != o.StateDir || len(c.Scan) != 1 {
		t.Fatalf("cfg %+v", c)
	}
	if bcrypt.CompareHashAndPassword([]byte(c.PasswordHash), []byte("correct-horse-1")) != nil {
		t.Fatal("hash does not verify")
	}
	b, _ := os.ReadFile(o.ConfigPath)
	if !strings.Contains(string(b), "# ") {
		t.Fatal("expected comments in generated file")
	}
	if strings.Contains(so.String(), c.PasswordHash) || strings.Contains(so.String(), "correct-horse-1") {
		t.Fatalf("summary leaks secrets: %s", so.String())
	}
	if !strings.Contains(so.String(), "ssh -L") {
		t.Fatalf("missing ssh hint: %s", so.String())
	}
}

func TestInitRootStrandGroupOwnership(t *testing.T) {
	o, _, _ := initOpts(t)
	oldRoot, oldLook, oldChown := initIsRoot, initLookupStrand, chownFunc
	defer func() { initIsRoot, initLookupStrand, chownFunc = oldRoot, oldLook, oldChown }()
	initIsRoot = func() bool { return true }
	initLookupStrand = func() (int, int, bool) { return 996, 995, true }
	type ch struct {
		p        string
		uid, gid int
	}
	var got []ch
	chownFunc = func(p string, uid, gid int) error { got = append(got, ch{p, uid, gid}); return nil }
	if _, err := RunInit(o); err != nil {
		t.Fatal(err)
	}
	fi, _ := os.Stat(o.ConfigPath)
	if fi.Mode().Perm() != 0o640 {
		t.Fatalf("mode %v want 0640", fi.Mode().Perm())
	}
	var cfgOK, stOK bool
	for _, c := range got {
		if c.p == o.ConfigPath && c.uid == 0 && c.gid == 995 {
			cfgOK = true
		}
		if c.p == o.StateDir && c.uid == 996 && c.gid == 995 {
			stOK = true
		}
	}
	if !cfgOK || !stOK {
		t.Fatalf("chowns: %+v", got)
	}
}

func TestInitExistingKeptWithoutForce(t *testing.T) {
	o, _, se := initOpts(t)
	if _, err := RunInit(o); err != nil {
		t.Fatal(err)
	}
	o2 := o
	o2.Listen, o2.StateDir, o2.Scan, o2.Password = "127.0.0.1:9999", filepath.Join(t.TempDir(), "other"), []string{"/x/*.yaml"}, "another-pass-2"
	res, err := RunInit(o2)
	if err != nil || !res.KeptSettings {
		t.Fatalf("%+v %v", res, err)
	}
	c, _ := LoadConfig(o.ConfigPath)
	if c.Listen != "127.0.0.1:8080" || c.StateDir != o.StateDir || c.Scan[0] != "/etc/strand/*.yaml" {
		t.Fatalf("settings changed without --force: %+v", c)
	}
	if bcrypt.CompareHashAndPassword([]byte(c.PasswordHash), []byte("another-pass-2")) != nil {
		t.Fatal("password not updated")
	}
	if !strings.Contains(se.String(), "keeping") {
		t.Fatalf("must say settings were kept: %s", se.String())
	}
	if _, err := os.Stat(o2.StateDir); err == nil {
		t.Fatal("state dir of ignored settings must not be created")
	}
}

func TestInitForceUpdates(t *testing.T) {
	o, _, _ := initOpts(t)
	if _, err := RunInit(o); err != nil {
		t.Fatal(err)
	}
	o2 := o
	o2.Force = true
	o2.Listen, o2.Scan, o2.SecureCookies = "127.0.0.1:9999", []string{"/x/*.yaml", "/y/*.yaml"}, true
	if _, err := RunInit(o2); err != nil {
		t.Fatal(err)
	}
	c, _ := LoadConfig(o.ConfigPath)
	if c.Listen != "127.0.0.1:9999" || len(c.Scan) != 2 || !c.SecureCookies {
		t.Fatalf("%+v", c)
	}
}

func TestInitKeepsPasswordWhenNoneGiven(t *testing.T) {
	o, _, _ := initOpts(t)
	if _, err := RunInit(o); err != nil {
		t.Fatal(err)
	}
	c1, _ := LoadConfig(o.ConfigPath)
	o.Password = ""
	if _, err := RunInit(o); err != nil {
		t.Fatal(err)
	}
	c2, _ := LoadConfig(o.ConfigPath)
	if c1.PasswordHash != c2.PasswordHash {
		t.Fatal("hash changed")
	}
	// file mới + không có mật khẩu: lỗi, không ghi gì
	o3, _, _ := initOpts(t)
	o3.Password = ""
	if _, err := RunInit(o3); err == nil {
		t.Fatal("want error")
	}
	if _, err := os.Stat(o3.ConfigPath); err == nil {
		t.Fatal("nothing must be written")
	}
}

func TestInitGeneratedPasswordPrintedOnceAndVerifies(t *testing.T) {
	o, so, _ := initOpts(t)
	pw, err := GeneratePassword()
	if err != nil || len(pw) < 16 {
		t.Fatalf("%q %v", pw, err)
	}
	pw2, _ := GeneratePassword()
	if pw == pw2 {
		t.Fatal("not random")
	}
	o.Password, o.PrintPassword = pw, true
	if _, err := RunInit(o); err != nil {
		t.Fatal(err)
	}
	if n := strings.Count(so.String(), pw); n != 1 {
		t.Fatalf("password printed %d times", n)
	}
	if !strings.Contains(so.String(), "GENERATED ADMIN PASSWORD") {
		t.Fatal("must be labelled")
	}
	c, _ := LoadConfig(o.ConfigPath)
	if bcrypt.CompareHashAndPassword([]byte(c.PasswordHash), []byte(pw)) != nil {
		t.Fatal("hash does not verify")
	}
	if strings.Contains(so.String(), c.PasswordHash) {
		t.Fatal("hash printed")
	}
}

func TestInitToken(t *testing.T) {
	o, so, _ := initOpts(t)
	o.NewToken = true
	res, err := RunInit(o)
	if err != nil || res.Token == "" {
		t.Fatalf("%+v %v", res, err)
	}
	c, _ := LoadConfig(o.ConfigPath)
	if c.APITokenHash != HashAPIToken(res.Token) {
		t.Fatal("token hash mismatch")
	}
	if strings.Count(so.String(), res.Token) != 1 || strings.Contains(so.String(), c.APITokenHash) {
		t.Fatalf("token output: %s", so.String())
	}
}

func TestInitNonLoopbackWithoutTLSWritesNothing(t *testing.T) {
	o, _, _ := initOpts(t)
	o.Listen = "0.0.0.0:8080"
	if _, err := RunInit(o); err == nil || !strings.Contains(err.Error(), "TLS") {
		t.Fatalf("err=%v", err)
	}
	if _, err := os.Stat(o.ConfigPath); err == nil {
		t.Fatal("config written")
	}
	if _, err := os.Stat(o.StateDir); err == nil {
		t.Fatal("state dir created")
	}
	// Có TLS thì được.
	o.TLSCert, o.TLSKey = "/c.pem", "/k.pem"
	if _, err := RunInit(o); err != nil {
		t.Fatal(err)
	}
	c, _ := LoadConfig(o.ConfigPath)
	if err := c.Validate(); err != nil || c.TLSCert != "/c.pem" {
		t.Fatalf("%v %+v", err, c)
	}
	// --force với listen không an toàn trên file có sẵn: lỗi, file không đổi.
	before, _ := os.ReadFile(o.ConfigPath)
	o.Force, o.TLSCert, o.TLSKey = true, "", ""
	if _, err := RunInit(o); err == nil {
		t.Fatal("want error")
	}
	after, _ := os.ReadFile(o.ConfigPath)
	if !bytes.Equal(before, after) {
		t.Fatal("file modified on validation error")
	}
}

func TestInitIdempotent(t *testing.T) {
	o, _, _ := initOpts(t)
	if _, err := RunInit(o); err != nil {
		t.Fatal(err)
	}
	if _, err := RunInit(o); err != nil {
		t.Fatalf("second run: %v", err)
	}
	c, err := LoadConfig(o.ConfigPath)
	if err != nil || c.Validate() != nil || len(c.Scan) != 1 {
		t.Fatalf("%+v %v", c, err)
	}
	fi, _ := os.Stat(o.ConfigPath)
	if fi.Mode().Perm() != 0o600 {
		t.Fatal(fi.Mode())
	}
}

func TestInitEnableUsesRunnerAndPolls(t *testing.T) {
	o, so, _ := initOpts(t)
	var calls []string
	o.Runner = func(n string, a ...string) error {
		calls = append(calls, n+" "+strings.Join(a, " "))
		if len(a) > 0 && a[0] == "is-active" {
			return os.ErrNotExist // chưa chạy
		}
		return nil
	}
	var polled string
	o.Poll = func(url string, d time.Duration) error { polled = url; return nil }
	o.Enable = true
	o.Listen = "127.0.0.1:18080"
	if _, err := RunInit(o); err != nil {
		t.Fatal(err)
	}
	want := []string{"systemctl is-active --quiet strand-web", "systemctl enable --now strand-web"}
	if strings.Join(calls, "|") != strings.Join(want, "|") {
		t.Fatalf("calls %v", calls)
	}
	if polled != "http://127.0.0.1:18080/login" || !strings.Contains(so.String(), "http://127.0.0.1:18080") {
		t.Fatalf("polled %q out %s", polled, so.String())
	}
	// đã chạy: restart để nạp mật khẩu mới
	calls = nil
	o.Runner = func(n string, a ...string) error { calls = append(calls, strings.Join(a, " ")); return nil }
	if _, err := RunInit(o); err != nil {
		t.Fatal(err)
	}
	if calls[len(calls)-1] != "restart strand-web" {
		t.Fatalf("calls %v", calls)
	}
}

func TestInitEnableFailurePropagates(t *testing.T) {
	o, _, _ := initOpts(t)
	o.Enable = true
	o.Runner = func(n string, a ...string) error {
		if a[0] == "enable" {
			return os.ErrPermission
		}
		return os.ErrNotExist
	}
	if _, err := RunInit(o); err == nil {
		t.Fatal("want error")
	}
}

func TestProbeHost(t *testing.T) {
	for in, want := range map[string]string{"0.0.0.0:8443": "127.0.0.1:8443", ":8443": "127.0.0.1:8443", "[::]:1": "127.0.0.1:1", "10.0.0.1:2": "10.0.0.1:2"} {
		if got := probeHost(in); got != want {
			t.Errorf("%s: %s want %s", in, got, want)
		}
	}
}
