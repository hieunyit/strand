package main

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"

	"golang.org/x/crypto/bcrypt"

	"strand/internal/testutil"
	"strand/internal/web"
	"strand/internal/web/store"
)

func TestMain(m *testing.M) {
	code := m.Run()
	testutil.TerminatePostgres()
	os.Exit(code)
}

func openTestStore(t *testing.T, dsn string) store.Store {
	t.Helper()
	st, err := store.Open(context.Background(), dsn, store.OpenOptions{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { st.Close() })
	return st
}

// Với STRAND_WEB_DATABASE_DSN: init/passwd/token ghi vào PostgreSQL, không vào web.yaml.
func TestWebCLIWritesDatabaseWhenDSNSet(t *testing.T) {
	dsn := testutil.NewPostgresDSN(t)
	t.Setenv(web.EnvDatabaseDSN, dsn)
	d := t.TempDir()
	old := cfgPath
	defer func() { cfgPath = old }()
	cfgPath = filepath.Join(d, "web.yaml")
	st := openTestStore(t, dsn)
	ctx := context.Background()

	// init --generate-password --if-no-admin: in mật khẩu MỘT lần.
	so, _, err := runWebInit(t, "", "--generate-password", "--if-no-admin", "--quiet", "--listen", "0.0.0.0:8080", "--allow-plain-http",
		"--state-dir", filepath.Join(d, "st"), "--scan", d+"/*.yaml")
	if err != nil {
		t.Fatal(err)
	}
	m := regexp.MustCompile(`GENERATED ADMIN PASSWORD \(shown once, store it now\): (\S+)`).FindStringSubmatch(so)
	if m == nil {
		t.Fatalf("generated password not printed:\n%s", so)
	}
	adm, ok, _ := st.Admin(ctx)
	if !ok || bcrypt.CompareHashAndPassword([]byte(adm.PasswordHash), []byte(m[1])) != nil {
		t.Fatalf("generated password hash is not in the database")
	}
	if b, _ := os.ReadFile(cfgPath); strings.Contains(string(b), "\npassword_hash:") {
		t.Fatalf("hash written to web.yaml in DB mode:\n%s", b)
	}
	// Chạy lại (container restart): không đổi admin, không in mật khẩu mới.
	_ = os.Remove(cfgPath)
	so2, _, err := runWebInit(t, "", "--generate-password", "--if-no-admin", "--quiet", "--listen", "0.0.0.0:8080", "--allow-plain-http",
		"--state-dir", filepath.Join(d, "st"), "--scan", d+"/*.yaml")
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(so2, "GENERATED") {
		t.Fatalf("password printed again on re-run:\n%s", so2)
	}
	if adm2, _, _ := st.Admin(ctx); adm2 != adm {
		t.Fatal("re-run changed the admin account")
	}

	// passwd: đọc stdin (không phải terminal), ghi DB.
	pr, pw, _ := os.Pipe()
	_, _ = pw.WriteString("brand-new-pass-9\n")
	pw.Close()
	oldIn := os.Stdin
	os.Stdin = pr
	pcmd := newWebPasswdCmd()
	var out bytes.Buffer
	pcmd.SetOut(&out)
	pcmd.SetContext(ctx)
	perr := pcmd.Execute()
	os.Stdin = oldIn
	if perr != nil {
		t.Fatal(perr)
	}
	adm3, _, _ := st.Admin(ctx)
	if bcrypt.CompareHashAndPassword([]byte(adm3.PasswordHash), []byte("brand-new-pass-9")) != nil {
		t.Fatalf("passwd did not write the database")
	}
	if adm3.APITokenHash != adm.APITokenHash {
		t.Fatal("passwd clobbered the token hash")
	}

	// token: ghi sha256 vào DB, in token một lần.
	tcmd := newWebTokenCmd()
	var tout, terr bytes.Buffer
	tcmd.SetOut(&tout)
	tcmd.SetErr(&terr)
	tcmd.SetContext(ctx)
	if err := tcmd.Execute(); err != nil {
		t.Fatal(err)
	}
	tok := strings.TrimSpace(tout.String())
	adm4, _, _ := st.Admin(ctx)
	if tok == "" || adm4.APITokenHash != web.HashAPIToken(tok) {
		t.Fatalf("token hash not in the database")
	}
	if b, _ := os.ReadFile(cfgPath); strings.Contains(string(b), tok) || strings.Contains(string(b), adm4.APITokenHash) {
		t.Fatal("token material in web.yaml")
	}
}
