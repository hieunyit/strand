package main

import (
	"bytes"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"golang.org/x/crypto/bcrypt"

	"strand/internal/testutil"
	"strand/internal/web"
)

// initDB: DSN của một PostgreSQL thật cho `strand web init` (bắt buộc); khôi phục biến môi trường sau test
// (init đặt STRAND_WEB_DATABASE_DSN cho tiến trình).
func initDB(t *testing.T) string {
	t.Helper()
	t.Setenv(web.EnvDatabaseDSN, "")
	return testutil.NewPostgresDSN(t)
}

func adminFromDB(t *testing.T) (string, string) {
	t.Helper()
	st, err := web.CLIStore(t.Context(), cfgPath, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	a, _, err := st.Admin(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	return a.PasswordHash, a.APITokenHash
}

// PostgreSQL-only: init without a database DSN fails before writing anything and says what to do.
func TestWebInitRequiresDatabase(t *testing.T) {
	t.Setenv(web.EnvDatabaseDSN, "")
	d := t.TempDir()
	old := cfgPath
	defer func() { cfgPath = old }()
	cfgPath = filepath.Join(d, "web.yaml")
	_, _, err := runWebInit(t, "s3cret-pass-1\n", "--password-stdin", "--state-dir", filepath.Join(d, "st"))
	if err == nil || !strings.Contains(err.Error(), "--database-dsn") {
		t.Fatalf("err = %v", err)
	}
	if _, err := os.Stat(cfgPath); err == nil {
		t.Fatal("web.yaml written without a database")
	}
}

func runWebInit(t *testing.T, stdin string, args ...string) (string, string, error) {
	t.Helper()
	cmd := newWebInitCmd()
	var so, se bytes.Buffer
	cmd.SetOut(&so)
	cmd.SetErr(&se)
	cmd.SetIn(strings.NewReader(stdin))
	cmd.SetArgs(args)
	err := cmd.Execute()
	return so.String(), se.String(), err
}

func TestWebInitPasswordStdinAndToken(t *testing.T) {
	d := t.TempDir()
	old := cfgPath
	defer func() { cfgPath = old }()
	cfgPath = filepath.Join(d, "web.yaml")
	dsn := initDB(t)
	out, _, err := runWebInit(t, "s3cret-pass-1\n", "--password-stdin", "--token", "--database-dsn", dsn, "--state-dir", filepath.Join(d, "st"), "--scan", d+"/*.yaml", "--scan", d+"/b/*.yaml")
	if err != nil {
		t.Fatal(err)
	}
	c, err := web.LoadConfig(cfgPath)
	if err != nil || c.Validate() != nil || c.Database.DSN != dsn {
		t.Fatalf("%v %v", err, c)
	}
	pwh, tokh := adminFromDB(t)
	if bcrypt.CompareHashAndPassword([]byte(pwh), []byte("s3cret-pass-1")) != nil || tokh == "" || len(c.Scan) != 2 {
		t.Fatalf("%+v", c)
	}
	if c.PasswordHash != "" || c.APITokenHash != "" {
		t.Fatal("hashes written to web.yaml")
	}
	if strings.Contains(out, "s3cret-pass-1") || strings.Contains(out, pwh) {
		t.Fatal("secret leaked to stdout")
	}
}

func TestWebInitGeneratePassword(t *testing.T) {
	d := t.TempDir()
	old := cfgPath
	defer func() { cfgPath = old }()
	cfgPath = filepath.Join(d, "web.yaml")
	out, _, err := runWebInit(t, "", "--generate-password", "--database-dsn", initDB(t), "--state-dir", filepath.Join(d, "st"))
	if err != nil {
		t.Fatal(err)
	}
	var pw string
	for _, l := range strings.Split(out, "\n") {
		if strings.HasPrefix(l, "GENERATED ADMIN PASSWORD") {
			pw = strings.TrimSpace(l[strings.LastIndex(l, ":")+1:])
		}
	}
	pwh, _ := adminFromDB(t)
	if pw == "" || bcrypt.CompareHashAndPassword([]byte(pwh), []byte(pw)) != nil {
		t.Fatalf("printed password does not verify; out=%q", out)
	}
}

func TestWebInitRejectsOpenListenWithoutTLS(t *testing.T) {
	d := t.TempDir()
	old := cfgPath
	defer func() { cfgPath = old }()
	cfgPath = filepath.Join(d, "web.yaml")
	_, _, err := runWebInit(t, "s3cret-pass-1\n", "--password-stdin", "--listen", "0.0.0.0:8080", "--state-dir", filepath.Join(d, "st"))
	if err == nil {
		t.Fatal("want error")
	}
	if _, err := os.Stat(cfgPath); err == nil {
		t.Fatal("config written")
	}
}
