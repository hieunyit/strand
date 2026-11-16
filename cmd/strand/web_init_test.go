package main

import (
	"bytes"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"golang.org/x/crypto/bcrypt"

	"strand/internal/web"
)

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
	out, _, err := runWebInit(t, "s3cret-pass-1\n", "--password-stdin", "--token", "--state-dir", filepath.Join(d, "st"), "--scan", d+"/*.yaml", "--scan", d+"/b/*.yaml")
	if err != nil {
		t.Fatal(err)
	}
	c, err := web.LoadConfig(cfgPath)
	if err != nil || c.Validate() != nil {
		t.Fatalf("%v %v", err, c)
	}
	if bcrypt.CompareHashAndPassword([]byte(c.PasswordHash), []byte("s3cret-pass-1")) != nil || c.APITokenHash == "" || len(c.Scan) != 2 {
		t.Fatalf("%+v", c)
	}
	if strings.Contains(out, "s3cret-pass-1") || strings.Contains(out, c.PasswordHash) {
		t.Fatal("secret leaked to stdout")
	}
}

func TestWebInitGeneratePassword(t *testing.T) {
	d := t.TempDir()
	old := cfgPath
	defer func() { cfgPath = old }()
	cfgPath = filepath.Join(d, "web.yaml")
	out, _, err := runWebInit(t, "", "--generate-password", "--state-dir", filepath.Join(d, "st"))
	if err != nil {
		t.Fatal(err)
	}
	var pw string
	for _, l := range strings.Split(out, "\n") {
		if strings.HasPrefix(l, "GENERATED ADMIN PASSWORD") {
			pw = strings.TrimSpace(l[strings.LastIndex(l, ":")+1:])
		}
	}
	c, _ := web.LoadConfig(cfgPath)
	if pw == "" || bcrypt.CompareHashAndPassword([]byte(c.PasswordHash), []byte(pw)) != nil {
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
