package main

import (
	"bytes"
	"path/filepath"
	"strings"
	"testing"

	"golang.org/x/crypto/bcrypt"

	"strand/internal/web"
	"strand/internal/web/store"
)

func runWebUser(t *testing.T, stdin string, args ...string) (string, string, error) {
	t.Helper()
	cmd := newWebUserCmd()
	var so, se bytes.Buffer
	cmd.SetOut(&so)
	cmd.SetErr(&se)
	cmd.SetIn(strings.NewReader(stdin))
	cmd.SetArgs(args)
	err := cmd.Execute()
	return so.String(), se.String(), err
}

func runWebPasswd(t *testing.T, stdin string, args ...string) (string, string, error) {
	t.Helper()
	cmd := newWebPasswdCmd()
	var so, se bytes.Buffer
	cmd.SetOut(&so)
	cmd.SetErr(&se)
	cmd.SetIn(strings.NewReader(stdin))
	cmd.SetArgs(args)
	err := cmd.Execute()
	return so.String(), se.String(), err
}

func TestWebUserCLI(t *testing.T) {
	d := t.TempDir()
	old := cfgPath
	defer func() { cfgPath = old }()
	cfgPath = filepath.Join(d, "web.yaml")
	dsn := initDB(t)

	// Bootstrap web.yaml with admin
	_, _, err := runWebInit(t, "SecureAdminPassword123!\n", "--password-stdin", "--database-dsn", dsn, "--state-dir", filepath.Join(d, "st"))
	if err != nil {
		t.Fatalf("init: %v", err)
	}

	st, err := web.CLIStore(t.Context(), cfgPath, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()

	// 1. Add user alice with role operator
	out, _, err := runWebUser(t, "AliceSecretPass123!\n", "add", "alice", "--role", "operator", "--password-stdin", "--display-name", "Alice Ops")
	if err != nil {
		t.Fatalf("add alice: %v (out: %s)", err, out)
	}
	if !strings.Contains(out, `user "alice" created`) {
		t.Errorf("add alice output: %s", out)
	}

	// Verify in DB
	u, ok, err := st.GetUser(t.Context(), "alice")
	if !ok || err != nil {
		t.Fatalf("get alice: %v", err)
	}
	if u.Role != store.RoleOperator || u.DisplayName != "Alice Ops" {
		t.Errorf("alice in DB: %+v", u)
	}
	if bcrypt.CompareHashAndPassword([]byte(u.PasswordHash), []byte("AliceSecretPass123!")) != nil {
		t.Errorf("alice password hash mismatch")
	}

	// 2. Add user bob with generated password
	out, _, err = runWebUser(t, "", "add", "bob", "--role", "viewer", "--generate-password")
	if err != nil {
		t.Fatalf("add bob: %v", err)
	}
	if !strings.Contains(out, "GENERATED PASSWORD") {
		t.Errorf("bob output lacks generated password: %s", out)
	}
	uBob, ok, err := st.GetUser(t.Context(), "bob")
	if !ok || err != nil || uBob.Role != store.RoleViewer {
		t.Fatalf("get bob: %+v %v", uBob, err)
	}
	if !uBob.MustChangePassword {
		t.Errorf("bob must_change_password want true, got false")
	}

	// 3. List users
	out, _, err = runWebUser(t, "", "list")
	if err != nil {
		t.Fatalf("list users: %v", err)
	}
	if !strings.Contains(out, "admin") || !strings.Contains(out, "alice") || !strings.Contains(out, "bob") {
		t.Errorf("list users output missing user: %s", out)
	}

	// 4. Disable alice
	out, _, err = runWebUser(t, "", "disable", "alice")
	if err != nil {
		t.Fatalf("disable alice: %v", err)
	}
	u, _, _ = st.GetUser(t.Context(), "alice")
	if !u.Disabled {
		t.Errorf("alice disabled flag want true, got false")
	}

	// 5. Enable alice
	out, _, err = runWebUser(t, "", "enable", "alice")
	if err != nil {
		t.Fatalf("enable alice: %v", err)
	}
	u, _, _ = st.GetUser(t.Context(), "alice")
	if u.Disabled {
		t.Errorf("alice disabled flag want false, got true")
	}

	// 6. Set role alice -> admin
	out, _, err = runWebUser(t, "", "set-role", "alice", "admin")
	if err != nil {
		t.Fatalf("set-role alice: %v", err)
	}
	u, _, _ = st.GetUser(t.Context(), "alice")
	if u.Role != store.RoleAdmin {
		t.Errorf("alice role want admin, got %s", u.Role)
	}

	// 7. Reset password for alice
	out, _, err = runWebUser(t, "AliceNewSecretPass456!\n", "reset-password", "alice", "--password-stdin")
	if err != nil {
		t.Fatalf("reset-password alice: %v", err)
	}
	u, _, _ = st.GetUser(t.Context(), "alice")
	if bcrypt.CompareHashAndPassword([]byte(u.PasswordHash), []byte("AliceNewSecretPass456!")) != nil {
		t.Errorf("alice new password hash mismatch")
	}
	if !u.MustChangePassword {
		t.Errorf("alice reset-password want must_change_password=true")
	}

	// 8. strand web passwd --user alice
	out, _, err = runWebPasswd(t, "AlicePasswdCommand789!\nAlicePasswdCommand789!\n", "--user", "alice")
	if err != nil {
		t.Fatalf("strand web passwd --user alice: %v", err)
	}
	u, _, _ = st.GetUser(t.Context(), "alice")
	if bcrypt.CompareHashAndPassword([]byte(u.PasswordHash), []byte("AlicePasswdCommand789!")) != nil {
		t.Errorf("alice password hash after passwd command mismatch")
	}
}

func TestWebInitAdminUser(t *testing.T) {
	d := t.TempDir()
	old := cfgPath
	defer func() { cfgPath = old }()
	cfgPath = filepath.Join(d, "web.yaml")
	dsn := initDB(t)

	out, _, err := runWebInit(t, "CustomAdminPass123!\n", "--password-stdin", "--admin-user", "customadmin", "--database-dsn", dsn, "--state-dir", filepath.Join(d, "st"))
	if err != nil {
		t.Fatalf("init customadmin: %v (out: %s)", err, out)
	}

	st, err := web.CLIStore(t.Context(), cfgPath, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()

	u, ok, err := st.GetUser(t.Context(), "customadmin")
	if !ok || err != nil {
		t.Fatalf("customadmin user not found in DB: %v", err)
	}
	if u.Role != store.RoleAdmin {
		t.Errorf("customadmin role = %s, want admin", u.Role)
	}
	if bcrypt.CompareHashAndPassword([]byte(u.PasswordHash), []byte("CustomAdminPass123!")) != nil {
		t.Errorf("customadmin password hash mismatch")
	}
}
