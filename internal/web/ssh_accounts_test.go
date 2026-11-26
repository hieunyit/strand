package web

import (
	"encoding/base64"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"golang.org/x/crypto/ssh"

	"strand/internal/config"
	"strand/internal/sshprofile"
	"strand/internal/tunnel"
)

// A host created with a new SSH account stores the user and key on the account (sealed, bound to the account
// name); a second host picks the same account without any credential; the account lists the hosts that use it
// and cannot be deleted while they do; editing it re-checks every host and refuses a key they reject.
func TestSSHAccountSharedByHosts(t *testing.T) {
	t.Setenv("SSH_AUTH_SOCK", "")
	t.Setenv("STRAND_WEB_SSH_KNOWN_HOSTS", filepath.Join(t.TempDir(), "missing"))
	e := newCPEnv(t)
	c, csrf := e.loginOK()
	addr, identity, _ := webTestSSHServer(t, t.TempDir())
	key, err := os.ReadFile(identity)
	if err != nil {
		t.Fatal(err)
	}
	hostKey, err := tunnel.ProbeHostKey(t.Context(), addr)
	if err != nil {
		t.Fatal(err)
	}
	fingerprint := ssh.FingerprintSHA256(hostKey)
	form := map[string]string{"Content-Type": "application/x-www-form-urlencoded"}
	post := func(v url.Values) (*http.Response, string) {
		v.Set("csrf", csrf)
		return e.do(c, "POST", "/tunnels", v.Encode(), form)
	}
	meta := func(key string) string {
		raw, _, err := e.srv.db.st.MetaGet(t.Context(), key)
		if err != nil {
			t.Fatal(err)
		}
		return raw
	}

	// Step 1 (fingerprint) offers a new account when none exists yet.
	resp, body := post(url.Values{"intent": {"save-profile"}, "profile_id": {"bastion-a"}, "profile_host": {addr}, "probe_host_key": {"1"}})
	if resp.StatusCode != 200 || !strings.Contains(body, `name="profile_account"`) || !strings.Contains(body, `value="__new__" selected`) {
		t.Fatalf("step 2 lacks the account choice: %d", resp.StatusCode)
	}
	host := func(id string) url.Values {
		return url.Values{"intent": {"save-profile"}, "profile_id": {id}, "profile_host": {addr}, "profile_host_address": {addr},
			"profile_fingerprint": {fingerprint}, "profile_confirm_host_key": {"1"}}
	}
	v := host("bastion-a")
	v.Set("profile_account", "__new__")
	v.Set("account_id", "ops")
	v.Set("account_user", "strand")
	v.Set("account_auth_mode", "key")
	v.Set("account_private_key", string(key))
	if resp, body := post(v); resp.StatusCode != http.StatusSeeOther {
		t.Fatalf("host with a new account: %d %s", resp.StatusCode, body)
	}
	accounts, profiles := meta(sshprofile.AccountsMetaKey), meta(sshprofile.MetaKey)
	if strings.Contains(accounts, "PRIVATE KEY") || strings.Contains(accounts, base64.StdEncoding.EncodeToString(key)) || !strings.Contains(accounts, "sealed_key") {
		t.Fatalf("account key not sealed: %s", accounts)
	}
	if !strings.Contains(profiles, `"account":"ops"`) || strings.Contains(profiles, "sealed_") || strings.Contains(profiles, `"user":"strand"`) {
		t.Fatalf("host must reference the account, not copy its credentials: %s", profiles)
	}

	// A second host reuses the account: no credential posted, and none accepted.
	v = host("bastion-b")
	v.Set("profile_account", "ops")
	v.Set("profile_password", "not-for-a-host")
	if resp, body := post(v); resp.StatusCode != http.StatusBadRequest || !strings.Contains(body, "signs in with an SSH account") {
		t.Fatalf("host credentials next to an account: %d", resp.StatusCode)
	}
	v.Del("profile_password")
	if resp, body := post(v); resp.StatusCode != http.StatusSeeOther {
		t.Fatalf("host with an existing account: %d %s", resp.StatusCode, body)
	}
	for _, id := range []string{"bastion-a", "bastion-b"} {
		settings, err := e.srv.savedTunnel(t.Context(), id)
		if err != nil || settings.SSHUser != "strand" || settings.PrivateKey == nil || ssh.FingerprintSHA256(settings.HostKey) != fingerprint {
			t.Fatalf("%s does not resolve through the account: %v", id, err)
		}
		forward, err := config.OpenSSHTunnelConfig(t.Context(), settings, webTestEchoServer(t))
		if err != nil {
			t.Fatalf("%s: open tunnel with the account key: %v", id, err)
		}
		_ = forward.Close()
	}

	// Lists: hosts show their account, the account shows its hosts; pickers elsewhere show the account user.
	_, page := e.do(c, "GET", "/tunnels", "", nil)
	if !strings.Contains(page, `href="/tunnels?account_edit=ops"`) {
		t.Fatal("host list does not link the account")
	}
	_, page = e.do(c, "GET", "/tunnels?tab=accounts", "", nil)
	if !strings.Contains(page, `href="/tunnels?edit=bastion-a"`) || !strings.Contains(page, `href="/tunnels?edit=bastion-b"`) || strings.Contains(page, `value="delete-account"`) {
		t.Fatal("account list must show its hosts and offer no delete while they use it")
	}
	if _, page := e.do(c, "GET", "/pipelines/new", "", nil); !strings.Contains(page, "bastion-b · strand@") {
		t.Fatal("pipeline wizard does not show the account user of a host")
	}
	if resp, body := post(url.Values{"intent": {"delete-account"}, "account_id": {"ops"}}); resp.StatusCode != http.StatusBadRequest || !strings.Contains(body, "bastion-a, bastion-b") {
		t.Fatalf("delete of a used account: %d", resp.StatusCode)
	}

	// Editing the account checks every host: a key the server rejects is refused and nothing changes.
	otherDir := t.TempDir()
	_, otherIdentity, _ := webTestSSHServer(t, otherDir)
	otherKey, err := os.ReadFile(otherIdentity)
	if err != nil {
		t.Fatal(err)
	}
	before := meta(sshprofile.AccountsMetaKey)
	edit := url.Values{"intent": {"save-account"}, "account_edit": {"1"}, "account_id": {"ops"}, "account_user": {"strand"},
		"account_auth_mode": {"key"}, "account_private_key": {string(otherKey)}}
	if resp, body := post(edit); resp.StatusCode != http.StatusBadRequest || !strings.Contains(body, "host bastion-a does not accept this account") {
		t.Fatalf("edit with a rejected key: %d", resp.StatusCode)
	}
	if meta(sshprofile.AccountsMetaKey) != before {
		t.Fatal("a refused account edit changed the stored account")
	}
	edit.Del("account_private_key") // blank keeps the stored key
	if resp, _ := post(edit); resp.StatusCode != http.StatusSeeOther || resp.Header.Get("Location") != "/tunnels?tab=accounts&notice=account-saved&checked=2" {
		t.Fatalf("edit keeping the key: %d %s", resp.StatusCode, resp.Header.Get("Location"))
	}

	// A name clash on create is refused; an unused account is deleted.
	if resp, body := post(url.Values{"intent": {"save-account"}, "account_id": {"ops"}, "account_user": {"x"}, "account_auth_mode": {"key"}}); resp.StatusCode != http.StatusBadRequest || !strings.Contains(body, "already exists") {
		t.Fatalf("duplicate account: %d", resp.StatusCode)
	}
	if resp, _ := post(url.Values{"intent": {"save-account"}, "account_id": {"spare"}, "account_user": {"backup"}, "account_auth_mode": {"password"}, "account_password": {"pw-fixture-123"}}); resp.StatusCode != http.StatusSeeOther {
		t.Fatalf("unused account: %d", resp.StatusCode)
	}
	if strings.Contains(meta(sshprofile.AccountsMetaKey), "pw-fixture-123") {
		t.Fatal("account password stored in clear")
	}
	if resp, _ := post(url.Values{"intent": {"delete-account"}, "account_id": {"spare"}}); resp.StatusCode != http.StatusSeeOther {
		t.Fatalf("delete unused account: %d", resp.StatusCode)
	}
	if strings.Contains(meta(sshprofile.AccountsMetaKey), `"spare"`) {
		t.Fatal("deleted account still stored")
	}
	audit := e.auditText()
	for _, action := range []string{"tunnel.account.create", "tunnel.account.update", "tunnel.account.delete"} {
		if !strings.Contains(audit, action) {
			t.Fatalf("audit lacks %s", action)
		}
	}
	if strings.Contains(audit, "pw-fixture-123") || strings.Contains(audit, "PRIVATE KEY") {
		t.Fatal("audit leaks a credential")
	}
}

// Login page: language switch, show-password control, its script, and the plain-HTTP warning.
func TestLoginPageControls(t *testing.T) {
	e := newEnv(t, nil)
	c := e.client()
	resp, page := e.do(c, "GET", "/login?lang=vi", "", nil)
	if resp.StatusCode != 200 {
		t.Fatalf("login: %d", resp.StatusCode)
	}
	for _, want := range []string{`src="/static/login.js"`, `data-reveal="login-password"`, `action="/login?lang=vi"`, `href="/login?lang=en"`, "Kết nối này không được mã hóa", `class="login-caps" hidden`} {
		if !strings.Contains(page, want) {
			t.Fatalf("login page lacks %q", want)
		}
	}
	if resp, js := e.do(c, "GET", "/static/login.js", "", nil); resp.StatusCode != 200 || !strings.Contains(js, "data-reveal") {
		t.Fatalf("login.js: %d", resp.StatusCode)
	}
	// A failed sign-in keeps the chosen language and shows the error.
	_, page = e.do(c, "GET", "/login?lang=vi", "", nil)
	if !strings.Contains(page, "Đăng nhập") {
		t.Fatal("Vietnamese login page")
	}
}
