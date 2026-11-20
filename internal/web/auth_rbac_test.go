package web

import (
	"context"
	"net/http"
	"strings"
	"testing"

	"golang.org/x/crypto/bcrypt"

	"strand/internal/web/store"
)

func TestPasswordPolicy(t *testing.T) {
	tests := []struct {
		username string
		password string
		wantErr  bool
	}{
		{"alice", "short", true},                  // < 12 chars
		{"alice", "alice", true},                  // equals username
		{"Alice", "alice", true},                  // equals username case-insensitive
		{"alice", "SecurePassword123!", false},    // >= 12, != username
		{"bob", strings.Repeat("a", 73), true},   // > 72 bytes
		{"bob", strings.Repeat("a", 72), false},  // 72 bytes OK
	}

	for _, tc := range tests {
		err := ValidatePassword(tc.username, tc.password)
		if (err != nil) != tc.wantErr {
			t.Errorf("ValidatePassword(%q, %q) err = %v, wantErr = %v", tc.username, tc.password, err, tc.wantErr)
		}
	}
}

func TestBcryptCost(t *testing.T) {
	pw := "StrandSecurePass123!"
	hash, err := HashPassword(pw)
	if err != nil {
		t.Fatalf("HashPassword: %v", err)
	}
	cost, err := bcrypt.Cost([]byte(hash))
	if err != nil {
		t.Fatalf("bcrypt.Cost: %v", err)
	}
	if cost < 12 {
		t.Errorf("bcrypt cost = %d, want >= 12", cost)
	}
}

func TestScopedUserAPITokens(t *testing.T) {
	e := newEnv(t, nil)
	ctx := context.Background()

	// 1. Create operator user and viewer user
	h, _ := HashPassword("SecurePassword123!")
	operUser, err := e.srv.db.st.CreateUser(ctx, store.User{
		Username:     "oper_tok_user",
		PasswordHash: h,
		Role:         store.RoleOperator,
	})
	if err != nil {
		t.Fatal(err)
	}
	viewUser, err := e.srv.db.st.CreateUser(ctx, store.User{
		Username:     "view_tok_user",
		PasswordHash: h,
		Role:         store.RoleViewer,
	})
	if err != nil {
		t.Fatal(err)
	}

	// 2. Create tokens
	operTok := NewAPIToken()
	_, err = e.srv.db.st.CreateUserToken(ctx, store.UserToken{
		UserID:    operUser.ID,
		Name:      "oper-ci",
		TokenHash: hashKey(operTok),
	})
	if err != nil {
		t.Fatal(err)
	}

	viewTok := NewAPIToken()
	vTokRecord, err := e.srv.db.st.CreateUserToken(ctx, store.UserToken{
		UserID:    viewUser.ID,
		Name:      "view-ci",
		TokenHash: hashKey(viewTok),
	})
	if err != nil {
		t.Fatal(err)
	}

	c := e.client()

	// Viewer token accessing overview: 200
	resp, _ := e.do(c, "GET", "/api/v1/overview", "", map[string]string{
		"Authorization": "Bearer " + viewTok,
	})
	if resp.StatusCode != 200 {
		t.Errorf("Viewer token GET overview: %d, want 200", resp.StatusCode)
	}

	// Viewer token accessing actions: 403
	resp, _ = e.do(c, "POST", "/api/v1/pipelines/orders/actions/start", "{}", map[string]string{
		"Authorization": "Bearer " + viewTok,
		"Content-Type":  "application/json",
	})
	if resp.StatusCode != 403 {
		t.Errorf("Viewer token POST start: %d, want 403", resp.StatusCode)
	}

	// Operator token accessing actions: allowed (not 403)
	resp, _ = e.do(c, "POST", "/api/v1/pipelines/orders/actions/start", "{}", map[string]string{
		"Authorization": "Bearer " + operTok,
		"Content-Type":  "application/json",
	})
	if resp.StatusCode == 403 {
		t.Errorf("Operator token POST start: 403, want allowed")
	}

	// Operator token accessing users API: 403
	resp, _ = e.do(c, "GET", "/api/v1/users", "", map[string]string{
		"Authorization": "Bearer " + operTok,
	})
	if resp.StatusCode != 403 {
		t.Errorf("Operator token GET users: %d, want 403", resp.StatusCode)
	}

	// 3. Revoke viewer token -> 401
	if err := e.srv.db.st.RevokeUserToken(ctx, vTokRecord.ID, viewUser.ID); err != nil {
		t.Fatal(err)
	}
	resp, _ = e.do(c, "GET", "/api/v1/overview", "", map[string]string{
		"Authorization": "Bearer " + viewTok,
	})
	if resp.StatusCode != 401 {
		t.Errorf("Revoked viewer token GET overview: %d, want 401", resp.StatusCode)
	}

	// 4. Legacy global admin token
	legacyTok := NewAPIToken()
	if err := e.srv.db.st.SetAPITokenHash(ctx, hashKey(legacyTok)); err != nil {
		t.Fatal(err)
	}
	e.srv.db.invalidateAdminCache()
	resp, _ = e.do(c, "GET", "/api/v1/users", "", map[string]string{
		"Authorization": "Bearer " + legacyTok,
	})
	if resp.StatusCode != 200 {
		t.Errorf("Legacy global token GET users: %d, want 200", resp.StatusCode)
	}
	if dep := resp.Header.Get("Deprecation"); dep == "" {
		t.Errorf("Legacy global token lacks Deprecation header")
	}
}

func TestMustChangePasswordFlow(t *testing.T) {
	e := newEnv(t, nil)
	ctx := context.Background()

	oldPw := "InitialPassword123!"
	newPw := "NewPassword45678!"
	h, _ := HashPassword(oldPw)
	_, err := e.srv.db.st.CreateUser(ctx, store.User{
		Username:           "first_login_user",
		PasswordHash:       h,
		Role:               store.RoleOperator,
		MustChangePassword: true,
	})
	if err != nil {
		t.Fatal(err)
	}

	c := e.client()
	e.do(c, "GET", "/login", "", nil)
	var lc string
	for _, ck := range c.Jar.Cookies(nil) {
		if ck.Name == loginCookie {
			lc = ck.Value
		}
	}

	// 1. Login attempt without new password -> prompt / error indicating password change required
	body := `{"username":"first_login_user","password":"` + oldPw + `"}`
	resp, respBody := e.do(c, "POST", "/api/v1/login", body, map[string]string{
		csrfHeader:     lc,
		"Content-Type": "application/json",
	})
	if resp.StatusCode != http.StatusBadRequest && resp.StatusCode != http.StatusForbidden {
		t.Errorf("Login with must_change_password=true without new password returned %d, want 400 or 403", resp.StatusCode)
	}
	if !strings.Contains(respBody, "must_change_password") {
		t.Errorf("Body lacks must_change_password flag: %s", respBody)
	}

	// 2. Login attempt with new password -> success
	bodyWithNew := `{"username":"first_login_user","password":"` + oldPw + `","new_password":"` + newPw + `"}`
	resp2, respBody2 := e.do(c, "POST", "/api/v1/login", bodyWithNew, map[string]string{
		csrfHeader:     lc,
		"Content-Type": "application/json",
	})
	if resp2.StatusCode != 200 {
		t.Fatalf("Login with new_password failed: %d body: %s", resp2.StatusCode, respBody2)
	}

	// Verify must_change_password is now false in DB
	u, ok, err := e.srv.db.st.GetUser(ctx, "first_login_user")
	if !ok || err != nil {
		t.Fatalf("GetUser: %v", err)
	}
	if u.MustChangePassword {
		t.Errorf("MustChangePassword is still true after password change")
	}

	// Verify subsequent login with old password fails
	c2 := e.client()
	e.do(c2, "GET", "/login", "", nil)
	for _, ck := range c2.Jar.Cookies(nil) {
		if ck.Name == loginCookie {
			lc = ck.Value
		}
	}
	respOld, _ := e.do(c2, "POST", "/api/v1/login", `{"username":"first_login_user","password":"`+oldPw+`"}`, map[string]string{
		csrfHeader:     lc,
		"Content-Type": "application/json",
	})
	if respOld.StatusCode != 401 {
		t.Errorf("Login with old password returned %d, want 401", respOld.StatusCode)
	}

	// Verify subsequent login with new password succeeds
	respNew, _ := e.do(c2, "POST", "/api/v1/login", `{"username":"first_login_user","password":"`+newPw+`"}`, map[string]string{
		csrfHeader:     lc,
		"Content-Type": "application/json",
	})
	if respNew.StatusCode != 200 {
		t.Errorf("Login with new password returned %d, want 200", respNew.StatusCode)
	}
}

func TestLogoutEverywhere(t *testing.T) {
	e := newEnv(t, nil)
	ctx := context.Background()

	pw := "SecurePassword123!"
	h, _ := HashPassword(pw)
	u, err := e.srv.db.st.CreateUser(ctx, store.User{
		Username:     "logout_target_user",
		PasswordHash: h,
		Role:         store.RoleOperator,
	})
	if err != nil {
		t.Fatal(err)
	}

	// Establish session
	c := e.client()
	e.do(c, "GET", "/login", "", nil)
	var lc string
	for _, ck := range c.Jar.Cookies(nil) {
		if ck.Name == loginCookie {
			lc = ck.Value
		}
	}
	resp, _ := e.do(c, "POST", "/api/v1/login", `{"username":"logout_target_user","password":"`+pw+`"}`, map[string]string{
		csrfHeader:     lc,
		"Content-Type": "application/json",
	})
	if resp.StatusCode != 200 {
		t.Fatalf("login failed: %d", resp.StatusCode)
	}

	// Verify session is active
	resp, _ = e.do(c, "GET", "/api/v1/session", "", nil)
	if resp.StatusCode != 200 {
		t.Fatalf("session not active: %d", resp.StatusCode)
	}

	// Case 1: Disable user
	u.Disabled = true
	if err := e.srv.db.st.UpdateUser(ctx, u); err != nil {
		t.Fatal(err)
	}
	resp, _ = e.do(c, "GET", "/api/v1/session", "", nil)
	if resp.StatusCode != 401 {
		t.Errorf("Disabled user session GET /api/v1/session = %d, want 401", resp.StatusCode)
	}

	// Re-enable and login again
	u.Disabled = false
	if err := e.srv.db.st.UpdateUser(ctx, u); err != nil {
		t.Fatal(err)
	}
	c2 := e.client()
	e.do(c2, "GET", "/login", "", nil)
	for _, ck := range c2.Jar.Cookies(nil) {
		if ck.Name == loginCookie {
			lc = ck.Value
		}
	}
	e.do(c2, "POST", "/api/v1/login", `{"username":"logout_target_user","password":"`+pw+`"}`, map[string]string{
		csrfHeader:     lc,
		"Content-Type": "application/json",
	})
	resp, _ = e.do(c2, "GET", "/api/v1/session", "", nil)
	if resp.StatusCode != 200 {
		t.Fatalf("session 2 not active: %d", resp.StatusCode)
	}

	// Case 2: Change user password via SetUserPassword (which purges sessions)
	newH, _ := HashPassword("AnotherSecurePass456!")
	if err := e.srv.db.st.SetUserPassword(ctx, u.ID, newH, false); err != nil {
		t.Fatal(err)
	}
	if err := e.srv.db.st.PurgeUserSessions(ctx, u.ID); err != nil {
		t.Fatal(err)
	}
	resp, _ = e.do(c2, "GET", "/api/v1/session", "", nil)
	if resp.StatusCode != 401 {
		t.Errorf("Session after password change = %d, want 401", resp.StatusCode)
	}
}

func TestSessionRotationOnLogin(t *testing.T) {
	e := newEnv(t, nil)
	c := e.client()

	// GET /login establishes an initial anonymous/login cookie
	e.do(c, "GET", "/login", "", nil)
	var initialSession string
	for _, ck := range c.Jar.Cookies(nil) {
		if ck.Name == sessionCookie {
			initialSession = ck.Value
		}
	}

	// Perform login
	e.login(c, testPassword)

	var postLoginSession string
	for _, ck := range c.Jar.Cookies(nil) {
		if ck.Name == sessionCookie {
			postLoginSession = ck.Value
		}
	}

	if postLoginSession == "" {
		t.Fatal("no session cookie after login")
	}
	if initialSession != "" && initialSession == postLoginSession {
		t.Errorf("session ID was not rotated on login: %s", postLoginSession)
	}
}

func TestRateLimitingAndLockout(t *testing.T) {
	e := newEnv(t, nil)
	ctx := context.Background()

	pw := "SecurePassword123!"
	h, _ := HashPassword(pw)
	_, err := e.srv.db.st.CreateUser(ctx, store.User{
		Username:     "rate_limit_user",
		PasswordHash: h,
		Role:         store.RoleViewer,
	})
	if err != nil {
		t.Fatal(err)
	}

	c := e.client()
	e.do(c, "GET", "/login", "", nil)
	var lc string
	for _, ck := range c.Jar.Cookies(nil) {
		if ck.Name == loginCookie {
			lc = ck.Value
		}
	}

	// 5 failed login attempts with wrong password
	for i := 1; i <= MaxFailedLogins; i++ {
		resp, _ := e.do(c, "POST", "/api/v1/login", `{"username":"rate_limit_user","password":"WrongPassword123!"}`, map[string]string{
			csrfHeader:     lc,
			"Content-Type": "application/json",
		})
		if resp.StatusCode != 401 && resp.StatusCode != 429 {
			t.Fatalf("Attempt %d returned status %d", i, resp.StatusCode)
		}
	}

	// 6th attempt should be locked out (429 or lockout message)
	resp, body := e.do(c, "POST", "/api/v1/login", `{"username":"rate_limit_user","password":"WrongPassword123!"}`, map[string]string{
		csrfHeader:     lc,
		"Content-Type": "application/json",
	})
	if resp.StatusCode != http.StatusTooManyRequests && resp.StatusCode != http.StatusUnauthorized {
		t.Errorf("Attempt 6 status = %d, want 429 or 401", resp.StatusCode)
	}
	if !strings.Contains(body, "locked") && !strings.Contains(body, "try again in") && !strings.Contains(body, "rate limit") {
		t.Errorf("Attempt 6 body lacks lockout message: %s", body)
	}

	// Check DB user locked_until
	u, _, _ := e.srv.db.st.GetUser(ctx, "rate_limit_user")
	if u.LockedUntil == nil || u.LockedUntil.Before(e.clock.Now()) {
		t.Errorf("User locked_until was not set to future time: %v (clock: %v)", u.LockedUntil, e.clock.Now())
	}
}
