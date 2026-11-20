package web

import (
	"context"
	"encoding/json"
	"net/http"
	"strings"
	"testing"

	"strand/internal/web/store"
)

func TestRoleAllows(t *testing.T) {
	tests := []struct {
		role store.Role
		perm Permission
		want bool
	}{
		{store.RoleAdmin, PermRead, true},
		{store.RoleAdmin, PermOperator, true},
		{store.RoleAdmin, PermAdmin, true},

		{store.RoleOperator, PermRead, true},
		{store.RoleOperator, PermOperator, true},
		{store.RoleOperator, PermAdmin, false},

		{store.RoleViewer, PermRead, true},
		{store.RoleViewer, PermOperator, false},
		{store.RoleViewer, PermAdmin, false},

		{"unknown", PermRead, false},
		{store.RoleViewer, "invalid_perm", false},
	}

	for _, tc := range tests {
		got := RoleAllows(tc.role, tc.perm)
		if got != tc.want {
			t.Errorf("RoleAllows(%s, %s) = %v, want %v", tc.role, tc.perm, got, tc.want)
		}
	}
}

type rbacUser struct {
	username string
	password string
	role     store.Role
	client   *http.Client
	csrf     string
	token    string
}

func setupRBACUsers(t *testing.T, e *testEnv) map[store.Role]*rbacUser {
	t.Helper()
	ctx := context.Background()

	pw := "StrandSecurePass123!"
	h, err := HashPassword(pw)
	if err != nil {
		t.Fatal(err)
	}

	roles := []store.Role{store.RoleAdmin, store.RoleOperator, store.RoleViewer}
	users := make(map[store.Role]*rbacUser)

	for _, role := range roles {
		uname := string(role) + "_user"
		var u store.User
		if role == store.RoleAdmin {
			uname = "admin"
			existing, ok, err := e.srv.db.st.GetUser(ctx, "admin")
			if !ok || err != nil {
				t.Fatalf("get admin: %v", err)
			}
			if err := e.srv.db.st.SetUserPassword(ctx, existing.ID, h, false); err != nil {
				t.Fatalf("set admin password: %v", err)
			}
			existing.PasswordHash = h
			u = existing
		} else {
			created, err := e.srv.db.st.CreateUser(ctx, store.User{
				Username:           uname,
				DisplayName:        string(role),
				PasswordHash:       h,
				Role:               role,
				MustChangePassword: false,
			})
			if err != nil {
				t.Fatalf("create user %s: %v", uname, err)
			}
			u = created
		}

		// Also create a scoped API token
		rawTok := NewAPIToken()
		_, err = e.srv.db.st.CreateUserToken(ctx, store.UserToken{
			UserID:    u.ID,
			Name:      "test-token",
			TokenHash: hashKey(rawTok),
		})
		if err != nil {
			t.Fatalf("create token for %s: %v", uname, err)
		}

		c := e.client()
		e.do(c, "GET", "/login", "", nil)
		var lc string
		for _, ck := range c.Jar.Cookies(nil) {
			if ck.Name == loginCookie {
				lc = ck.Value
			}
		}
		body := `{"username":"` + uname + `","password":"` + pw + `"}`
		resp, respBody := e.do(c, "POST", "/api/v1/login", body, map[string]string{
			csrfHeader:     lc,
			"Content-Type": "application/json",
		})
		if resp.StatusCode != 200 {
			t.Fatalf("login %s: status %d body %s", uname, resp.StatusCode, respBody)
		}
		var parsed map[string]any
		_ = json.Unmarshal([]byte(respBody), &parsed)
		csrf, _ := parsed["csrf"].(string)

		users[role] = &rbacUser{
			username: uname,
			password: pw,
			role:     role,
			client:   c,
			csrf:     csrf,
			token:    rawTok,
		}
	}

	return users
}

func TestSensitiveReadsRBAC(t *testing.T) {
	e := newEnv(t, nil)
	users := setupRBACUsers(t, e)

	routes := []struct {
		name       string
		path       string
		isAPI      bool
		wantAdmin  int
		wantOper   int
		wantViewer int
	}{
		{name: "users_page", path: "/users", isAPI: false, wantAdmin: 200, wantOper: 403, wantViewer: 403},
		{name: "settings_page", path: "/settings", isAPI: false, wantAdmin: 200, wantOper: 403, wantViewer: 403},
		{name: "users_api", path: "/api/v1/users", isAPI: true, wantAdmin: 200, wantOper: 403, wantViewer: 403},
		{name: "settings_api", path: "/api/v1/settings", isAPI: true, wantAdmin: 200, wantOper: 403, wantViewer: 403},
		{name: "env_api", path: "/api/v1/pipelines/orders/env", isAPI: true, wantAdmin: 200, wantOper: 403, wantViewer: 403},
		{name: "audit_api", path: "/api/v1/audit", isAPI: true, wantAdmin: 404, wantOper: 403, wantViewer: 403}, // 404 for admin because no handler yet, but 403 for operator/viewer
	}

	for _, tc := range routes {
		t.Run(tc.name, func(t *testing.T) {
			// Test Viewer
			respV, bodyV := e.do(users[store.RoleViewer].client, "GET", tc.path, "", nil)
			if respV.StatusCode != tc.wantViewer {
				t.Errorf("Viewer GET %s status = %d, want %d", tc.path, respV.StatusCode, tc.wantViewer)
			}
			if tc.isAPI {
				if ct := respV.Header.Get("Content-Type"); !strings.Contains(ct, "application/json") {
					t.Errorf("Viewer GET %s content-type = %s, want json", tc.path, ct)
				}
				if !strings.Contains(bodyV, "forbidden") && !strings.Contains(bodyV, "Forbidden") {
					t.Errorf("Viewer GET %s body = %s, want forbidden message", tc.path, bodyV)
				}
			} else {
				if ct := respV.Header.Get("Content-Type"); !strings.Contains(ct, "text/html") {
					t.Errorf("Viewer GET %s content-type = %s, want text/html", tc.path, ct)
				}
				if !strings.Contains(bodyV, "403 Forbidden") {
					t.Errorf("Viewer GET %s body lacks 403 Forbidden: %s", tc.path, bodyV)
				}
			}

			// Test Operator
			respO, _ := e.do(users[store.RoleOperator].client, "GET", tc.path, "", nil)
			if respO.StatusCode != tc.wantOper {
				t.Errorf("Operator GET %s status = %d, want %d", tc.path, respO.StatusCode, tc.wantOper)
			}

			// Test Admin
			respA, _ := e.do(users[store.RoleAdmin].client, "GET", tc.path, "", nil)
			if respA.StatusCode != tc.wantAdmin {
				t.Errorf("Admin GET %s status = %d, want %d", tc.path, respA.StatusCode, tc.wantAdmin)
			}
		})
	}
}

func TestOperationalWritesRBAC(t *testing.T) {
	e := newEnv(t, nil)
	users := setupRBACUsers(t, e)

	actions := []string{
		"start",
		"stop",
		"restart",
		"pause",
		"resume",
		"throttle",
		"snapshot-add",
		"verify",
	}

	for _, act := range actions {
		t.Run("action_"+act, func(t *testing.T) {
			path := "/api/v1/pipelines/orders/actions/" + act
			body := "{}"
			if act == "throttle" {
				body = `{"rate_mb": 10}`
			}

			// Viewer must be forbidden (403)
			vHeaders := map[string]string{
				csrfHeader:     users[store.RoleViewer].csrf,
				"Content-Type": "application/json",
			}
			respV, respBodyV := e.do(users[store.RoleViewer].client, "POST", path, body, vHeaders)
			if respV.StatusCode != http.StatusForbidden {
				t.Errorf("Viewer POST %s = %d, want 403. Body: %s", path, respV.StatusCode, respBodyV)
			}

			// Operator must be allowed (status != 403)
			oHeaders := map[string]string{
				csrfHeader:     users[store.RoleOperator].csrf,
				"Content-Type": "application/json",
			}
			respO, respBodyO := e.do(users[store.RoleOperator].client, "POST", path, body, oHeaders)
			if respO.StatusCode == http.StatusForbidden {
				t.Errorf("Operator POST %s = 403 forbidden, want allowed. Body: %s", path, respBodyO)
			}

			// Admin must be allowed (status != 403)
			aHeaders := map[string]string{
				csrfHeader:     users[store.RoleAdmin].csrf,
				"Content-Type": "application/json",
			}
			respA, respBodyA := e.do(users[store.RoleAdmin].client, "POST", path, body, aHeaders)
			if respA.StatusCode == http.StatusForbidden {
				t.Errorf("Admin POST %s = 403 forbidden, want allowed. Body: %s", path, respBodyA)
			}
		})
	}
}

func TestAdminOnlyWritesRBAC(t *testing.T) {
	e := newEnv(t, nil)
	users := setupRBACUsers(t, e)

	routes := []struct {
		name       string
		method     string
		path       string
		body       string
		isForm     bool
		wantAdmin  int
		wantOper   int
		wantViewer int
	}{
		{
			name:       "api_user_create",
			method:     "POST",
			path:       "/api/v1/users",
			body:       `{"username":"testuser2","role":"viewer","password":"ValidPass1234!"}`,
			isForm:     false,
			wantAdmin:  201,
			wantOper:   403,
			wantViewer: 403,
		},
		{
			name:       "api_pipeline_env",
			method:     "POST",
			path:       "/api/v1/pipelines/orders/env",
			body:       `{"env":{"FOO":"bar"}}`,
			isForm:     false,
			wantAdmin:  200,
			wantOper:   403,
			wantViewer: 403,
		},
		{
			name:       "api_config_save",
			method:     "POST",
			path:       "/api/v1/pipelines/orders/config",
			body:       `{"raw_yaml":"invalid"}`,
			isForm:     false,
			wantAdmin:  200,
			wantOper:   403,
			wantViewer: 403,
		},
		{
			name:       "form_user_create",
			method:     "POST",
			path:       "/users/create",
			body:       "username=testuser3&role=viewer&password=ValidPass1234!",
			isForm:     true,
			wantAdmin:  200,
			wantOper:   403,
			wantViewer: 403,
		},
	}

	for _, tc := range routes {
		t.Run(tc.name, func(t *testing.T) {
			// Viewer -> 403
			vHdr := map[string]string{csrfHeader: users[store.RoleViewer].csrf}
			if tc.isForm {
				vHdr["Content-Type"] = "application/x-www-form-urlencoded"
			} else {
				vHdr["Content-Type"] = "application/json"
			}
			respV, bodyV := e.do(users[store.RoleViewer].client, tc.method, tc.path, tc.body, vHdr)
			if respV.StatusCode != tc.wantViewer {
				t.Errorf("Viewer %s %s status = %d, want %d (body: %s)", tc.method, tc.path, respV.StatusCode, tc.wantViewer, bodyV)
			}

			// Operator -> 403
			oHdr := map[string]string{csrfHeader: users[store.RoleOperator].csrf}
			if tc.isForm {
				oHdr["Content-Type"] = "application/x-www-form-urlencoded"
			} else {
				oHdr["Content-Type"] = "application/json"
			}
			respO, bodyO := e.do(users[store.RoleOperator].client, tc.method, tc.path, tc.body, oHdr)
			if respO.StatusCode != tc.wantOper {
				t.Errorf("Operator %s %s status = %d, want %d (body: %s)", tc.method, tc.path, respO.StatusCode, tc.wantOper, bodyO)
			}

			// Admin -> wantAdmin
			aHdr := map[string]string{csrfHeader: users[store.RoleAdmin].csrf}
			if tc.isForm {
				aHdr["Content-Type"] = "application/x-www-form-urlencoded"
			} else {
				aHdr["Content-Type"] = "application/json"
			}
			respA, bodyA := e.do(users[store.RoleAdmin].client, tc.method, tc.path, tc.body, aHdr)
			if respA.StatusCode != tc.wantAdmin {
				t.Errorf("Admin %s %s status = %d, want %d (body: %s)", tc.method, tc.path, respA.StatusCode, tc.wantAdmin, bodyA)
			}
		})
	}
}

func TestSensitiveConfigMaskedEvenForAdmin(t *testing.T) {
	e := newEnv(t, nil)
	users := setupRBACUsers(t, e)

	resp, body := e.do(users[store.RoleAdmin].client, "GET", "/api/v1/pipelines/orders/config", "", nil)
	if resp.StatusCode != 200 {
		t.Fatalf("Admin GET config status = %d, body = %s", resp.StatusCode, body)
	}

	// Secret in pipelineYAML is "tgtSecretPw"
	if strings.Contains(body, "tgtSecretPw") {
		t.Fatalf("Config leaked plaintext secret: %s", body)
	}
	if !strings.Contains(body, "********") && !strings.Contains(body, "[masked") {
		t.Errorf("Config does not contain masked indication: %s", body)
	}
}
