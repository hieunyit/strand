package web

import (
	"encoding/json"
	"fmt"
	"net/http"
	"regexp"
	"strings"

	"strand/internal/web/store"
)

var validUsernameRE = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9_.-]{0,62}$`)

type usersPageBody struct {
	Users             []store.User
	Message           string
	Error             string
	CreatedToken      string
	GeneratedPassword string
	PasswordUser      string
}

func (s *Server) pageUsers(w http.ResponseWriter, r *http.Request, sess *Session) {
	ctx, cancel := dbCtx()
	defer cancel()

	users, err := s.db.st.ListUsers(ctx)
	if err != nil {
		s.render(w, "users", sess, pageData{Title: "Users", Active: "users", Body: usersPageBody{Error: err.Error()}})
		return
	}
	s.render(w, "users", sess, pageData{Title: "Users", Active: "users", Body: usersPageBody{Users: users}})
}

func (s *Server) handleUserCreateForm(w http.ResponseWriter, r *http.Request, sess *Session) {
	r.Body = http.MaxBytesReader(w, r.Body, 1<<20)
	if err := r.ParseForm(); err != nil {
		s.renderUsersMsg(w, sess, "", "Invalid form submission")
		return
	}
	username := strings.TrimSpace(r.PostForm.Get("username"))
	if !validUsernameRE.MatchString(username) {
		s.renderUsersMsg(w, sess, "", "Invalid username: must be 1-63 alphanumeric characters (may contain . _ -)")
		return
	}
	displayName := strings.TrimSpace(r.PostForm.Get("display_name"))
	role := store.Role(r.PostForm.Get("role"))
	if !role.Valid() {
		s.renderUsersMsg(w, sess, "", "Invalid role: must be admin, operator, or viewer")
		return
	}

	pw := r.PostForm.Get("password")
	generated := ""
	if pw == "" {
		g, err := GeneratePassword()
		if err != nil {
			s.renderUsersMsg(w, sess, "", "Failed to generate password: "+err.Error())
			return
		}
		pw = g
		generated = g
	} else {
		if err := ValidatePassword(username, pw); err != nil {
			s.renderUsersMsg(w, sess, "", err.Error())
			return
		}
	}

	h, err := HashPassword(pw)
	if err != nil {
		s.renderUsersMsg(w, sess, "", err.Error())
		return
	}

	mustChange := r.PostForm.Get("must_change_password") == "1" || generated != ""
	ctx, cancel := dbCtx()
	defer cancel()

	u, err := s.db.st.CreateUser(ctx, store.User{
		Username:           username,
		DisplayName:        displayName,
		PasswordHash:       h,
		Role:               role,
		MustChangePassword: mustChange,
	})
	if err != nil {
		s.renderUsersMsg(w, sess, "", "Failed to create user: "+err.Error())
		return
	}

	_ = s.audit.Log(AuditEntry{
		IP:     s.clientIP(r),
		User:   sess.Username,
		Action: "user.create",
		Result: "ok",
		Detail: map[string]any{"target_user": username, "role": string(role)},
	})

	users, _ := s.db.st.ListUsers(ctx)
	body := usersPageBody{
		Users:             users,
		Message:           fmt.Sprintf("User %q created successfully.", u.Username),
		GeneratedPassword: generated,
		PasswordUser:      u.Username,
	}
	s.render(w, "users", sess, pageData{Title: "Users", Active: "users", Body: body})
}

func (s *Server) handleUserDisableForm(w http.ResponseWriter, r *http.Request, sess *Session) {
	_ = r.ParseForm()
	username := strings.TrimSpace(r.PostForm.Get("username"))
	if strings.EqualFold(username, sess.Username) {
		s.renderUsersMsg(w, sess, "", "You cannot disable your own account")
		return
	}
	ctx, cancel := dbCtx()
	defer cancel()

	u, ok, err := s.db.st.GetUser(ctx, username)
	if err != nil || !ok {
		s.renderUsersMsg(w, sess, "", "User not found")
		return
	}
	u.Disabled = true
	if err := s.db.st.UpdateUser(ctx, u); err != nil {
		s.renderUsersMsg(w, sess, "", err.Error())
		return
	}
	_ = s.db.st.PurgeUserSessions(ctx, u.ID)
	_ = s.audit.Log(AuditEntry{IP: s.clientIP(r), User: sess.Username, Action: "user.disable", Result: "ok", Detail: map[string]any{"target_user": username}})
	s.renderUsersMsg(w, sess, fmt.Sprintf("User %q has been disabled and all their sessions were terminated.", username), "")
}

func (s *Server) handleUserEnableForm(w http.ResponseWriter, r *http.Request, sess *Session) {
	_ = r.ParseForm()
	username := strings.TrimSpace(r.PostForm.Get("username"))
	ctx, cancel := dbCtx()
	defer cancel()

	u, ok, err := s.db.st.GetUser(ctx, username)
	if err != nil || !ok {
		s.renderUsersMsg(w, sess, "", "User not found")
		return
	}
	u.Disabled = false
	if err := s.db.st.UpdateUser(ctx, u); err != nil {
		s.renderUsersMsg(w, sess, "", err.Error())
		return
	}
	_ = s.audit.Log(AuditEntry{IP: s.clientIP(r), User: sess.Username, Action: "user.enable", Result: "ok", Detail: map[string]any{"target_user": username}})
	s.renderUsersMsg(w, sess, fmt.Sprintf("User %q has been enabled.", username), "")
}

func (s *Server) handleUserResetPasswordForm(w http.ResponseWriter, r *http.Request, sess *Session) {
	_ = r.ParseForm()
	username := strings.TrimSpace(r.PostForm.Get("username"))
	ctx, cancel := dbCtx()
	defer cancel()

	u, ok, err := s.db.st.GetUser(ctx, username)
	if err != nil || !ok {
		s.renderUsersMsg(w, sess, "", "User not found")
		return
	}
	generated, err := GeneratePassword()
	if err != nil {
		s.renderUsersMsg(w, sess, "", err.Error())
		return
	}
	h, err := HashPassword(generated)
	if err != nil {
		s.renderUsersMsg(w, sess, "", err.Error())
		return
	}
	if err := s.db.st.SetUserPassword(ctx, u.ID, h, true); err != nil {
		s.renderUsersMsg(w, sess, "", err.Error())
		return
	}
	_ = s.db.st.PurgeUserSessions(ctx, u.ID)
	_ = s.audit.Log(AuditEntry{IP: s.clientIP(r), User: sess.Username, Action: "user.reset_password", Result: "ok", Detail: map[string]any{"target_user": username}})

	users, _ := s.db.st.ListUsers(ctx)
	body := usersPageBody{
		Users:             users,
		Message:           fmt.Sprintf("Password for %q was reset.", username),
		GeneratedPassword: generated,
		PasswordUser:      username,
	}
	s.render(w, "users", sess, pageData{Title: "Users", Active: "users", Body: body})
}

func (s *Server) handleUserRevokeTokensForm(w http.ResponseWriter, r *http.Request, sess *Session) {
	_ = r.ParseForm()
	username := strings.TrimSpace(r.PostForm.Get("username"))
	ctx, cancel := dbCtx()
	defer cancel()

	u, ok, err := s.db.st.GetUser(ctx, username)
	if err != nil || !ok {
		s.renderUsersMsg(w, sess, "", "User not found")
		return
	}
	toks, err := s.db.st.ListUserTokens(ctx, u.ID)
	if err != nil {
		s.renderUsersMsg(w, sess, "", err.Error())
		return
	}
	for _, tok := range toks {
		if tok.RevokedAt == nil {
			_ = s.db.st.RevokeUserToken(ctx, tok.ID, u.ID)
		}
	}
	_ = s.audit.Log(AuditEntry{IP: s.clientIP(r), User: sess.Username, Action: "user.revoke_tokens", Result: "ok", Detail: map[string]any{"target_user": username}})
	s.renderUsersMsg(w, sess, fmt.Sprintf("All API tokens for %q have been revoked.", username), "")
}

func (s *Server) handleUserDeleteForm(w http.ResponseWriter, r *http.Request, sess *Session) {
	_ = r.ParseForm()
	username := strings.TrimSpace(r.PostForm.Get("username"))
	if strings.EqualFold(username, sess.Username) {
		s.renderUsersMsg(w, sess, "", "You cannot delete your own account")
		return
	}
	ctx, cancel := dbCtx()
	defer cancel()

	u, ok, err := s.db.st.GetUser(ctx, username)
	if err != nil || !ok {
		s.renderUsersMsg(w, sess, "", "User not found")
		return
	}
	if err := s.db.st.DeleteUser(ctx, u.ID); err != nil {
		s.renderUsersMsg(w, sess, "", err.Error())
		return
	}
	_ = s.audit.Log(AuditEntry{IP: s.clientIP(r), User: sess.Username, Action: "user.delete", Result: "ok", Detail: map[string]any{"target_user": username}})
	s.renderUsersMsg(w, sess, fmt.Sprintf("User %q was deleted.", username), "")
}

func (s *Server) renderUsersMsg(w http.ResponseWriter, sess *Session, msg, errMsg string) {
	ctx, cancel := dbCtx()
	defer cancel()
	users, _ := s.db.st.ListUsers(ctx)
	s.render(w, "users", sess, pageData{Title: "Users", Active: "users", Body: usersPageBody{Users: users, Message: msg, Error: errMsg}})
}

// ---- REST API Handlers for User Management ----

func (s *Server) apiUsersList(w http.ResponseWriter, r *http.Request, _ *Session) {
	ctx, cancel := dbCtx()
	defer cancel()

	users, err := s.db.st.ListUsers(ctx)
	if err != nil {
		writeErr(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"users": users})
}

type userCreateRequest struct {
	Username           string     `json:"username"`
	DisplayName        string     `json:"display_name"`
	Role               store.Role `json:"role"`
	Password           string     `json:"password"`
	MustChangePassword *bool      `json:"must_change_password"`
}

func (s *Server) apiUserCreate(w http.ResponseWriter, r *http.Request, sess *Session) {
	var req userCreateRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeErr(w, http.StatusBadRequest, "invalid request body")
		return
	}
	username := strings.TrimSpace(req.Username)
	if !validUsernameRE.MatchString(username) {
		writeErr(w, http.StatusBadRequest, "invalid username: must be 1-63 alphanumeric characters (may contain . _ -)")
		return
	}
	if !req.Role.Valid() {
		writeErr(w, http.StatusBadRequest, "invalid role: must be admin, operator, or viewer")
		return
	}

	pw := req.Password
	generated := ""
	if pw == "" {
		g, err := GeneratePassword()
		if err != nil {
			writeErr(w, http.StatusInternalServerError, "failed to generate password")
			return
		}
		pw = g
		generated = g
	} else {
		if err := ValidatePassword(username, pw); err != nil {
			writeErr(w, http.StatusBadRequest, err.Error())
			return
		}
	}

	h, err := HashPassword(pw)
	if err != nil {
		writeErr(w, http.StatusBadRequest, err.Error())
		return
	}

	mustChange := true
	if req.MustChangePassword != nil {
		mustChange = *req.MustChangePassword
	}

	ctx, cancel := dbCtx()
	defer cancel()

	u, err := s.db.st.CreateUser(ctx, store.User{
		Username:           username,
		DisplayName:        strings.TrimSpace(req.DisplayName),
		PasswordHash:       h,
		Role:               req.Role,
		MustChangePassword: mustChange,
	})
	if err != nil {
		writeErr(w, http.StatusConflict, err.Error())
		return
	}

	_ = s.audit.Log(AuditEntry{
		IP:     s.clientIP(r),
		User:   sess.Username,
		Action: "user.create",
		Result: "ok",
		Detail: map[string]any{"target_user": username, "role": string(req.Role)},
	})

	res := map[string]any{
		"user": u,
	}
	if generated != "" {
		res["generated_password"] = generated
	}
	writeJSON(w, http.StatusCreated, res)
}

func (s *Server) apiUserGet(w http.ResponseWriter, r *http.Request, _ *Session) {
	username := r.PathValue("username")
	ctx, cancel := dbCtx()
	defer cancel()

	u, ok, err := s.db.st.GetUser(ctx, username)
	if err != nil {
		writeErr(w, http.StatusInternalServerError, err.Error())
		return
	}
	if !ok {
		writeErr(w, http.StatusNotFound, "user not found")
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"user": u})
}

func (s *Server) apiUserSetRole(w http.ResponseWriter, r *http.Request, sess *Session) {
	username := r.PathValue("username")
	var body struct {
		Role store.Role `json:"role"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		writeErr(w, http.StatusBadRequest, "invalid request body")
		return
	}
	if !body.Role.Valid() {
		writeErr(w, http.StatusBadRequest, "invalid role")
		return
	}
	ctx, cancel := dbCtx()
	defer cancel()

	u, ok, err := s.db.st.GetUser(ctx, username)
	if err != nil || !ok {
		writeErr(w, http.StatusNotFound, "user not found")
		return
	}
	u.Role = body.Role
	if err := s.db.st.UpdateUser(ctx, u); err != nil {
		writeErr(w, http.StatusInternalServerError, err.Error())
		return
	}
	_ = s.audit.Log(AuditEntry{IP: s.clientIP(r), User: sess.Username, Action: "user.set_role", Result: "ok", Detail: map[string]any{"target_user": username, "role": string(body.Role)}})
	writeJSON(w, http.StatusOK, map[string]any{"ok": true, "user": u})
}

func (s *Server) apiUserDisable(w http.ResponseWriter, r *http.Request, sess *Session) {
	username := r.PathValue("username")
	if strings.EqualFold(username, sess.Username) {
		writeErr(w, http.StatusBadRequest, "cannot disable own account")
		return
	}
	ctx, cancel := dbCtx()
	defer cancel()

	u, ok, err := s.db.st.GetUser(ctx, username)
	if err != nil || !ok {
		writeErr(w, http.StatusNotFound, "user not found")
		return
	}
	u.Disabled = true
	if err := s.db.st.UpdateUser(ctx, u); err != nil {
		writeErr(w, http.StatusInternalServerError, err.Error())
		return
	}
	_ = s.db.st.PurgeUserSessions(ctx, u.ID)
	_ = s.audit.Log(AuditEntry{IP: s.clientIP(r), User: sess.Username, Action: "user.disable", Result: "ok", Detail: map[string]any{"target_user": username}})
	writeJSON(w, http.StatusOK, map[string]any{"ok": true})
}

func (s *Server) apiUserEnable(w http.ResponseWriter, r *http.Request, sess *Session) {
	username := r.PathValue("username")
	ctx, cancel := dbCtx()
	defer cancel()

	u, ok, err := s.db.st.GetUser(ctx, username)
	if err != nil || !ok {
		writeErr(w, http.StatusNotFound, "user not found")
		return
	}
	u.Disabled = false
	if err := s.db.st.UpdateUser(ctx, u); err != nil {
		writeErr(w, http.StatusInternalServerError, err.Error())
		return
	}
	_ = s.audit.Log(AuditEntry{IP: s.clientIP(r), User: sess.Username, Action: "user.enable", Result: "ok", Detail: map[string]any{"target_user": username}})
	writeJSON(w, http.StatusOK, map[string]any{"ok": true})
}

func (s *Server) apiUserResetPassword(w http.ResponseWriter, r *http.Request, sess *Session) {
	username := r.PathValue("username")
	ctx, cancel := dbCtx()
	defer cancel()

	u, ok, err := s.db.st.GetUser(ctx, username)
	if err != nil || !ok {
		writeErr(w, http.StatusNotFound, "user not found")
		return
	}

	var body struct {
		Password string `json:"password"`
	}
	_ = json.NewDecoder(r.Body).Decode(&body)

	pw := body.Password
	generated := ""
	if pw == "" {
		g, err := GeneratePassword()
		if err != nil {
			writeErr(w, http.StatusInternalServerError, "failed to generate password")
			return
		}
		pw = g
		generated = g
	} else {
		if err := ValidatePassword(username, pw); err != nil {
			writeErr(w, http.StatusBadRequest, err.Error())
			return
		}
	}

	h, err := HashPassword(pw)
	if err != nil {
		writeErr(w, http.StatusBadRequest, err.Error())
		return
	}

	if err := s.db.st.SetUserPassword(ctx, u.ID, h, true); err != nil {
		writeErr(w, http.StatusInternalServerError, err.Error())
		return
	}
	_ = s.db.st.PurgeUserSessions(ctx, u.ID)
	_ = s.audit.Log(AuditEntry{IP: s.clientIP(r), User: sess.Username, Action: "user.reset_password", Result: "ok", Detail: map[string]any{"target_user": username}})

	res := map[string]any{"ok": true}
	if generated != "" {
		res["generated_password"] = generated
	}
	writeJSON(w, http.StatusOK, res)
}

func (s *Server) apiUserRevokeTokens(w http.ResponseWriter, r *http.Request, sess *Session) {
	username := r.PathValue("username")
	ctx, cancel := dbCtx()
	defer cancel()

	u, ok, err := s.db.st.GetUser(ctx, username)
	if err != nil || !ok {
		writeErr(w, http.StatusNotFound, "user not found")
		return
	}
	toks, err := s.db.st.ListUserTokens(ctx, u.ID)
	if err != nil {
		writeErr(w, http.StatusInternalServerError, err.Error())
		return
	}
	revokedCount := 0
	for _, tok := range toks {
		if tok.RevokedAt == nil {
			if err := s.db.st.RevokeUserToken(ctx, tok.ID, u.ID); err == nil {
				revokedCount++
			}
		}
	}
	_ = s.audit.Log(AuditEntry{IP: s.clientIP(r), User: sess.Username, Action: "user.revoke_tokens", Result: "ok", Detail: map[string]any{"target_user": username, "revoked_count": revokedCount}})
	writeJSON(w, http.StatusOK, map[string]any{"ok": true, "revoked_count": revokedCount})
}

func (s *Server) apiUserDelete(w http.ResponseWriter, r *http.Request, sess *Session) {
	username := r.PathValue("username")
	if strings.EqualFold(username, sess.Username) {
		writeErr(w, http.StatusBadRequest, "cannot delete own account")
		return
	}
	ctx, cancel := dbCtx()
	defer cancel()

	u, ok, err := s.db.st.GetUser(ctx, username)
	if err != nil || !ok {
		writeErr(w, http.StatusNotFound, "user not found")
		return
	}
	if err := s.db.st.DeleteUser(ctx, u.ID); err != nil {
		writeErr(w, http.StatusInternalServerError, err.Error())
		return
	}
	_ = s.audit.Log(AuditEntry{IP: s.clientIP(r), User: sess.Username, Action: "user.delete", Result: "ok", Detail: map[string]any{"target_user": username}})
	writeJSON(w, http.StatusOK, map[string]any{"ok": true})
}
