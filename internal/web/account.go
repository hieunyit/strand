package web

import (
	"encoding/json"
	"net/http"
	"strconv"
	"strings"

	"golang.org/x/crypto/bcrypt"

	"strand/internal/web/store"
)

type accountPageBody struct {
	User               store.User
	Tokens             []store.UserToken
	Sessions           []store.Session
	CurrentSessionHash string
	Message            string
	Error              string
	CreatedToken       string
}

func (s *Server) pageAccount(w http.ResponseWriter, r *http.Request, sess *Session) {
	ctx, cancel := dbCtx()
	defer cancel()

	u, ok, err := s.db.st.GetUserByID(ctx, sess.UserID)
	if err != nil || !ok {
		u = store.User{ID: sess.UserID, Username: sess.Username, Role: sess.Role, DisplayName: sess.Username}
	}

	tokens, _ := s.db.st.ListUserTokens(ctx, sess.UserID)
	sessions, _ := s.db.st.ListUserSessions(ctx, sess.UserID)

	currentHash := ""
	if c, err := r.Cookie(sessionCookie); err == nil {
		currentHash = hashKey(c.Value)
	}

	s.render(w, "account", sess, pageData{
		Title:  "My account",
		Active: "account",
		Body: accountPageBody{
			User:               u,
			Tokens:             tokens,
			Sessions:           sessions,
			CurrentSessionHash: currentHash,
		},
	})
}

func (s *Server) handleAccountPasswordForm(w http.ResponseWriter, r *http.Request, sess *Session) {
	_ = r.ParseForm()
	curPw := r.PostForm.Get("current_password")
	newPw := r.PostForm.Get("new_password")
	confirmPw := r.PostForm.Get("confirm_password")

	ctx, cancel := dbCtx()
	defer cancel()

	u, ok, err := s.db.st.GetUserByID(ctx, sess.UserID)
	if err != nil || !ok {
		s.renderAccountMsg(w, r, sess, "", "User account not found")
		return
	}

	if bcrypt.CompareHashAndPassword([]byte(u.PasswordHash), []byte(curPw)) != nil {
		s.renderAccountMsg(w, r, sess, "", "Current password is incorrect")
		return
	}

	if newPw != confirmPw {
		s.renderAccountMsg(w, r, sess, "", "New passwords do not match")
		return
	}

	if err := ValidatePassword(u.Username, newPw); err != nil {
		s.renderAccountMsg(w, r, sess, "", err.Error())
		return
	}

	h, err := HashPassword(newPw)
	if err != nil {
		s.renderAccountMsg(w, r, sess, "", err.Error())
		return
	}

	if err := s.db.st.SetUserPassword(ctx, u.ID, h, false); err != nil {
		s.renderAccountMsg(w, r, sess, "", "Failed to update password: "+err.Error())
		return
	}

	// Password change: log out everywhere else
	currentHash := ""
	if c, err := r.Cookie(sessionCookie); err == nil {
		currentHash = hashKey(c.Value)
	}
	_ = s.db.st.PurgeOtherUserSessions(ctx, u.ID, currentHash)

	_ = s.audit.Log(AuditEntry{
		IP:     s.clientIP(r),
		User:   sess.Username,
		Action: "account.change_password",
		Result: "ok",
	})

	s.renderAccountMsg(w, r, sess, "Password updated successfully. Other active sessions were signed out.", "")
}

func (s *Server) handleAccountTokenCreateForm(w http.ResponseWriter, r *http.Request, sess *Session) {
	_ = r.ParseForm()
	name := strings.TrimSpace(r.PostForm.Get("name"))
	if name == "" {
		name = "Personal API token"
	}

	tok := NewAPIToken()
	tokHash := HashAPIToken(tok)
	prefix := tok[:min(len(tok), 12)]

	ctx, cancel := dbCtx()
	defer cancel()

	_, err := s.db.st.CreateUserToken(ctx, store.UserToken{
		UserID:      sess.UserID,
		Name:        name,
		TokenHash:   tokHash,
		TokenPrefix: prefix,
	})
	if err != nil {
		s.renderAccountMsg(w, r, sess, "", "Failed to create token: "+err.Error())
		return
	}

	_ = s.audit.Log(AuditEntry{
		IP:     s.clientIP(r),
		User:   sess.Username,
		Action: "account.token_create",
		Result: "ok",
		Detail: map[string]any{"token_name": name, "prefix": prefix},
	})

	u, _, _ := s.db.st.GetUserByID(ctx, sess.UserID)
	tokens, _ := s.db.st.ListUserTokens(ctx, sess.UserID)
	sessions, _ := s.db.st.ListUserSessions(ctx, sess.UserID)
	currentHash := ""
	if c, err := r.Cookie(sessionCookie); err == nil {
		currentHash = hashKey(c.Value)
	}

	s.render(w, "account", sess, pageData{
		Title:  "My account",
		Active: "account",
		Body: accountPageBody{
			User:               u,
			Tokens:             tokens,
			Sessions:           sessions,
			CurrentSessionHash: currentHash,
			CreatedToken:       tok,
			Message:            "New API token created successfully. Store it safely.",
		},
	})
}

func (s *Server) handleAccountTokenRevokeForm(w http.ResponseWriter, r *http.Request, sess *Session) {
	_ = r.ParseForm()
	tokenIDStr := r.PostForm.Get("token_id")
	tokenID, _ := strconv.ParseInt(tokenIDStr, 10, 64)

	ctx, cancel := dbCtx()
	defer cancel()

	if err := s.db.st.RevokeUserToken(ctx, tokenID, sess.UserID); err != nil {
		s.renderAccountMsg(w, r, sess, "", "Failed to revoke token: "+err.Error())
		return
	}

	_ = s.audit.Log(AuditEntry{
		IP:     s.clientIP(r),
		User:   sess.Username,
		Action: "account.token_revoke",
		Result: "ok",
		Detail: map[string]any{"token_id": tokenID},
	})

	s.renderAccountMsg(w, r, sess, "API token has been revoked.", "")
}

func (s *Server) handleAccountSignoutOthersForm(w http.ResponseWriter, r *http.Request, sess *Session) {
	currentHash := ""
	if c, err := r.Cookie(sessionCookie); err == nil {
		currentHash = hashKey(c.Value)
	}

	ctx, cancel := dbCtx()
	defer cancel()

	if err := s.db.st.PurgeOtherUserSessions(ctx, sess.UserID, currentHash); err != nil {
		s.renderAccountMsg(w, r, sess, "", "Failed to sign out other sessions: "+err.Error())
		return
	}

	_ = s.audit.Log(AuditEntry{
		IP:     s.clientIP(r),
		User:   sess.Username,
		Action: "account.signout_others",
		Result: "ok",
	})

	s.renderAccountMsg(w, r, sess, "Signed out of all other sessions.", "")
}

func (s *Server) renderAccountMsg(w http.ResponseWriter, r *http.Request, sess *Session, msg, errMsg string) {
	ctx, cancel := dbCtx()
	defer cancel()

	u, _, _ := s.db.st.GetUserByID(ctx, sess.UserID)
	tokens, _ := s.db.st.ListUserTokens(ctx, sess.UserID)
	sessions, _ := s.db.st.ListUserSessions(ctx, sess.UserID)
	currentHash := ""
	if c, err := r.Cookie(sessionCookie); err == nil {
		currentHash = hashKey(c.Value)
	}

	s.render(w, "account", sess, pageData{
		Title:  "My account",
		Active: "account",
		Body: accountPageBody{
			User:               u,
			Tokens:             tokens,
			Sessions:           sessions,
			CurrentSessionHash: currentHash,
			Message:            msg,
			Error:              errMsg,
		},
	})
}

// ---- REST API Handlers for My Account ----

func (s *Server) apiAccountGet(w http.ResponseWriter, r *http.Request, sess *Session) {
	ctx, cancel := dbCtx()
	defer cancel()

	u, ok, err := s.db.st.GetUserByID(ctx, sess.UserID)
	if err != nil {
		writeErr(w, http.StatusInternalServerError, err.Error())
		return
	}
	if !ok {
		u = store.User{ID: sess.UserID, Username: sess.Username, Role: sess.Role}
	}
	writeJSON(w, http.StatusOK, map[string]any{"user": u})
}

func (s *Server) apiAccountChangePassword(w http.ResponseWriter, r *http.Request, sess *Session) {
	var body struct {
		CurrentPassword string `json:"current_password"`
		NewPassword     string `json:"new_password"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		writeErr(w, http.StatusBadRequest, "invalid request body")
		return
	}

	ctx, cancel := dbCtx()
	defer cancel()

	u, ok, err := s.db.st.GetUserByID(ctx, sess.UserID)
	if err != nil || !ok {
		writeErr(w, http.StatusNotFound, "user not found")
		return
	}

	if bcrypt.CompareHashAndPassword([]byte(u.PasswordHash), []byte(body.CurrentPassword)) != nil {
		writeErr(w, http.StatusUnauthorized, "current password incorrect")
		return
	}

	if err := ValidatePassword(u.Username, body.NewPassword); err != nil {
		writeErr(w, http.StatusBadRequest, err.Error())
		return
	}

	h, err := HashPassword(body.NewPassword)
	if err != nil {
		writeErr(w, http.StatusBadRequest, err.Error())
		return
	}

	if err := s.db.st.SetUserPassword(ctx, u.ID, h, false); err != nil {
		writeErr(w, http.StatusInternalServerError, err.Error())
		return
	}

	currentHash := ""
	if c, err := r.Cookie(sessionCookie); err == nil {
		currentHash = hashKey(c.Value)
	}
	_ = s.db.st.PurgeOtherUserSessions(ctx, u.ID, currentHash)

	_ = s.audit.Log(AuditEntry{
		IP:     s.clientIP(r),
		User:   sess.Username,
		Action: "account.change_password",
		Result: "ok",
	})

	writeJSON(w, http.StatusOK, map[string]any{"ok": true, "message": "password updated"})
}

func (s *Server) apiAccountTokensList(w http.ResponseWriter, r *http.Request, sess *Session) {
	ctx, cancel := dbCtx()
	defer cancel()

	tokens, err := s.db.st.ListUserTokens(ctx, sess.UserID)
	if err != nil {
		writeErr(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"tokens": tokens})
}

func (s *Server) apiAccountTokenCreate(w http.ResponseWriter, r *http.Request, sess *Session) {
	var body struct {
		Name string `json:"name"`
	}
	_ = json.NewDecoder(r.Body).Decode(&body)
	name := strings.TrimSpace(body.Name)
	if name == "" {
		name = "Personal API token"
	}

	tok := NewAPIToken()
	tokHash := HashAPIToken(tok)
	prefix := tok[:min(len(tok), 12)]

	ctx, cancel := dbCtx()
	defer cancel()

	created, err := s.db.st.CreateUserToken(ctx, store.UserToken{
		UserID:      sess.UserID,
		Name:        name,
		TokenHash:   tokHash,
		TokenPrefix: prefix,
	})
	if err != nil {
		writeErr(w, http.StatusInternalServerError, err.Error())
		return
	}

	_ = s.audit.Log(AuditEntry{
		IP:     s.clientIP(r),
		User:   sess.Username,
		Action: "account.token_create",
		Result: "ok",
		Detail: map[string]any{"token_name": name, "prefix": prefix},
	})

	writeJSON(w, http.StatusCreated, map[string]any{
		"token":        created,
		"secret_token": tok,
	})
}

func (s *Server) apiAccountTokenRevoke(w http.ResponseWriter, r *http.Request, sess *Session) {
	idStr := r.PathValue("id")
	id, _ := strconv.ParseInt(idStr, 10, 64)

	ctx, cancel := dbCtx()
	defer cancel()

	if err := s.db.st.RevokeUserToken(ctx, id, sess.UserID); err != nil {
		writeErr(w, http.StatusInternalServerError, err.Error())
		return
	}

	_ = s.audit.Log(AuditEntry{
		IP:     s.clientIP(r),
		User:   sess.Username,
		Action: "account.token_revoke",
		Result: "ok",
		Detail: map[string]any{"token_id": id},
	})

	writeJSON(w, http.StatusOK, map[string]any{"ok": true})
}

func (s *Server) apiAccountSessionsList(w http.ResponseWriter, r *http.Request, sess *Session) {
	ctx, cancel := dbCtx()
	defer cancel()

	sessions, err := s.db.st.ListUserSessions(ctx, sess.UserID)
	if err != nil {
		writeErr(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"sessions": sessions})
}

func (s *Server) apiAccountSignoutOthers(w http.ResponseWriter, r *http.Request, sess *Session) {
	currentHash := ""
	if c, err := r.Cookie(sessionCookie); err == nil {
		currentHash = hashKey(c.Value)
	}

	ctx, cancel := dbCtx()
	defer cancel()

	if err := s.db.st.PurgeOtherUserSessions(ctx, sess.UserID, currentHash); err != nil {
		writeErr(w, http.StatusInternalServerError, err.Error())
		return
	}

	_ = s.audit.Log(AuditEntry{
		IP:     s.clientIP(r),
		User:   sess.Username,
		Action: "account.signout_others",
		Result: "ok",
	})

	writeJSON(w, http.StatusOK, map[string]any{"ok": true})
}
