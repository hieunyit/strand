package web

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"net/http"
	"os"
	"strings"
	"time"

	"strand/internal/backup"
	"strand/internal/backup/runner"
)

// Notification channels and rules (webhook | slack | teams | email). The destination URL (or SMTP password) is
// sealed with the Web secret key (AAD = channel id) and write-only: responses carry secret_set only. The
// STRAND_BACKUP_NOTIFY_* environment stays a read-only channel "env" ("from environment") that follows
// STRAND_BACKUP_NOTIFY_ON; stored channels are driven by rules (event x scope -> channel).
// Reading channels and rules is admin only (they reveal where alerts go); writing is admin (fail-closed
// default); "send test" is admin too: it makes the server contact an external destination, so it stays with
// the people allowed to configure it.

func (s *Server) backupNotifyRoutes(mux *http.ServeMux) {
	mux.Handle("GET /api/v1/backup-notify-channels", s.auth(true, s.apiChannelList))
	mux.Handle("POST /api/v1/backup-notify-channels", s.auth(true, s.apiChannelSave(false)))
	mux.Handle("GET /api/v1/backup-notify-channels/{id}", s.auth(true, s.apiChannelGet))
	mux.Handle("PUT /api/v1/backup-notify-channels/{id}", s.auth(true, s.apiChannelSave(true)))
	mux.Handle("DELETE /api/v1/backup-notify-channels/{id}", s.auth(true, s.apiChannelDelete))
	mux.Handle("POST /api/v1/backup-notify-channels/{id}/test", s.auth(true, s.apiChannelTest))
	mux.Handle("GET /api/v1/backup-notify-rules", s.auth(true, s.apiRuleList))
	mux.Handle("POST /api/v1/backup-notify-rules", s.auth(true, s.apiRuleCreate))
	mux.Handle("PUT /api/v1/backup-notify-rules/{id}", s.auth(true, s.apiRuleUpdate))
	mux.Handle("DELETE /api/v1/backup-notify-rules/{id}", s.auth(true, s.apiRuleDelete))
}

type channelRequest struct {
	ID       string   `json:"id"`
	Name     string   `json:"name"`
	Kind     string   `json:"kind"`
	Enabled  *bool    `json:"enabled"`
	URL      *string  `json:"url"`      // webhook | slack | teams destination (write-only)
	Password *string  `json:"password"` // email: SMTP password (write-only)
	SMTPAddr string   `json:"smtp_addr"`
	From     string   `json:"from"`
	To       []string `json:"to"`
	User     string   `json:"user"`
}

type channelView struct {
	backup.NotifyChannel
	// Destinations / Events: only for the environment channel.
	Destinations []string `json:"destinations,omitempty"`
	Events       []string `json:"events,omitempty"`
}

// envChannel describes STRAND_BACKUP_NOTIFY_* as a read-only channel; ok is false when none is configured.
func (s *Server) envChannel() (channelView, bool) {
	n := &runner.Notifier{Getenv: os.Getenv}
	var dest []string
	for _, d := range []struct{ kind, env string }{{"webhook", runner.EnvNotifyWebhook}, {"slack", runner.EnvNotifySlack},
		{"teams", runner.EnvNotifyTeams}, {"email", runner.EnvNotifySMTPAddr}} {
		if os.Getenv(d.env) != "" {
			dest = append(dest, d.kind)
		}
	}
	if len(dest) == 0 {
		return channelView{}, false
	}
	var events []string
	for _, e := range backup.NotifyEvents {
		if n.Enabled(e) {
			events = append(events, e)
		}
	}
	return channelView{NotifyChannel: backup.NotifyChannel{ID: backup.EnvChannelID, Name: "From environment", Kind: "environment", Enabled: true,
		Source: "environment", ReadOnly: true}, Destinations: dest, Events: events}, true
}

func randomID(prefix string) (string, error) {
	var b [10]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", err
	}
	return prefix + "-" + hex.EncodeToString(b[:]), nil
}

func (s *Server) apiChannelList(w http.ResponseWriter, r *http.Request, _ *Session) {
	if !s.needDB(w) {
		return
	}
	cs, err := s.db.st.ListNotifyChannels(r.Context())
	if err != nil {
		writeErr(w, http.StatusServiceUnavailable, "could not read notification channels")
		return
	}
	rules, _ := s.db.st.ListNotifyRules(r.Context())
	out := []channelView{}
	if env, ok := s.envChannel(); ok {
		out = append(out, env)
	}
	for _, c := range cs {
		out = append(out, channelView{NotifyChannel: c})
	}
	if rules == nil {
		rules = []backup.NotifyRule{}
	}
	writeJSON(w, http.StatusOK, map[string]any{"channels": out, "rules": rules, "events": backup.NotifyEvents})
}

func (s *Server) apiChannelGet(w http.ResponseWriter, r *http.Request, _ *Session) {
	if !s.needDB(w) {
		return
	}
	id := r.PathValue("id")
	if id == backup.EnvChannelID {
		if env, ok := s.envChannel(); ok {
			writeJSON(w, http.StatusOK, map[string]any{"channel": env, "rules": []backup.NotifyRule{}})
			return
		}
		writeErr(w, http.StatusNotFound, "no STRAND_BACKUP_NOTIFY_* destination is configured")
		return
	}
	c, ok, err := s.db.st.GetNotifyChannel(r.Context(), id)
	if err != nil {
		writeErr(w, http.StatusServiceUnavailable, "could not read the notification channel")
		return
	}
	if !ok {
		writeErr(w, http.StatusNotFound, "notification channel not found")
		return
	}
	var mine []backup.NotifyRule
	all, _ := s.db.st.ListNotifyRules(r.Context())
	for _, ru := range all {
		if ru.ChannelID == id {
			mine = append(mine, ru)
		}
	}
	if mine == nil {
		mine = []backup.NotifyRule{}
	}
	writeJSON(w, http.StatusOK, map[string]any{"channel": channelView{NotifyChannel: c}, "rules": mine})
}

func (s *Server) channelFromRequest(req channelRequest, cur *backup.NotifyChannel) (backup.NotifyChannel, error) {
	c := backup.NotifyChannel{ID: req.ID, Name: req.Name, Kind: req.Kind, Enabled: true, SMTPAddr: req.SMTPAddr, From: req.From, To: req.To, User: req.User}
	if req.Enabled != nil {
		c.Enabled = *req.Enabled
	} else if cur != nil {
		c.Enabled = cur.Enabled
	}
	if cur != nil {
		c.ID = cur.ID
	}
	isURL := c.Kind == backup.ChannelWebhook || c.Kind == backup.ChannelSlack || c.Kind == backup.ChannelTeams
	if isURL && (req.Password != nil || len(req.To) > 0 || req.SMTPAddr != "" || req.From != "" || req.User != "") {
		return c, errors.New("email settings are only valid for kind email")
	}
	if !isURL && c.Kind == backup.ChannelEmail && req.URL != nil {
		return c, errors.New("url is only valid for webhook, slack and teams channels")
	}
	secret, set := req.URL, req.URL != nil
	if c.Kind == backup.ChannelEmail {
		secret, set = req.Password, req.Password != nil
	}
	// Keep the stored secret on an update that does not carry a new one, when the kind still uses it.
	if cur != nil && !set && cur.Kind == c.Kind {
		c.SecretSealed = cur.SecretSealed
	}
	if set {
		if *secret == "" {
			c.SecretSealed = nil
		} else {
			if isURL && !backup.ValidWebhookURL(*secret) {
				return c, errors.New("url must be an http(s) URL")
			}
			if len(*secret) > 2048 || strings.ContainsAny(*secret, "\x00\r\n") {
				return c, errors.New("secret is too long or contains a control character")
			}
			sealed, err := s.db.box.Seal([]byte(*secret), c.ID)
			if err != nil {
				return c, errors.New("could not seal the secret")
			}
			c.SecretSealed = sealed
		}
	}
	c.SecretSet = len(c.SecretSealed) > 0
	return c, nil
}

func channelChanges(old *backup.NotifyChannel, nw backup.NotifyChannel) map[string]any {
	if old == nil {
		out := map[string]any{"name": nw.Name, "kind": nw.Kind, "enabled": nw.Enabled}
		if nw.SecretSet {
			out["secret"] = "set"
		}
		return out
	}
	out := changedFields(old, nw)
	delete(out, "secret_set")
	if v := sealedChange(old.SecretSealed, nw.SecretSealed); v != "" {
		out["secret"] = v
	}
	return out
}

func (s *Server) apiChannelSave(update bool) authedHandler {
	return func(w http.ResponseWriter, r *http.Request, sess *Session) {
		if !s.needDB(w) {
			return
		}
		var req channelRequest
		if !decodeStrict(w, r, &req) {
			return
		}
		var cur *backup.NotifyChannel
		if update {
			req.ID = r.PathValue("id")
			if req.ID == backup.EnvChannelID {
				writeErr(w, http.StatusConflict, "the environment channel is read-only (STRAND_BACKUP_NOTIFY_*)")
				return
			}
			c, ok, err := s.db.st.GetNotifyChannel(r.Context(), req.ID)
			if err != nil {
				writeErr(w, http.StatusServiceUnavailable, "could not read the notification channel")
				return
			}
			if !ok {
				writeErr(w, http.StatusNotFound, "notification channel not found")
				return
			}
			cur = &c
		} else if req.ID == "" {
			id, err := randomID("chan")
			if err != nil {
				writeErr(w, http.StatusInternalServerError, "could not create a channel id")
				return
			}
			req.ID = id
		}
		if req.ID == backup.EnvChannelID {
			writeErr(w, http.StatusBadRequest, "id env is reserved for the environment channel")
			return
		}
		c, err := s.channelFromRequest(req, cur)
		if err != nil {
			writeErr(w, http.StatusBadRequest, err.Error())
			return
		}
		if err := c.Validate(); err != nil {
			writeErr(w, http.StatusBadRequest, err.Error())
			return
		}
		action := "backup_notify_channel.create"
		if update {
			action = "backup_notify_channel.update"
		}
		if !s.auditBackupOp(w, r, action, map[string]any{"channel": c.ID, "changes": channelChanges(cur, c)}) {
			return
		}
		if update {
			ok, err := s.db.st.UpdateNotifyChannel(r.Context(), c)
			switch {
			case errors.Is(err, backup.ErrConflict):
				writeErr(w, http.StatusConflict, "a channel with this name already exists")
			case errors.Is(err, backup.ErrInvalid):
				writeErr(w, http.StatusBadRequest, err.Error())
			case err != nil:
				writeErr(w, http.StatusInternalServerError, "could not update the notification channel")
			case !ok:
				writeErr(w, http.StatusNotFound, "notification channel not found")
			default:
				got, _, _ := s.db.st.GetNotifyChannel(r.Context(), c.ID)
				writeJSON(w, http.StatusOK, map[string]any{"channel": channelView{NotifyChannel: got}})
			}
			return
		}
		c.CreatedBy = sess.Username
		got, err := s.db.st.CreateNotifyChannel(r.Context(), c)
		switch {
		case errors.Is(err, backup.ErrConflict):
			writeErr(w, http.StatusConflict, "a channel with this id or name already exists")
		case errors.Is(err, backup.ErrInvalid):
			writeErr(w, http.StatusBadRequest, err.Error())
		case err != nil:
			writeErr(w, http.StatusInternalServerError, "could not create the notification channel")
		default:
			writeJSON(w, http.StatusCreated, map[string]any{"channel": channelView{NotifyChannel: got}})
		}
	}
}

func (s *Server) apiChannelDelete(w http.ResponseWriter, r *http.Request, _ *Session) {
	if !s.needDB(w) {
		return
	}
	id := r.PathValue("id")
	if id == backup.EnvChannelID {
		writeErr(w, http.StatusConflict, "the environment channel is read-only (STRAND_BACKUP_NOTIFY_*)")
		return
	}
	if !s.auditBackupOp(w, r, "backup_notify_channel.delete", map[string]any{"channel": id}) {
		return
	}
	ok, err := s.db.st.DeleteNotifyChannel(r.Context(), id)
	switch {
	case err != nil:
		writeErr(w, http.StatusInternalServerError, "could not delete the notification channel")
	case !ok:
		writeErr(w, http.StatusNotFound, "notification channel not found")
	default:
		w.WriteHeader(http.StatusNoContent)
	}
}

// ---- rules ----

type ruleRequest struct {
	ID        string `json:"id"`
	Event     string `json:"event"`
	ScopeKind string `json:"scope_kind"`
	ScopeID   string `json:"scope_id"`
	ChannelID string `json:"channel_id"`
	Enabled   *bool  `json:"enabled"`
}

func (s *Server) apiRuleList(w http.ResponseWriter, r *http.Request, _ *Session) {
	if !s.needDB(w) {
		return
	}
	rules, err := s.db.st.ListNotifyRules(r.Context())
	if err != nil {
		writeErr(w, http.StatusServiceUnavailable, "could not read notification rules")
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"rules": rules, "events": backup.NotifyEvents})
}

// scopeExists checks that a policy/job scope names something that exists.
func (s *Server) scopeExists(ctx context.Context, kind, id string) bool {
	switch kind {
	case backup.ScopePolicy:
		_, ok, _ := s.db.st.GetBackupPolicy(ctx, id)
		return ok
	case backup.ScopeJob:
		_, ok, _ := s.db.st.GetBackupJob(ctx, id)
		return ok
	}
	return true
}

func (s *Server) apiRuleCreate(w http.ResponseWriter, r *http.Request, _ *Session) {
	if !s.needDB(w) {
		return
	}
	var req ruleRequest
	if !decodeStrict(w, r, &req) {
		return
	}
	if req.ID == "" {
		id, err := randomID("rule")
		if err != nil {
			writeErr(w, http.StatusInternalServerError, "could not create a rule id")
			return
		}
		req.ID = id
	}
	rule := backup.NotifyRule{ID: req.ID, Event: req.Event, ScopeKind: req.ScopeKind, ScopeID: req.ScopeID, ChannelID: req.ChannelID, Enabled: true}
	if req.ScopeKind == "" {
		rule.ScopeKind = backup.ScopeAll
	}
	if req.Enabled != nil {
		rule.Enabled = *req.Enabled
	}
	if err := rule.Validate(); err != nil {
		writeErr(w, http.StatusBadRequest, err.Error())
		return
	}
	if _, ok, _ := s.db.st.GetNotifyChannel(r.Context(), rule.ChannelID); !ok {
		writeErr(w, http.StatusBadRequest, "channel_id does not name a channel")
		return
	}
	if !s.scopeExists(r.Context(), rule.ScopeKind, rule.ScopeID) {
		writeErr(w, http.StatusBadRequest, "scope_id does not name a "+rule.ScopeKind)
		return
	}
	if !s.auditBackupOp(w, r, "backup_notify_rule.create", map[string]any{"rule": rule.ID, "event": rule.Event, "scope_kind": rule.ScopeKind,
		"scope_id": rule.ScopeID, "channel": rule.ChannelID, "enabled": rule.Enabled}) {
		return
	}
	got, err := s.db.st.CreateNotifyRule(r.Context(), rule)
	switch {
	case errors.Is(err, backup.ErrConflict):
		writeErr(w, http.StatusConflict, "an identical rule already exists")
	case errors.Is(err, backup.ErrInvalid):
		writeErr(w, http.StatusBadRequest, "the channel does not exist")
	case err != nil:
		writeErr(w, http.StatusInternalServerError, "could not create the notification rule")
	default:
		writeJSON(w, http.StatusCreated, map[string]any{"rule": got})
	}
}

func (s *Server) apiRuleUpdate(w http.ResponseWriter, r *http.Request, _ *Session) {
	if !s.needDB(w) {
		return
	}
	var req struct {
		Enabled *bool `json:"enabled"`
	}
	if !decodeStrict(w, r, &req) {
		return
	}
	if req.Enabled == nil {
		writeErr(w, http.StatusBadRequest, "enabled is required (a rule's event, scope and channel are fixed: delete and create a new one)")
		return
	}
	id := r.PathValue("id")
	cur, ok, err := s.db.st.GetNotifyRule(r.Context(), id)
	if err != nil {
		writeErr(w, http.StatusServiceUnavailable, "could not read the notification rule")
		return
	}
	if !ok {
		writeErr(w, http.StatusNotFound, "notification rule not found")
		return
	}
	if !s.auditBackupOp(w, r, "backup_notify_rule.update", map[string]any{"rule": id, "changes": map[string]any{"enabled": map[string]any{"from": cur.Enabled, "to": *req.Enabled}}}) {
		return
	}
	if _, err := s.db.st.SetNotifyRuleEnabled(r.Context(), id, *req.Enabled); err != nil {
		writeErr(w, http.StatusInternalServerError, "could not update the notification rule")
		return
	}
	cur.Enabled = *req.Enabled
	writeJSON(w, http.StatusOK, map[string]any{"rule": cur})
}

func (s *Server) apiRuleDelete(w http.ResponseWriter, r *http.Request, _ *Session) {
	if !s.needDB(w) {
		return
	}
	id := r.PathValue("id")
	if !s.auditBackupOp(w, r, "backup_notify_rule.delete", map[string]any{"rule": id}) {
		return
	}
	ok, err := s.db.st.DeleteNotifyRule(r.Context(), id)
	switch {
	case err != nil:
		writeErr(w, http.StatusInternalServerError, "could not delete the notification rule")
	case !ok:
		writeErr(w, http.StatusNotFound, "notification rule not found")
	default:
		w.WriteHeader(http.StatusNoContent)
	}
}

// ---- delivery ----

// channelNotifier builds a Notifier delivering every event to one stored channel only.
func (s *Server) channelNotifier(ch backup.NotifyChannel) (*runner.Notifier, error) {
	secret := ""
	if len(ch.SecretSealed) > 0 {
		b, err := s.db.box.Open(ch.SecretSealed, ch.ID)
		if err != nil {
			return nil, errors.New("the stored secret cannot be decrypted (wrong STRAND_WEB_SECRET_KEY?)")
		}
		secret = string(b)
	}
	env := map[string]string{}
	switch ch.Kind {
	case backup.ChannelWebhook:
		env[runner.EnvNotifyWebhook] = secret
	case backup.ChannelSlack:
		env[runner.EnvNotifySlack] = secret
	case backup.ChannelTeams:
		env[runner.EnvNotifyTeams] = secret
	case backup.ChannelEmail:
		env[runner.EnvNotifySMTPAddr], env[runner.EnvNotifySMTPFrom] = ch.SMTPAddr, ch.From
		env[runner.EnvNotifySMTPTo] = strings.Join(ch.To, ",")
		env[runner.EnvNotifySMTPUser], env[runner.EnvNotifySMTPPass] = ch.User, secret
	}
	return &runner.Notifier{Getenv: func(k string) string { return env[k] }, Client: s.notifyHTTP, SendMail: s.notifyMail, Force: true}, nil
}

// sendEvent delivers ev with a deadline that also covers SMTP (smtp.SendMail ignores contexts).
func sendEvent(ctx context.Context, n *runner.Notifier, ev runner.Event) error {
	done := make(chan error, 1)
	go func() { done <- n.Send(ctx, ev) }()
	select {
	case err := <-done:
		return err
	case <-ctx.Done():
		return errors.New("delivery timed out")
	}
}

type notifyResult struct {
	OK     bool      `json:"ok"`
	Status string    `json:"status"` // ok | failed
	Detail string    `json:"detail"`
	At     time.Time `json:"at"`
}

// apiChannelTest sends a test event to the channel. 200 with the result even when delivery failed.
func (s *Server) apiChannelTest(w http.ResponseWriter, r *http.Request, sess *Session) {
	if !s.needDB(w) {
		return
	}
	id := r.PathValue("id")
	var n *runner.Notifier
	var stored *backup.NotifyChannel
	if id == backup.EnvChannelID {
		if _, ok := s.envChannel(); !ok {
			writeErr(w, http.StatusNotFound, "no STRAND_BACKUP_NOTIFY_* destination is configured")
			return
		}
		n = &runner.Notifier{Getenv: os.Getenv, Client: s.notifyHTTP, SendMail: s.notifyMail, Force: true}
	} else {
		c, ok, err := s.db.st.GetNotifyChannel(r.Context(), id)
		if err != nil {
			writeErr(w, http.StatusServiceUnavailable, "could not read the notification channel")
			return
		}
		if !ok {
			writeErr(w, http.StatusNotFound, "notification channel not found")
			return
		}
		stored = &c
		if n, err = s.channelNotifier(c); err != nil {
			writeErr(w, http.StatusConflict, err.Error())
			return
		}
	}
	if !s.auditBackupOp(w, r, "backup_notify_channel.test", map[string]any{"channel": id}) {
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 25*time.Second)
	defer cancel()
	now := s.now().UTC()
	err := sendEvent(ctx, n, runner.Event{Kind: "test", Message: "This is a test notification from the Strand console (sent by " + sess.Username + ").", At: now})
	res := notifyResult{OK: err == nil, Status: "ok", Detail: "delivered", At: now}
	if err != nil {
		res.Status, res.Detail = "failed", err.Error()
	}
	if stored != nil {
		_, _ = s.db.st.SetNotifyChannelTest(r.Context(), id, now, res.Status, res.Detail)
	}
	writeJSON(w, http.StatusOK, res)
}

// notifyTargets lists the stored channels that an event of job (policy policyID) must reach, by rules.
func (s *Server) notifyTargets(ctx context.Context, event, jobID, policyID string) []backup.NotifyChannel {
	rules, err := s.db.st.ListNotifyRules(ctx)
	if err != nil || len(rules) == 0 {
		return nil
	}
	want := map[string]bool{}
	for _, ru := range rules {
		if ru.Matches(event, jobID, policyID) {
			want[ru.ChannelID] = true
		}
	}
	var out []backup.NotifyChannel
	for id := range want {
		if c, ok, _ := s.db.st.GetNotifyChannel(ctx, id); ok && c.Enabled {
			out = append(out, c)
		}
	}
	return out
}

// notifyConfigured: the environment has a destination or at least one enabled rule exists.
func (s *Server) notifyConfigured(ctx context.Context) bool {
	if (&runner.Notifier{Getenv: os.Getenv}).Configured() {
		return true
	}
	rules, _ := s.db.st.ListNotifyRules(ctx)
	for _, r := range rules {
		if r.Enabled {
			return true
		}
	}
	return false
}

// notifyBackupEvent delivers ev to the environment channel (when STRAND_BACKUP_NOTIFY_ON lists the event) and
// to every stored channel a rule routes it to. Every delivery is audited (backup.notify, without destination).
func (s *Server) notifyBackupEvent(ev runner.Event, policyID string) {
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()
	record := func(channel string, err error) {
		result := "ok"
		detail := map[string]any{"event": ev.Kind, "job": ev.Job, "channel": channel}
		if ev.Source != "" {
			detail["source"] = ev.Source
		}
		if err != nil {
			result, detail["error"] = "error", err.Error()
			s.log.Warn("backup notification", "event", ev.Kind, "channel", channel, "error", err)
		}
		_ = s.audit.Log(AuditEntry{Action: "backup.notify", Result: result, Detail: detail})
	}
	env := &runner.Notifier{Getenv: os.Getenv, Client: s.notifyHTTP, SendMail: s.notifyMail}
	if env.Configured() && env.Enabled(ev.Kind) {
		record(backup.EnvChannelID, sendEvent(ctx, env, ev))
	}
	for _, ch := range s.notifyTargets(ctx, ev.Kind, ev.Job, policyID) {
		n, err := s.channelNotifier(ch)
		if err != nil {
			record(ch.ID, err)
			continue
		}
		record(ch.ID, sendEvent(ctx, n, ev))
	}
}

// jobPolicy returns the policy id of a job ("" when none or unknown).
func (s *Server) jobPolicy(ctx context.Context, jobID string) string {
	if jobID == "" {
		return ""
	}
	if j, ok, _ := s.db.st.GetBackupJob(ctx, jobID); ok {
		return j.PolicyID
	}
	return ""
}
