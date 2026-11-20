package backup

import (
	"fmt"
	"net/mail"
	"net/url"
	"strings"
	"time"
)

// Notification channel kinds.
const (
	ChannelWebhook = "webhook" // generic JSON POST
	ChannelSlack   = "slack"   // Slack/Mattermost incoming webhook {"text": ...}
	ChannelTeams   = "teams"   // Microsoft Teams incoming webhook (MessageCard)
	ChannelEmail   = "email"   // SMTP
)

// EnvChannelID is the id of the read-only channel built from STRAND_BACKUP_NOTIFY_* ("from environment").
const EnvChannelID = "env"

// Notification events (rules match on these).
const (
	EventFailure         = "failure"
	EventMissed          = "missed"
	EventVerifyFailed    = "verify_failed"
	EventSuccess         = "success"
	EventRPOBreach       = "rpo_breach"
	EventRepoUnreachable = "repo_unreachable"
)

// NotifyEvents lists every event a rule can match.
var NotifyEvents = []string{EventFailure, EventMissed, EventVerifyFailed, EventSuccess, EventRPOBreach, EventRepoUnreachable}

// ValidNotifyEvent reports a known event.
func ValidNotifyEvent(e string) bool {
	for _, k := range NotifyEvents {
		if k == e {
			return true
		}
	}
	return false
}

// Rule scopes.
const (
	ScopeAll    = "all"
	ScopePolicy = "policy"
	ScopeJob    = "job"
)

// NotifyChannel is a stored notification destination. The destination URL (webhook/slack/teams) or the SMTP
// password (email) is the secret: sealed with store.Box (AAD = channel id), never serialized.
type NotifyChannel struct {
	ID      string `json:"id"`
	Name    string `json:"name"`
	Kind    string `json:"kind"`
	Enabled bool   `json:"enabled"`
	// Email settings (not secret).
	SMTPAddr string   `json:"smtp_addr,omitempty"`
	From     string   `json:"from,omitempty"`
	To       []string `json:"to,omitempty"`
	User     string   `json:"user,omitempty"`

	SecretSealed []byte `json:"-"`
	SecretSet    bool   `json:"secret_set"`

	// Source: "stored" for database channels, "environment" for the read-only STRAND_BACKUP_NOTIFY_* channel.
	Source         string     `json:"source"`
	ReadOnly       bool       `json:"read_only,omitempty"`
	CreatedBy      string     `json:"created_by,omitempty"`
	CreatedAt      time.Time  `json:"created_at,omitempty"`
	UpdatedAt      time.Time  `json:"updated_at,omitempty"`
	LastTestAt     *time.Time `json:"last_test_at,omitempty"`
	LastTestStatus string     `json:"last_test_status,omitempty"`
	LastTestDetail string     `json:"last_test_detail,omitempty"`
}

// ValidWebhookURL checks a webhook destination: http(s) with a host.
func ValidWebhookURL(s string) bool {
	if s == "" || len(s) > 2048 || hasControl(s) {
		return false
	}
	u, err := url.Parse(s)
	return err == nil && (u.Scheme == "http" || u.Scheme == "https") && u.Host != ""
}

// Validate checks the non-secret definition (the secret is validated by the caller before sealing).
func (c NotifyChannel) Validate() error {
	bad := func(s string) error { return fmt.Errorf("%w: notification channel %s", ErrInvalid, s) }
	if !validID(c.ID) {
		return bad("id is invalid")
	}
	if !safeText(c.Name, 96) {
		return bad("name is required (at most 96 characters)")
	}
	switch c.Kind {
	case ChannelWebhook, ChannelSlack, ChannelTeams:
		if len(c.SecretSealed) == 0 {
			return bad("needs a destination URL")
		}
		if c.SMTPAddr != "" || c.From != "" || len(c.To) > 0 || c.User != "" {
			return bad("email settings are only valid for kind email")
		}
	case ChannelEmail:
		if !safeText(c.SMTPAddr, 255) || strings.ContainsAny(c.SMTPAddr, " /\\") || !strings.Contains(c.SMTPAddr, ":") {
			return bad("smtp_addr must be host:port")
		}
		if _, err := mail.ParseAddress(c.From); err != nil || strings.ContainsAny(c.From, "\r\n") {
			return bad("from is not a valid address")
		}
		if len(c.To) == 0 || len(c.To) > 20 {
			return bad("to needs 1-20 recipients")
		}
		for _, a := range c.To {
			if _, err := mail.ParseAddress(a); err != nil || strings.ContainsAny(a, "\r\n,") {
				return bad("to contains an invalid address")
			}
		}
		if c.User != "" && (len(c.User) > 255 || hasControl(c.User)) {
			return bad("invalid smtp user")
		}
		if c.User == "" && len(c.SecretSealed) > 0 {
			return bad("an SMTP password needs a user")
		}
	default:
		return bad("kind must be webhook, slack, teams or email")
	}
	return nil
}

// NotifyRule routes one event within a scope to a channel.
type NotifyRule struct {
	ID        string    `json:"id"`
	Event     string    `json:"event"`
	ScopeKind string    `json:"scope_kind"` // all | policy | job
	ScopeID   string    `json:"scope_id,omitempty"`
	ChannelID string    `json:"channel_id"`
	Enabled   bool      `json:"enabled"`
	CreatedAt time.Time `json:"created_at,omitempty"`
}

// Validate checks a rule.
func (r NotifyRule) Validate() error {
	bad := func(s string) error { return fmt.Errorf("%w: notification rule %s", ErrInvalid, s) }
	if !validID(r.ID) {
		return bad("id is invalid")
	}
	if !ValidNotifyEvent(r.Event) {
		return bad("event must be one of " + strings.Join(NotifyEvents, ", "))
	}
	switch r.ScopeKind {
	case ScopeAll:
		if r.ScopeID != "" {
			return bad("scope_id must be empty for scope all")
		}
	case ScopePolicy, ScopeJob:
		if !validID(r.ScopeID) {
			return bad("scope_id is required for scope " + r.ScopeKind)
		}
	default:
		return bad("scope_kind must be all, policy or job")
	}
	if r.ChannelID == EnvChannelID {
		return bad("the environment channel is read-only and follows STRAND_BACKUP_NOTIFY_ON; it cannot be a rule target")
	}
	if !validID(r.ChannelID) {
		return bad("channel_id is invalid")
	}
	return nil
}

// Matches reports whether the rule applies to an event of the job (jobID/policyID may be empty).
func (r NotifyRule) Matches(event, jobID, policyID string) bool {
	if !r.Enabled || r.Event != event {
		return false
	}
	switch r.ScopeKind {
	case ScopeAll:
		return true
	case ScopePolicy:
		return policyID != "" && r.ScopeID == policyID
	case ScopeJob:
		return jobID != "" && r.ScopeID == jobID
	}
	return false
}
