package runner

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/smtp"
	"strings"
	"time"
)

// Notification environment. Destinations are secrets (webhook URLs, SMTP password) and are therefore
// read from the environment only, never stored in the catalog or shown in the UI.
const (
	EnvNotifyWebhook  = "STRAND_BACKUP_NOTIFY_WEBHOOK"       // generic JSON POST
	EnvNotifySlack    = "STRAND_BACKUP_NOTIFY_SLACK_WEBHOOK" // Slack/Mattermost/Teams-compatible {"text":...}
	EnvNotifyTeams    = "STRAND_BACKUP_NOTIFY_TEAMS_WEBHOOK" // Microsoft Teams incoming webhook (MessageCard)
	EnvNotifySMTPAddr = "STRAND_BACKUP_NOTIFY_SMTP_ADDR"     // host:port (STARTTLS required unless loopback)
	EnvNotifySMTPFrom = "STRAND_BACKUP_NOTIFY_SMTP_FROM"
	EnvNotifySMTPTo   = "STRAND_BACKUP_NOTIFY_SMTP_TO" // comma separated
	EnvNotifySMTPUser = "STRAND_BACKUP_NOTIFY_SMTP_USER"
	EnvNotifySMTPPass = "STRAND_BACKUP_NOTIFY_SMTP_PASSWORD"
	EnvNotifyOn       = "STRAND_BACKUP_NOTIFY_ON" // events: failure,success,missed,verify_failed (default: failure,missed,verify_failed,rpo_breach,repo_unreachable)
)

// Event is one backup notification.
type Event struct {
	Kind    string    `json:"event"` // failure | success | missed | verify_failed | rpo_breach | repo_unreachable | test
	Job     string    `json:"job,omitempty"`
	JobName string    `json:"job_name,omitempty"`
	Source  string    `json:"source,omitempty"`
	Message string    `json:"message"`
	At      time.Time `json:"at"`
}

// Notifier delivers events to the configured destinations.
type Notifier struct {
	Getenv Getenv
	Client *http.Client
	// SendMail is smtp.SendMail (replaceable in tests).
	SendMail func(addr string, a smtp.Auth, from string, to []string, msg []byte) error
	// Force delivers every event regardless of STRAND_BACKUP_NOTIFY_ON (a channel "send test").
	Force bool
}

// Enabled reports whether events of kind are delivered.
func (n *Notifier) Enabled(kind string) bool {
	if n.Force {
		return true
	}
	on := strings.TrimSpace(n.Getenv(EnvNotifyOn))
	if on == "" {
		on = "failure,missed,verify_failed,rpo_breach,repo_unreachable"
	}
	for _, k := range strings.Split(on, ",") {
		if strings.TrimSpace(k) == kind || strings.TrimSpace(k) == "all" {
			return true
		}
	}
	return false
}

// Configured reports whether any destination is set.
func (n *Notifier) Configured() bool {
	return n.Getenv(EnvNotifyWebhook) != "" || n.Getenv(EnvNotifySlack) != "" || n.Getenv(EnvNotifyTeams) != "" || n.Getenv(EnvNotifySMTPAddr) != ""
}

// Send delivers ev to every configured destination; errors never contain the destination URLs.
func (n *Notifier) Send(ctx context.Context, ev Event) error {
	if !n.Enabled(ev.Kind) {
		return nil
	}
	client := n.Client
	if client == nil {
		client = &http.Client{Timeout: 15 * time.Second, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
	}
	var errs []error
	post := func(label, url string, body any) {
		b, _ := json.Marshal(body)
		req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(b))
		if err != nil {
			errs = append(errs, fmt.Errorf("%s notification: invalid URL", label))
			return
		}
		req.Header.Set("Content-Type", "application/json")
		resp, err := client.Do(req)
		if err != nil {
			errs = append(errs, fmt.Errorf("%s notification: request failed", label))
			return
		}
		resp.Body.Close()
		if resp.StatusCode >= 300 {
			errs = append(errs, fmt.Errorf("%s notification: HTTP %d", label, resp.StatusCode))
		}
	}
	text := fmt.Sprintf("[strand backup] %s: %s", strings.ToUpper(ev.Kind), ev.Message)
	if u := n.Getenv(EnvNotifyWebhook); u != "" {
		post("webhook", u, ev)
	}
	if u := n.Getenv(EnvNotifySlack); u != "" {
		post("slack", u, map[string]string{"text": text})
	}
	if u := n.Getenv(EnvNotifyTeams); u != "" {
		post("teams", u, map[string]string{"@type": "MessageCard", "@context": "https://schema.org/extensions", "summary": text,
			"title": "Strand backup", "text": text})
	}
	if addr := n.Getenv(EnvNotifySMTPAddr); addr != "" {
		if err := n.mail(addr, ev, text); err != nil {
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}

func (n *Notifier) mail(addr string, ev Event, subject string) error {
	from, to := n.Getenv(EnvNotifySMTPFrom), strings.Split(n.Getenv(EnvNotifySMTPTo), ",")
	if from == "" || len(to) == 0 || strings.TrimSpace(to[0]) == "" {
		return errors.New("email notification: STRAND_BACKUP_NOTIFY_SMTP_FROM and _TO are required")
	}
	for i := range to {
		to[i] = strings.TrimSpace(to[i])
	}
	for _, h := range append([]string{from, subject}, to...) {
		if strings.ContainsAny(h, "\r\n") {
			return errors.New("email notification: header injection refused")
		}
	}
	var auth smtp.Auth
	if u := n.Getenv(EnvNotifySMTPUser); u != "" {
		host, _, _ := net.SplitHostPort(addr)
		auth = smtp.PlainAuth("", u, n.Getenv(EnvNotifySMTPPass), host) // refuses plaintext except to localhost
	}
	body, _ := json.MarshalIndent(ev, "", "  ")
	msg := "From: " + from + "\r\nTo: " + strings.Join(to, ", ") + "\r\nSubject: " + subject +
		"\r\nContent-Type: text/plain; charset=utf-8\r\n\r\n" + ev.Message + "\r\n\r\n" + string(body) + "\r\n"
	send := n.SendMail
	if send == nil {
		send = smtp.SendMail
	}
	if err := send(addr, auth, from, to, []byte(msg)); err != nil {
		return fmt.Errorf("email notification: %v", redactAll(err.Error(), []string{n.Getenv(EnvNotifySMTPPass)}))
	}
	return nil
}
