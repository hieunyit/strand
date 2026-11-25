package runner

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"net/smtp"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestNotifierDestinations(t *testing.T) {
	var mu sync.Mutex
	got := map[string]string{}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		b, _ := io.ReadAll(r.Body)
		mu.Lock()
		got[r.URL.Path] = string(b)
		mu.Unlock()
		if r.URL.Path == "/broken" {
			w.WriteHeader(500)
		}
	}))
	defer srv.Close()
	var mail []byte
	env := map[string]string{EnvNotifyWebhook: srv.URL + "/hook?token=SECRET", EnvNotifySlack: srv.URL + "/slack",
		EnvNotifySMTPAddr: "127.0.0.1:2525", EnvNotifySMTPFrom: "strand@example.com", EnvNotifySMTPTo: "ops@example.com, dba@example.com"}
	n := &Notifier{Getenv: func(k string) string { return env[k] }, SendMail: func(addr string, a smtp.Auth, from string, to []string, msg []byte) error {
		if len(to) != 2 || to[1] != "dba@example.com" {
			t.Fatalf("to = %v", to)
		}
		mail = msg
		return nil
	}}
	ev := Event{Kind: "failure", Job: "bjob-1", Source: "mysql-prod", Message: "mysqldump failed: access denied", At: time.Now()}
	if err := n.Send(context.Background(), ev); err != nil {
		t.Fatal(err)
	}
	var hook Event
	if err := json.Unmarshal([]byte(got["/hook"]), &hook); err != nil || hook.Kind != "failure" || hook.Source != "mysql-prod" {
		t.Fatalf("webhook body %q", got["/hook"])
	}
	if !strings.Contains(got["/slack"], `"text":"[strand backup] FAILURE: mysqldump failed`) {
		t.Fatalf("slack %q", got["/slack"])
	}
	if !strings.Contains(string(mail), "Subject: [strand backup] FAILURE") {
		t.Fatalf("mail %q", mail)
	}
	// Success is off by default.
	got = map[string]string{}
	if err := n.Send(context.Background(), Event{Kind: "success", Message: "ok"}); err != nil || len(got) != 0 {
		t.Fatalf("success delivered by default: %v %v", got, err)
	}
	// Failing destination: error without the URL (it may carry a token).
	env[EnvNotifyWebhook] = srv.URL + "/broken?token=SECRET"
	delete(env, EnvNotifySMTPAddr)
	err := n.Send(context.Background(), ev)
	if err == nil || strings.Contains(err.Error(), "SECRET") || !strings.Contains(err.Error(), "HTTP 500") {
		t.Fatalf("broken webhook err = %v", err)
	}
	// Header injection refused.
	env[EnvNotifySMTPAddr] = "127.0.0.1:2525"
	env[EnvNotifySMTPTo] = "ops@example.com\r\nBcc: evil@example.com"
	delete(env, EnvNotifyWebhook)
	if err := n.Send(context.Background(), ev); err == nil || !strings.Contains(err.Error(), "injection") {
		t.Fatalf("injection: %v", err)
	}
}

func TestNotifierTeamsForceAndDefaultEvents(t *testing.T) {
	var mu sync.Mutex
	var bodies []string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		b, _ := io.ReadAll(r.Body)
		mu.Lock()
		bodies = append(bodies, string(b))
		mu.Unlock()
	}))
	defer srv.Close()
	env := map[string]string{EnvNotifyTeams: srv.URL + "/teams"}
	n := &Notifier{Getenv: func(k string) string { return env[k] }}
	if !n.Configured() {
		t.Fatal("a Teams webhook alone is a destination")
	}
	// Default events include the new rpo_breach and repo_unreachable, but not success or the test event.
	for kind, want := range map[string]bool{"failure": true, "missed": true, "verify_failed": true, "rpo_breach": true, "repo_unreachable": true, "success": false, "test": false} {
		if n.Enabled(kind) != want {
			t.Errorf("Enabled(%q) = %v, want %v", kind, !want, want)
		}
	}
	if err := n.Send(context.Background(), Event{Kind: "success", Message: "ok", At: time.Now()}); err != nil || len(bodies) != 0 {
		t.Fatalf("a disabled event was delivered: %v %v", err, bodies)
	}
	n.Force = true
	if err := n.Send(context.Background(), Event{Kind: "test", Message: "hello", At: time.Now()}); err != nil {
		t.Fatal(err)
	}
	if len(bodies) != 1 || !strings.Contains(bodies[0], `"@type":"MessageCard"`) || !strings.Contains(bodies[0], "TEST: hello") {
		t.Fatalf("teams body = %v", bodies)
	}
}

// verify_failed messages carry a multi-line stderr tail: the email is still sent, with the first line
// (control characters removed) as its subject and the full message in the body.
func TestNotifierMailSubjectIsOneLine(t *testing.T) {
	var mail []byte
	env := map[string]string{EnvNotifySMTPAddr: "127.0.0.1:2525", EnvNotifySMTPFrom: "strand@example.com", EnvNotifySMTPTo: "ops@example.com"}
	n := &Notifier{Getenv: func(k string) string { return env[k] }, SendMail: func(addr string, a smtp.Auth, from string, to []string, msg []byte) error {
		mail = msg
		return nil
	}}
	ev := Event{Kind: "verify_failed", Source: "pg-prod", At: time.Now(),
		Message: "test restore of bkp-1 failed:\x1b[31m pg_restore failed\r\npg_restore: error: could not execute query\nBcc: evil@example.com\n" + strings.Repeat("x", 500)}
	if err := n.Send(context.Background(), ev); err != nil {
		t.Fatal(err)
	}
	head, body, ok := strings.Cut(string(mail), "\r\n\r\n")
	if !ok {
		t.Fatalf("mail %q", mail)
	}
	var subject string
	for _, h := range strings.Split(head, "\r\n") {
		if strings.HasPrefix(h, "Bcc:") {
			t.Fatalf("header injected: %q", head)
		}
		if s, ok := strings.CutPrefix(h, "Subject: "); ok {
			subject = s
		}
	}
	if subject != "[strand backup] VERIFY_FAILED: test restore of bkp-1 failed:[31m pg_restore failed" {
		t.Fatalf("subject = %q", subject)
	}
	if !strings.Contains(body, "could not execute query") || !strings.Contains(body, strings.Repeat("x", 500)) {
		t.Fatalf("body lacks the full message: %q", body)
	}
}
