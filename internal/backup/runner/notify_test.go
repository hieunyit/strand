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
