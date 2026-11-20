package web

import (
	"net/http"
	"net/url"
	"strings"
	"testing"

	"strand/internal/web/store"
)

// Invariant: Every key present in messagesVi MUST exist in messagesEn.
// If an engineer adds a Vietnamese translation key without English counterpart, the test fails.
func TestI18nAllViKeysInEn(t *testing.T) {
	for k := range messagesVi {
		if _, ok := messagesEn[k]; !ok {
			t.Errorf("messagesVi contains key %q which is missing from messagesEn", k)
		}
	}
}

// Invariant: Missing Vietnamese key must fall back to English.
func TestI18nFallbackToEnglish(t *testing.T) {
	// A hypothetical key in messagesEn
	messagesEn["test.fallback_key"] = "Fallback English Text"
	delete(messagesVi, "test.fallback_key")
	defer delete(messagesEn, "test.fallback_key")

	got := T(LangVI, "test.fallback_key")
	if got != "Fallback English Text" {
		t.Fatalf("expected fallback to English 'Fallback English Text', got %q", got)
	}

	// Unknown key returns itself
	unknown := T(LangVI, "non.existent.random.key")
	if unknown != "non.existent.random.key" {
		t.Fatalf("expected non.existent.random.key, got %q", unknown)
	}
}

func TestI18nTranslationsNonEmpty(t *testing.T) {
	for k, v := range messagesEn {
		if v == "" {
			t.Errorf("messagesEn key %q is empty", k)
		}
	}
	for k, v := range messagesVi {
		if v == "" {
			t.Errorf("messagesVi key %q is empty", k)
		}
	}
}

func TestUserLanguagePersistenceAcrossRestart(t *testing.T) {
	st := store.NewMem()
	e := dbEnv(t, st, nil, nil)
	c, csrf := e.loginOK()

	// Set language to Vietnamese via /account/preferences
	form := url.Values{"language": {LangVI}}
	resp, _ := e.do(c, "POST", "/account/preferences", form.Encode(), map[string]string{
		"Content-Type": "application/x-www-form-urlencoded",
		"X-CSRF-Token": csrf,
	})
	if resp.StatusCode != http.StatusFound {
		t.Fatalf("expected 302 from preferences form, got %d", resp.StatusCode)
	}

	// Verify on initial server that language is vi
	_, b1 := e.do(c, "GET", "/account", "", nil)
	if !strings.Contains(b1, `<html lang="vi">`) {
		t.Fatalf("expected <html lang=\"vi\"> on initial server, got html: %s", b1[:min(len(b1), 300)])
	}

	// Now simulate server restart: create a new Server e2 over the EXACT same store st
	e2 := dbEnv(t, st, nil, nil)
	c2, _ := e2.loginOK()
	resp2, body2 := e2.do(c2, "GET", "/account", "", nil)
	if resp2.StatusCode != 200 {
		t.Fatalf("expected 200 from /account, got %d", resp2.StatusCode)
	}
	if !strings.Contains(body2, `<html lang="vi">`) {
		t.Fatalf("expected <html lang=\"vi\"> after restart, got html: %s", body2[:min(len(body2), 300)])
	}
}
