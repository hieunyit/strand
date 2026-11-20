package web

import (
	"testing"
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
