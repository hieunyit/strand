package pipeline

import "testing"

// Admin socket mở trước khi pipeline biết có bootstrap hay không: trong khoảng đó trạng thái phải là
// "starting", không được là "synced" (script vận hành / e2e đọc "synced" là đã bắt kịp).
func TestAdminStateStartingBeforeRunning(t *testing.T) {
	p := &Pipeline{}
	h := &adminHandler{p: p}
	if got := h.state(); got != "starting" {
		t.Fatalf("state before running = %q, want starting", got)
	}
	p.bootstrap.Store(true)
	if got := h.state(); got != "starting" {
		t.Fatalf("state before running (bootstrap flagged) = %q, want starting", got)
	}
	p.running.Store(true)
	if got := h.state(); got != "bootstrapping" {
		t.Fatalf("state while bootstrapping = %q, want bootstrapping", got)
	}
	p.bootstrap.Store(false)
	if got := h.state(); got != "synced" {
		t.Fatalf("state after bootstrap = %q, want synced", got)
	}
}
