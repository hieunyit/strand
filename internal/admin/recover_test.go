package admin_test

import (
	"context"
	"strings"
	"testing"
	"time"

	"strand/internal/admin"
)

// panicTopHandler: Top panic (như nil deref khi pipeline chưa dựng xong).
type panicTopHandler struct{ *mockHandler }

func (h panicTopHandler) Top(ctx context.Context) (*admin.TopInfo, error) {
	var p *admin.TopInfo
	_ = p.PipelineID // nil deref
	return p, nil
}

// review R12: panic trong handler trả lỗi cho request đó, server vẫn sống phục vụ request sau.
func TestServerRecoversHandlerPanic(t *testing.T) {
	_, client, cleanup := setupTestServer(t, panicTopHandler{newMockHandler()})
	defer cleanup()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if _, err := client.Top(ctx); err == nil || !strings.Contains(err.Error(), "internal error") {
		t.Fatalf("Top over panicking handler: err = %v, want internal error", err)
	}
	if _, err := client.Pause(ctx); err != nil {
		t.Fatalf("server not serving after a handler panic: %v", err)
	}
}
