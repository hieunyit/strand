package main

// Verify khi pipeline đang giữ khóa nhưng bị `strand pause` (review item 9): trước đây verify coi là
// đang chạy, chờ target apply 2 phút rồi lỗi "target did not apply". Nay hỏi admin socket: state
// "paused" thì báo pipeline_paused ngay; không hỏi được / socket của pipeline khác thì chờ như trước.

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"strings"
	"sync"
	"testing"

	"strand/internal/admin"
	"strand/internal/exitcode"
)

func TestFailIfPausedDecision(t *testing.T) {
	top := func(info *admin.TopInfo, err error) func(context.Context) (*admin.TopInfo, error) {
		return func(context.Context) (*admin.TopInfo, error) { return info, err }
	}
	cases := []struct {
		name   string
		top    func(context.Context) (*admin.TopInfo, error)
		paused bool
	}{
		{"paused", top(&admin.TopInfo{PipelineID: "p1", State: "paused"}, nil), true},
		{"paused without id", top(&admin.TopInfo{State: "paused"}, nil), true},
		{"synced", top(&admin.TopInfo{PipelineID: "p1", State: "synced"}, nil), false},
		{"bootstrapping", top(&admin.TopInfo{PipelineID: "p1", State: "bootstrapping"}, nil), false},
		{"other pipeline paused", top(&admin.TopInfo{PipelineID: "p2", State: "paused"}, nil), false},
		{"socket unreachable", top(nil, errors.New("connect to admin socket: no such file")), false},
		{"no probe", nil, false},
	}
	for _, c := range cases {
		err := failIfPaused(context.Background(), c.top, "p1")
		if !c.paused {
			if err != nil {
				t.Errorf("%s: err = %v, want nil (fall back to waiting)", c.name, err)
			}
			continue
		}
		var xe *exitcode.Error
		if !errors.As(err, &xe) || xe.Code != exitcode.ErrPipelinePaused || xe.ExitCode != exitcode.Fenced {
			t.Fatalf("%s: err = %v, want pipeline_paused exit %d", c.name, err, exitcode.Fenced)
		}
		if !strings.Contains(err.Error(), "pipeline p1 is paused: resume it") {
			t.Errorf("%s: message %q lacks the resume/stop hint", c.name, err.Error())
		}
	}
}

// adminTop qua socket thật (server admin trong process): pipeline pause → failIfPaused báo ngay.
func TestFailIfPausedViaAdminSocket(t *testing.T) {
	sock := t.TempDir() + "/a.sock"
	h := &pausedHandler{}
	h.set("paused")
	srv := admin.NewServer(sock, h, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err := srv.Start(); err != nil {
		t.Fatal(err)
	}
	defer srv.Close()
	err := failIfPaused(context.Background(), adminTop(sock), "p1")
	var xe *exitcode.Error
	if !errors.As(err, &xe) || xe.Code != exitcode.ErrPipelinePaused {
		t.Fatalf("paused via socket: err = %v, want pipeline_paused", err)
	}
	h.set("synced")
	if err := failIfPaused(context.Background(), adminTop(sock), "p1"); err != nil {
		t.Fatalf("synced via socket: err = %v, want nil", err)
	}
	if err := failIfPaused(context.Background(), adminTop(t.TempDir()+"/none.sock"), "p1"); err != nil {
		t.Fatalf("missing socket: err = %v, want nil", err)
	}
}

type pausedHandler struct {
	mu    sync.Mutex
	state string
}

func (h *pausedHandler) set(s string) { h.mu.Lock(); h.state = s; h.mu.Unlock() }

func (h *pausedHandler) Top(context.Context) (*admin.TopInfo, error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	return &admin.TopInfo{PipelineID: "p1", State: h.state}, nil
}
func (h *pausedHandler) Pause(context.Context) (*admin.PauseResult, error) { return nil, nil }
func (h *pausedHandler) Resume(context.Context, bool) (*admin.ResumeResult, error) {
	return nil, nil
}
func (h *pausedHandler) ThrottleSet(context.Context, admin.Request) (*admin.ThrottleResult, error) {
	return nil, nil
}
func (h *pausedHandler) CheckpointShow(context.Context) (*admin.CheckpointResult, error) {
	return nil, nil
}
func (h *pausedHandler) CheckpointRewind(context.Context, string) (*admin.CheckpointResult, error) {
	return nil, nil
}
