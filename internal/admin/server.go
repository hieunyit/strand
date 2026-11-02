package admin

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"os"
	"path/filepath"
	"sync"
	"time"
)

// Server lắng nghe lệnh vận hành qua UNIX domain socket theo spec/02 §10.
type Server struct {
	socketPath string
	handler    PipelineAdminHandler
	logger     *slog.Logger
	listener   net.Listener
	mu         sync.Mutex
	closed     bool
	wg         sync.WaitGroup
}

// NewServer khởi tạo Server.
func NewServer(socketPath string, handler PipelineAdminHandler, logger *slog.Logger) *Server {
	if logger == nil {
		logger = slog.Default()
	}
	return &Server{
		socketPath: socketPath,
		handler:    handler,
		logger:     logger,
	}
}

// Start bắt đầu lắng nghe trên UNIX socket.
func (s *Server) Start() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.listener != nil {
		return errors.New("server already started")
	}

	// Đảm bảo thư mục chứa socket tồn tại với quyền 0700 (spec: 03 §4, §7).
	// Nếu thư mục đã tồn tại trước đó thì không tự đổi quyền của thư mục có sẵn.
	dir := filepath.Dir(s.socketPath)
	_, dirStatErr := os.Stat(dir)
	dirCreated := errors.Is(dirStatErr, os.ErrNotExist)
	if err := os.MkdirAll(dir, 0700); err != nil {
		return fmt.Errorf("create socket dir: %w", err)
	}
	if dirCreated {
		if err := os.Chmod(dir, 0700); err != nil {
			return fmt.Errorf("chmod socket dir: %w", err)
		}
	}

	// Nếu đường dẫn socket đã tồn tại, thử kết nối trước khi xóa: nếu kết nối thành công
	// thì đang có một instance sống lắng nghe trên socket này.
	if _, err := os.Lstat(s.socketPath); err == nil {
		conn, dialErr := net.DialTimeout("unix", s.socketPath, 500*time.Millisecond)
		if dialErr == nil {
			_ = conn.Close()
			return fmt.Errorf("admin socket %s already in use by a live server", s.socketPath)
		}
		if err := os.Remove(s.socketPath); err != nil && !errors.Is(err, os.ErrNotExist) {
			return fmt.Errorf("remove stale socket %s: %w", s.socketPath, err)
		}
	}

	ln, err := net.Listen("unix", s.socketPath)
	if err != nil {
		return fmt.Errorf("listen unix socket %s: %w", s.socketPath, err)
	}
	// spec: 03 §4, §7 — Unix socket 0700 (chạy Chmod sau Listen để không phụ thuộc umask).
	if err := os.Chmod(s.socketPath, 0700); err != nil {
		_ = ln.Close()
		_ = os.Remove(s.socketPath)
		return fmt.Errorf("chmod unix socket %s: %w", s.socketPath, err)
	}

	s.listener = ln
	s.closed = false

	s.wg.Add(1)
	go s.acceptLoop(ln)

	s.logger.Info("admin socket server started", "path", s.socketPath)
	return nil
}

func (s *Server) acceptLoop(ln net.Listener) {
	defer s.wg.Done()
	for {
		conn, err := ln.Accept() // không đọc s.listener: Close() đặt nó về nil
		if err != nil {
			s.mu.Lock()
			isClosed := s.closed
			s.mu.Unlock()
			if isClosed {
				return
			}
			s.logger.Warn("admin socket accept error", "error", err)
			continue
		}

		s.wg.Add(1)
		go func(c net.Conn) {
			defer s.wg.Done()
			defer c.Close()
			s.handleConn(c)
		}(conn)
	}
}

func (s *Server) handleConn(conn net.Conn) {
	_ = conn.SetDeadline(time.Now().Add(10 * time.Second))

	decoder := json.NewDecoder(conn)
	encoder := json.NewEncoder(conn)

	var req Request
	if err := decoder.Decode(&req); err != nil {
		if !errors.Is(err, io.EOF) {
			_ = encoder.Encode(Response{OK: false, Error: fmt.Sprintf("invalid request JSON: %v", err)})
		}
		return
	}

	// Verify delta có thể chạy lâu (đọc range ở cả hai phía): cho phép tới 30 phút.
	timeout := 10 * time.Second
	if req.Command == CmdVerifyDelta {
		timeout = 30 * time.Minute
	}
	_ = conn.SetDeadline(time.Now().Add(timeout))
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	resp := s.dispatch(ctx, req)
	_ = encoder.Encode(resp)
}

func (s *Server) dispatch(ctx context.Context, req Request) Response {
	if s.handler == nil {
		return Response{OK: false, Error: "no handler registered"}
	}

	switch req.Command {
	case CmdTop:
		top, err := s.handler.Top(ctx)
		if err != nil {
			return Response{OK: false, Error: err.Error()}
		}
		return Response{OK: true, Top: top}

	case CmdPause:
		res, err := s.handler.Pause(ctx)
		if err != nil {
			return Response{OK: false, Error: err.Error()}
		}
		return Response{OK: true, PauseResult: res}

	case CmdPauseSnapshot:
		sp, ok := s.handler.(SnapshotPauser)
		if !ok {
			return Response{OK: false, Error: "pause snapshot is not supported by this pipeline"}
		}
		res, err := sp.PauseSnapshot(ctx)
		if err != nil {
			return Response{OK: false, Error: err.Error()}
		}
		return Response{OK: true, PauseResult: res}

	case CmdResume:
		res, err := s.handler.Resume(ctx, req.SnapshotOnly)
		if err != nil {
			return Response{OK: false, Error: err.Error()}
		}
		return Response{OK: true, ResumeResult: res}

	case CmdThrottleSet:
		res, err := s.handler.ThrottleSet(ctx, req)
		if err != nil {
			return Response{OK: false, Error: err.Error()}
		}
		return Response{OK: true, ThrottleResult: res}

	case CmdCheckpointShow:
		res, err := s.handler.CheckpointShow(ctx)
		if err != nil {
			return Response{OK: false, Error: err.Error()}
		}
		return Response{OK: true, CheckpointResult: res}

	case CmdCheckpointRewind:
		res, err := s.handler.CheckpointRewind(ctx, req.RewindTo)
		if err != nil {
			return Response{OK: false, Error: err.Error()}
		}
		return Response{OK: true, CheckpointResult: res}

	case CmdVerifyDelta:
		dv, ok := s.handler.(DeltaVerifier)
		if !ok {
			return Response{OK: false, Error: "verify delta is not supported by this pipeline"}
		}
		msg, err := dv.VerifyDelta(ctx)
		if err != nil {
			return Response{OK: false, Error: err.Error()}
		}
		return Response{OK: true, Message: msg}

	case CmdSnapshotAdd:
		sa, ok := s.handler.(SnapshotAdder)
		if !ok {
			return Response{OK: false, Error: "snapshot add is not supported by this pipeline"}
		}
		if err := sa.SnapshotAdd(ctx, req.Table); err != nil {
			return Response{OK: false, Error: err.Error()}
		}
		return Response{OK: true}

	default:
		return Response{OK: false, Error: fmt.Sprintf("unknown command: %s", req.Command)}
	}
}

// Close đóng listener và dọn dẹp file socket.
func (s *Server) Close() error {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return nil
	}
	s.closed = true
	var err error
	hadListener := s.listener != nil
	if hadListener {
		err = s.listener.Close()
		s.listener = nil
	}
	s.mu.Unlock()

	s.wg.Wait()
	if hadListener {
		_ = os.Remove(s.socketPath)
	}
	return err
}
