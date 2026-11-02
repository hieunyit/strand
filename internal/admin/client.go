package admin

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"time"
)

// Client kết nối tới admin socket server để gửi lệnh vận hành.
type Client struct {
	socketPath string
}

// NewClient khởi tạo Client với đường dẫn socket.
func NewClient(socketPath string) *Client {
	return &Client{socketPath: socketPath}
}

// Call gửi một Request qua socket và nhận Response.
func (c *Client) Call(ctx context.Context, req Request) (*Response, error) {
	var d net.Dialer
	conn, err := d.DialContext(ctx, "unix", c.socketPath)
	if err != nil {
		return nil, fmt.Errorf("connect to admin socket %s: %w", c.socketPath, err)
	}
	defer conn.Close()

	deadline, ok := ctx.Deadline()
	if !ok {
		deadline = time.Now().Add(5 * time.Second)
	}
	_ = conn.SetDeadline(deadline)

	if err := json.NewEncoder(conn).Encode(req); err != nil {
		return nil, fmt.Errorf("send request: %w", err)
	}

	var resp Response
	if err := json.NewDecoder(conn).Decode(&resp); err != nil {
		return nil, fmt.Errorf("decode response: %w", err)
	}

	if !resp.OK {
		return nil, errors.New(resp.Error)
	}

	return &resp, nil
}

// Top lấy thông tin trạng thái pipeline qua socket.
func (c *Client) Top(ctx context.Context) (*TopInfo, error) {
	resp, err := c.Call(ctx, Request{Command: CmdTop})
	if err != nil {
		return nil, err
	}
	if resp.Top == nil {
		return nil, errors.New("empty top info returned")
	}
	return resp.Top, nil
}

// Pause tạm dừng pipeline qua socket.
func (c *Client) Pause(ctx context.Context) (*PauseResult, error) {
	resp, err := c.Call(ctx, Request{Command: CmdPause})
	if err != nil {
		return nil, err
	}
	if resp.PauseResult == nil {
		return nil, errors.New("empty pause result returned")
	}
	return resp.PauseResult, nil
}

// Resume tiếp tục pipeline qua socket.
func (c *Client) Resume(ctx context.Context, snapshotOnly bool) (*ResumeResult, error) {
	resp, err := c.Call(ctx, Request{Command: CmdResume, SnapshotOnly: snapshotOnly})
	if err != nil {
		return nil, err
	}
	if resp.ResumeResult == nil {
		return nil, errors.New("empty resume result returned")
	}
	return resp.ResumeResult, nil
}

// ThrottleSet thay đổi giới hạn tài nguyên động qua socket.
func (c *Client) ThrottleSet(ctx context.Context, sourceIOPS, targetIOPS int, soft bool) (*ThrottleResult, error) {
	resp, err := c.Call(ctx, Request{
		Command:    CmdThrottleSet,
		SourceIOPS: sourceIOPS,
		TargetIOPS: targetIOPS,
		Soft:       soft,
	})
	if err != nil {
		return nil, err
	}
	if resp.ThrottleResult == nil {
		return nil, errors.New("empty throttle result returned")
	}
	return resp.ThrottleResult, nil
}

// CheckpointShow lấy thông tin checkpoint hiện tại qua socket.
func (c *Client) CheckpointShow(ctx context.Context) (*CheckpointResult, error) {
	resp, err := c.Call(ctx, Request{Command: CmdCheckpointShow})
	if err != nil {
		return nil, err
	}
	if resp.CheckpointResult == nil {
		return nil, errors.New("empty checkpoint result returned")
	}
	return resp.CheckpointResult, nil
}

// CheckpointRewind tua lại checkpoint vị trí qua socket.
func (c *Client) CheckpointRewind(ctx context.Context, to string) (*CheckpointResult, error) {
	resp, err := c.Call(ctx, Request{Command: CmdCheckpointRewind, RewindTo: to})
	if err != nil {
		return nil, err
	}
	if resp.CheckpointResult == nil {
		return nil, errors.New("empty checkpoint result returned")
	}
	return resp.CheckpointResult, nil
}

// SnapshotAdd yêu cầu pipeline đang chạy snapshot thêm một bảng.
func (c *Client) SnapshotAdd(ctx context.Context, table string) error {
	resp, err := c.Call(ctx, Request{Command: CmdSnapshotAdd, Table: table})
	if err != nil {
		return err
	}
	if !resp.OK {
		return fmt.Errorf("%s", resp.Error)
	}
	return nil
}

// VerifyDelta yêu cầu pipeline verify các khóa CDC đã chạm.
func (c *Client) VerifyDelta(ctx context.Context) (string, error) {
	resp, err := c.Call(ctx, Request{Command: CmdVerifyDelta})
	if err != nil {
		return "", err
	}
	return resp.Message, nil
}
