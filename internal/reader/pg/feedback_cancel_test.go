package pg

import (
	"context"
	"net"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
)

type observedFeedbackConn struct {
	net.Conn
	writing chan struct{}
}

func (c observedFeedbackConn) Write(p []byte) (int, error) {
	select {
	case <-c.writing:
	default:
		close(c.writing)
	}
	return c.Conn.Write(p)
}

func TestCloseUnblocksStalledFeedback(t *testing.T) {
	client, server := net.Pipe() // The server intentionally never reads feedback.
	defer server.Close()
	socket := observedFeedbackConn{Conn: client, writing: make(chan struct{})}
	cfg, err := pgconn.ParseConfig("postgres://test@localhost/test?sslmode=disable")
	if err != nil {
		t.Fatal(err)
	}
	conn, err := pgconn.Construct(&pgconn.HijackedConn{Conn: socket, Config: cfg})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := replicationContext(context.Background(), socket)
	defer cancel()
	r := &Reader{conn: conn, cancel: cancel}
	r.wg.Add(1)
	go func() {
		defer r.wg.Done()
		_ = r.sendStatus(ctx, false)
	}()
	select {
	case <-socket.writing:
	case <-time.After(time.Second):
		t.Fatal("replication feedback did not start")
	}
	closed := make(chan error, 1)
	go func() { closed <- r.Close() }()
	select {
	case err := <-closed:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("Close waited for a blocked feedback write")
	}
}
