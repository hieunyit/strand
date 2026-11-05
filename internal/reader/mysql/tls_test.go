package mysql_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"strand/internal/checkpoint"
	readermysql "strand/internal/reader/mysql"
)

// review R15: DSN source có tls=skip-verify thì kết nối replication cũng phải dùng TLS. Server đặt
// require_secure_transport=ON: binlog plaintext bị từ chối (3159).
func TestR15_BinlogConnectionHonoursDSNTLS(t *testing.T) {
	db, host, port, cleanup := startMySQLServer(t, 1)
	defer cleanup()
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	if _, err := db.ExecContext(ctx, "SET GLOBAL require_secure_transport = ON"); err != nil {
		t.Fatal(err)
	}
	defer db.ExecContext(context.Background(), "SET GLOBAL require_secure_transport = OFF")
	r, err := readermysql.New(readermysql.Config{DSN: fmt.Sprintf("root:dev@tcp(%s:%d)/?tls=skip-verify", host, port), ServerID: 719930})
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	if _, err := r.Start(ctx, checkpoint.NewMySQLAppliedSet()); err != nil {
		t.Fatalf("binlog stream with tls=skip-verify: %v", err)
	}
}
