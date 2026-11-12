package promote_test

import (
	"context"
	"strings"
	"testing"
	"time"

	"strand/internal/promote"
	"strand/internal/testutil"
)

// Fence FTWRL trên MySQL 5.6: (1) câu lệnh chạy lâu chặn FTWRL → thất bại rõ ràng và read_only được trả
// về giá trị cũ; (2) fence thành công giữ khóa, mất kết nối giữ khóa bị phát hiện.
func TestMySQL56_FenceFTWRL_TimeoutAndLostLock(t *testing.T) {
	db := testutil.LegacyMySQL(t, "mysql:5.6")
	ctx := context.Background()
	if _, err := db.Exec("CREATE DATABASE app"); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec("CREATE TABLE app.t (id INT PRIMARY KEY) ENGINE=InnoDB"); err != nil {
		t.Fatal(err)
	}
	// (1) Một truy vấn dài đang chạy → FTWRL không lấy được khóa trong 2 giây.
	go func() { _, _ = db.Exec("SELECT SLEEP(20) FROM app.t UNION SELECT SLEEP(20)") }()
	time.Sleep(time.Second)
	d := promote.NewMySQLSourceDriver(db).WithFenceTimeout(2 * time.Second)
	err := d.Fence(ctx)
	if err == nil || !strings.Contains(err.Error(), "FLUSH TABLES WITH READ LOCK") {
		t.Fatalf("Fence with a long-running statement: err = %v", err)
	}
	var ro int
	if err := db.QueryRow("SELECT @@GLOBAL.read_only").Scan(&ro); err != nil || ro != 0 {
		t.Fatalf("read_only after failed fence = %d, %v (must be restored)", ro, err)
	}
	// Dọn truy vấn dài.
	rows, _ := db.Query("SELECT id FROM information_schema.PROCESSLIST WHERE info LIKE 'SELECT SLEEP%' AND id <> CONNECTION_ID()")
	var ids []int64
	for rows.Next() {
		var id int64
		_ = rows.Scan(&id)
		ids = append(ids, id)
	}
	rows.Close()
	for _, id := range ids {
		_, _ = db.Exec("KILL " + itoa(id))
	}
	time.Sleep(time.Second)

	// (2) Fence thành công, rồi kill kết nối giữ khóa.
	d = promote.NewMySQLSourceDriver(db).WithFenceTimeout(10 * time.Second)
	if err := d.Fence(ctx); err != nil {
		t.Fatalf("Fence: %v", err)
	}
	defer d.ReleaseFence()
	if err := d.FenceHeld(ctx); err != nil {
		t.Fatalf("FenceHeld right after Fence: %v", err)
	}
	// Kill mọi session đang rảnh (trong đó có kết nối giữ khóa; chỉ nó mới quan trọng).
	rows, _ = db.Query("SELECT id FROM information_schema.PROCESSLIST WHERE command = 'Sleep' AND id <> CONNECTION_ID()")
	ids = nil
	for rows.Next() {
		var id int64
		_ = rows.Scan(&id)
		ids = append(ids, id)
	}
	rows.Close()
	for _, id := range ids {
		_, _ = db.Exec("KILL " + itoa(id))
	}
	deadline := time.Now().Add(20 * time.Second)
	for d.FenceHealthy(ctx) == nil {
		if time.Now().After(deadline) {
			t.Fatal("lost fence connection was not detected")
		}
		time.Sleep(500 * time.Millisecond)
	}
	if err := d.FenceHeld(ctx); err == nil {
		t.Fatal("FenceHeld must fail after the lock connection died")
	}
}

func itoa(n int64) string {
	b := [20]byte{}
	i := len(b)
	for n > 0 || i == len(b) {
		i--
		b[i] = byte('0' + n%10)
		n /= 10
	}
	return string(b[i:])
}
