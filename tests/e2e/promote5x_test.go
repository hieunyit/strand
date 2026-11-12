package e2e

// promote từ nguồn MySQL 5.5/5.6 (không GTID, không super_read_only) sang MySQL 8.4 ở chế độ vị trí binlog:
// fence = read_only + FLUSH TABLES WITH READ LOCK giữ trong suốt promote, kill session app, bước 3 chờ
// checkpoint bền vững tới vị trí binlog sau fence, AUTO_INCREMENT trên target không trùng khóa.
// Image: STRAND_E2E_PROMOTE5X_IMAGES (mặc định mysql:5.5,mysql:5.6).

import (
	"bytes"
	"context"
	"database/sql"
	"fmt"
	"os"
	"os/exec"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func promote5xImages() []string {
	if v := os.Getenv("STRAND_E2E_PROMOTE5X_IMAGES"); v != "" {
		return strings.Split(v, ",")
	}
	return []string{"mysql:5.5", "mysql:5.6"}
}

func aiSnapshot(db *sql.DB) (string, error) {
	rows, err := db.Query("SELECT id, v FROM app.ai ORDER BY id")
	if err != nil {
		return "", err
	}
	defer rows.Close()
	var sb strings.Builder
	for rows.Next() {
		var id int64
		var v string
		if err := rows.Scan(&id, &v); err != nil {
			return "", err
		}
		fmt.Fprintf(&sb, "%d|%s\n", id, v)
	}
	return sb.String(), rows.Err()
}

func TestPromote5x_E2E_PositionMode(t *testing.T) {
	for _, image := range promote5xImages() {
		image := image
		t.Run(strings.ReplaceAll(image, ":", "_"), func(t *testing.T) {
			src := startMySQLNoGTID(t, image, "src", 1)
			dst := startMySQL(t, mysqlImage("target"), "dst", 2, nil)
			aiTable := "CREATE TABLE app.ai (id INT AUTO_INCREMENT PRIMARY KEY, v VARCHAR(32)) ENGINE=InnoDB"
			src.exec(t, posSchema("MyISAM")...)
			src.exec(t, aiTable,
				"CREATE USER 'app'@'%' IDENTIFIED BY 'app'",
				"GRANT SELECT, INSERT, UPDATE, DELETE ON app.* TO 'app'@'%'",
				"CREATE USER 'sup'@'%' IDENTIFIED BY 'sup'",
				"GRANT ALL PRIVILEGES ON *.* TO 'sup'@'%'")
			dst.exec(t, posSchema("InnoDB")...)
			dst.exec(t, aiTable, "CREATE DATABASE IF NOT EXISTS strand",
				"CREATE USER 'app'@'%' IDENTIFIED BY 'app'",
				"GRANT SELECT, INSERT, UPDATE, DELETE ON app.* TO 'app'@'%'")

			cfg := posStrandCfg(t, "promote5x", src, dst, "relaxed", testDir(t))
			cfgPath := cfg.write(t)
			f, pos := src.masterPos(t)
			start := fmt.Sprintf("%s:%d", f, pos)
			p := startStrand(t, cfgPath, "--start-at", start)

			// Tải: workload chung + chèn AUTO_INCREMENT.
			wl := &posWorkload{db: src.DB}
			wl.Start(4, 7, 300)
			var aiStop atomic.Bool
			aiDone := make(chan struct{})
			go func() {
				defer close(aiDone)
				for i := 0; !aiStop.Load(); i++ {
					_, _ = src.DB.Exec("INSERT INTO app.ai (v) VALUES (?)", fmt.Sprintf("load%d", i))
					time.Sleep(3 * time.Millisecond)
				}
			}()
			time.Sleep(6 * time.Second)
			wl.Stop()
			aiStop.Store(true)
			<-aiDone
			if p.exited() {
				t.Fatalf("strand exited during load (code %d)\n%s", p.exitCode(), p.tail())
			}
			// Dừng pipeline, rồi ghi thêm trên source: các Txn này CHƯA được apply, và FLUSH LOGS sau Txn cuối
			// để đầu binlog có event không thuộc Txn (Rotate/Format_desc).
			if code := p.stop(t); code != 0 {
				t.Fatalf("pipeline stop: exit %d\n%s", code, p.tail())
			}
			for i := 0; i < 40; i++ {
				src.exec(t, fmt.Sprintf("INSERT INTO app.ai (v) VALUES ('tail%d')", i),
					fmt.Sprintf("INSERT INTO app.t1 (id, ver, v, n) VALUES (%d, 1, 'tail', 0) ON DUPLICATE KEY UPDATE v='tail2'", 1000+i))
			}
			// Source xóa các id cao nhất của app.ai: counter nguồn > MAX(id)+1. Target không được cấp lại id đó.
			src.exec(t, "INSERT INTO app.ai (v) VALUES ('doomed1'), ('doomed2'), ('doomed3')",
				"DELETE FROM app.ai WHERE v LIKE 'doomed%'")
			var srcCounter int64
			if err := src.DB.QueryRow("SELECT AUTO_INCREMENT FROM information_schema.TABLES WHERE TABLE_SCHEMA='app' AND TABLE_NAME='ai'").Scan(&srcCounter); err != nil {
				t.Fatal(err)
			}
			src.exec(t, "FLUSH LOGS")

			// Session app mở sẵn: phải bị kill khi fence.
			appDB, err := sql.Open("mysql", fmt.Sprintf("app:app@tcp(%s)/app", src.Addr))
			if err != nil {
				t.Fatal(err)
			}
			defer appDB.Close()
			appConn, err := appDB.Conn(context.Background())
			if err != nil {
				t.Fatal(err)
			}
			if _, err := appConn.ExecContext(context.Background(), "SELECT 1"); err != nil {
				t.Fatal(err)
			}

			// promote chạy nền; pipeline đang dừng nên bước 3 chờ → có cửa sổ kiểm tra FTWRL.
			buf := &bytes.Buffer{}
			cmd := exec.Command(strandBin, "promote", "--target", dst.RootDSN+"strand", "--fence", src.RootDSN,
				"--pipeline", cfg.Name, "--source-app-roles", "app", "--fence-timeout", "30s", "--catchup-timeout", "150s")
			cmd.Stdout, cmd.Stderr = buf, buf
			if err := cmd.Start(); err != nil {
				t.Fatal(err)
			}
			pdone := make(chan error, 1)
			go func() { pdone <- cmd.Wait() }()
			t.Cleanup(func() { _ = cmd.Process.Kill() })

			// SUPER user: ghi bị chặn bởi FTWRL (read_only một mình không chặn được). Thử tới khi bị chặn.
			supDB, err := sql.Open("mysql", fmt.Sprintf("sup:sup@tcp(%s)/app", src.Addr))
			if err != nil {
				t.Fatal(err)
			}
			defer supDB.Close()
			sc, err := supDB.Conn(context.Background())
			if err != nil {
				t.Fatal(err)
			}
			if _, err := sc.ExecContext(context.Background(), "SET SESSION lock_wait_timeout = 2"); err != nil {
				t.Fatal(err)
			}
			okWrites, blocked := 0, false
			for i := 0; i < 60 && !blocked; i++ {
				_, err := sc.ExecContext(context.Background(), fmt.Sprintf("INSERT INTO app.m (id, ver, v) VALUES (%d, 1, 'sup')", 100000+i))
				if err == nil {
					okWrites++
					time.Sleep(300 * time.Millisecond)
					continue
				}
				blocked = true
				t.Logf("SUPER write blocked as expected after %d early writes: %v", okWrites, err)
			}
			if !blocked {
				t.Fatalf("SUPER user could still write during promote\n%s", buf.String())
			}
			for i := 0; i < 3; i++ { // vẫn bị chặn (khóa được giữ suốt bước 3)
				if _, err := sc.ExecContext(context.Background(), "INSERT INTO app.m (id, ver, v) VALUES (999999, 1, 'sup-late')"); err == nil {
					t.Fatalf("SUPER write succeeded while FTWRL should be held\n%s", buf.String())
				}
			}
			var n int
			if err := src.DB.QueryRow("SELECT COUNT(*) FROM app.m WHERE v LIKE 'sup%'").Scan(&n); err != nil || n != okWrites {
				t.Fatalf("SUPER rows on source = %d (err %v), want %d (only writes before the lock)", n, err, okWrites)
			}
			// Session app đã bị kill.
			if err := appConn.PingContext(context.Background()); err == nil {
				if _, err := appConn.ExecContext(context.Background(), "SELECT 1"); err == nil {
					t.Fatalf("application session was not killed\n%s", buf.String())
				}
			}
			// promote chưa xong (đang chờ pipeline).
			select {
			case err := <-pdone:
				t.Fatalf("promote finished before the pipeline caught up: %v\n%s", err, buf.String())
			default:
			}

			// Bật lại pipeline: apply nốt, promote hoàn tất.
			p2 := startStrand(t, cfgPath)
			select {
			case err := <-pdone:
				if err != nil {
					t.Fatalf("promote failed: %v\n%s\n%s", err, buf.String(), p2.tail())
				}
			case <-time.After(4 * time.Minute):
				t.Fatalf("promote did not finish\n%s\n%s", buf.String(), p2.tail())
			}
			if !strings.Contains(buf.String(), "thành công") {
				t.Fatalf("unexpected promote output:\n%s", buf.String())
			}
			if code := p2.stop(t); code != 0 {
				t.Fatalf("pipeline stop: exit %d\n%s", code, p2.tail())
			}

			// FTWRL đã nhả khi promote thoát, read_only vẫn ON: user thường bị chặn.
			var ro int
			if err := src.DB.QueryRow("SELECT @@GLOBAL.read_only").Scan(&ro); err != nil || ro != 1 {
				t.Fatalf("source read_only = %d, %v", ro, err)
			}
			if _, err := appDB.Exec("INSERT INTO app.ai (v) VALUES ('after')"); err == nil {
				t.Fatal("application could write to the source after promote")
			}
			// Dữ liệu y hệt.
			if tb, ok := posEqual(src.DB, dst.DB); !ok {
				t.Fatalf("%s differs between source and target", tb)
			}
			a, err1 := aiSnapshot(src.DB)
			b, err2 := aiSnapshot(dst.DB)
			if err1 != nil || err2 != nil || a != b || a == "" {
				t.Fatalf("app.ai differs (%v, %v)", err1, err2)
			}
			// Ứng dụng ghi vào target với AUTO_INCREMENT: không trùng khóa, id mới > mọi id cũ.
			var maxSrc int64
			if err := dst.DB.QueryRow("SELECT MAX(id) FROM app.ai").Scan(&maxSrc); err != nil {
				t.Fatal(err)
			}
			dApp, err := sql.Open("mysql", fmt.Sprintf("app:app@tcp(%s)/app", dst.Addr))
			if err != nil {
				t.Fatal(err)
			}
			defer dApp.Close()
			for i := 0; i < 50; i++ {
				if _, err := dApp.Exec("INSERT INTO app.ai (v) VALUES (?)", fmt.Sprintf("new%d", i)); err != nil {
					t.Fatalf("app insert on target #%d: %v", i, err)
				}
			}
			var minNew int64
			if err := dst.DB.QueryRow("SELECT MIN(id) FROM app.ai WHERE v LIKE 'new%'").Scan(&minNew); err != nil || minNew <= maxSrc {
				t.Fatalf("new auto_increment ids start at %d, must be > %d (%v)", minNew, maxSrc, err)
			}
			// Counter nguồn (sau khi xóa các id cao nhất) phải được mang sang: id đầu tiên ≥ counter nguồn.
			if srcCounter < maxSrc+3 || minNew < srcCounter {
				t.Fatalf("first target id %d must be >= source counter %d (surviving max id %d)", minNew, srcCounter, maxSrc)
			}
		})
	}
}
