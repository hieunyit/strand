package e2e

import (
	"fmt"
	"math/rand"
	"testing"
	"time"
)

// Regression (review việc B, 2026-11-04): bảng MySQL có cột generated STORED và VIRTUAL. Trước sửa,
// snapshot ghi cả giá trị cột generated sang target → Error 3105 và pipeline dừng. Target được
// Strand tự tạo (create_schema: auto) từ SHOW CREATE TABLE nên cũng có cột generated.
// invariant: CP-1..5 — trạng thái cuối giống hệt source, kể cả cột generated (target tự tính).
func TestB_E2E_MySQLGeneratedColumnsSnapshotAndStream(t *testing.T) {
	seed := seedFromEnv(t)
	rng := rand.New(rand.NewSource(seed))
	src := startMySQL(t, mysqlImage("source"), "src", 1, nil)
	dst := startMySQL(t, mysqlImage("target"), "dst", 2, nil)
	src.exec(t, "CREATE DATABASE app",
		"CREATE TABLE app.g (id BIGINT PRIMARY KEY, a INT NOT NULL, s INT AS (a*2) STORED, v INT AS (a*3) VIRTUAL, "+
			"created DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6), ver INT NOT NULL DEFAULT 0, KEY ks (s))")
	const preload = 3000
	for i := 1; i <= preload; i += 500 {
		q := "INSERT INTO app.g (id, a) VALUES "
		for j := i; j < i+500; j++ {
			if j > i {
				q += ","
			}
			q += fmt.Sprintf("(%d,%d)", j, j%97)
		}
		src.exec(t, q)
	}
	dst.exec(t, "CREATE DATABASE IF NOT EXISTS strand")
	cfg := strandCfg{Name: "e2e-gencol", SourceDSN: src.RootDSN, TargetDSN: dst.RootDSN + "strand",
		Include: []string{"app.*"}, Dir: t.TempDir(), SourceIOPS: 60}
	p := startStrand(t, cfg.write(t))

	// Ghi trong lúc snapshot và stream: insert mới, update đổi a (cột generated đổi theo), delete.
	stop := time.Now().Add(15 * time.Second)
	next := preload + 1
	for time.Now().Before(stop) && !p.exited() {
		switch rng.Intn(3) {
		case 0:
			src.exec(t, fmt.Sprintf("INSERT INTO app.g (id, a) VALUES (%d, %d)", next, rng.Intn(1000)))
			next++
		case 1:
			src.exec(t, fmt.Sprintf("UPDATE app.g SET a = %d, ver = ver + 1 WHERE id = %d", rng.Intn(1000), 1+rng.Intn(next-1)))
		default:
			src.exec(t, fmt.Sprintf("DELETE FROM app.g WHERE id = %d", 1+rng.Intn(next-1)))
		}
	}
	if p.exited() {
		t.Fatalf("strand exited during load\n%s", p.tail())
	}
	waitCaughtUp(t, src, dst, &cfg, p, 3*time.Minute)
	if code := p.stop(t); code != 0 {
		t.Fatalf("strand exit code %d\n%s", code, p.tail())
	}
	compareFinal(t, src, dst, "app.g")
	var gen int
	if err := dst.DB.QueryRow(`SELECT COUNT(*) FROM information_schema.COLUMNS WHERE TABLE_SCHEMA='app' AND TABLE_NAME='g' AND GENERATION_EXPRESSION <> ''`).Scan(&gen); err != nil || gen != 2 {
		t.Fatalf("target must keep both generated columns, got %d (err %v)", gen, err)
	}
}
