package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"
)

func execLocal(ctx context.Context, argv []string, logPath string) error {
	cmd := exec.CommandContext(ctx, argv[0], argv[1:]...)
	f, err := os.Create(logPath)
	if err != nil {
		return err
	}
	defer f.Close()
	cmd.Stdout, cmd.Stderr = f, f
	if err := cmd.Run(); err != nil {
		b, _ := os.ReadFile(logPath)
		return fmt.Errorf("%s: %v: %s", strings.Join(redactArgv(argv), " "), err, tailString(string(b), 600))
	}
	return nil
}

func redactArgv(a []string) []string {
	out := make([]string, len(a))
	for i, s := range a {
		switch {
		case strings.HasPrefix(s, "--mysql-password="):
			s = "--mysql-password=***"
		case strings.HasPrefix(s, "PGPASSWORD="):
			s = "PGPASSWORD=***"
		}
		out[i] = s
	}
	return out
}

func tailString(s string, n int) string {
	if len(s) > n {
		return s[len(s)-n:]
	}
	return s
}

const bigDDLMySQL = `CREATE TABLE bench.big (id BIGINT PRIMARY KEY, a INT NOT NULL, b BIGINT NOT NULL,
  c VARCHAR(100) NOT NULL, d VARCHAR(100) NOT NULL, ver INT NOT NULL DEFAULT 0, KEY ka (a)) ENGINE=InnoDB`

const bigDDLPG = `CREATE TABLE bench.big (id bigint PRIMARY KEY, a int NOT NULL, b bigint NOT NULL,
  c varchar(100) NOT NULL, d varchar(100) NOT NULL, ver int NOT NULL DEFAULT 0)`

// populateSource dựng dữ liệu nguồn tất định theo seed: bảng snapshot bench.big do SQL sinh,
// bảng ứng dụng do chính sysbench prepare / pgbench -i sinh (một lần; mọi lần đo sau đó khôi phục lại bản này).
func populateSource(ctx context.Context, o Opts, src *sql.DB, ci connInfo, dir string) error {
	if o.Engine == "postgres" {
		return populatePG(ctx, o, src, ci, dir)
	}
	return populateMySQL(ctx, o, src, ci, dir)
}

func populateMySQL(ctx context.Context, o Opts, src *sql.DB, ci connInfo, dir string) error {
	conn, err := src.Conn(ctx)
	if err != nil {
		return err
	}
	defer conn.Close()
	for _, q := range []string{"DROP DATABASE IF EXISTS bench", "CREATE DATABASE bench", bigDDLMySQL} {
		if _, err := conn.ExecContext(ctx, q); err != nil {
			return err
		}
	}
	digits := "(SELECT 0 d UNION ALL SELECT 1 UNION ALL SELECT 2 UNION ALL SELECT 3 UNION ALL SELECT 4 UNION ALL SELECT 5 UNION ALL SELECT 6 UNION ALL SELECT 7 UNION ALL SELECT 8 UNION ALL SELECT 9)"
	const batch = 1_000_000
	for lo := 0; lo < o.BigRows; lo += batch {
		n := o.BigRows - lo
		if n > batch {
			n = batch
		}
		gen := fmt.Sprintf(`SELECT %[3]d + a.d+10*b.d+100*c.d+1000*e.d+10000*f.d+100000*g.d + 1 AS n FROM %[1]s a, %[1]s b, %[1]s c, %[1]s e, %[1]s f, %[1]s g
		  WHERE a.d+10*b.d+100*c.d+1000*e.d+10000*f.d+100000*g.d < %[2]d`, digits, n, lo)
		q := fmt.Sprintf(`INSERT INTO bench.big (id,a,b,c,d) SELECT n, CRC32(CONCAT(n,'a%[1]d')) %% 100000, CRC32(CONCAT(n,'b%[1]d'))*7,
		   CONCAT(MD5(CONCAT(n,'c%[1]d')),MD5(CONCAT(n,'x'))), MD5(CONCAT(n,'d%[1]d')) FROM (%[2]s) t`, o.Seed, gen)
		if _, err := conn.ExecContext(ctx, q); err != nil {
			return fmt.Errorf("big batch %d: %w", lo, err)
		}
	}
	// sysbench prepare (một luồng cho dữ liệu dễ lặp; fixture chỉ dựng một lần).
	argv := wrapExec(o, nil, o.ClientBinary, append(sysbenchConn(ci, o), "--threads=1", "--rand-seed="+fmt.Sprint(o.Seed), "oltp_read_write", "prepare"))
	if err := execLocal(ctx, argv, filepath.Join(dir, "sysbench-prepare.log")); err != nil {
		return fmt.Errorf("sysbench prepare: %w", err)
	}
	_, err = conn.ExecContext(ctx, "RESET BINARY LOGS AND GTIDS") // pipeline mới mỗi lần: binlog nhỏ, mốc GTID thuộc fixture
	return err
}

func populatePG(ctx context.Context, o Opts, src *sql.DB, ci connInfo, dir string) error {
	for _, q := range []string{"DROP SCHEMA IF EXISTS bench CASCADE", "CREATE SCHEMA bench", bigDDLPG,
		fmt.Sprintf(`INSERT INTO bench.big (id,a,b,c,d) SELECT n, (('x'||substr(md5(n::text||'a%[1]d'),1,8))::bit(32)::bigint %% 100000)::int,
		  ('x'||substr(md5(n::text||'b%[1]d'),1,8))::bit(32)::bigint*7, md5(n::text||'c%[1]d')||md5(n::text||'x'), md5(n::text||'d%[1]d')
		  FROM generate_series(1,%[2]d) n`, o.Seed, o.BigRows)} {
		if _, err := src.ExecContext(ctx, q); err != nil {
			return fmt.Errorf("%.60s: %w", q, err)
		}
	}
	// pgbench -i trong schema bench; sinh dữ liệu phía server (G) cho tất định.
	argv := wrapExec(o, []string{"PGPASSWORD=" + ci.Password, "PGOPTIONS=-c search_path=bench"}, o.ClientBinary,
		[]string{"-h", ci.Host, "-p", ci.Port, "-U", ci.User, "-d", ci.DB, "-i", "-I", "dtGp", "-s", fmt.Sprint(o.PGScale)})
	if err := execLocal(ctx, argv, filepath.Join(dir, "pgbench-init.log")); err != nil {
		return fmt.Errorf("pgbench -i: %w", err)
	}
	// pgbench_history không có PK; thêm cột hid (INSERT của pgbench nêu rõ cột nên không bị ảnh hưởng).
	for _, q := range []string{"ALTER TABLE bench.pgbench_history ADD COLUMN hid bigserial PRIMARY KEY",
		"DROP PUBLICATION IF EXISTS g2bench_pub",
		"CREATE PUBLICATION g2bench_pub FOR TABLE bench.big, bench.pgbench_accounts, bench.pgbench_branches, bench.pgbench_tellers, bench.pgbench_history WITH (publish = 'insert, update, delete')",
		"CHECKPOINT"} {
		if _, err := src.ExecContext(ctx, q); err != nil {
			return fmt.Errorf("%.60s: %w", q, err)
		}
	}
	return nil
}

func prepareTargetEmpty(ctx context.Context, o Opts, dst *sql.DB) error {
	qs := []string{"DROP DATABASE IF EXISTS bench", "DROP DATABASE IF EXISTS strand", "CREATE DATABASE bench", "CREATE DATABASE strand", "RESET BINARY LOGS AND GTIDS"}
	if o.Engine == "postgres" {
		qs = []string{"DROP SCHEMA IF EXISTS bench CASCADE", "CREATE SCHEMA bench", "CHECKPOINT"}
	}
	for _, q := range qs {
		if _, err := dst.ExecContext(ctx, q); err != nil {
			return fmt.Errorf("target %q: %w", q, err)
		}
	}
	return nil
}

// FixtureCreate dựng nguồn + đích rỗng, ghi manifest, chụp datadir hai container (tar + hash cây).
func FixtureCreate(ctx context.Context, o Opts) error {
	if o.FixtureDir == "" || o.SourceContainer == "" || o.TargetContainer == "" || o.HelperImage == "" {
		return fmt.Errorf("fixture-dir, source-container, target-container, helper-image are required")
	}
	if err := os.MkdirAll(o.FixtureDir, 0o755); err != nil {
		return err
	}
	if _, err := os.Stat(filepath.Join(o.FixtureDir, "fixture.json")); err == nil {
		return fmt.Errorf("%s/fixture.json already exists: the restore point is immutable, use a new directory", o.FixtureDir)
	}
	fixDir, err := filepath.Abs(o.FixtureDir)
	if err != nil {
		return err
	}
	ci, err := parseConn(o.Engine, o.SourceDSN)
	if err != nil {
		return err
	}
	src, err := openDB(ctx, o.Engine, o.SourceDSN, 3*time.Minute)
	if err != nil {
		return err
	}
	defer src.Close()
	dst, err := openDB(ctx, o.Engine, o.TargetDSN, 3*time.Minute)
	if err != nil {
		return err
	}
	defer dst.Close()
	if err := populateSource(ctx, o, src, ci, fixDir); err != nil {
		return fmt.Errorf("populate source: %w", err)
	}
	if err := prepareTargetEmpty(ctx, o, dst); err != nil {
		return err
	}
	man, err := ComputeManifest(ctx, src, o)
	if err != nil {
		return err
	}
	src.Close()
	dst.Close()
	fx := Fixture{Created: time.Now().UTC(), Engine: o.Engine, Manifest: man, ManifestSHA256: man.Hash()}
	b, _ := json.Marshal(struct {
		Big, App, Tables, Scale int
		Seed                    int64
	}{o.BigRows, o.AppRows, o.AppTables, o.PGScale, o.Seed})
	fx.Params = string(b)
	sn := dockerNode{Container: o.SourceContainer, Image: o.HelperImage}
	tn := dockerNode{Container: o.TargetContainer, Image: o.HelperImage}
	if fx.SourceTar, fx.SourceTree, err = sn.Snapshot(ctx, fixDir, "source"); err != nil {
		return fmt.Errorf("snapshot source: %w", err)
	}
	if fx.TargetTar, fx.TargetTree, err = tn.Snapshot(ctx, fixDir, "target"); err != nil {
		return fmt.Errorf("snapshot target: %w", err)
	}
	out, _ := json.MarshalIndent(fx, "", "  ")
	return os.WriteFile(filepath.Join(fixDir, "fixture.json"), out, 0o644)
}
