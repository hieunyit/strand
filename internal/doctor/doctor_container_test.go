package doctor

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"strand/internal/config"
	"strand/internal/findings"
	"strand/internal/model"
	"strand/internal/pipeline"
	"strand/internal/testutil"
	"strand/internal/validate"
)

const doctorSecret = "srcpw-Secret-42"

// TestDoctor_Container_MariaDBToMySQL dựng lại tình huống production (MariaDB 10.6 → MySQL 8.4, auto_seed,
// create_schema auto) với NHIỀU lỗi cùng lúc và kiểm doctor báo tất cả trong một lần chạy, trong khi đường
// validate/sync (fail-fast) chỉ báo được lỗi đầu tiên. Cấu hình sạch thì không có vấn đề mức error.
func TestDoctor_Container_MariaDBToMySQL(t *testing.T) {
	src := testutil.LegacyMySQL(t, "mariadb:10.6")
	// One-member Group Replication target, as in production.
	dst := testutil.LegacyMySQL(t, "mysql:8.4", "--gtid-mode=ON", "--enforce-gtid-consistency=ON",
		"--plugin-load-add=group_replication.so",
		"--loose-group-replication-group-name=aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee",
		"--loose-group-replication-start-on-boot=OFF",
		"--loose-group-replication-local-address=127.0.0.1:33061",
		"--loose-group-replication-group-seeds=127.0.0.1:33061")
	mustExec(t, dst, "SET GLOBAL group_replication_bootstrap_group=ON; START GROUP_REPLICATION; SET GLOBAL group_replication_bootstrap_group=OFF")
	srcAddr, dstAddr := testutil.LegacyMySQLAddr(src), testutil.LegacyMySQLAddr(dst)
	ctx := context.Background()
	mustExec(t, src, `
		CREATE DATABASE bad;
		CREATE TABLE bad.parent (id INT PRIMARY KEY) ENGINE=InnoDB;
		CREATE TABLE bad.a (id INT PRIMARY KEY, parent_id INT, CONSTRAINT fk_a_parent FOREIGN KEY (parent_id) REFERENCES bad.parent (id) ON DELETE CASCADE) ENGINE=InnoDB;
		CREATE TABLE bad.b (id INT PRIMARY KEY, parent_id INT, CONSTRAINT fk_b_parent FOREIGN KEY (parent_id) REFERENCES bad.parent (id) ON DELETE CASCADE) ENGINE=InnoDB;
		CREATE TABLE bad.c (id INT PRIMARY KEY, a_id INT, b_id INT,
			CONSTRAINT fk_c_a FOREIGN KEY (a_id) REFERENCES bad.a (id) ON DELETE CASCADE,
			CONSTRAINT fk_c_b FOREIGN KEY (b_id) REFERENCES bad.b (id) ON DELETE CASCADE) ENGINE=InnoDB;
		CREATE TABLE bad.nokey (v DOUBLE) ENGINE=InnoDB;
		CREATE TABLE bad.kl (k VARCHAR(20), v VARCHAR(20)) ENGINE=InnoDB;
		CREATE USER 'strand_bad'@'%' IDENTIFIED BY '`+doctorSecret+`';
		GRANT SELECT ON bad.* TO 'strand_bad'@'%';

		CREATE DATABASE good;
		CREATE TABLE good.parent (id INT PRIMARY KEY, name VARCHAR(20)) ENGINE=InnoDB;
		CREATE TABLE good.child (id INT PRIMARY KEY, parent_id INT, CONSTRAINT fk_child_parent FOREIGN KEY (parent_id) REFERENCES good.parent (id) ON DELETE CASCADE) ENGINE=InnoDB;
		INSERT INTO good.parent VALUES (1, 'x');
		CREATE USER 'strand_good'@'%' IDENTIFIED BY '`+doctorSecret+`';
		GRANT SELECT, LOCK TABLES, PROCESS, REPLICATION SLAVE, BINLOG MONITOR, SLAVE MONITOR ON *.* TO 'strand_good'@'%';
		GRANT SELECT ON performance_schema.* TO 'strand_good'@'%';`)
	mustExec(t, dst, `
		CREATE DATABASE strand_meta;
		CREATE DATABASE bad;
		CREATE TABLE bad.parent (id INT PRIMARY KEY) ENGINE=InnoDB;
		INSERT INTO bad.parent VALUES (1);
		CREATE TABLE bad.a (id INT PRIMARY KEY, parent_id INT) ENGINE=InnoDB;
		CREATE TABLE bad.other (id INT PRIMARY KEY, x INT, CONSTRAINT fk_c_b FOREIGN KEY (x) REFERENCES bad.parent (id)) ENGINE=InnoDB;`)
	t.Setenv("DOCTOR_SRC_PW", doctorSecret)

	t.Run("broken setup reports every problem at once", func(t *testing.T) {
		cfg := loadConfig(t, "bad", srcAddr, dstAddr, "strand_bad")
		c := &findings.Collector{}
		Run(ctx, cfg, c)
		report := findings.NewReport(cfg.Name, c)
		var human, js bytes.Buffer
		findings.WriteHuman(&human, report)
		if err := findings.WriteJSON(&js, report); err != nil {
			t.Fatal(err)
		}
		t.Log(human.String())
		codes := c.Codes()
		for _, want := range []string{
			"source_privilege_missing",  // REPLICATION SLAVE / BINLOG MONITOR / LOCK TABLES
			"source_table_no_key",       // bad.nokey
			"source_fk_cascade_overlap", // parent -> a,b -> c
			"target_not_empty",          // bad.parent has a row
			"target_fk_name_clash",      // fk_c_b already used by bad.other
			"target_gr_keyless",         // bad.kl runs keyless; Group Replication rejects its rows
		} {
			if !codes[want] {
				t.Errorf("missing finding %s; got %v", want, codes)
			}
		}
		if !c.HasErrors() || report.OK {
			t.Fatal("broken setup must have error findings (non-zero exit)")
		}
		if report.Summary.Errors < 4 {
			t.Errorf("want several error findings in one run, got %d", report.Summary.Errors)
		}
		for _, f := range report.Findings {
			if f.Severity == findings.Error && f.Fix == "" {
				t.Errorf("error finding without a fix: %+v", f)
			}
		}
		for _, out := range []string{human.String(), js.String()} {
			if strings.Contains(out, doctorSecret) {
				t.Fatal("doctor output leaks the source password")
			}
		}
		var decoded findings.Report
		if err := json.Unmarshal(js.Bytes(), &decoded); err != nil || decoded.OK || len(decoded.Findings) != len(report.Findings) {
			t.Fatalf("JSON report does not round-trip: %v %+v", err, decoded.Summary)
		}
		if !strings.Contains(human.String(), "fix: GRANT ") {
			t.Errorf("human output lacks the GRANT fix:\n%s", human.String())
		}

		// A/B: the fail-fast path used by validate and sync stops at the first problem, so it can name
		// only one of the problems doctor lists together.
		_, err := validate.CheckMySQLSourceOpts(ctx, openAs(t, srcAddr, "strand_bad", "bad"), []string{"bad.a", "bad.b", "bad.c", "bad.nokey", "bad.parent"}, validate.KeylessOpts{RequireBinlogPosition: true})
		if err == nil || !strings.Contains(err.Error(), "no usable row key") {
			t.Fatalf("validate source check: %v", err)
		}
		ids := []model.TableID{{Schema: "bad", Table: "a"}, {Schema: "bad", Table: "b"}, {Schema: "bad", Table: "c"}, {Schema: "bad", Table: "parent"}}
		err = pipeline.ValidateAutoSeedPreflight(ctx, src, openAs(t, dstAddr, "root", "strand_meta"), true, ids, nil, nil)
		if err == nil {
			t.Fatal("validate auto-seed preflight accepted the broken target")
		}
		mentions := 0
		for _, needle := range []string{"already contains data", "fk_c_b", "outside the replication scope"} {
			if strings.Contains(err.Error(), needle) {
				mentions++
			}
		}
		if mentions != 1 {
			t.Errorf("fail-fast preflight is expected to report exactly one of the problems, got: %v", err)
		}
	})

	t.Run("clean setup has no errors", func(t *testing.T) {
		cfg := loadConfig(t, "good", srcAddr, dstAddr, "strand_good")
		c := &findings.Collector{}
		Run(ctx, cfg, c)
		report := findings.NewReport(cfg.Name, c)
		var human bytes.Buffer
		findings.WriteHuman(&human, report)
		t.Log(human.String())
		if !report.OK {
			t.Fatalf("clean setup reported errors:\n%s", human.String())
		}
		if !c.Codes()["target_tables_to_create"] || !c.Codes()["target_group_replication"] {
			t.Errorf("want the info about tables to create, got %v", c.Codes())
		}
	})
}

func mustExec(t *testing.T, db *sql.DB, q string) {
	t.Helper()
	if _, err := db.Exec(q); err != nil {
		t.Fatal(err)
	}
}

func openAs(t *testing.T, addr, user, schema string) *sql.DB {
	t.Helper()
	pw := doctorSecret
	if user == "root" {
		pw = "dev"
	}
	db, err := sql.Open("mysql", fmt.Sprintf("%s:%s@tcp(%s)/%s", user, pw, addr, schema))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	return db
}

func loadConfig(t *testing.T, schema, srcAddr, dstAddr, user string) *config.Config {
	t.Helper()
	dir := t.TempDir()
	yaml := fmt.Sprintf(`version: 1
name: doctor-%s
source:
  kind: mysql
  dsn: "%s:${DOCTOR_SRC_PW}@tcp(%s)/"
target:
  kind: mysql
  dsn: "root:dev@tcp(%s)/strand_meta"
tables: { include: ["%s.*"], keyless: full_row }
bootstrap: { auto_seed: true }
resources: { spool: { path: %s } }
`, schema, user, srcAddr, dstAddr, schema, filepath.Join(dir, "spool"))
	path := filepath.Join(dir, "p.yaml")
	if err := os.WriteFile(path, []byte(yaml), 0o600); err != nil {
		t.Fatal(err)
	}
	cfg, err := config.LoadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	return cfg
}
