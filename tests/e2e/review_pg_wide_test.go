package e2e

import (
	"fmt"
	"strings"
	"testing"
	"time"
)

// RS-1/SG-1: binary thật bootstrap PK 14 cột, chunk 5000 row (>65535 keeper binds), rồi stream.
// Audit đếm theo transaction chứng minh fixture thật sự có chunk lớn, không xanh nhờ chia chunk nhỏ.
func TestReview_E2E_PGWideKeySnapshot(t *testing.T) {
	_ = seedFromEnv(t)
	src := startPG(t, "wide-src")
	dst := startPG(t, "wide-dst")
	var cols, pk []string
	for i := 0; i < 14; i++ {
		n := fmt.Sprintf("k%d", i)
		cols = append(cols, n+" integer NOT NULL")
		pk = append(pk, n)
	}
	ddl := "CREATE TABLE app.wide (" + strings.Join(cols, ",") + ", v integer, PRIMARY KEY (" + strings.Join(pk, ",") + "))"
	for _, n := range []*pgNode{src, dst} {
		n.exec(t, "CREATE SCHEMA app", ddl, "CREATE TABLE app.marker(id bigint PRIMARY KEY,v text)")
	}
	dst.exec(t, `CREATE TABLE app.audit(x bigint)`, `CREATE FUNCTION app.audit_wide() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN INSERT INTO app.audit VALUES (txid_current()); RETURN NEW; END; $$`, `CREATE TRIGGER audit_wide AFTER INSERT OR UPDATE ON app.wide FOR EACH ROW EXECUTE FUNCTION app.audit_wide()`)
	src.exec(t, "INSERT INTO app.wide SELECT g"+strings.Repeat(",0", 13)+",g FROM generate_series(1,5000) g", "CREATE PUBLICATION wide_pub FOR TABLE app.wide,app.marker")
	cfg := strandCfg{Name: "wide-snapshot", SourceKind: "postgres", TargetKind: "postgres", SourceDSN: src.DSN, TargetDSN: dst.DSN, SourceExtra: ", slot: wide_slot, publication: wide_pub, create_slot: true", Include: []string{"app.wide", "app.marker"}, Dir: testDir(t), Bootstrap: "logical", SourceIOPS: 20000, ChunkRows: "5000"}
	p := startStrand(t, cfg.write(t))
	waitSynced(t, &cfg, p, 3*time.Minute)
	var largest int
	if err := dst.DB.QueryRow("SELECT COALESCE(max(n),0) FROM (SELECT count(*) n FROM app.audit GROUP BY x) s").Scan(&largest); err != nil {
		t.Fatal(err)
	}
	if largest < 5000 {
		t.Fatalf("fixture never exercised a 5000-row flush: largest transaction=%d", largest)
	}
	src.exec(t, "UPDATE app.wide SET v=v+10000 WHERE k0=2500")
	waitPGCaughtUp(t, src, dst, &cfg, p, 2*time.Minute)
	if code := p.stop(t); code != 0 {
		t.Fatalf("exit %d\n%s", code, p.tail())
	}
	var count, bad int
	if err := dst.DB.QueryRow("SELECT count(*),count(*) FILTER(WHERE v <> CASE WHEN k0=2500 THEN k0+10000 ELSE k0 END) FROM app.wide").Scan(&count, &bad); err != nil {
		t.Fatal(err)
	}
	if count != 5000 || bad != 0 {
		t.Fatalf("count=%d mismatches=%d", count, bad)
	}
}
