package main

import (
	"strings"
	"testing"
)

func sampleManifest() Manifest {
	return Manifest{Engine: "mysql", Tables: []TableSum{{"bench.big", 300000, "111"}, {"bench.sbtest1", 100000, "222"}}}
}

func TestManifestHashStableAndSensitive(t *testing.T) {
	a := sampleManifest()
	b := sampleManifest()
	b.Tables[0], b.Tables[1] = b.Tables[1], b.Tables[0] // thứ tự bảng không đổi hash
	if a.Hash() != b.Hash() {
		t.Fatal("hash must not depend on table order")
	}
	c := sampleManifest()
	c.Tables[1].Checksum = "223"
	if a.Hash() == c.Hash() {
		t.Fatal("checksum change must change the hash")
	}
}

func TestDiffManifestDetectsEveryKindOfDrift(t *testing.T) {
	want := sampleManifest()
	if d := DiffManifest(want, sampleManifest()); len(d) != 0 {
		t.Fatalf("identical manifests differ: %v", d)
	}
	cases := map[string]func(m *Manifest){
		"rows":       func(m *Manifest) { m.Tables[0].Rows++ },
		"checksum":   func(m *Manifest) { m.Tables[1].Checksum = "x" },
		"missing":    func(m *Manifest) { m.Tables = m.Tables[:1] },
		"unexpected": func(m *Manifest) { m.Tables = append(m.Tables, TableSum{Name: "bench.extra"}) },
	}
	for name, f := range cases {
		got := sampleManifest()
		f(&got)
		if len(DiffManifest(want, got)) == 0 {
			t.Errorf("%s drift not detected", name)
		}
	}
}

func TestFoldTreeHash(t *testing.T) {
	h := strings.Repeat("a", 64)
	lines := h + "  ./a\n" + h + "  ./b\n"
	x, err := FoldTreeHash(lines)
	if err != nil || len(x) != 64 {
		t.Fatalf("%v %q", err, x)
	}
	y, _ := FoldTreeHash(h + "  ./a\n" + h + "  ./c\n")
	if x == y {
		t.Fatal("path change must change the tree hash")
	}
	if _, err := FoldTreeHash(""); err == nil {
		t.Fatal("empty datadir must be an error")
	}
	if _, err := FoldTreeHash("garbage"); err == nil {
		t.Fatal("malformed line must be an error")
	}
}

func TestPickDataMount(t *testing.T) {
	m, err := pickDataMount("/etc/x|bind||/etc\n/var/lib/mysql|volume|vol1|/var/lib/docker/volumes/vol1/_data\n")
	if err != nil || m != "vol1" {
		t.Fatalf("%q %v", m, err)
	}
	m, err = pickDataMount("/var/lib/postgresql|bind||/data/pg\n")
	if err != nil || m != "/data/pg" {
		t.Fatalf("%q %v", m, err)
	}
	if _, err := pickDataMount("/tmp|volume|v|/x\n"); err == nil {
		t.Fatal("no data mount must be an error")
	}
}

func TestRestoreEvidenceVerify(t *testing.T) {
	ok := RestoreEvidence{SourceTreeWant: "a", SourceTreeGot: "a", TargetTreeWant: "b", TargetTreeGot: "b", ManifestWant: "c", ManifestGot: "c"}
	if err := ok.Verify(); err != nil {
		t.Fatal(err)
	}
	for name, f := range map[string]func(e *RestoreEvidence){
		"source differs":   func(e *RestoreEvidence) { e.SourceTreeGot = "z" },
		"target differs":   func(e *RestoreEvidence) { e.TargetTreeGot = "z" },
		"manifest differs": func(e *RestoreEvidence) { e.ManifestGot = "z" },
		"manifest absent":  func(e *RestoreEvidence) { e.ManifestGot = "" },
		"want absent":      func(e *RestoreEvidence) { e.SourceTreeWant = "" },
	} {
		e := ok
		f(&e)
		if e.Verify() == nil {
			t.Errorf("%s: must fail", name)
		}
	}
}

func TestLoadFixtureRejectsTamperedManifest(t *testing.T) {
	dir := t.TempDir()
	fx := Fixture{Engine: "mysql", Manifest: sampleManifest(), SourceTree: "s", TargetTree: "t"}
	fx.ManifestSHA256 = fx.Manifest.Hash()
	if err := writeJSON(dir+"/fixture.json", fx); err != nil {
		t.Fatal(err)
	}
	if _, err := LoadFixture(dir); err != nil {
		t.Fatal(err)
	}
	fx.Manifest.Tables[0].Rows++
	_ = writeJSON(dir+"/fixture.json", fx)
	if _, err := LoadFixture(dir); err == nil {
		t.Fatal("tampered manifest must be rejected")
	}
}
