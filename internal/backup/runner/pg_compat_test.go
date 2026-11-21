package runner

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestPGMajorAndArchive(t *testing.T) {
	for in, want := range map[string]int{
		"16.4 (Debian 16.4-1.pgdg120+2)": 16,
		"13.18":                          13,
		"19.0":                           19,
		"9.6.24":                         9,
		"pg_restore (PostgreSQL) 19.0 (Debian 19.0-2.pgdg12+1)": 19,
		"pg_dump (PostgreSQL) 16.16":                            16,
		"":                                                      0,
	} {
		if got := pgMajor(in); got != want {
			t.Errorf("pgMajor(%q) = %d, want %d", in, got, want)
		}
	}
	for _, c := range []struct {
		h    []byte
		want int
	}{
		{[]byte("PGDMP\x01\x10\x00"), 17}, // pg_dump 17+ (1.16), header of a pg_dump 19.0 archive
		{[]byte("PGDMP\x01\x0f\x00"), 16}, // pg_dump 16 (1.15)
		{[]byte("PGDMP\x01\x0e\x00"), 12},
		{[]byte("PGDMP\x01\x0d\x00"), 0},
		{[]byte("PGDM"), 0},
		{nil, 0},
	} {
		if got := pgArchiveMinReader(c.h); got != c.want {
			t.Errorf("pgArchiveMinReader(%q) = %d, want %d", c.h, got, c.want)
		}
	}
}

// toolsDir writes fake tools printing the given --version line.
func toolsDir(t *testing.T, tools map[string]string) Getenv {
	t.Helper()
	dir := t.TempDir()
	for name, ver := range tools {
		if err := os.WriteFile(filepath.Join(dir, name), []byte("#!/bin/sh\necho '"+ver+"'\n"), 0o755); err != nil {
			t.Fatal(err)
		}
	}
	return func(k string) string {
		if k == EnvToolsDir {
			return dir
		}
		return ""
	}
}

func TestSelectPGRestoreAndDump(t *testing.T) {
	ctx := context.Background()
	v19 := "pg_restore (PostgreSQL) 19.0"
	full := toolsDir(t, map[string]string{"pg_restore": v19, "pg_restore-16": "pg_restore (PostgreSQL) 16.16",
		"pg_dump": "pg_dump (PostgreSQL) 19.0", "pg_dump-16": "pg_dump (PostgreSQL) 16.16"})
	bare := toolsDir(t, map[string]string{"pg_restore": v19, "pg_dump": "pg_dump (PostgreSQL) 19.0"})
	old := toolsDir(t, map[string]string{"pg_restore": "pg_restore (PostgreSQL) 16.4", "pg_restore-15": "pg_restore (PostgreSQL) 15.1"})
	h16 := []byte("PGDMP\x01\x0f\x00") // written by pg_dump 16
	h19 := []byte("PGDMP\x01\x10\x00") // written by pg_dump 17+
	h14 := []byte("PGDMP\x01\x0e\x00")
	withExact := toolsDir(t, map[string]string{"pg_restore": v19, "pg_restore-15": "pg_restore (PostgreSQL) 15.1", "pg_restore-16": "x 16"})

	for _, c := range []struct {
		name    string
		getenv  Getenv
		server  string
		header  []byte
		want    string
		wantErr string
	}{
		{"target 17 uses default", full, "17.2", h19, "pg_restore", ""},
		{"target 18 uses default", full, "18.1 (Debian)", h16, "pg_restore", ""},
		{"target 16 uses pg_restore-16", full, "16.4 (Debian 16.4-1)", h16, "pg_restore-16", ""},
		{"target 13 uses pg_restore-16", full, "13.18", h16, "pg_restore-16", ""},
		{"exact major preferred", withExact, "15.8", h14, "pg_restore-15", ""},
		{"exact major too old for archive", withExact, "15.8", h16, "pg_restore-16", ""},
		{"target unknown uses default", full, "", h16, "pg_restore", ""},
		{"missing legacy tool", bare, "16.4", h16, "", "native tool pg_restore-16 not found"},
		{"archive from pg_dump 17+ into 16", full, "16.4", h19, "", "written by pg_dump 17 or newer"},
		{"default already 16", old, "15.2", h16, "pg_restore", ""},
	} {
		got, err := SelectPGRestore(ctx, c.getenv, c.server, c.header)
		if c.wantErr != "" {
			if err == nil || !strings.Contains(err.Error(), c.wantErr) {
				t.Errorf("%s: err = %v, want %q", c.name, err, c.wantErr)
			}
			continue
		}
		if err != nil || got != c.want {
			t.Errorf("%s: got %q, %v; want %q", c.name, got, err, c.want)
		}
	}

	if tool, note := SelectPGDump(ctx, full, "16.4"); tool != "pg_dump-16" || note != "" {
		t.Errorf("pg_dump for 16: %q %q", tool, note)
	}
	if tool, note := SelectPGDump(ctx, full, "17.0"); tool != "pg_dump" || note != "" {
		t.Errorf("pg_dump for 17: %q %q", tool, note)
	}
	if tool, note := SelectPGDump(ctx, bare, "15.3"); tool != "pg_dump" || !strings.Contains(note, "only be restorable into PostgreSQL 17") {
		t.Errorf("pg_dump for 15 without pg_dump-16: %q %q", tool, note)
	}

	names := map[string]bool{}
	for _, o := range OptionalTools() {
		names[o.Name] = true
	}
	for _, n := range []string{"mysqldump-8.0", "pg_dump-16", "pg_restore-16"} {
		if !names[n] {
			t.Errorf("OptionalTools misses %s", n)
		}
	}
}
