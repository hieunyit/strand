package seed_test

import (
	"bytes"
	"compress/gzip"
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"strand/internal/exitcode"
	"strand/internal/seed"
)

func TestSD1_SeedPlan_Mysqldump(t *testing.T) {
	ctx := context.Background()
	planner := seed.NewMysqldumpPlanner()

	const (
		uuid1 = "3E11FA47-71CA-11E1-9E33-C80AA9429562"
		uuid2 = "4E22FB58-82DB-22F2-AF44-D91BB0530673"
	)

	largePadding := strings.Repeat("-- "+strings.Repeat("x", 1000)+"\n", 2200) // > 2 MiB

	cases := []struct {
		name           string
		filename       string
		gzip           bool
		content        string
		restoredGTID   string
		wantErr        bool
		wantExitCode   int
		wantErrSubstr  string
		wantGTID       string
		wantToolVer    string
		wantWarnings   int
		wantBinlogFile string
		wantBinlogPos  uint64
	}{
		{
			name:     "SingleUUIDGTID",
			filename: "single.sql",
			content: `-- MySQL dump 10.13  Distrib 8.4.3, for Linux (x86_64)
--
-- Host: localhost    Database: app
-- ------------------------------------------------------
SET @@SESSION.SQL_LOG_BIN= 0;
SET @@GLOBAL.GTID_PURGED=/*!80000 '+'*/ '` + uuid1 + `:1-982341';
-- CHANGE REPLICATION SOURCE TO SOURCE_LOG_FILE='binlog.000003', SOURCE_LOG_POS=154;
USE ` + "`app`" + `;
CREATE TABLE ` + "`orders`" + ` (` + "`id`" + ` bigint PRIMARY KEY);
INSERT INTO ` + "`orders`" + ` VALUES (1);
-- Dump completed on 2026-11-02 10:00:00
`,
			restoredGTID:   uuid1 + ":1-982341",
			wantGTID:       uuid1 + ":1-982341",
			wantToolVer:    "8.4.3",
			wantWarnings:   1,
			wantBinlogFile: "binlog.000003",
			wantBinlogPos:  154,
		},
		{
			name:     "MultiLineMultiUUIDGTID",
			filename: "multiline.sql",
			content: `-- MySQL dump 10.13  Distrib 8.4.3, for Linux (x86_64)
SET @@GLOBAL.GTID_PURGED=/*!80000 '+'*/ '` + uuid1 + `:1-100,
` + uuid2 + `:1-200';
CREATE TABLE ` + "`orders`" + ` (` + "`id`" + ` bigint PRIMARY KEY);
-- Dump completed on 2026-11-02 10:00:00
`,
			restoredGTID: uuid1 + ":1-100," + uuid2 + ":1-200",
			wantGTID:     uuid1 + ":1-100," + uuid2 + ":1-200",
			wantToolVer:  "8.4.3",
			wantWarnings: 1,
		},
		{
			name:     "FormWithoutPlusComment",
			filename: "no_plus.sql",
			content: `-- MySQL dump 10.13  Distrib 8.0.36, for Linux (x86_64)
SET @@GLOBAL.GTID_PURGED='` + uuid1 + `:1-500';
-- CHANGE MASTER TO MASTER_LOG_FILE='mysql-bin.000002', MASTER_LOG_POS=789;
INSERT INTO ` + "`orders`" + ` VALUES (1);
-- Dump completed on 2026-11-02 10:00:00
`,
			restoredGTID:   uuid1 + ":1-500",
			wantGTID:       uuid1 + ":1-500",
			wantToolVer:    "8.0.36",
			wantWarnings:   1,
			wantBinlogFile: "mysql-bin.000002",
			wantBinlogPos:  789,
		},
		{
			name:     "MissingGTIDPurged_Rejected",
			filename: "no_gtid.sql",
			content: `-- MySQL dump 10.13  Distrib 8.4.3, for Linux (x86_64)
CREATE TABLE ` + "`orders`" + ` (` + "`id`" + ` bigint PRIMARY KEY);
-- Dump completed on 2026-11-02 10:00:00
`,
			wantErr:       true,
			wantExitCode:  exitcode.Rejected,
			wantErrSubstr: "mysqldump file has no GTID_PURGED (dump with --set-gtid-purged=ON on a GTID source)",
		},
		{
			name:     "MissingVersionLine_Rejected",
			filename: "no_version.sql",
			content: `-- Not a mysqldump header
SET @@GLOBAL.GTID_PURGED=/*!80000 '+'*/ '` + uuid1 + `:1-100';
-- Dump completed on 2026-11-02 10:00:00
`,
			wantErr:       true,
			wantExitCode:  exitcode.Rejected,
			wantErrSubstr: "missing version header",
		},
		{
			name:     "TruncatedNoTrailer_Rejected",
			filename: "truncated.sql",
			content: `-- MySQL dump 10.13  Distrib 8.4.3, for Linux (x86_64)
SET @@GLOBAL.GTID_PURGED=/*!80000 '+'*/ '` + uuid1 + `:1-100';
CREATE TABLE ` + "`orders`" + ` (` + "`id`" + ` bigint PRIMARY KEY);
INSERT INTO ` + "`orders`" + ` VALUES (1);
`,
			wantErr:       true,
			wantExitCode:  exitcode.Rejected,
			wantErrSubstr: "mysqldump file is truncated (no '-- Dump completed' trailer)",
		},
		{
			name:     "GzipFileParsed",
			filename: "dump.sql.gz",
			gzip:     true,
			content: `-- MySQL dump 10.13  Distrib 8.4.3, for Linux (x86_64)
SET @@GLOBAL.GTID_PURGED=/*!80000 '+'*/ '` + uuid1 + `:1-777';
CREATE TABLE ` + "`orders`" + ` (` + "`id`" + ` bigint PRIMARY KEY);
`,
			restoredGTID: uuid1 + ":1-777",
			wantGTID:     uuid1 + ":1-777",
			wantToolVer:  "8.4.3",
			wantWarnings: 2,
		},
		{
			name:     "HeaderOver2MiBWithoutGTID_Rejected",
			filename: "over_2mib.sql",
			content: "-- MySQL dump 10.13  Distrib 8.4.3, for Linux (x86_64)\n" +
				largePadding +
				"SET @@GLOBAL.GTID_PURGED=/*!80000 '+'*/ '" + uuid1 + ":1-100';\n" +
				"-- Dump completed on 2026-11-02 10:00:00\n",
			wantErr:       true,
			wantExitCode:  exitcode.Rejected,
			wantErrSubstr: "mysqldump file has no GTID_PURGED (dump with --set-gtid-purged=ON on a GTID source)",
		},
		{
			name:     "CrossCheckMismatch_Rejected",
			filename: "mismatch.sql",
			content: `-- MySQL dump 10.13  Distrib 8.4.3, for Linux (x86_64)
SET @@GLOBAL.GTID_PURGED=/*!80000 '+'*/ '` + uuid1 + `:1-500';
-- Dump completed on 2026-11-02 10:00:00
`,
			restoredGTID:  uuid1 + ":1-400",
			wantErr:       true,
			wantExitCode:  exitcode.Rejected,
			wantErrSubstr: "position cross-check mismatch",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			path := filepath.Join(dir, tc.filename)

			var data []byte
			if tc.gzip {
				var buf bytes.Buffer
				gz := gzip.NewWriter(&buf)
				if _, err := gz.Write([]byte(tc.content)); err != nil {
					t.Fatalf("gzip write: %v", err)
				}
				if err := gz.Close(); err != nil {
					t.Fatalf("gzip close: %v", err)
				}
				data = buf.Bytes()
			} else {
				data = []byte(tc.content)
			}

			if err := os.WriteFile(path, data, 0o640); err != nil {
				t.Fatalf("write fixture: %v", err)
			}

			manifest, err := planner.Plan(ctx, seed.SeedSource{
				Engine:             "mysql",
				Kind:               "mysqldump",
				BackupRef:          path,
				Tables:             []string{"app.orders"},
				TargetVersion:      "8.4.3",
				TargetRestoredGTID: tc.restoredGTID,
			})

			if tc.wantErr {
				if err == nil {
					t.Fatalf("expected error, got nil")
				}
				if code := exitcode.GetExitCode(err); code != tc.wantExitCode {
					t.Fatalf("exit code = %d, want %d (err: %v)", code, tc.wantExitCode, err)
				}
				if tc.wantErrSubstr != "" && !strings.Contains(err.Error(), tc.wantErrSubstr) {
					t.Fatalf("error %q does not contain %q", err.Error(), tc.wantErrSubstr)
				}
				return
			}

			if err != nil {
				t.Fatalf("Plan failed: %v", err)
			}
			if manifest.Kind != "mysqldump" {
				t.Fatalf("Kind = %q, want mysqldump", manifest.Kind)
			}
			if manifest.ToolVersion != tc.wantToolVer {
				t.Fatalf("ToolVersion = %q, want %q", manifest.ToolVersion, tc.wantToolVer)
			}
			if manifest.StartPosition.GTIDSet != tc.wantGTID {
				t.Fatalf("StartPosition.GTIDSet = %q, want %q", manifest.StartPosition.GTIDSet, tc.wantGTID)
			}
			if tc.restoredGTID != "" && (manifest.PositionCrossCheck == nil || !manifest.PositionCrossCheck.Match) {
				t.Fatalf("expected PositionCrossCheck.Match == true")
			}
			if len(manifest.Warnings) != tc.wantWarnings {
				t.Fatalf("len(Warnings) = %d, want %d: %v", len(manifest.Warnings), tc.wantWarnings, manifest.Warnings)
			}
			if !strings.Contains(manifest.Warnings[0], "WARNING: mysqldump does not record its options") {
				t.Fatalf("unexpected first warning: %q", manifest.Warnings[0])
			}

			if tc.wantBinlogFile != "" || tc.wantBinlogPos != 0 {
				meta, mErr := seed.ParseMysqldumpMetadata(path)
				if mErr != nil {
					t.Fatalf("ParseMysqldumpMetadata: %v", mErr)
				}
				if meta.BinlogFile != tc.wantBinlogFile || meta.BinlogPos != tc.wantBinlogPos {
					t.Fatalf("binlog = (%q, %d), want (%q, %d)", meta.BinlogFile, meta.BinlogPos, tc.wantBinlogFile, tc.wantBinlogPos)
				}
			}
		})
	}
}
