package web

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"testing"
	"time"

	"strand/internal/backup"
	"strand/internal/web/store"
)

// Helper to seed a rich backup record into catalog
func seedRichBackup(t *testing.T, st store.Store, id, src, engine, tool, kind string, at time.Time, pos backup.Position, hold bool, retain *time.Time, lockMode string, start, end *time.Time, parentID string, details map[string]string) backup.Record {
	t.Helper()
	cat, err := backup.NewCatalog(st)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	method := tool
	if tool == "mysqldump" {
		method = backup.MethodMySQLDump
	} else if tool == "binlog" {
		method = backup.MethodBinlog
	} else if tool == "pg_dump" {
		method = backup.MethodPGDump
	}
	r := backup.Record{
		ID:          id,
		Source:      src,
		Engine:      engine,
		Tool:        tool,
		ToolVersion: "8.4.1",
		Bucket:      "test-repo",
		ObjectKey:   "backups/" + id + ".bin",
		ManifestURI: "s3://test-repo/backups/" + id + ".json",
		Status:      backup.StatusQueued,
		CreatedAt:   at,
		Kind:        kind,
		ParentID:    parentID,
		Method:      method,
		Details:     details,
	}
	if details["source_identity"] != "" {
		r.JobID = "job-" + src
	}
	if _, _, err := cat.Create(ctx, r); err != nil {
		t.Fatal(err)
	}
	_, _, _ = cat.UpdateStatus(ctx, id, backup.StatusUploading, nil)
	stTime := at
	if start != nil {
		stTime = *start
	}
	meta := backup.Meta{
		Position:    pos,
		StartTime:   &stTime,
		Format:      backup.FormatSBK1,
		LockMode:    lockMode,
		RetainUntil: retain,
		Details:     details,
	}
	if method == backup.MethodBinlog && pos.BinlogFile != "" {
		meta.EndPosition = backup.Position{BinlogFile: pos.BinlogFile, BinlogPos: 2048}
	}
	if method == backup.MethodPGBaseBackup {
		meta.EndPosition = pos
	}
	if method == backup.MethodWAL {
		meta.EndPosition = backup.Position{LSN: details["end_lsn"]}
	}
	if end != nil {
		meta.EndTime = end
	} else if kind == backup.KindLog {
		ed := at.Add(30 * time.Minute)
		meta.EndTime = &ed
	}
	if _, _, err := cat.UpdateMeta(ctx, id, engine, meta); err != nil {
		t.Fatal(err)
	}
	_, _, _ = cat.UpdateStatus(ctx, id, backup.StatusVerifying, nil)
	ver := at.Add(time.Minute)
	rec, _, err := cat.Finalize(ctx, id, backup.StatusVerifying, 1048576, strings.Repeat("a", 64), r.ManifestURI, ver)
	if err != nil {
		t.Fatal(err)
	}
	if hold {
		rec, _, err = cat.SetLegalHold(ctx, id, true)
		if err != nil {
			t.Fatal(err)
		}
	}
	return rec
}

func TestCatalogFilterAndTimeline(t *testing.T) {
	e := newCPEnv(t)
	now := time.Now().UTC().Truncate(time.Second)

	// Seed backups
	pos1 := backup.Position{GTIDSet: "0000-1111:1-100", BinlogFile: "mysql-bin.000001", BinlogPos: 1234}
	b1 := seedRichBackup(t, e.st, "bkp-full-1", "mysql-app", "mysql", "mysqldump", backup.KindFull, now.Add(-3*time.Hour), pos1, false, nil, "", nil, nil, "", nil)
	pos2 := backup.Position{GTIDSet: "0000-1111:1-200"}
	seedRichBackup(t, e.st, "bkp-inc-1", "mysql-app", "mysql", "mysqldump", backup.KindIncremental, now.Add(-2*time.Hour), pos2, true, nil, "", nil, nil, "bkp-full-1", nil)
	seedRichBackup(t, e.st, "bkp-log-1", "mysql-app", "mysql", "binlog", backup.KindLog, now.Add(-90*time.Minute), backup.Position{}, false, nil, "", nil, nil, "", nil)
	seedRichBackup(t, e.st, "bkp-pg-1", "pg-main", "postgres", "pg_dump", backup.KindFull, now.Add(-1*time.Hour), backup.Position{LSN: "0/16B3748"}, false, nil, "", nil, nil, "", nil)

	adm := e.admin()

	// 1. Catalog recovery uses only the displayed records and never promises PITR from unproven log spans
	code, body := adm.do("GET", "/backups/catalog", nil)
	if code != http.StatusOK {
		t.Fatalf("catalog returned %d", code)
	}
	for _, want := range []string{
		"Recovery by source",
		"Show timeline records",
		"Logs available · restorable window unconfirmed",
		`href="/backups/catalog/bkp-log-1"`,
		`href="/backups/restore?backup=bkp-full-1"`,
		b1.ID,
	} {
		if !strings.Contains(body, want) {
			t.Errorf("catalog missing %q in body", want)
		}
	}

	// 2. Query filter: source
	code, body = adm.do("GET", "/backups/catalog?source=pg-main", nil)
	if code != http.StatusOK {
		t.Fatalf("filtered catalog returned %d", code)
	}
	if !strings.Contains(body, "bkp-pg-1") || strings.Contains(body, "bkp-full-1") {
		t.Errorf("filter source failed: %s", body)
	}

	// 3. Query filter: legal_hold=1
	code, body = adm.do("GET", "/backups/catalog?legal_hold=1", nil)
	if code != http.StatusOK {
		t.Fatalf("legal_hold=1 returned %d", code)
	}
	if !strings.Contains(body, "bkp-inc-1") || strings.Contains(body, "bkp-full-1") {
		t.Errorf("filter legal_hold=1 failed: %s", body)
	}

	// 4. Query filter: engine
	code, body = adm.do("GET", "/backups/catalog?engine=postgres", nil)
	if code != http.StatusOK {
		t.Fatalf("engine=postgres returned %d", code)
	}
	if !strings.Contains(body, "bkp-pg-1") || strings.Contains(body, "bkp-full-1") {
		t.Errorf("filter engine failed: %s", body)
	}
}

func TestBackupDetailAndRBAC(t *testing.T) {
	e := newCPEnv(t)
	now := time.Now().UTC().Truncate(time.Second)
	lockTime := now.Add(30 * 24 * time.Hour)
	pos := backup.Position{GTIDSet: "0000-1111:1-500", BinlogFile: "mysql-bin.000002", BinlogPos: 5678}
	bkp := seedRichBackup(t, e.st, "bkp-detail-1", "mysql-app", "mysql", "mysqldump", backup.KindFull, now.Add(-2*time.Hour), pos, false, &lockTime, "COMPLIANCE", nil, nil, "", nil)

	ctx := context.Background()
	_ = e.st.CreateBackupRepo(ctx, backup.Repo{ID: "vault-repo", Name: "Vault", Bucket: "vault-b", EnvPrefix: "STRAND_BACKUP_REPO_VAULT_"})

	// Admin view
	adm := e.admin()
	code, body := adm.do("GET", "/backups/catalog/"+bkp.ID, nil)
	if code != http.StatusOK {
		t.Fatalf("admin detail returned %d", code)
	}
	for _, want := range []string{
		bkp.ID,
		"8.4.1",
		"COMPLIANCE",
		`data-copy="0000-1111:1-500"`,
		`data-copy="mysql-bin.000002:5678"`,
		"Restore from this backup",
		"Verify",
		"Copy to…",
		"Set legal hold",
		"Download manifest",
		`id="copy-drawer"`,
	} {
		if !strings.Contains(body, want) {
			t.Errorf("admin detail missing %q", want)
		}
	}

	// Operator view
	oper := e.roleClient(store.RoleOperator)
	code, body = oper.do("GET", "/backups/catalog/"+bkp.ID, nil)
	if code != http.StatusOK {
		t.Fatalf("operator detail returned %d", code)
	}
	if !strings.Contains(body, "Verify") || !strings.Contains(body, "Download manifest") {
		t.Errorf("operator should see Verify and Download manifest")
	}
	for _, forbidden := range []string{"Restore from this backup", "Copy to…", "Set legal hold", "id=\"copy-drawer\""} {
		if strings.Contains(body, forbidden) {
			t.Errorf("operator detail should NOT contain %q", forbidden)
		}
	}

	// Viewer view
	view := e.roleClient(store.RoleViewer)
	code, body = view.do("GET", "/backups/catalog/"+bkp.ID, nil)
	if code != http.StatusOK {
		t.Fatalf("viewer detail returned %d", code)
	}
	if !strings.Contains(body, "Download manifest") {
		t.Errorf("viewer should see Download manifest")
	}
	for _, forbidden := range []string{"Restore from this backup", "Verify", "Copy to…", "Set legal hold", "id=\"copy-drawer\""} {
		if strings.Contains(body, forbidden) {
			t.Errorf("viewer detail should NOT contain %q", forbidden)
		}
	}
}

func TestBackupManifestDownload(t *testing.T) {
	e := newCPEnv(t)
	now := time.Now().UTC().Truncate(time.Second)
	pos := backup.Position{GTIDSet: "0000-1111:1-500"}
	bkp := seedRichBackup(t, e.st, "bkp-manifest-1", "mysql-app", "mysql", "mysqldump", backup.KindFull, now, pos, false, nil, "", nil, nil, "", nil)

	for _, client := range []*apiClient{e.admin(), e.roleClient(store.RoleOperator), e.roleClient(store.RoleViewer)} {
		code, body := client.do("GET", "/api/v1/backups/"+bkp.ID+"/manifest", nil)
		if code != http.StatusOK {
			t.Fatalf("manifest download returned %d", code)
		}
		var parsed backup.Record
		if err := json.Unmarshal([]byte(body), &parsed); err != nil {
			t.Fatalf("invalid json manifest: %v", err)
		}
		if parsed.ID != bkp.ID || parsed.Source != bkp.Source || parsed.Position.GTIDSet != pos.GTIDSet {
			t.Errorf("manifest mismatch: %+v", parsed)
		}
	}
}

func TestBackupActionsRBAC(t *testing.T) {
	e := newCPEnv(t)
	now := time.Now().UTC().Truncate(time.Second)
	bkp := seedRichBackup(t, e.st, "bkp-rbac-1", "mysql-app", "mysql", "mysqldump", backup.KindFull, now, backup.Position{}, false, nil, "", nil, nil, "", nil)
	ctx := context.Background()
	_ = e.st.CreateBackupRepo(ctx, backup.Repo{ID: "vault-repo", Name: "Vault", Bucket: "vault-b", EnvPrefix: "STRAND_BACKUP_REPO_VAULT_"})

	adm := e.admin()
	oper := e.roleClient(store.RoleOperator)
	view := e.roleClient(store.RoleViewer)

	// 1. Verify: Operator + Admin allowed (202), Viewer forbidden (403)
	code, _ := view.do("POST", "/api/v1/backups/"+bkp.ID+"/verify", nil)
	if code != http.StatusForbidden {
		t.Errorf("viewer verify should be 403, got %d", code)
	}
	code, _ = oper.do("POST", "/api/v1/backups/"+bkp.ID+"/verify", nil)
	if code != http.StatusAccepted {
		t.Errorf("operator verify should be 202, got %d", code)
	}
	code, _ = adm.do("POST", "/api/v1/backups/"+bkp.ID+"/verify", nil)
	if code != http.StatusAccepted {
		t.Errorf("admin verify should be 202, got %d", code)
	}

	// 2. Legal hold: Admin only (200), Operator and Viewer forbidden (403)
	code, _ = view.do("POST", "/api/v1/backups/"+bkp.ID+"/legal-hold", map[string]any{"hold": true})
	if code != http.StatusForbidden {
		t.Errorf("viewer legal-hold should be 403, got %d", code)
	}
	code, _ = oper.do("POST", "/api/v1/backups/"+bkp.ID+"/legal-hold", map[string]any{"hold": true})
	if code != http.StatusForbidden {
		t.Errorf("operator legal-hold should be 403, got %d", code)
	}
	code, _ = adm.do("POST", "/api/v1/backups/"+bkp.ID+"/legal-hold", map[string]any{"hold": true})
	if code != http.StatusOK {
		t.Errorf("admin legal-hold should be 200, got %d", code)
	}

	// 3. Copy: Admin only (202), Operator and Viewer forbidden (403)
	code, _ = view.do("POST", "/api/v1/backups/"+bkp.ID+"/copy", map[string]any{"repository": "vault-repo"})
	if code != http.StatusForbidden {
		t.Errorf("viewer copy should be 403, got %d", code)
	}
	code, _ = oper.do("POST", "/api/v1/backups/"+bkp.ID+"/copy", map[string]any{"repository": "vault-repo"})
	if code != http.StatusForbidden {
		t.Errorf("operator copy should be 403, got %d", code)
	}
	code, _ = adm.do("POST", "/api/v1/backups/"+bkp.ID+"/copy", map[string]any{"repository": "vault-repo"})
	if code != http.StatusAccepted {
		t.Errorf("admin copy should be 202, got %d", code)
	}
}

func TestRestoreWizardPageAndRBAC(t *testing.T) {
	e := newCPEnv(t)
	now := time.Now().UTC().Truncate(time.Second)
	bkp := seedRichBackup(t, e.st, "bkp-wiz-1", "mysql-prod", "mysql", "mysqldump", backup.KindFull, now, backup.Position{}, false, nil, "", nil, nil, "", nil)

	adm := e.admin()
	code, body := adm.do("GET", "/backups/restore?backup="+bkp.ID, nil)
	if code != http.StatusOK {
		t.Fatalf("restore page returned %d", code)
	}
	for _, want := range []string{
		"restore-stepper",
		"aria-current=\"step\"",
		"Restore point",
		"Target server",
		"Restore options",
		"Review and confirm",
		"restore-form",
		"start-restore-btn",
	} {
		if !strings.Contains(body, want) {
			t.Errorf("restore page missing %q", want)
		}
	}

	// Operator / Viewer on page: sees admin restriction note
	oper := e.roleClient(store.RoleOperator)
	code, body = oper.do("GET", "/backups/restore?backup="+bkp.ID, nil)
	if code != http.StatusOK {
		t.Fatalf("operator restore page returned %d", code)
	}
	if !strings.Contains(body, "Starting a restore requires admin permissions") {
		t.Errorf("operator should see admin restriction note on restore page")
	}

	// Restore API: Operator and Viewer get 403
	req := map[string]any{
		"backup_id":       bkp.ID,
		"connection_id":   restoreConn(t, e.st, "mysql"),
		"target_database": "new_db",
		"confirm":         "new_db",
	}
	view := e.roleClient(store.RoleViewer)
	code, _ = view.do("POST", "/api/v1/backup-restores", req)
	if code != http.StatusForbidden {
		t.Errorf("viewer POST restore should be 403, got %d", code)
	}
	code, _ = oper.do("POST", "/api/v1/backup-restores", req)
	if code != http.StatusForbidden {
		t.Errorf("operator POST restore should be 403, got %d", code)
	}
}

func TestRestoreLegacyIncrementalRequiresNativeTool(t *testing.T) {
	e := newCPEnv(t)
	at := time.Now().UTC()
	base := seedRichBackup(t, e.st, "legacy-base", "legacy", "mysql", "mysqldump", backup.KindFull, at, backup.Position{}, false, nil, "", nil, nil, "", nil)
	inc := seedRichBackup(t, e.st, "legacy-inc", base.Source, base.Engine, base.Method, backup.KindIncremental, at.Add(time.Minute), backup.Position{}, false, nil, "", nil, nil, base.ID, nil)
	adm := e.admin()
	code, body := adm.do("GET", "/backups/restore?backup="+inc.ID, nil)
	if code != http.StatusOK || !strings.Contains(body, "native restore tool") {
		t.Fatalf("wizard should direct legacy incremental to native restore: status %d", code)
	}
	code, body = adm.do("POST", "/api/v1/backup-restores", map[string]any{"backup_id": inc.ID})
	if code != http.StatusBadRequest || !strings.Contains(body, "native restore tool") {
		t.Fatalf("legacy incremental restore: status %d body %s", code, body)
	}
}

func TestRestorePITRWindowRejection(t *testing.T) {
	e := newCPEnv(t)
	baseTime := time.Date(2026, 11, 20, 10, 0, 0, 0, time.UTC)
	// Base backup at 10:00
	pos := backup.Position{GTIDSet: "0000:1-100", BinlogFile: "mysql-bin.000001", BinlogPos: 1234}
	baseEnd := baseTime.Add(time.Minute)
	seedRichBackup(t, e.st, "bkp-pitr-base", "mysql-pitr", "mysql", "mysqldump", backup.KindFull, baseTime, pos, false, nil, "", nil, &baseEnd, "", map[string]string{"source_identity": "mysql-fixture"})

	// 2 continuous log segments: 10:00 - 11:00, and 11:00 - 12:00
	l1Start := baseTime
	l1End := baseTime.Add(1 * time.Hour)
	seedRichBackup(t, e.st, "bkp-pitr-log1", "mysql-pitr", "mysql", "binlog", backup.KindLog, baseTime, backup.Position{GTIDSet: "0000:101-200", BinlogFile: "mysql-bin.000001", BinlogPos: 4}, false, nil, "", &l1Start, &l1End, "", map[string]string{"seq": "1", "source_identity": "mysql-fixture", "next_file": "mysql-bin.000002"})

	l2Start := baseTime.Add(1 * time.Hour)
	l2End := baseTime.Add(2 * time.Hour)
	seedRichBackup(t, e.st, "bkp-pitr-log2", "mysql-pitr", "mysql", "binlog", backup.KindLog, baseTime.Add(1*time.Hour), backup.Position{GTIDSet: "0000:201-300", BinlogFile: "mysql-bin.000002", BinlogPos: 4}, false, nil, "", &l2Start, &l2End, "", map[string]string{"seq": "2", "source_identity": "mysql-fixture"})

	adm := e.admin()

	// 1. Target time BEFORE window (09:00 < 10:00) -> 400 Bad Request
	reqBefore := map[string]any{
		"source":          "mysql-pitr",
		"target_time":     "2026-11-20T09:00:00Z",
		"connection_id":   restoreConn(t, e.st, "mysql"),
		"target_database": "pitr_early",
		"confirm":         "pitr_early",
	}
	code, body := adm.do("POST", "/api/v1/backup-restores", reqBefore)
	if code != http.StatusBadRequest {
		t.Fatalf("expected 400 for time before window, got %d: %s", code, body)
	}
	if !strings.Contains(body, "outside PITR recovery window") {
		t.Errorf("error message should state outside PITR recovery window: %s", body)
	}
	reqBefore["target_time"] = baseTime.Add(30 * time.Second).Format(time.RFC3339)
	if code, body = adm.do("POST", "/api/v1/backup-restores", reqBefore); code != http.StatusBadRequest || !strings.Contains(body, "outside PITR recovery window") {
		t.Fatalf("expected 400 for a time during the dump, got %d: %s", code, body)
	}

	// 2. Target time AFTER window (13:00 > 12:00) -> 400 Bad Request
	reqAfter := map[string]any{
		"source":          "mysql-pitr",
		"target_time":     "2026-11-20T13:00:00Z",
		"connection_id":   restoreConn(t, e.st, "mysql"),
		"target_database": "pitr_late",
		"confirm":         "pitr_late",
	}
	code, body = adm.do("POST", "/api/v1/backup-restores", reqAfter)
	if code != http.StatusBadRequest {
		t.Fatalf("expected 400 for time after window, got %d: %s", code, body)
	}
	if !strings.Contains(body, "outside PITR recovery window") {
		t.Errorf("error message should state outside PITR recovery window: %s", body)
	}

	// 3. Target time INSIDE window (11:30) -> 202 Accepted
	reqValid := map[string]any{
		"source":          "mysql-pitr",
		"target_time":     "2026-11-20T11:30:00Z",
		"connection_id":   restoreConn(t, e.st, "mysql"),
		"target_database": "pitr_valid",
		"confirm":         "pitr_valid",
	}
	code, body = adm.do("POST", "/api/v1/backup-restores", reqValid)
	if code != http.StatusAccepted {
		t.Fatalf("expected 202 for valid PITR time, got %d: %s", code, body)
	}
}

func TestRestoreTargetPasswordPassedViaEnvNotArgv(t *testing.T) {
	e := newCPEnv(t)
	now := time.Now().UTC().Truncate(time.Second)
	bkp := seedRichBackup(t, e.st, "bkp-env-1", "mysql-app", "mysql", "mysqldump", backup.KindFull, now, backup.Position{}, false, nil, "", nil, nil, "", nil)

	// Create a stored connection with password secret
	ctx := context.Background()
	secretPW := "SuperSecretTargetPass123!"
	sealed, err := e.srv.db.box.Seal([]byte(secretPW), "conn-target")
	if err != nil {
		t.Fatal(err)
	}
	conn := backup.Connection{
		ID:             "conn-target",
		Name:           "Target DB",
		Engine:         "mysql",
		Host:           "restore-host.internal",
		Port:           3306,
		User:           "restore_user",
		SecretMode:     backup.SecretStored,
		PasswordSealed: sealed,
		PasswordSet:    true,
	}
	if _, err := e.st.CreateBackupConnection(ctx, conn); err != nil {
		t.Fatal(err)
	}

	adm := e.admin()
	req := map[string]any{
		"backup_id":       bkp.ID,
		"connection_id":   conn.ID,
		"target_database": "restored_db",
		"confirm":         "restored_db",
	}
	code, body := adm.do("POST", "/api/v1/backup-restores", req)
	if code != http.StatusAccepted {
		t.Fatalf("expected 202, got %d: %s", code, body)
	}

	// Verify the started job spec
	specs := e.rec.take()
	if len(specs) != 1 {
		t.Fatalf("expected 1 recorded job spec, got %d", len(specs))
	}
	st := specs[0]

	// The secret password MUST NOT be in argv (args)
	for _, arg := range st.Args {
		if strings.Contains(arg, secretPW) {
			t.Fatalf("password leaked in argv: %q", arg)
		}
	}

	// The secret password MUST be in environment
	var foundEnv bool
	for _, env := range st.Env {
		if env == "STRAND_RESTORE_TARGET_PASSWORD="+secretPW || env == "MYSQL_PWD="+secretPW {
			foundEnv = true
			break
		}
	}
	if !foundEnv {
		t.Fatalf("target password was not passed via process environment: %v", st.Env)
	}
}

func TestRestoreRejectTargetOverridesWhenConnectionIDSet(t *testing.T) {
	e := newCPEnv(t)
	now := time.Now().UTC().Truncate(time.Second)
	bkp := seedRichBackup(t, e.st, "bkp-b8-1", "mysql-app", "mysql", "mysqldump", backup.KindFull, now, backup.Position{}, false, nil, "", nil, nil, "", nil)

	ctx := context.Background()
	conn := backup.Connection{
		ID:          "conn-b8",
		Name:        "Target DB",
		Engine:      "mysql",
		Host:        "restore-host.internal",
		Port:        3306,
		User:        "restore_user",
		SecretMode:  backup.SecretEnv,
		PasswordEnv: "PASS",
	}
	if _, err := e.st.CreateBackupConnection(ctx, conn); err != nil {
		t.Fatal(err)
	}

	adm := e.admin()
	req := map[string]any{
		"backup_id":       bkp.ID,
		"connection_id":   conn.ID,
		"target":          map[string]any{"host": "evil"},
		"target_database": "restored_db",
		"confirm":         "restored_db",
	}
	code, body := adm.do("POST", "/api/v1/backup-restores", req)
	if code != http.StatusBadRequest {
		t.Fatalf("expected 400 for target override with connection_id, got %d: %s", code, body)
	}
	if !strings.Contains(body, "target fields are taken from the saved connection") {
		t.Fatalf("expected error message to state target fields are taken from the saved connection, got %s", body)
	}
}

func TestRestoreRejectThreadsOption(t *testing.T) {
	e := newCPEnv(t)
	now := time.Now().UTC().Truncate(time.Second)
	bkp := seedRichBackup(t, e.st, "bkp-b4-1", "mysql-app", "mysql", "mysqldump", backup.KindFull, now, backup.Position{}, false, nil, "", nil, nil, "", nil)

	adm := e.admin()
	req := map[string]any{
		"backup_id":       bkp.ID,
		"threads":         4,
		"connection_id":   restoreConn(t, e.st, "mysql"),
		"target_database": "restored_db",
		"confirm":         "restored_db",
	}
	code, body := adm.do("POST", "/api/v1/backup-restores", req)
	if code != http.StatusBadRequest {
		t.Fatalf("expected 400 for threads option, got %d: %s", code, body)
	}
}

func TestRestoreRejectTargetTimeWhenNoContinuousPITRWindow(t *testing.T) {
	e := newCPEnv(t)
	now := time.Now().UTC().Truncate(time.Second)
	// Only full backups, no continuous log window
	seedRichBackup(t, e.st, "bkp-b5-full", "mysql-nocont", "mysql", "mysqldump", backup.KindFull, now.Add(-2*time.Hour), backup.Position{}, false, nil, "", nil, nil, "", nil)

	adm := e.admin()
	req := map[string]any{
		"source":          "mysql-nocont",
		"target_time":     now.Add(-1 * time.Hour).Format(time.RFC3339),
		"connection_id":   restoreConn(t, e.st, "mysql"),
		"target_database": "restored_db",
		"confirm":         "restored_db",
	}
	code, body := adm.do("POST", "/api/v1/backup-restores", req)
	if code != http.StatusBadRequest {
		t.Fatalf("expected 400 when no continuous PITR window, got %d: %s", code, body)
	}
	if !strings.Contains(body, "has no continuous PITR window") {
		t.Fatalf("expected error to mention no continuous PITR window, got %s", body)
	}
}

func TestPostgresRestoreOffersReplayProofWithoutContinuousWindow(t *testing.T) {
	e := newCPEnv(t)
	baseTime := time.Date(2026, 11, 20, 10, 0, 0, 0, time.UTC)
	source := "pg-pitr"
	base := seedRichBackup(t, e.st, "pg-base", source, backup.EnginePostgres, backup.MethodPGBaseBackup,
		backup.KindFull, baseTime, backup.Position{LSN: "0/18"}, false, nil, "", nil, nil, "",
		map[string]string{"source_identity": "pg-system", "timeline": "1"})
	req := map[string]any{"source": source, "target_time": baseTime.Add(time.Hour).Format(time.RFC3339),
		"target_dir": "/tmp/pg-pitr-proof", "confirm": "/tmp/pg-pitr-proof"}
	admin := e.admin()
	admin.must("POST", "/api/v1/backup-restores", req, http.StatusBadRequest)
	seedRichBackup(t, e.st, "pg-wal", source, backup.EnginePostgres, backup.MethodWAL, backup.KindLog,
		baseTime, backup.Position{LSN: "0/10"}, false, nil, "", nil, nil, "",
		map[string]string{"source_identity": "pg-system", "timeline": "1", "end_lsn": "0/20", "seq": "1"})
	win, err := e.srv.recoveryWindow(t.Context(), source)
	if err != nil || win.Continuous || !win.VerifyOnReplay {
		t.Fatalf("PostgreSQL window: %+v, %v", win, err)
	}
	html := admin.must("GET", "/backups/restore", nil, http.StatusOK)
	if !strings.Contains(html, "verify_on_replay") {
		t.Fatal("restore wizard hides provisional PostgreSQL PITR")
	}
	dumpPage(t, "restore_postgres_pitr.html", html)
	admin.must("POST", "/api/v1/backup-restores", req, http.StatusAccepted)
	delete(req, "source")
	req["backup_id"] = base.ID
	admin.must("POST", "/api/v1/backup-restores", req, http.StatusAccepted)
}

func TestRestoreRejectConnectionEngineMismatch(t *testing.T) {
	e := newCPEnv(t)
	now := time.Now().UTC().Truncate(time.Second)
	bkp := seedRichBackup(t, e.st, "bkp-b2-mysql", "mysql-app", "mysql", "mysqldump", backup.KindFull, now, backup.Position{}, false, nil, "", nil, nil, "", nil)

	ctx := context.Background()
	conn := backup.Connection{
		ID:          "conn-pg",
		Name:        "Postgres Target",
		Engine:      "postgres",
		Host:        "pg.internal",
		Port:        5432,
		User:        "postgres",
		SecretMode:  backup.SecretEnv,
		PasswordEnv: "PASS",
	}
	if _, err := e.st.CreateBackupConnection(ctx, conn); err != nil {
		t.Fatal(err)
	}

	adm := e.admin()
	req := map[string]any{
		"backup_id":       bkp.ID,
		"connection_id":   conn.ID,
		"target_database": "restored_db",
		"confirm":         "restored_db",
	}
	code, body := adm.do("POST", "/api/v1/backup-restores", req)
	if code != http.StatusBadRequest {
		t.Fatalf("expected 400 for engine mismatch, got %d: %s", code, body)
	}
	if !strings.Contains(body, "target connection engine postgres does not match backup engine mysql") {
		t.Fatalf("expected engine mismatch error message, got %s", body)
	}
}

func TestCatalogLegalHoldPaging(t *testing.T) {
	e := newCPEnv(t)
	now := time.Now().UTC().Truncate(time.Second)

	// Seed 6 backups: 4 not held (newer), 2 held (older)
	// With limit 2, page 1 and page 2 are not held, page 3 has the held ones.
	for i := 1; i <= 4; i++ {
		seedRichBackup(t, e.st, fmt.Sprintf("bkp-nh-%d", i), "mysql-app", "mysql", "mysqldump", backup.KindFull, now.Add(-time.Duration(i)*time.Hour), backup.Position{}, false, nil, "", nil, nil, "", nil)
	}
	for i := 1; i <= 2; i++ {
		seedRichBackup(t, e.st, fmt.Sprintf("bkp-hold-%d", i), "mysql-app", "mysql", "mysqldump", backup.KindFull, now.Add(-time.Duration(4+i)*time.Hour), backup.Position{}, true, nil, "", nil, nil, "", nil)
	}

	// 1. Direct store query with limit 2 and LegalHold = true must return the 2 held backups immediately
	holdTrue := true
	res, err := e.st.ListBackups(context.Background(), backup.Filter{Limit: 2, LegalHold: &holdTrue})
	if err != nil {
		t.Fatal(err)
	}
	if len(res) != 2 {
		t.Fatalf("expected 2 held backups in first page of legal_hold=true, got %d", len(res))
	}

	for _, r := range res {
		if !r.LegalHold {
			t.Fatalf("expected legal_hold=true, got record %s with legal_hold=false", r.ID)
		}
	}

	// 2. HTTP page query /backups/catalog?legal_hold=1
	adm := e.admin()
	code, body := adm.do("GET", "/backups/catalog?legal_hold=1", nil)
	if code != http.StatusOK {
		t.Fatalf("expected 200 from catalog, got %d", code)
	}
	if !strings.Contains(body, "bkp-hold-1") || !strings.Contains(body, "bkp-hold-2") {
		t.Fatalf("expected held backups in catalog response, got: %s", body)
	}
	if strings.Contains(body, "bkp-nh-1") {
		t.Fatalf("expected non-held backups excluded when legal_hold=1, got: %s", body)
	}
}

func TestBackupCatalogDatabaseScopeUsesRecordedMetadata(t *testing.T) {
	e := newCPEnv(t)
	now := time.Now().UTC()
	for _, tc := range []struct {
		id      string
		details map[string]string
		want    string
	}{
		{"bkp-scope-unknown", nil, "Unknown"},
		{"bkp-scope-selected", map[string]string{"databases": "shop"}, "<code>shop</code>"},
		{"bkp-scope-all", map[string]string{"database_scope": "all"}, "All databases"},
	} {
		seedRichBackup(t, e.st, tc.id, tc.id, "mysql", "mysqldump", backup.KindFull, now,
			backup.Position{}, false, nil, "", nil, nil, "", tc.details)
		for _, path := range []string{"/backups/catalog?source=" + tc.id, "/backups/catalog/" + tc.id} {
			code, body := e.admin().do("GET", path, nil)
			if code != http.StatusOK || !strings.Contains(body, "Database scope") || !strings.Contains(body, tc.want+"</td>") {
				t.Errorf("%s: status %d, expected scope %q", path, code, tc.want)
			}
		}
	}
}

func TestCatalogRecoveryWindowStartsRestoreAtSelectedTime(t *testing.T) {
	e := newCPEnv(t)
	end := time.Date(2026, 11, 20, 12, 0, 5, 0, time.UTC)
	baseAt := end.Add(-2 * time.Hour)
	start := end.Add(-3 * time.Hour)
	const file = "mysql-bin.000001"
	base := seedRichBackup(t, e.st, "timeline-base", "mysql-prod", "mysql", "mysqldump", backup.KindFull, baseAt,
		backup.Position{GTIDSet: "uuid:1-100", BinlogFile: file, BinlogPos: 1234}, false, nil, "", &baseAt, &baseAt, "", map[string]string{"source_identity": "mysql-fixture"})
	log := seedRichBackup(t, e.st, "timeline-log", "mysql-prod", "mysql", "binlog", backup.KindLog, end,
		backup.Position{BinlogFile: file, BinlogPos: 4}, false, nil, "", &start, &end, "", map[string]string{"seq": "1", "source_identity": "mysql-fixture"})

	rows := catalogRecoverySources([]backup.Record{base, log})
	if len(rows) != 1 || !rows[0].Window.Continuous || rows[0].RestoreURL == "" {
		t.Fatalf("wanted a confirmed recovery window, got %+v", rows)
	}
	_, page := e.admin().do("GET", rows[0].RestoreURL, nil)
	if !strings.Contains(page, `value="2026-11-20T19:00:05"`) {
		t.Fatal("selected recovery time was not prefilled in UTC+7")
	}
}
