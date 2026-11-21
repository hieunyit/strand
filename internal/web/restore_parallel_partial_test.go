package web

import (
	"context"
	"encoding/json"
	"net/http"
	"strings"
	"testing"
	"time"

	"strand/internal/backup"
	"strand/internal/web/store"
)

func TestRestoreArgsParallelPartial(t *testing.T) {
	// 1. Valid logical restore with threads and objects for pg_dump
	req := RestoreRequest{
		BackupID: "bkp-pg1",
		Target: backup.ConnSpec{
			Host: "pg-restore.internal",
			Port: 5432,
			User: "postgres",
		},
		TargetDB: "pg_new",
		Confirm:  "pg_new",
		Threads:  4,
		Objects:  []string{"users", "public.orders", "audit_log"},
	}
	args, err := restoreArgs(req)
	if err != nil {
		t.Fatalf("expected valid restoreArgs, got: %v", err)
	}
	argsStr := strings.Join(args, " ")
	if !strings.Contains(argsStr, "--threads 4") {
		t.Errorf("expected --threads 4 in args, got: %v", args)
	}
	if !strings.Contains(argsStr, "--objects users,public.orders,audit_log") {
		t.Errorf("expected --objects in args, got: %v", args)
	}

	// 2. Physical restore: threads rejected
	reqPhysThreads := RestoreRequest{
		BackupID:  "bkp-phys1",
		TargetDir: "/var/lib/strand/restore-dir",
		Confirm:   "/var/lib/strand/restore-dir",
		Threads:   4,
	}
	if _, err := restoreArgs(reqPhysThreads); err == nil || !strings.Contains(err.Error(), "parallel restore is not available for physical backups") {
		t.Errorf("expected error rejecting threads on physical restore, got: %v", err)
	}

	// 3. Physical restore: objects rejected
	reqPhysObjects := RestoreRequest{
		BackupID:  "bkp-phys2",
		TargetDir: "/var/lib/strand/restore-dir",
		Confirm:   "/var/lib/strand/restore-dir",
		Objects:   []string{"table1"},
	}
	if _, err := restoreArgs(reqPhysObjects); err == nil || !strings.Contains(err.Error(), "partial restore is not available for physical backups") {
		t.Errorf("expected error rejecting objects on physical restore, got: %v", err)
	}

	// 4. Invalid threads range (<0 or >16)
	reqBadThreads := req
	reqBadThreads.Threads = 17
	if _, err := restoreArgs(reqBadThreads); err == nil || !strings.Contains(err.Error(), "threads must be between 1 and 16") {
		t.Errorf("expected error for threads=17, got: %v", err)
	}
	reqBadThreads.Threads = -1
	if _, err := restoreArgs(reqBadThreads); err == nil || !strings.Contains(err.Error(), "threads must be between 1 and 16") {
		t.Errorf("expected error for threads=-1, got: %v", err)
	}

	// 5. Invalid object names rejected
	reqBadObjectName := req
	reqBadObjectName.Objects = []string{"table1", "bad;name--"}
	if _, err := restoreArgs(reqBadObjectName); err == nil || !strings.Contains(err.Error(), "invalid object name") {
		t.Errorf("expected error for invalid object name, got: %v", err)
	}

	// 6. Too many objects (>1000) rejected
	tooMany := make([]string, 1001)
	for i := range tooMany {
		tooMany[i] = "tbl"
	}
	reqTooMany := req
	reqTooMany.Objects = tooMany
	if _, err := restoreArgs(reqTooMany); err == nil || !strings.Contains(err.Error(), "too many objects") {
		t.Errorf("expected error for >1000 objects, got: %v", err)
	}
}

func TestAPIRestoreParallelPartialMatrix(t *testing.T) {
	e := newCPEnv(t)
	now := time.Date(2026, 11, 20, 12, 0, 0, 0, time.UTC)
	ctx := context.Background()

	// Seed backups for pg_dump, mysqldump, and pg_basebackup
	seedRichBackup(t, e.st, "bkp-pg", "pg-prod", "postgres", "pg_dump", backup.KindFull, now, backup.Position{}, false, nil, "", nil, nil, "", map[string]string{"objects": "users,orders"})
	seedRichBackup(t, e.st, "bkp-my", "my-prod", "mysql", "mysqldump", backup.KindFull, now, backup.Position{}, false, nil, "", nil, nil, "", map[string]string{"objects": "tbl1,tbl2", "databases": "shop"})
	seedRichBackup(t, e.st, "bkp-phys", "phys-prod", "postgres", "pg_basebackup", backup.KindFull, now, backup.Position{}, false, nil, "", nil, nil, "", nil)
	seedRichBackup(t, e.st, "bkp-mongo-whole", "mongo-prod", "mongodb", "mongodump", backup.KindFull, now, backup.Position{}, false, nil, "", nil, nil, "", nil)

	adm := e.admin()

	// A. pg_dump: threads=4 and objects=["users"] accepted (202)
	code, body := adm.do("POST", "/api/v1/backup-restores", map[string]any{
		"backup_id":       "bkp-pg",
		"target":          map[string]any{"host": "pg-restore.internal", "port": 5432},
		"target_database": "pg_restored",
		"confirm":         "pg_restored",
		"threads":         4,
		"objects":         []string{"users"},
	})
	if code != http.StatusAccepted {
		t.Fatalf("expected 202 for pg_dump parallel+partial, got %d: %s", code, body)
	}

	// Verify audit detail recorded threads and object_count
	auditEntries, err := e.st.ListAudit(ctx, 10)
	if err != nil {
		t.Fatalf("ListAudit: %v", err)
	}
	var foundRestoreAudit bool
	for _, a := range auditEntries {
		if a.Action == "backup.restore" {
			foundRestoreAudit = true
			var entry struct {
				Detail map[string]any `json:"detail"`
			}
			if err := json.Unmarshal(a.Data, &entry); err != nil {
				t.Fatalf("unmarshal audit data: %v", err)
			}
			detail := entry.Detail
			if th, ok := detail["threads"].(float64); !ok || int(th) != 4 {
				t.Errorf("audit detail threads want 4, got: %v", detail["threads"])
			}
			if cnt, ok := detail["object_count"].(float64); !ok || int(cnt) != 1 {
				t.Errorf("audit detail object_count want 1, got: %v", detail["object_count"])
			}
			break
		}
	}
	if !foundRestoreAudit {
		t.Error("expected audit log for backup.restore")
	}

	// B. mysqldump: threads > 1 rejected (400)
	code, body = adm.do("POST", "/api/v1/backup-restores", map[string]any{
		"backup_id":       "bkp-my",
		"target":          map[string]any{"host": "my-restore.internal", "port": 3306},
		"target_database": "my_restored",
		"confirm":         "my_restored",
		"threads":         4,
	})
	if code != http.StatusBadRequest {
		t.Fatalf("expected 400 for mysqldump with threads > 1, got %d: %s", code, body)
	}
	if !strings.Contains(body, "parallel restore is not available for mysqldump backups") {
		t.Errorf("expected mysqldump threads rejection message, got: %s", body)
	}

	// C. mysqldump: partial restore without target_time accepted (202)
	code, body = adm.do("POST", "/api/v1/backup-restores", map[string]any{
		"backup_id":       "bkp-my",
		"target":          map[string]any{"host": "my-restore.internal", "port": 3306},
		"target_database": "my_restored_tbl1",
		"confirm":         "my_restored_tbl1",
		"objects":         []string{"tbl1"},
	})
	if code != http.StatusAccepted {
		t.Fatalf("expected 202 for mysqldump partial restore, got %d: %s", code, body)
	}

	// D. mysqldump: partial restore WITH target_time rejected (400)
	code, body = adm.do("POST", "/api/v1/backup-restores", map[string]any{
		"backup_id":       "bkp-my",
		"target_time":     now.Format(time.RFC3339),
		"target":          map[string]any{"host": "my-restore.internal", "port": 3306},
		"target_database": "my_pitr_partial",
		"confirm":         "my_pitr_partial",
		"objects":         []string{"tbl1"},
	})
	if code != http.StatusBadRequest {
		t.Fatalf("expected 400 for mysqldump partial+target_time, got %d: %s", code, body)
	}
	if !strings.Contains(body, "partial restore with point-in-time recovery is not supported for MySQL") {
		t.Errorf("expected mysql pitr+partial rejection message, got: %s", body)
	}

	// E. pg_basebackup: objects rejected (400)
	code, body = adm.do("POST", "/api/v1/backup-restores", map[string]any{
		"backup_id":  "bkp-phys",
		"target_dir": "/var/lib/strand/restore-phys",
		"confirm":    "/var/lib/strand/restore-phys",
		"objects":    []string{"tbl1"},
	})
	if code != http.StatusBadRequest {
		t.Fatalf("expected 400 for pg_basebackup with objects, got %d: %s", code, body)
	}
	if !strings.Contains(body, "partial restore is not available for physical backups") {
		t.Errorf("expected physical partial rejection message, got: %s", body)
	}

	// F. pg_basebackup: threads rejected (400)
	code, body = adm.do("POST", "/api/v1/backup-restores", map[string]any{
		"backup_id":  "bkp-phys",
		"target_dir": "/var/lib/strand/restore-phys2",
		"confirm":    "/var/lib/strand/restore-phys2",
		"threads":    4,
	})
	if code != http.StatusBadRequest {
		t.Fatalf("expected 400 for pg_basebackup with threads, got %d: %s", code, body)
	}
	if !strings.Contains(body, "parallel restore is not available for physical backups") {
		t.Errorf("expected physical threads rejection message, got: %s", body)
	}

	// G. MongoDB whole-deployment: partial rejected (400)
	code, body = adm.do("POST", "/api/v1/backup-restores", map[string]any{
		"backup_id":       "bkp-mongo-whole",
		"target":          map[string]any{"host": "mongo.internal", "port": 27017},
		"target_database": "mongo_new",
		"confirm":         "mongo_new",
		"objects":         []string{"coll1"},
	})
	if code != http.StatusBadRequest {
		t.Fatalf("expected 400 for MongoDB whole-deployment partial restore, got %d: %s", code, body)
	}
	if !strings.Contains(body, "partial restore is not available for whole-deployment MongoDB backups") {
		t.Errorf("expected mongo whole-deployment partial rejection message, got: %s", body)
	}

	// H. RBAC unchanged: operator and viewer get 403 Forbidden
	view := e.roleClient(store.RoleViewer)
	code, _ = view.do("POST", "/api/v1/backup-restores", map[string]any{
		"backup_id":       "bkp-pg",
		"target":          map[string]any{"host": "pg-restore.internal"},
		"target_database": "pg_view",
		"confirm":         "pg_view",
	})
	if code != http.StatusForbidden {
		t.Errorf("expected 403 for viewer, got %d", code)
	}

	oper := e.roleClient(store.RoleOperator)
	code, _ = oper.do("POST", "/api/v1/backup-restores", map[string]any{
		"backup_id":       "bkp-pg",
		"target":          map[string]any{"host": "pg-restore.internal"},
		"target_database": "pg_oper",
		"confirm":         "pg_oper",
	})
	if code != http.StatusForbidden {
		t.Errorf("expected 403 for operator, got %d", code)
	}
}

func TestAPIUnknownTableWithKnownListRejected(t *testing.T) {
	e := newCPEnv(t)
	now := time.Date(2026, 11, 20, 12, 0, 0, 0, time.UTC)
	adm := e.admin()

	// Seed backup with known objects
	seedRichBackup(t, e.st, "bkp-my-known", "my-known", "mysql", "mysqldump", backup.KindFull, now, backup.Position{}, false, nil, "", nil, nil, "", map[string]string{"objects": "tbl1,tbl2", "databases": "shop"})

	code, body := adm.do("POST", "/api/v1/backup-restores", map[string]any{
		"backup_id":       "bkp-my-known",
		"target":          map[string]any{"host": "mysql.internal", "port": 3306},
		"target_database": "shop_restored",
		"confirm":         "shop_restored",
		"objects":         []string{"unknown_tbl"},
	})
	if code != http.StatusBadRequest {
		t.Fatalf("expected 400 for unknown table with known list, got %d: %s", code, body)
	}
	if !strings.Contains(body, `unknown_tbl`) || !strings.Contains(body, `not found in backup`) {
		t.Errorf("expected table not found error, got: %s", body)
	}
}

func TestAPIRestoreSourceBaseWithLogAccepted(t *testing.T) {
	e := newCPEnv(t)
	tt := time.Date(2026, 11, 20, 10, 0, 0, 0, time.UTC)
	adm := e.admin()

	tBase := tt.Add(-2 * time.Hour)
	tLogStart := tt.Add(-3 * time.Hour)
	tLogEnd := tt.Add(2 * time.Hour)

	// Seed base mongodump backup followed by a newer oplog log segment covering target_time
	posBase := backup.Position{OplogTS: "7000000000:1"}
	seedRichBackup(t, e.st, "bkp-mongo-base", "mongo-src", "mongodb", "mongodump", backup.KindFull, tBase, posBase, false, nil, "", &tBase, nil, "", map[string]string{"databases": "app"})
	seedRichBackup(t, e.st, "bkp-mongo-oplog", "mongo-src", "mongodb", "oplog", backup.KindLog, tLogEnd, backup.Position{OplogTS: "7000000010:1"}, false, nil, "", &tLogStart, &tLogEnd, "", map[string]string{"seq": "1"})

	code, body := adm.do("POST", "/api/v1/backup-restores", map[string]any{
		"source":          "mongo-src",
		"target_time":     tt.Format(time.RFC3339),
		"threads":         4,
		"target":          map[string]any{"host": "mongo.internal", "port": 27017},
		"target_database": "mongo_restored",
		"confirm":         "mongo_restored",
	})
	if code != http.StatusAccepted {
		t.Fatalf("expected 202 for source restore with mongodump base followed by oplog, got %d: %s", code, body)
	}
}
