package web

import (
	"context"
	"strings"
	"testing"
	"time"

	"strand/internal/objectstore"
	"strand/internal/testutil"
)

// The repository test checklist against a real S3-compatible server (MinIO): list/put/get/delete of the test
// object, latency, and Object Lock detection on a bucket created with and without it.
func TestRepoTestChecklistAgainstMinIO(t *testing.T) {
	endpoint := testutil.MinIOEndpoint(t)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	mkBucket := func(name string, lock bool) {
		c, err := objectstore.New(objectstore.Config{Endpoint: endpoint, Bucket: name, AccessKey: "minioadmin", SecretKey: "minioadmin", AllowInsecureLocal: true})
		if err != nil {
			t.Fatal(err)
		}
		if lock {
			err = c.CreateBucketWithObjectLock(ctx)
		} else {
			err = c.CreateBucket(ctx)
		}
		if err != nil {
			t.Fatalf("create bucket %s: %v", name, err)
		}
	}
	mkBucket("plain-bucket", false)
	mkBucket("locked-bucket", true)

	e := newP56Env(t, nil)
	a := e.admin()
	mkRepo := func(name, bucket, secret string, lockMode string) string {
		body := map[string]any{"name": name, "secret_mode": "stored", "endpoint": endpoint, "region": "us-east-1", "bucket": bucket, "prefix": "strand",
			"allow_insecure_local": true, "access_key": "minioadmin", "secret_key": secret}
		if lockMode != "" {
			body["lock_mode"], body["lock_days"] = lockMode, 1
		}
		return decode(t, a.must("POST", "/api/v1/backup-repositories", body, 200))["repository"].(map[string]any)["id"].(string)
	}
	run := func(id string) (map[string]any, map[string]map[string]any) {
		out := a.must("POST", "/api/v1/backup-repositories/"+id+"/test", nil, 200)
		res := decode(t, out)
		steps := map[string]map[string]any{}
		for _, s := range res["steps"].([]any) {
			m := s.(map[string]any)
			steps[m["name"].(string)] = m
		}
		return res, steps
	}

	// Plain bucket: everything passes, Object Lock reported as off, latency recorded.
	res, steps := run(mkRepo("Plain", "plain-bucket", "minioadmin", ""))
	if res["ok"] != true || res["status"] != "ok" || res["object_lock_enabled"] != false {
		t.Fatalf("plain bucket result = %v", res)
	}
	for _, n := range []string{"list", "put", "get", "delete", "object_lock"} {
		if steps[n]["status"] != "ok" {
			t.Fatalf("step %s = %v", n, steps[n])
		}
	}
	if _, ok := res["latency_ms"].(float64); !ok || steps["put"]["latency_ms"] == nil {
		t.Fatalf("latency missing: %v", res)
	}
	if !strings.Contains(steps["object_lock"]["detail"].(string), "not enabled") {
		t.Fatalf("object lock detail = %v", steps["object_lock"])
	}

	// Locked bucket + COMPLIANCE repository: Object Lock detected; the test object is written unlocked and deleted.
	res, steps = run(mkRepo("Vault", "locked-bucket", "minioadmin", "COMPLIANCE"))
	if res["ok"] != true || res["object_lock_enabled"] != true || res["immutable_configured"] != true || steps["delete"]["status"] != "ok" {
		t.Fatalf("locked bucket result = %v", res)
	}

	// A repository that must be immutable on a bucket without Object Lock fails the Object Lock step.
	res, steps = run(mkRepo("FakeVault", "plain-bucket", "minioadmin", "COMPLIANCE"))
	if res["ok"] != false || steps["object_lock"]["status"] != "failed" || !strings.Contains(steps["object_lock"]["detail"].(string), "COMPLIANCE") || steps["put"]["status"] != "ok" {
		t.Fatalf("immutable repo on a plain bucket = %v", res)
	}

	// Wrong secret key: the first step fails, the rest are skipped, and the key is not echoed.
	wrong := "wrong-secret-" + canary
	id := mkRepo("BadKey", "plain-bucket", wrong, "")
	out := a.must("POST", "/api/v1/backup-repositories/"+id+"/test", nil, 200)
	if strings.Contains(out, wrong) || strings.Contains(out, canary) {
		t.Fatalf("the secret key is echoed: %s", out)
	}
	res, steps = run(id)
	if res["ok"] != false || steps["list"]["status"] != "failed" || steps["put"]["status"] != "skipped" {
		t.Fatalf("wrong key result = %v", res)
	}

	// Missing bucket.
	body := map[string]any{"name": "NoBucket", "secret_mode": "stored", "endpoint": endpoint, "bucket": "does-not-exist", "allow_insecure_local": true,
		"access_key": "minioadmin", "secret_key": "minioadmin"}
	id = decode(t, a.must("POST", "/api/v1/backup-repositories", body, 200))["repository"].(map[string]any)["id"].(string)
	if res, steps = run(id); res["ok"] != false || steps["list"]["status"] != "failed" {
		t.Fatalf("missing bucket result = %v", res)
	}
}
