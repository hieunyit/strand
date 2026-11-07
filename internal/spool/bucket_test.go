package spool_test

import (
	"context"
	"strand/internal/testutil"
	"testing"
	"time"

	"strand/internal/spool"
)

func TestMinIOBackend_Direct(t *testing.T) {
	endpoint := testutil.MinIOEndpoint(t)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	b, err := spool.NewMinIOBackend(spool.MinIOConfig{
		Endpoint:  endpoint,
		Bucket:    "strand-test-bucket",
		AccessKey: "minioadmin",
		SecretKey: "minioadmin",
	})
	if err != nil {
		t.Fatalf("NewMinIOBackend failed: %v", err)
	}
	defer b.Close()

	key := "test/hello.txt"
	data := []byte("hello strand minio staging")

	// 1. Put
	if err := b.Put(ctx, key, data); err != nil {
		t.Fatalf("Put failed: %v", err)
	}

	// 2. Get
	got, err := b.Get(ctx, key)
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}
	if string(got) != string(data) {
		t.Fatalf("Get content mismatch: got %q, want %q", string(got), string(data))
	}

	// 3. List
	keys, err := b.List(ctx, "test/")
	if err != nil {
		t.Fatalf("List failed: %v", err)
	}
	if len(keys) == 0 || keys[0] != key {
		t.Fatalf("List keys mismatch: got %v", keys)
	}

	// 4. Delete
	if err := b.Delete(ctx, key); err != nil {
		t.Fatalf("Delete failed: %v", err)
	}

	// 5. Get after delete should fail
	_, err = b.Get(ctx, key)
	if err == nil {
		t.Fatalf("expected error after delete, got nil")
	}
}
