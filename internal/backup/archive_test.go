package backup

import (
	"archive/tar"
	"context"
	"io"
	"os"
	"path/filepath"
	"testing"
)

func TestOpenArtifactDirectoryTar(t *testing.T) {
	d := filepath.Join(t.TempDir(), "xb")
	os.MkdirAll(filepath.Join(d, "a"), 0o700)
	os.WriteFile(filepath.Join(d, "a", "f1"), []byte("hello"), 0o600)
	r, size, err := OpenArtifact(context.Background(), d)
	if err != nil || size != -1 {
		t.Fatalf("open: %v size %d", err, size)
	}
	defer r.Close()
	tr := tar.NewReader(r)
	names := map[string]string{}
	for {
		h, err := tr.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		b, _ := io.ReadAll(tr)
		names[h.Name] = string(b)
	}
	if names["xb/a/f1"] != "hello" {
		t.Fatalf("tar entries = %v", names)
	}
}

func TestOpenArtifactRejectsSymlinks(t *testing.T) {
	d := t.TempDir()
	os.WriteFile(filepath.Join(d, "f"), []byte("x"), 0o600)
	os.Symlink("/etc/passwd", filepath.Join(d, "link"))
	r, _, err := OpenArtifact(context.Background(), d)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	if _, err := io.Copy(io.Discard, r); err == nil {
		t.Fatal("a symlink inside the artifact must fail the stream")
	}
	if _, _, err := OpenArtifact(context.Background(), filepath.Join(d, "link")); err == nil {
		t.Fatal("a symlink root must be rejected")
	}
}

func TestCursorRoundTrip(t *testing.T) {
	c := Cursor{ID: "bkp-1"}
	got, err := ParseCursor(c.Encode())
	if err != nil || got.ID != "bkp-1" {
		t.Fatalf("cursor = %+v %v", got, err)
	}
}
