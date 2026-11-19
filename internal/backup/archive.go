package backup

import (
	"archive/tar"
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
)

// OpenArtifact opens a regular backup file for streaming, or streams a directory as a tar archive.
// Directory archives have unknown size (-1) and never buffer the complete artifact in memory.
// Symlinks and special files are rejected so a later restore cannot silently follow paths outside
// the artifact root.
func OpenArtifact(ctx context.Context, path string) (io.ReadCloser, int64, error) {
	if ctx == nil {
		return nil, 0, fmt.Errorf("backup artifact: nil context")
	}
	abs, err := filepath.Abs(path)
	if err != nil {
		return nil, 0, fmt.Errorf("backup artifact path: %w", err)
	}
	rootInfo, err := os.Lstat(abs)
	if err != nil {
		return nil, 0, fmt.Errorf("stat backup artifact: %w", err)
	}
	if rootInfo.Mode()&os.ModeSymlink != 0 {
		return nil, 0, fmt.Errorf("backup artifact root must not be a symlink")
	}
	if rootInfo.Mode().IsRegular() {
		f, err := os.Open(abs)
		if err != nil {
			return nil, 0, fmt.Errorf("open backup artifact: %w", err)
		}
		opened, err := f.Stat()
		if err != nil || !opened.Mode().IsRegular() || !os.SameFile(rootInfo, opened) || opened.Size() != rootInfo.Size() {
			_ = f.Close()
			if err != nil {
				return nil, 0, fmt.Errorf("stat opened backup artifact: %w", err)
			}
			return nil, 0, fmt.Errorf("backup artifact changed while opening")
		}
		return f, rootInfo.Size(), nil
	}
	if !rootInfo.IsDir() {
		return nil, 0, fmt.Errorf("backup artifact must be a regular file or directory")
	}
	if abs == string(filepath.Separator) {
		return nil, 0, fmt.Errorf("backup artifact cannot be the filesystem root")
	}
	archiveRoot := filepath.Base(abs)
	if archiveRoot == string(filepath.Separator) || archiveRoot == "." || archiveRoot == "" {
		// Defensive fallback: archive member names must always be relative.
		archiveRoot = "artifact"
	}
	r, w := io.Pipe()
	jobCtx, cancel := context.WithCancel(ctx)
	stream := &archiveReadCloser{ReadCloser: r, cancel: cancel}
	go func() {
		tw := tar.NewWriter(w)
		err := filepath.WalkDir(abs, func(current string, entry os.DirEntry, walkErr error) error {
			if walkErr != nil {
				return walkErr
			}
			select {
			case <-jobCtx.Done():
				return jobCtx.Err()
			default:
			}
			info, err := os.Lstat(current)
			if err != nil {
				return err
			}
			if info.Mode()&os.ModeSymlink != 0 {
				return fmt.Errorf("backup artifact contains symlink %q; symlinks are not archived", current)
			}
			if !info.IsDir() && !info.Mode().IsRegular() {
				return fmt.Errorf("backup artifact contains unsupported file type at %q", current)
			}
			rel, err := filepath.Rel(abs, current)
			if err != nil {
				return err
			}
			name := archiveRoot
			if rel != "." {
				name = filepath.Join(name, rel)
			}
			header, err := tar.FileInfoHeader(info, "")
			if err != nil {
				return err
			}
			header.Name = strings.TrimPrefix(filepath.ToSlash(name), "./")
			if err := tw.WriteHeader(header); err != nil {
				return err
			}
			if !info.Mode().IsRegular() {
				return nil
			}
			f, err := os.Open(current)
			if err != nil {
				return err
			}
			opened, err := f.Stat()
			if err == nil && (!opened.Mode().IsRegular() || !os.SameFile(info, opened) || opened.Size() != info.Size()) {
				err = fmt.Errorf("backup artifact file changed while being archived: %q", current)
			}
			if err == nil {
				_, err = io.CopyN(tw, f, info.Size())
			}
			closeErr := f.Close()
			if err != nil {
				return err
			}
			return closeErr
		})
		closeErr := tw.Close()
		if err == nil {
			err = closeErr
		}
		_ = w.CloseWithError(err)
	}()
	return stream, -1, nil
}

type archiveReadCloser struct {
	io.ReadCloser
	cancel context.CancelFunc
}

func (r *archiveReadCloser) Close() error {
	r.cancel()
	return r.ReadCloser.Close()
}
