package backup

import (
	"errors"
	"io"
	"sync/atomic"
)

// RepositoryError marks a failure of the backup repository (object storage) as opposed to a failure of
// the database, the native tool or the local encoder. NormalizeErrorCode maps it to repo_unreachable.
type RepositoryError struct{ Err error }

func (e *RepositoryError) Error() string { return "backup repository: " + e.Err.Error() }
func (e *RepositoryError) Unwrap() error { return e.Err }

// errUploadEnded is what the encoder side of the pipe sees once the uploader stopped reading.
var errUploadEnded = errors.New("backup repository upload ended before the stream was fully read")

// pipeSink records whether a write into the upload pipe failed: that only happens after the uploader
// stopped reading, so the upload error (not the resulting write error) is the cause of the failure.
type pipeSink struct {
	w      io.Writer
	failed atomic.Bool
}

func (s *pipeSink) Write(p []byte) (int, error) {
	n, err := s.w.Write(p)
	if err != nil {
		s.failed.Store(true)
	}
	return n, err
}

// EncodeUpload streams src through an Encoder (compression/encryption per opt) into upload, which reads
// the encoded stream. The returned error names the real cause: a source/tool or encoder failure is
// returned as is; a failure of the upload is returned as a *RepositoryError, even though it also makes
// the encoder's pending write fail.
func EncodeUpload[T any](src io.Reader, opt EncodeOptions, upload func(io.Reader) (T, error)) (T, StreamStats, error) {
	pr, pw := io.Pipe()
	sink := &pipeSink{w: pw}
	var enc *Encoder
	done := make(chan error, 1)
	go func() {
		var err error
		if enc, err = NewEncoder(sink, opt); err == nil {
			if _, err = io.Copy(enc, src); err == nil {
				err = enc.Close()
			}
		}
		_ = pw.CloseWithError(err)
		done <- err
	}()
	out, upErr := upload(pr)
	_ = pr.CloseWithError(errUploadEnded)
	srcErr := <-done
	switch {
	case upErr != nil && (srcErr == nil || sink.failed.Load()):
		return out, StreamStats{}, &RepositoryError{Err: upErr}
	case srcErr != nil:
		return out, StreamStats{}, srcErr
	}
	return out, enc.Stats(), nil
}
