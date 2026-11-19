package objectstore

import "context"

// InitiateForTest starts a multipart upload and abandons it, reproducing what a killed uploader leaves
// behind. It exists for cross-package crash-recovery tests.
func (s *S3) InitiateForTest(ctx context.Context, key string) error {
	_, err := s.initiateMultipart(ctx, key, "")
	return err
}
