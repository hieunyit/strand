package main

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/spf13/cobra"

	"strand/internal/backup"
	"strand/internal/objectstore"
	"strand/internal/seed"
	"strand/internal/web"
)

func newBackupCmd() *cobra.Command {
	cmd := &cobra.Command{Use: "backup", Short: "Manage centralized database backups"}
	cmd.AddCommand(newBackupPushCmd())
	return cmd
}

func newBackupPushCmd() *cobra.Command {
	var manifestPath, artifactPath, bucket, prefix, id string
	cmd := &cobra.Command{
		Use:   "push",
		Short: "Upload a backup artifact and register it in the central catalog",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			ctx, stop := signal.NotifyContext(cmd.Context(), syscall.SIGTERM, os.Interrupt)
			defer stop()

			if manifestPath == "" || artifactPath == "" || bucket == "" {
				return errors.New("--manifest, --artifact, and --bucket are required")
			}
			manifestBytes, manifest, err := readBackupManifest(manifestPath)
			if err != nil {
				return err
			}
			position, engine, err := backupPosition(manifest)
			if err != nil {
				return err
			}
			if id == "" {
				id, err = newBackupID()
				if err != nil {
					return fmt.Errorf("generate backup ID: %w", err)
				}
			}
			cleanPrefix, err := sanitizeBackupPrefix(prefix)
			if err != nil {
				return err
			}
			artifactRoot, err := os.Lstat(artifactPath)
			if err != nil {
				return fmt.Errorf("stat backup artifact: %w", err)
			}
			if artifactRoot.Mode()&os.ModeSymlink != 0 || (!artifactRoot.IsDir() && !artifactRoot.Mode().IsRegular()) {
				return errors.New("backup artifact must be a regular file or directory, not a symlink or special file")
			}
			artifactKey, manifestKey := backupObjectKeys(cleanPrefix, id, artifactRoot.IsDir())
			manifestURI := "s3://" + bucket + "/" + manifestKey
			artifactURI := "s3://" + bucket + "/" + artifactKey

			endpoint := strings.TrimSpace(os.Getenv("STRAND_BACKUP_S3_ENDPOINT"))
			region := strings.TrimSpace(os.Getenv("STRAND_BACKUP_S3_REGION"))
			allowInsecureLocal := strings.EqualFold(strings.TrimSpace(os.Getenv("STRAND_BACKUP_S3_ALLOW_INSECURE_LOCAL")), "true") || strings.TrimSpace(os.Getenv("STRAND_BACKUP_S3_ALLOW_INSECURE_LOCAL")) == "1"
			obj, err := objectstore.New(objectstore.Config{
				Endpoint: endpoint, Bucket: bucket, Region: region,
				AccessKey:          os.Getenv("STRAND_BACKUP_S3_ACCESS_KEY"),
				SecretKey:          os.Getenv("STRAND_BACKUP_S3_SECRET_KEY"),
				SessionToken:       os.Getenv("STRAND_BACKUP_S3_SESSION_TOKEN"),
				AllowInsecureLocal: allowInsecureLocal,
			})
			if err != nil {
				// The endpoint comes from an environment variable and may accidentally
				// contain user info. Do not return configuration values in diagnostics.
				return errors.New("configure backup object storage: invalid settings; check STRAND_BACKUP_S3_ENDPOINT, STRAND_BACKUP_S3_REGION, and credential variables")
			}
			catalogCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
			st, err := web.CLIStore(catalogCtx, cfgPath, nil)
			cancel()
			if err != nil {
				return fmt.Errorf("open backup metadata store: %w", err)
			}
			if st == nil {
				return errors.New("backup metadata database is required; provide --config web.yaml with database.dsn or set STRAND_WEB_DATABASE_DSN")
			}
			defer st.Close()
			catalog, err := backup.NewCatalog(st)
			if err != nil {
				return fmt.Errorf("initialize backup catalog: %w", err)
			}
			record := backup.Record{
				ID: id, Source: manifest.SourceIdentity, Engine: engine,
				Tool: manifest.Kind, ToolVersion: manifest.ToolVersion,
				Bucket: bucket, ObjectKey: artifactKey, Position: position,
				ManifestURI: manifestURI, Status: backup.StatusQueued, CreatedAt: time.Now().UTC(),
				LegalHold: manifest.LegalHoldRef != nil && strings.TrimSpace(*manifest.LegalHoldRef) != "",
			}
			if manifest.BackupExpiresAt != nil && strings.TrimSpace(*manifest.BackupExpiresAt) != "" {
				expiresAt, parseErr := time.Parse(time.RFC3339, strings.TrimSpace(*manifest.BackupExpiresAt))
				if parseErr != nil {
					return errors.New("backup manifest backup_expires_at must be RFC3339")
				}
				expiresAt = expiresAt.UTC()
				record.ExpiresAt = &expiresAt
			}
			return st.Locked(ctx, "backup-upload:"+record.ID, func(lockCtx context.Context) error {
				return pushBackupArtifact(lockCtx, cmd, obj, catalog, record, artifactPath, artifactKey, manifestKey, artifactURI, manifestURI, manifestBytes)
			})
		},
	}
	cmd.Flags().StringVar(&manifestPath, "manifest", "", "seed manifest JSON path")
	cmd.Flags().StringVar(&artifactPath, "artifact", "", "backup artifact file or directory")
	cmd.Flags().StringVar(&bucket, "bucket", "", "destination S3 bucket")
	cmd.Flags().StringVar(&prefix, "prefix", "", "optional object key prefix")
	cmd.Flags().StringVar(&id, "id", "", "optional stable backup ID")
	return cmd
}

func pushBackupArtifact(ctx context.Context, cmd *cobra.Command, obj *objectstore.S3, catalog *backup.Catalog, requested backup.Record, artifactPath, artifactKey, manifestKey, artifactURI, manifestURI string, manifestBytes []byte) error {
	record, inserted, err := catalog.Create(ctx, requested)
	if err != nil {
		return fmt.Errorf("create backup catalog record: %w", err)
	}
	if !inserted && record.Status == backup.StatusComplete {
		fmt.Fprintf(cmd.OutOrStdout(), "backup %s is already complete\n", record.ID)
		return nil
	}
	if record.Status == backup.StatusUploading || record.Status == backup.StatusVerifying {
		// The advisory lock proves no uploader for this ID is still active. Recover a
		// stale in-progress record left by a crash before replacing the same S3 keys.
		_, _, err = catalog.UpdateStatus(ctx, record.ID, backup.StatusFailed, nil)
		if err != nil {
			return fmt.Errorf("recover interrupted backup upload: %w", err)
		}
		record.Status = backup.StatusFailed
	}
	if record.Status != backup.StatusQueued && record.Status != backup.StatusFailed {
		return fmt.Errorf("backup %s has unsupported status %q", record.ID, record.Status)
	}
	_, changed, err := catalog.UpdateStatus(ctx, record.ID, backup.StatusUploading, nil)
	if err != nil {
		return fmt.Errorf("start backup upload: %w", err)
	}
	if !changed {
		return fmt.Errorf("backup %s is already being processed", record.ID)
	}
	fail := func(cause error) error {
		failedCtx, failCancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
		defer failCancel()
		_, _, _ = catalog.UpdateStatus(failedCtx, record.ID, backup.StatusFailed, nil)
		return cause
	}
	reader, size, err := backup.OpenArtifact(ctx, artifactPath)
	if err != nil {
		return fail(fmt.Errorf("open backup artifact: %w", err))
	}
	artifactInfo, uploadErr := obj.Upload(ctx, artifactKey, reader, size, "")
	closeErr := reader.Close()
	if uploadErr != nil {
		return fail(fmt.Errorf("upload backup artifact: %w", uploadErr))
	}
	if closeErr != nil {
		return fail(fmt.Errorf("close backup artifact stream: %w", closeErr))
	}
	_, changed, err = catalog.UpdateStatus(ctx, record.ID, backup.StatusVerifying, nil)
	if err != nil {
		return fail(fmt.Errorf("start backup verification: %w", err))
	}
	if !changed {
		return fail(fmt.Errorf("backup %s left uploading state before verification", record.ID))
	}
	head, err := obj.Head(ctx, artifactKey)
	if err != nil {
		return fail(fmt.Errorf("verify uploaded backup artifact: %w", err))
	}
	if head.Size != artifactInfo.Size {
		return fail(fmt.Errorf("verify uploaded backup artifact: object size %d differs from streamed size %d", head.Size, artifactInfo.Size))
	}
	if artifactInfo.SHA256 == "" {
		return fail(errors.New("verify uploaded backup artifact: upload did not return a streamed SHA-256 digest"))
	}
	// Replace the machine-local backup path in the sidecar with the managed object
	// URI so catalog consumers never mistake the producer's local path for storage.
	managedManifest, err := setManifestBackupRef(manifestBytes, artifactURI)
	if err != nil {
		return fail(fmt.Errorf("encode managed backup manifest: %w", err))
	}
	manifestInfo, err := obj.Upload(ctx, manifestKey, bytes.NewReader(managedManifest), int64(len(managedManifest)), "")
	if err != nil {
		return fail(fmt.Errorf("upload backup manifest sidecar: %w", err))
	}
	if manifestInfo.Size != int64(len(managedManifest)) {
		return fail(errors.New("upload backup manifest sidecar: uploaded size does not match manifest size"))
	}
	verifiedAt := time.Now().UTC()
	_, _, err = catalog.Finalize(ctx, record.ID, backup.StatusVerifying, artifactInfo.Size, artifactInfo.SHA256, manifestURI, verifiedAt)
	if err != nil {
		return fail(fmt.Errorf("finalize backup catalog record: %w", err))
	}
	fmt.Fprintf(cmd.OutOrStdout(), "backup %s complete (%d bytes)\n", record.ID, artifactInfo.Size)
	return nil
}

func setManifestBackupRef(raw []byte, artifactURI string) ([]byte, error) {
	var manifest map[string]json.RawMessage
	if err := json.Unmarshal(raw, &manifest); err != nil {
		return nil, fmt.Errorf("decode manifest object: %w", err)
	}
	ref, err := json.Marshal(artifactURI)
	if err != nil {
		return nil, err
	}
	manifest["backup_ref"] = ref
	return json.MarshalIndent(manifest, "", "  ")
}

func readBackupManifest(path string) ([]byte, seed.SeedManifest, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, seed.SeedManifest{}, fmt.Errorf("read backup manifest: %w", err)
	}
	var manifest seed.SeedManifest
	dec := json.NewDecoder(bytes.NewReader(raw))
	if err := dec.Decode(&manifest); err != nil {
		return nil, seed.SeedManifest{}, fmt.Errorf("decode backup manifest: %w", err)
	}
	var extra any
	if err := dec.Decode(&extra); err != io.EOF {
		return nil, seed.SeedManifest{}, errors.New("decode backup manifest: expected exactly one JSON value")
	}
	if manifest.Version != 1 || strings.TrimSpace(manifest.SourceIdentity) == "" || strings.TrimSpace(manifest.Kind) == "" {
		return nil, seed.SeedManifest{}, errors.New("backup manifest must be v1 and include kind and source_identity")
	}
	return raw, manifest, nil
}

func backupPosition(manifest seed.SeedManifest) (backup.Position, string, error) {
	engine := strings.ToLower(strings.TrimSpace(manifest.Engine))
	if engine == "pg" {
		engine = "postgres"
	}
	switch engine {
	case "mysql":
		if manifest.StartPosition.GTIDSet == "" && (manifest.StartPosition.BinlogFile == "" || manifest.StartPosition.BinlogPos == 0) {
			return backup.Position{}, "", errors.New("MySQL backup manifest must contain a GTID set or a binlog file and positive binlog position")
		}
		if manifest.StartPosition.BinlogPos > math.MaxInt64 {
			return backup.Position{}, "", errors.New("backup manifest binlog position exceeds supported range")
		}
		return backup.Position{
			GTIDSet:    manifest.StartPosition.GTIDSet,
			BinlogFile: manifest.StartPosition.BinlogFile,
			BinlogPos:  int64(manifest.StartPosition.BinlogPos),
		}, engine, nil
	case "postgres":
		if strings.TrimSpace(manifest.StartPosition.LSN) == "" {
			return backup.Position{}, "", errors.New("PostgreSQL backup manifest must contain a start_position.lsn")
		}
		return backup.Position{LSN: manifest.StartPosition.LSN}, engine, nil
	default:
		return backup.Position{}, "", errors.New("backup manifest engine must be mysql, pg, or postgres")
	}
}

func newBackupID() (string, error) {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", err
	}
	return "bkp-" + hex.EncodeToString(b[:]), nil
}

func sanitizeBackupPrefix(prefix string) (string, error) {
	if len(prefix) > 512 {
		return "", errors.New("--prefix must be no longer than 512 bytes")
	}
	prefix = strings.Trim(prefix, "/")
	if prefix == "" {
		return "", nil
	}
	parts := strings.Split(prefix, "/")
	clean := make([]string, 0, len(parts))
	for _, part := range parts {
		var b strings.Builder
		for _, r := range part {
			if r >= 'a' && r <= 'z' || r >= 'A' && r <= 'Z' || r >= '0' && r <= '9' || r == '-' || r == '_' || r == '.' {
				b.WriteRune(r)
			} else {
				b.WriteByte('-')
			}
		}
		segment := strings.Trim(b.String(), ".-_ ")
		if segment != "" {
			clean = append(clean, segment)
		}
	}
	return strings.Join(clean, "/"), nil
}

func backupObjectKeys(prefix, id string, directory bool) (string, string) {
	base := id
	if prefix != "" {
		base = prefix + "/" + id
	}
	artifact := base + "/artifact"
	if directory {
		artifact += ".tar"
	}
	return artifact, base + "/manifest.json"
}
