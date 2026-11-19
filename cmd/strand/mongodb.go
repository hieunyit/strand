package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/spf13/cobra"
	"strand/internal/mongocdc"
)

func newMongoDBCmd() *cobra.Command {
	var pipeline, sourceURIEnv, sourceDB, targetURIEnv, targetDB string
	var include, exclude []string
	var overwrite bool
	var leaseTTL time.Duration
	var snapshotBatch, streamBatch int
	root := &cobra.Command{
		Use:   "mongodb",
		Short: "MongoDB CDC commands",
	}
	syncCmd := &cobra.Command{
		Use:   "sync",
		Short: "Snapshot MongoDB collections and follow change streams to MongoDB",
		Long: `Copies selected collections, then continuously applies MongoDB change streams.
EXPERIMENTAL. Source and target must be replica sets (a single-node replica set is
fine); sharded clusters are refused. Checkpoints and the process lease are stored in
the target database _strand_cdc and committed in the same target transaction as the
data they cover. URI values should be supplied through
environment variables to avoid exposing credentials in shell history or process lists.`,
		RunE: func(cmd *cobra.Command, _ []string) error {
			missing := make([]string, 0, 5)
			for name, value := range map[string]string{"pipeline": pipeline, "source-uri-env": sourceURIEnv, "source-database": sourceDB, "target-uri-env": targetURIEnv, "target-database": targetDB} {
				if strings.TrimSpace(value) == "" {
					missing = append(missing, "--"+name)
				}
			}
			if len(missing) != 0 {
				return fmt.Errorf("required flags missing: %s", strings.Join(missing, ", "))
			}
			sourceURI := os.Getenv(sourceURIEnv)
			targetURI := os.Getenv(targetURIEnv)
			if sourceURI == "" || targetURI == "" {
				return errors.New("MongoDB URI environment variable is empty")
			}
			ctx, stop := signal.NotifyContext(cmd.Context(), os.Interrupt, syscall.SIGTERM)
			defer stop()
			logger := slog.New(slog.NewJSONHandler(os.Stderr, nil))
			err := mongocdc.Run(ctx, mongocdc.Config{
				Pipeline: pipeline, SourceURI: sourceURI, SourceDB: sourceDB,
				TargetURI: targetURI, TargetDB: targetDB,
				Include: include, Exclude: exclude, OverwriteTarget: overwrite,
				Logger: logger, LeaseDuration: leaseTTL, SnapshotBatch: snapshotBatch, StreamBatch: streamBatch,
			})
			if errors.Is(err, context.Canceled) && ctx.Err() != nil {
				return nil
			}
			return err
		},
	}
	syncCmd.Flags().StringVar(&pipeline, "pipeline", "", "Stable checkpoint and lease name")
	syncCmd.Flags().StringVar(&sourceURIEnv, "source-uri-env", "MONGO_SOURCE_URI", "Environment variable containing the MongoDB source URI")
	syncCmd.Flags().StringVar(&sourceDB, "source-database", "", "Source database")
	syncCmd.Flags().StringVar(&targetURIEnv, "target-uri-env", "MONGO_TARGET_URI", "Environment variable containing the MongoDB target URI")
	syncCmd.Flags().StringVar(&targetDB, "target-database", "", "Target database")
	syncCmd.Flags().StringSliceVar(&include, "include", nil, "Included database.collection glob (repeatable)")
	syncCmd.Flags().StringSliceVar(&exclude, "exclude", nil, "Excluded database.collection glob (repeatable)")
	syncCmd.Flags().BoolVar(&overwrite, "overwrite-target", false, "Drop and recreate matching target collections during initial snapshot")
	syncCmd.Flags().DurationVar(&leaseTTL, "lease-ttl", 0, "Lease time-to-live on the target clock (default 45s)")
	syncCmd.Flags().IntVar(&snapshotBatch, "snapshot-batch", 0, "Documents per snapshot write transaction (default 1000)")
	syncCmd.Flags().IntVar(&streamBatch, "stream-batch", 0, "Change events per apply transaction (default 2000)")
	_ = syncCmd.Flags().MarkHidden("lease-ttl")
	_ = syncCmd.MarkFlagRequired("pipeline")
	_ = syncCmd.MarkFlagRequired("source-database")
	_ = syncCmd.MarkFlagRequired("target-database")
	_ = syncCmd.MarkFlagRequired("include")
	root.AddCommand(syncCmd)
	return root
}
