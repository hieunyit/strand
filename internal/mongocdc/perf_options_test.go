package mongocdc

import "os"

// setPerfOptions applies throughput-test tuning from the environment:
// STRAND_MONGO_PERF_UPDATES=delta|lookup, STRAND_MONGO_PERF_PARALLEL=<collections>.
func setPerfOptions(cfg *Config) {
	cfg.UpdateMode = os.Getenv("STRAND_MONGO_PERF_UPDATES")
	if os.Getenv("STRAND_MONGO_PERF_PARALLEL") != "" {
		cfg.SnapshotParallel = envInt("STRAND_MONGO_PERF_PARALLEL", 1)
	}
}
