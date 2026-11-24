//go:build !unix

package web

// lockPipelineFiles is a no-op where flock is unavailable (Windows). The
// database compare-and-swap still rejects a config save based on a stale
// version; only the materialize-versus-save window on one host is unguarded.
func lockPipelineFiles(string) (func(), error) { return func() {}, nil }
