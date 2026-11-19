package backup

import (
	"fmt"
	"net"
	"path/filepath"
	"strings"
	"time"
)

type RunStatus string

const (
	RunStatusRunning     RunStatus = "running"
	RunStatusSucceeded   RunStatus = "succeeded"
	RunStatusFailed      RunStatus = "failed"
	RunStatusCancelled   RunStatus = "cancelled"
	RunStatusInterrupted RunStatus = "interrupted"
)

// Job is a persisted recurring backup wrapper. Secrets are read from the Web
// process environment; SSH authentication is by key path or SSH agent.
type Job struct {
	ID               string     `json:"id"`
	Name             string     `json:"name"`
	Command          string     `json:"command"`
	EverySeconds     int        `json:"every_seconds"`
	ArtifactTemplate string     `json:"artifact_template"`
	ManifestTemplate string     `json:"manifest_template"`
	Bucket           string     `json:"bucket"`
	Prefix           string     `json:"prefix,omitempty"`
	Enabled          bool       `json:"enabled"`
	TunnelSSH        string     `json:"tunnel_ssh,omitempty"`
	TunnelUser       string     `json:"tunnel_user,omitempty"`
	TunnelIdentity   string     `json:"tunnel_identity_file,omitempty"`
	TunnelKnownHosts string     `json:"tunnel_known_hosts,omitempty"`
	TunnelRemote     string     `json:"tunnel_remote,omitempty"`
	CleanupLocal     bool       `json:"cleanup_local"`
	NextRunAt        *time.Time `json:"next_run_at,omitempty"`
	UpdatedAt        time.Time  `json:"updated_at,omitempty"`
	RunStartedAt     *time.Time `json:"run_started_at,omitempty"`
	RunFinishedAt    *time.Time `json:"run_finished_at,omitempty"`
	LastRunStatus    RunStatus  `json:"last_run_status,omitempty"`
}

func ValidateJob(j Job) error {
	bad := func(s string) error { return fmt.Errorf("%w: %s", ErrInvalid, s) }
	if !validID(j.ID) {
		return bad("invalid backup job id")
	}
	if !safeText(j.Name, 96) {
		return bad("job name is required and must be at most 96 characters")
	}
	if !filepath.IsAbs(j.Command) || strings.ContainsAny(j.Command, "\x00\r\n") {
		return bad("command must be an absolute executable path")
	}
	if j.EverySeconds < 60 || j.EverySeconds > 365*24*60*60 {
		return bad("interval must be from 60 seconds to 365 days")
	}
	for label, path := range map[string]string{"artifact": j.ArtifactTemplate, "manifest": j.ManifestTemplate} {
		if !filepath.IsAbs(path) || !strings.Contains(path, "{id}") || strings.ContainsAny(path, "\x00\r\n") {
			return bad(label + " path must be absolute and contain {id}")
		}
		// Local cleanup removes these paths recursively: no "..", "//" or "." segments, so the expanded
		// path can never climb out of the directory the operator wrote.
		if filepath.Clean(path) != path || strings.Contains(path, "/../") || strings.HasSuffix(path, "/..") {
			return bad(label + " path must be a clean absolute path without '..' segments")
		}
	}
	if !safeBucket(j.Bucket) {
		return bad("invalid object bucket")
	}
	if len(j.Prefix) > 512 || hasControl(j.Prefix) || strings.HasPrefix(j.Prefix, "/") {
		return bad("invalid object prefix")
	}
	for _, part := range strings.Split(strings.Trim(j.Prefix, "/"), "/") {
		if part == ".." {
			return bad("object prefix cannot contain parent path segments")
		}
	}
	tunnelSet := j.TunnelSSH != "" || j.TunnelUser != "" || j.TunnelRemote != "" || j.TunnelIdentity != "" || j.TunnelKnownHosts != ""
	if tunnelSet {
		if strings.TrimSpace(j.TunnelSSH) == "" || strings.TrimSpace(j.TunnelUser) == "" || strings.TrimSpace(j.TunnelRemote) == "" {
			return bad("SSH tunnel requires ssh host, user, and remote host:port")
		}
		if _, _, err := net.SplitHostPort(j.TunnelRemote); err != nil {
			return bad("SSH tunnel remote must be host:port")
		}
	}
	if j.TunnelIdentity != "" && !filepath.IsAbs(j.TunnelIdentity) {
		return bad("SSH identity file path must be absolute")
	}
	if j.TunnelKnownHosts != "" && !filepath.IsAbs(j.TunnelKnownHosts) {
		return bad("known_hosts path must be absolute")
	}
	if j.TunnelUser != "" && strings.ContainsAny(j.TunnelUser, "\x00\r\n") {
		return bad("invalid SSH username")
	}
	return nil
}
