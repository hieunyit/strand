// Package backup contains the central backup catalog contract and validation rules.
// Storage and execution backends implement Repository; this package has no dependency on web/store.
package backup

import (
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"net/url"
	"regexp"
	"strings"
	"time"
)

type Status string

const (
	StatusQueued    Status = "queued"
	StatusUploading Status = "uploading"
	StatusVerifying Status = "verifying"
	StatusComplete  Status = "complete"
	StatusFailed    Status = "failed"
	StatusExpired   Status = "expired"
)

var (
	ErrConflict   = errors.New("backup catalog: id already exists with different immutable metadata")
	ErrInvalid    = errors.New("backup catalog: invalid record or filter")
	ErrTransition = errors.New("backup catalog: invalid status transition")
)

var (
	idRE     = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9_-]{0,127}$`)
	slugRE   = regexp.MustCompile(`^[a-z][a-z0-9_-]{0,63}$`)
	sha256RE = regexp.MustCompile(`^[a-fA-F0-9]{64}$`)
	bucketRE = regexp.MustCompile(`^[a-z0-9][a-z0-9.-]{1,61}[a-z0-9]$`)
)

// Position is the database recovery point captured by the backup.
// MySQL may provide GTIDSet, BinlogFile/BinlogPos, or both; PostgreSQL uses LSN.
type Position struct {
	GTIDSet    string `json:"gtid_set,omitempty"`
	LSN        string `json:"lsn,omitempty"`
	BinlogFile string `json:"binlog_file,omitempty"`
	BinlogPos  int64  `json:"binlog_pos,omitempty"`
}

// Record is immutable backup identity plus its managed lifecycle state.
type Record struct {
	ID          string     `json:"id"`
	Source      string     `json:"source"`
	Engine      string     `json:"engine"`
	Tool        string     `json:"tool"`
	ToolVersion string     `json:"tool_version,omitempty"`
	Bucket      string     `json:"bucket"`
	ObjectKey   string     `json:"object_key"`
	Bytes       int64      `json:"bytes"`
	SHA256      string     `json:"sha256,omitempty"`
	Position    Position   `json:"position"`
	ManifestURI string     `json:"manifest_uri,omitempty"`
	Status      Status     `json:"status"`
	CreatedAt   time.Time  `json:"created_at"`
	VerifiedAt  *time.Time `json:"verified_at,omitempty"`
	ExpiresAt   *time.Time `json:"expires_at,omitempty"`
	LegalHold   bool       `json:"legal_hold"`
}

// Filter bounds catalog listing. Limit is clamped to MaxListLimit by the catalog service.
type Filter struct {
	Source        string
	Engine        string
	Status        Status
	CreatedAfter  *time.Time
	CreatedBefore *time.Time
	// After continues a listing: only records strictly older than the cursor in the listing order
	// (created_at DESC, id DESC) are returned. Use NextCursor of the previous page.
	After *Cursor
	Limit int
}

// Cursor is a keyset position in the catalog listing order.
type Cursor struct {
	CreatedAt time.Time
	ID        string
}

// Encode returns an opaque URL-safe token for c.
func (c Cursor) Encode() string {
	return base64.RawURLEncoding.EncodeToString([]byte(c.CreatedAt.UTC().Format(time.RFC3339Nano) + "|" + c.ID))
}

// ParseCursor decodes a token produced by Cursor.Encode.
func ParseCursor(token string) (Cursor, error) {
	raw, err := base64.RawURLEncoding.DecodeString(token)
	if err != nil || len(token) > 512 {
		return Cursor{}, fmt.Errorf("%w: malformed cursor", ErrInvalid)
	}
	ts, id, ok := strings.Cut(string(raw), "|")
	t, terr := time.Parse(time.RFC3339Nano, ts)
	if !ok || terr != nil || !validID(id) {
		return Cursor{}, fmt.Errorf("%w: malformed cursor", ErrInvalid)
	}
	return Cursor{CreatedAt: t, ID: id}, nil
}

// NextCursor returns the cursor for the page after rows when the page is full, else "".
func NextCursor(rows []Record, limit int) string {
	if limit <= 0 || len(rows) < limit {
		return ""
	}
	last := rows[len(rows)-1]
	return Cursor{CreatedAt: last.CreatedAt, ID: last.ID}.Encode()
}

const MaxListLimit = 500

// Repository is implemented by durable metadata stores.
type Repository interface {
	CreateBackup(context.Context, Record) (Record, bool, error)
	GetBackup(context.Context, string) (Record, bool, error)
	ListBackups(context.Context, Filter) ([]Record, error)
	UpdateBackupStatus(context.Context, string, Status, Status, *time.Time) (Record, bool, error)
	FinalizeBackup(context.Context, string, Status, int64, string, string, time.Time) (Record, bool, error)
}

// Catalog validates callers and provides idempotent create/status-update operations.
type Catalog struct{ repo Repository }

func NewCatalog(repo Repository) (*Catalog, error) {
	if repo == nil {
		return nil, fmt.Errorf("%w: nil repository", ErrInvalid)
	}
	return &Catalog{repo: repo}, nil
}

// Create returns the existing record when ID and immutable metadata match.
func (c *Catalog) Create(ctx context.Context, r Record) (Record, bool, error) {
	if r.CreatedAt.IsZero() {
		r.CreatedAt = time.Now().UTC()
	}
	if err := ValidateCreate(r); err != nil {
		return Record{}, false, err
	}
	r.SHA256 = strings.ToLower(r.SHA256)
	return c.repo.CreateBackup(ctx, r)
}

// ValidateCreate prevents callers from inserting records directly into a
// verified/terminal lifecycle state; completion must go through Finalize.
func ValidateCreate(r Record) error {
	if err := Validate(r); err != nil {
		return err
	}
	if r.Status != StatusQueued || r.VerifiedAt != nil || r.SHA256 != "" {
		return fmt.Errorf("%w: new backups must start queued and cannot include verification metadata", ErrInvalid)
	}
	return nil
}

func (c *Catalog) Get(ctx context.Context, id string) (Record, bool, error) {
	if !validID(id) {
		return Record{}, false, fmt.Errorf("%w: invalid id", ErrInvalid)
	}
	return c.repo.GetBackup(ctx, id)
}

func (c *Catalog) List(ctx context.Context, f Filter) ([]Record, error) {
	if f.Source != "" && !safeText(f.Source, 256) {
		return nil, fmt.Errorf("%w: invalid source filter", ErrInvalid)
	}
	if f.Engine != "" && f.Engine != "mysql" && f.Engine != "postgres" {
		return nil, fmt.Errorf("%w: engine must be mysql or postgres", ErrInvalid)
	}
	if f.Status != "" && !validStatus(f.Status) {
		return nil, fmt.Errorf("%w: unsupported status", ErrInvalid)
	}
	if f.CreatedAfter != nil && f.CreatedBefore != nil && f.CreatedAfter.After(*f.CreatedBefore) {
		return nil, fmt.Errorf("%w: created_after is after created_before", ErrInvalid)
	}
	if f.Limit <= 0 || f.Limit > MaxListLimit {
		f.Limit = MaxListLimit
	}
	return c.repo.ListBackups(ctx, f)
}

func (c *Catalog) UpdateStatus(ctx context.Context, id string, next Status, verifiedAt *time.Time) (Record, bool, error) {
	if !validID(id) || !validStatus(next) {
		return Record{}, false, fmt.Errorf("%w: invalid id or status", ErrInvalid)
	}
	current, ok, err := c.repo.GetBackup(ctx, id)
	if err != nil || !ok {
		return current, ok, err
	}
	if current.Status == next {
		return current, false, nil
	}
	if !allowedTransition(current.Status, next) {
		return Record{}, false, fmt.Errorf("%w: %s to %s", ErrTransition, current.Status, next)
	}
	if current.LegalHold && next == StatusExpired {
		return Record{}, false, fmt.Errorf("%w: legal-hold backup cannot be expired", ErrInvalid)
	}
	if verifiedAt != nil {
		return Record{}, false, fmt.Errorf("%w: verified_at is set by finalization", ErrInvalid)
	}
	return c.repo.UpdateBackupStatus(ctx, id, current.Status, next, verifiedAt)
}

// Finalize atomically stores actual upload metadata and marks a verifying backup complete.
// Identical retries are idempotent if the first response was lost.
func (c *Catalog) Finalize(ctx context.Context, id string, expected Status, bytes int64, sha256, manifestURI string, verifiedAt time.Time) (Record, bool, error) {
	if !validID(id) || expected != StatusVerifying {
		return Record{}, false, fmt.Errorf("%w: invalid finalization metadata", ErrInvalid)
	}
	if err := ValidateFinalization(bytes, sha256, manifestURI, verifiedAt); err != nil {
		return Record{}, false, err
	}
	return c.repo.FinalizeBackup(ctx, id, expected, bytes, strings.ToLower(sha256), manifestURI, verifiedAt.UTC())
}

// ValidateFinalization checks upload metadata shared by durable repository implementations.
func ValidateFinalization(bytes int64, sha256, manifestURI string, verifiedAt time.Time) error {
	if bytes < 0 || !sha256RE.MatchString(sha256) || verifiedAt.IsZero() {
		return fmt.Errorf("%w: invalid finalization metadata", ErrInvalid)
	}
	return validateManifestURI(manifestURI)
}

func Validate(r Record) error {
	bad := func(reason string) error { return fmt.Errorf("%w: %s", ErrInvalid, reason) }
	if !validID(r.ID) {
		return bad("id must be 1-128 letters, digits, '_' or '-' and start with a letter or digit")
	}
	if !safeText(r.Source, 256) {
		return bad("source is required and must be at most 256 characters")
	}
	if r.Engine != "mysql" && r.Engine != "postgres" {
		return bad("engine must be mysql or postgres")
	}
	if !slugRE.MatchString(r.Tool) {
		return bad("tool must be a lowercase tool identifier")
	}
	if len(r.ToolVersion) > 128 || hasControl(r.ToolVersion) {
		return bad("invalid tool_version")
	}
	if !safeBucket(r.Bucket) {
		return bad("invalid object bucket")
	}
	if !safeObjectKey(r.ObjectKey) {
		return bad("invalid object key")
	}
	if r.Bytes < 0 {
		return bad("bytes cannot be negative")
	}
	if r.SHA256 != "" && !sha256RE.MatchString(r.SHA256) {
		return bad("sha256 must be 64 hexadecimal characters")
	}
	if r.Position.BinlogPos < 0 {
		return bad("binlog_pos cannot be negative")
	}
	if r.Engine == "postgres" && (r.Position.GTIDSet != "" || r.Position.BinlogFile != "" || r.Position.BinlogPos != 0) {
		return bad("postgres position cannot contain MySQL coordinates")
	}
	if r.Engine == "mysql" && r.Position.LSN != "" {
		return bad("mysql position cannot contain an LSN")
	}
	for _, p := range []string{r.Position.GTIDSet, r.Position.LSN, r.Position.BinlogFile} {
		if hasControl(p) || len(p) > 4096 {
			return bad("invalid recovery position")
		}
	}
	if r.ManifestURI != "" {
		if err := validateManifestURI(r.ManifestURI); err != nil {
			return err
		}
	}
	if !validStatus(r.Status) {
		return bad("unsupported status")
	}
	if r.CreatedAt.IsZero() {
		return bad("created_at is required")
	}
	if r.VerifiedAt != nil && r.VerifiedAt.Before(r.CreatedAt) {
		return bad("verified_at precedes created_at")
	}
	if r.ExpiresAt != nil && r.ExpiresAt.Before(r.CreatedAt) {
		return bad("expires_at precedes created_at")
	}
	if r.Status == StatusComplete && (r.VerifiedAt == nil || r.SHA256 == "" || r.ManifestURI == "") {
		return bad("complete records require verified_at, sha256, and manifest_uri")
	}
	return nil
}

// SameIdentity compares the create-time identity, allowing retries after upload finalization.
func SameIdentity(a, b Record) bool {
	return a.ID == b.ID && a.Source == b.Source && a.Engine == b.Engine && a.Tool == b.Tool &&
		a.ToolVersion == b.ToolVersion && a.Bucket == b.Bucket && a.ObjectKey == b.ObjectKey &&
		a.Position == b.Position && a.ManifestURI == b.ManifestURI && a.LegalHold == b.LegalHold &&
		equalOptionalTime(a.ExpiresAt, b.ExpiresAt)
}

func equalOptionalTime(a, b *time.Time) bool {
	if a == nil || b == nil {
		return a == nil && b == nil
	}
	return a.Equal(*b)
}

func validateManifestURI(raw string) error {
	u, err := url.Parse(raw)
	if err != nil || u.Scheme == "" || u.User != nil || u.Fragment != "" || u.RawQuery != "" || hasControl(raw) {
		return fmt.Errorf("%w: manifest_uri must be a valid URI without credentials, query, or fragment", ErrInvalid)
	}
	switch strings.ToLower(u.Scheme) {
	case "s3":
		if u.Host == "" || strings.Trim(u.Path, "/") == "" {
			return fmt.Errorf("%w: s3 manifest_uri requires bucket and object key", ErrInvalid)
		}
	case "file":
		if u.Path == "" {
			return fmt.Errorf("%w: file manifest_uri requires a path", ErrInvalid)
		}
	case "https":
		if u.Host == "" {
			return fmt.Errorf("%w: https manifest_uri requires a host", ErrInvalid)
		}
	default:
		return fmt.Errorf("%w: unsupported manifest_uri scheme %q", ErrInvalid, u.Scheme)
	}
	return nil
}

func validID(id string) bool { return idRE.MatchString(id) }

// ValidID reports whether s is safe to use as a catalog or managed-job ID.
func ValidID(s string) bool { return validID(s) }
func validStatus(s Status) bool {
	switch s {
	case StatusQueued, StatusUploading, StatusVerifying, StatusComplete, StatusFailed, StatusExpired:
		return true
	}
	return false
}

// CanTransition reports whether a persisted record may move between lifecycle states.
func CanTransition(from, to Status) bool {
	return validStatus(from) && validStatus(to) && (from == to || allowedTransition(from, to))
}
func allowedTransition(from, to Status) bool {
	switch from {
	case StatusQueued:
		return to == StatusUploading || to == StatusFailed
	case StatusUploading:
		return to == StatusVerifying || to == StatusFailed
	case StatusVerifying:
		return to == StatusFailed
	case StatusFailed:
		return to == StatusUploading
	case StatusComplete:
		return to == StatusExpired
	}
	return false
}
func safeText(s string, max int) bool {
	return strings.TrimSpace(s) != "" && strings.TrimSpace(s) == s && len(s) <= max && !hasControl(s)
}
func hasControl(s string) bool {
	for _, r := range s {
		if r < 0x20 || r == 0x7f {
			return true
		}
	}
	return false
}
func safeBucket(s string) bool {
	return len(s) >= 3 && len(s) <= 63 && bucketRE.MatchString(s) &&
		!strings.Contains(s, "..") && !strings.Contains(s, ".-") && !strings.Contains(s, "-.")
}
func safeObjectKey(s string) bool {
	if s == "" || len(s) > 1024 || strings.HasPrefix(s, "/") || hasControl(s) {
		return false
	}
	for _, part := range strings.Split(s, "/") {
		if part == "" || part == "." || part == ".." {
			return false
		}
	}
	return true
}
