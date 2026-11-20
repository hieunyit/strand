// Package conncheck tests a backup connection against a live server: version, the privileges the backup
// tools need, replication prerequisites for point-in-time recovery and the list of databases.
package conncheck

import (
	"context"
	"strings"
	"time"

	"strand/internal/backup"
)

// Target is what to connect to. Secrets are used for the connection only and never appear in a Result.
type Target struct {
	Engine        string // backup.ConnMySQL | ConnMariaDB | ConnPostgres | ConnMongoDB
	Host          string
	Port          int
	User          string
	Password      string
	URI           string // mongodb:// URI with credentials (overrides host/port/user/password)
	TLSMode       string
	CACertPEM     string
	TLSServerName string // when connected via tunnel, the original host for TLS hostname verification
}

// Check is one step of the test.
type Check struct {
	Name   string `json:"name"`
	Status string `json:"status"` // ok | warn | fail
	Detail string `json:"detail,omitempty"`
}

// Result is the outcome of a test. OK means the server was reached and authenticated; missing privileges
// do not make OK false (the UI shows them), they only list what a backup would lack.
type Result struct {
	OK                bool     `json:"ok"`
	Engine            string   `json:"engine"`
	Version           string   `json:"version,omitempty"`
	LatencyMS         int64    `json:"latency_ms,omitempty"`
	GrantedPrivileges []string `json:"granted_privileges,omitempty"`
	MissingPrivileges []string `json:"missing_privileges"`
	GTIDMode          string   `json:"gtid_mode,omitempty"`
	BinlogEnabled     *bool    `json:"binlog_enabled,omitempty"`
	WALLevel          string   `json:"wal_level,omitempty"`
	ReplicationRole   *bool    `json:"replication_role,omitempty"`
	IsReplicaSet      *bool    `json:"is_replica_set,omitempty"`
	ReplicaSet        string   `json:"replica_set,omitempty"`
	Databases         []string `json:"databases"`
	Checks            []Check  `json:"checks"`
	// PITR: computed with the backup.ValidatePITR rules plus the server prerequisites measured here.
	PITRAllowed bool     `json:"pitr_allowed"`
	PITRReason  string   `json:"pitr_reason,omitempty"`
	PITRMethods []string `json:"pitr_methods,omitempty"`
	Error       string   `json:"error,omitempty"`
}

// Checker runs a connection test (an interface so web tests use a fake).
type Checker interface {
	Check(ctx context.Context, t Target) Result
}

// Live is the real Checker.
type Live struct{ Timeout time.Duration }

func (l Live) timeout() time.Duration {
	if l.Timeout > 0 {
		return l.Timeout
	}
	return 15 * time.Second
}

// Check dispatches on the engine.
func (l Live) Check(ctx context.Context, t Target) Result {
	ctx, cancel := context.WithTimeout(ctx, l.timeout())
	defer cancel()
	res := Result{Engine: t.Engine, MissingPrivileges: []string{}, Databases: []string{}, Checks: []Check{}}
	switch t.Engine {
	case backup.ConnMySQL, backup.ConnMariaDB:
		checkMySQL(ctx, t, &res)
	case backup.ConnPostgres:
		checkPostgres(ctx, t, &res)
	case backup.ConnMongoDB:
		checkMongo(ctx, t, &res)
	default:
		res.Error = "unknown engine"
	}
	res.Error = redact(res.Error, t)
	for i := range res.Checks {
		res.Checks[i].Detail = redact(res.Checks[i].Detail, t)
	}
	return res
}

func redact(s string, t Target) string {
	for _, secret := range []string{t.Password, passwordOfURI(t.URI), t.URI} {
		if len(secret) >= 3 {
			s = strings.ReplaceAll(s, secret, "xxxxx")
		}
	}
	return s
}

func (r *Result) add(name, status, detail string) {
	r.Checks = append(r.Checks, Check{Name: name, Status: status, Detail: detail})
}

func (r *Result) fail(name string, err error) {
	r.add(name, "fail", err.Error())
	if r.Error == "" {
		r.Error = name + ": " + err.Error()
	}
}

func boolp(b bool) *bool { return &b }

func (r *Result) assessPITR(caps backup.PITRCaps) {
	r.PITRAllowed, r.PITRMethods, r.PITRReason = backup.PITRAssessment(r.Engine, caps)
}

func hostPort(t Target) string {
	port := t.Port
	if port == 0 {
		port = backup.DefaultPort(t.Engine)
	}
	return joinHostPort(t.Host, port)
}
