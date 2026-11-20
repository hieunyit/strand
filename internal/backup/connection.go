package backup

import (
	"errors"
	"fmt"
	"path/filepath"
	"strings"
	"time"
)

// Environment variables through which the Web process hands a STORED connection secret to the backup
// processes it starts (never argv). The runner reads the password/URI through ConnSpec.PasswordEnv/URIEnv,
// which a stored connection points at these names.
const (
	EnvConnPassword = "STRAND_BACKUP_CONN_PASSWORD"
	EnvConnURI      = "STRAND_BACKUP_CONN_URI"
)

// Connection engines (MariaDB is a distinct connection engine; its catalog engine is mysql).
const (
	ConnMySQL    = "mysql"
	ConnMariaDB  = "mariadb"
	ConnPostgres = "postgres"
	ConnMongoDB  = "mongodb"
)

// Secret modes.
const (
	SecretEnv    = "env"
	SecretStored = "stored"
)

// ErrInUse: a connection or policy cannot be deleted while jobs reference it. Wrapped by *InUseError.
var ErrInUse = errors.New("backup: still in use by backup jobs")

// InUseError lists the jobs that block a delete.
type InUseError struct{ Jobs []string }

func (e *InUseError) Error() string {
	return fmt.Sprintf("%v: %s", ErrInUse, strings.Join(e.Jobs, ", "))
}
func (e *InUseError) Is(t error) bool { return t == ErrInUse }

// SSHTunnel describes an SSH bastion for a connection. IdentityRef and KnownHostsRef are absolute paths on
// the Strand host (no key material is stored).
type SSHTunnel struct {
	Host          string `json:"host"`
	Port          int    `json:"port,omitempty"`
	User          string `json:"user"`
	IdentityRef   string `json:"identity_ref,omitempty"`
	KnownHostsRef string `json:"known_hosts_ref,omitempty"`
}

// Connection is a reusable database source. Sealed secrets never leave the store layer: they are not
// serialized (json:"-"); API responses carry only password_set / uri_set.
type Connection struct {
	ID          string            `json:"id"`
	Name        string            `json:"name"`
	Engine      string            `json:"engine"`
	Host        string            `json:"host,omitempty"`
	Port        int               `json:"port,omitempty"`
	User        string            `json:"user,omitempty"`
	SecretMode  string            `json:"secret_mode"`
	PasswordEnv string            `json:"password_env,omitempty"`
	URIEnv      string            `json:"uri_env,omitempty"`
	TLSMode     string            `json:"tls_mode,omitempty"`
	CACertPEM   string            `json:"ca_cert_pem,omitempty"`
	SSHTunnel   *SSHTunnel        `json:"ssh_tunnel,omitempty"`
	Labels      map[string]string `json:"labels,omitempty"`

	PasswordSet bool `json:"password_set"`
	URISet      bool `json:"uri_set"`
	// PasswordSealed / URISealed: store.Box ciphertext (AAD = connection id). Never serialized.
	PasswordSealed []byte `json:"-"`
	URISealed      []byte `json:"-"`

	CreatedBy      string     `json:"created_by,omitempty"`
	CreatedAt      time.Time  `json:"created_at"`
	UpdatedAt      time.Time  `json:"updated_at"`
	LastTestAt     *time.Time `json:"last_test_at,omitempty"`
	LastTestStatus string     `json:"last_test_status,omitempty"`
	LastTestDetail string     `json:"last_test_detail,omitempty"`
}

// CatalogEngine maps the connection engine to the catalog/method engine (mariadb -> mysql).
func (c Connection) CatalogEngine() string {
	if c.Engine == ConnMariaDB {
		return EngineMySQL
	}
	return c.Engine
}

// DefaultPort is the engine's well-known port.
func DefaultPort(engine string) int {
	switch engine {
	case ConnMySQL, ConnMariaDB:
		return 3306
	case ConnPostgres:
		return 5432
	case ConnMongoDB:
		return 27017
	}
	return 0
}

// Validate checks the connection definition (secrets are checked by the caller that seals them).
func (c Connection) Validate() error {
	bad := func(s string) error { return fmt.Errorf("%w: connection %s", ErrInvalid, s) }
	if !validID(c.ID) {
		return bad("id is invalid")
	}
	if !safeText(c.Name, 96) {
		return bad("name is required and must be at most 96 characters")
	}
	switch c.Engine {
	case ConnMySQL, ConnMariaDB, ConnPostgres, ConnMongoDB:
	default:
		return bad("engine must be mysql, mariadb, postgres or mongodb")
	}
	if err := validateConn("connection", ConnSpec{Host: c.Host, Port: c.Port, User: c.User, PasswordEnv: c.PasswordEnv, URIEnv: c.URIEnv},
		c.CatalogEngine(), false); err != nil {
		return err
	}
	if c.Engine == ConnMongoDB && c.Host == "" && c.URIEnv == "" && !(c.SecretMode == SecretStored && c.URISet) {
		return bad("mongodb needs a host or a URI")
	}
	switch c.SecretMode {
	case SecretEnv:
		if c.PasswordSealed != nil || c.URISealed != nil {
			return bad("secret_mode env cannot hold stored secrets")
		}
	case SecretStored:
		if c.PasswordEnv != "" || c.URIEnv != "" {
			return bad("secret_mode stored cannot also name password_env/uri_env")
		}
	default:
		return bad("secret_mode must be env or stored")
	}
	switch c.TLSMode {
	case "", "disable", "require", "verify-ca", "verify-full":
	default:
		return bad("tls_mode must be disable, require, verify-ca or verify-full")
	}
	if len(c.CACertPEM) > 64<<10 || (c.CACertPEM != "" && !strings.Contains(c.CACertPEM, "BEGIN CERTIFICATE")) {
		return bad("ca_cert_pem must be a PEM certificate")
	}
	if len(c.Labels) > 32 {
		return bad("has too many labels")
	}
	for k, v := range c.Labels {
		if !safeText(k, 63) || len(v) > 255 || hasControl(v) {
			return bad("label " + fmt.Sprintf("%q", k) + " is invalid")
		}
	}
	if t := c.SSHTunnel; t != nil {
		if !hostRE.MatchString(t.Host) || strings.HasPrefix(t.Host, "-") || !userNameRE.MatchString(t.User) || t.Port < 0 || t.Port > 65535 {
			return bad("ssh_tunnel needs a valid host, user and port")
		}
		for _, p := range []string{t.IdentityRef, t.KnownHostsRef} {
			if p != "" && !filepath.IsAbs(p) {
				return bad("ssh_tunnel identity_ref and known_hosts_ref must be absolute paths")
			}
		}
	}
	return nil
}

// ApplyConnection fills the job's connection from c (host/port/user and the secret environment variable
// names; Databases stay the job's). A stored connection points the job at EnvConnPassword/EnvConnURI.
func ApplyConnection(j Job, c Connection) (Job, error) {
	if j.IsCustom() {
		return j, fmt.Errorf("%w: custom jobs do not use connections", ErrInvalid)
	}
	if e := j.Engine(); e != c.CatalogEngine() {
		return j, fmt.Errorf("%w: method %q needs a %s connection, %q is %s", ErrInvalid, j.Method, e, c.Name, c.Engine)
	}
	dbs := j.Conn.Databases
	j.Conn = ConnSpec{Host: c.Host, Port: c.Port, User: c.User, Databases: dbs}
	if j.Conn.Port == 0 && c.Host != "" {
		j.Conn.Port = DefaultPort(c.Engine)
	}
	if c.SecretMode == SecretStored {
		if c.PasswordSet {
			j.Conn.PasswordEnv = EnvConnPassword
		}
		if c.URISet {
			j.Conn.URIEnv = EnvConnURI
		}
	} else {
		j.Conn.PasswordEnv, j.Conn.URIEnv = c.PasswordEnv, c.URIEnv
	}
	if j.Source == "" {
		j.Source = c.Name
	}
	return j, nil
}

// pitrProbe builds the job ValidatePITR judges for a method.
func pitrProbe(method string) Job {
	return Job{JobOptions: JobOptions{Method: method, LogArchive: true, Conn: ConnSpec{Databases: []string{"db"}}}}
}

// PITRCaps are the server facts a connection test established.
type PITRCaps struct {
	GTIDOn          bool // mysql: gtid_mode=ON
	WALLevelOK      bool // postgres: wal_level replica or logical
	ReplicationRole bool // postgres: REPLICATION attribute (or superuser)
	ReplicaSet      bool // mongodb: member of a replica set
}

// PITRAssessment says whether point-in-time recovery can be enabled on a connection and with which base
// methods, using the same rules as ValidatePITR (a method is listed only when ValidatePITR accepts a probe
// job with exactly one database) plus the server prerequisites the test measured.
func PITRAssessment(connEngine string, caps PITRCaps) (allowed bool, methods []string, reason string) {
	var candidates []string
	switch connEngine {
	case ConnMariaDB:
		candidates = []string{MethodMariaDBDump}
	case ConnMySQL:
		candidates = []string{MethodMySQLDump, MethodMydumper}
	case ConnPostgres:
		candidates = []string{MethodPGBaseBackup, MethodPGDump}
	case ConnMongoDB:
		candidates = []string{MethodMongodump}
	default:
		return false, nil, "unknown engine"
	}
	var firstErr string
	for _, m := range candidates {
		if err := ValidatePITR(pitrProbe(m)); err != nil {
			if firstErr == "" {
				firstErr = strings.TrimPrefix(err.Error(), ErrInvalid.Error()+": ")
			}
			continue
		}
		methods = append(methods, m)
	}
	if len(methods) == 0 {
		return false, nil, firstErr
	}
	switch connEngine {
	case ConnMySQL:
		if !caps.GTIDOn {
			return false, nil, "MySQL PITR needs gtid_mode=ON on the server"
		}
	case ConnPostgres:
		if !caps.WALLevelOK {
			return false, nil, "PostgreSQL PITR needs wal_level = replica or logical"
		}
		if !caps.ReplicationRole {
			return false, nil, "PostgreSQL PITR needs a role with the REPLICATION attribute"
		}
	case ConnMongoDB:
		if !caps.ReplicaSet {
			return false, nil, "MongoDB PITR needs a replica set (oplog)"
		}
	}
	return true, methods, ""
}
