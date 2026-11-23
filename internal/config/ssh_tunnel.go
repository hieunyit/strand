package config

import (
	"context"
	"crypto/tls"
	"database/sql"
	"errors"
	"fmt"
	"net"
	"net/url"
	"strconv"
	"strings"
	"sync"

	"github.com/go-sql-driver/mysql"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/stdlib"

	"strand/internal/tunnel"
)

// SSHTunnelConfig describes a bastion for a single SQL endpoint.
type SSHTunnelConfig struct {
	Host         string `yaml:"host" json:"host"`
	Port         int    `yaml:"port,omitempty" json:"port,omitempty"`
	User         string `yaml:"user" json:"user"`
	IdentityFile string `yaml:"identity_file,omitempty" json:"identity_file,omitempty"`
}

func (c *SSHTunnelConfig) Validate(kind string) error {
	if c == nil {
		return nil
	}
	switch strings.ToLower(strings.TrimSpace(kind)) {
	case "mysql", "mariadb", "postgres", "postgresql", "pg":
	default:
		return errors.New("SSH tunneling is only supported for MySQL, MariaDB and PostgreSQL endpoints")
	}
	if c.Host == "" || strings.ContainsAny(c.Host, " \t\r\n\x00,[]") || strings.Contains(c.Host, ":") && net.ParseIP(c.Host) == nil {
		return errors.New("SSH bastion host must be a hostname or an IP address")
	}
	if c.User == "" || strings.ContainsAny(c.User, " \t\r\n\x00") {
		return errors.New("SSH bastion user is required and must not contain whitespace")
	}
	if c.Port < 0 || c.Port > 65535 {
		return errors.New("SSH bastion port must be between 1 and 65535")
	}
	if strings.ContainsAny(c.IdentityFile, "\r\n\x00") {
		return errors.New("SSH identity file path contains an invalid character")
	}
	return nil
}

// Open forwards one database address. The bastion's trusted keys and the
// identity allowlist are configured by the account running Strand.
func (c *SSHTunnelConfig) Open(ctx context.Context, kind, remote string) (*tunnel.Forward, error) {
	if c == nil {
		return nil, errors.New("SSH tunnel configuration is required")
	}
	if err := c.Validate(kind); err != nil {
		return nil, err
	}
	keys, err := tunnel.ConfiguredKnownHosts()
	if err != nil {
		return nil, err
	}
	if err := tunnel.ValidateSecurity(c.IdentityFile, keys); err != nil {
		return nil, err
	}
	port := c.Port
	if port == 0 {
		port = 22
	}
	return tunnel.Start(ctx, tunnel.Config{
		SSHAddress: net.JoinHostPort(c.Host, strconv.Itoa(port)),
		SSHUser:    c.User, IdentityFile: c.IdentityFile, HostKeysFile: keys,
		Remote: remote,
	})
}

type pgTunnelTLS struct {
	primary   *tls.Config
	fallbacks []*tls.Config
}

// A rewritten PostgreSQL DSN identifies one unique loopback listener. Keep the
// TLS settings parsed with the actual server name for every connection to it.
var pgTunnelSettings sync.Map

// OpenSSHTunnels temporarily routes a command's configured SQL DSNs through its
// bastions. Call the returned cleanup after all database connections have closed.
func OpenSSHTunnels(ctx context.Context, cfg *Config) (func(), error) {
	if cfg.Source.SSHTunnel == nil && cfg.Target.SSHTunnel == nil {
		return func() {}, nil
	}
	lifetime, cancel := context.WithCancel(context.WithoutCancel(ctx))
	stopStartupWatch := context.AfterFunc(ctx, cancel)
	defer stopStartupWatch()
	cleanups := []func(){cancel}
	var once sync.Once
	cleanup := func() {
		once.Do(func() {
			for i := len(cleanups) - 1; i >= 0; i-- {
				cleanups[i]()
			}
		})
	}
	for _, endpoint := range []struct {
		name, kind string
		dsn        *string
		ssh        *SSHTunnelConfig
	}{
		{"source", cfg.Source.Kind, &cfg.Source.DSN, cfg.Source.SSHTunnel},
		{"target", cfg.Target.Kind, &cfg.Target.DSN, cfg.Target.SSHTunnel},
	} {
		if endpoint.ssh == nil {
			continue
		}
		original := *endpoint.dsn
		routed, closeRoute, err := endpoint.ssh.routeDSN(lifetime, endpoint.kind, original)
		if err != nil {
			cleanup()
			return nil, fmt.Errorf("%s SSH tunnel: %w", endpoint.name, err)
		}
		*endpoint.dsn = routed
		cleanups = append(cleanups, func() {
			*endpoint.dsn = original
			closeRoute()
		})
	}
	if err := ctx.Err(); err != nil {
		cleanup()
		return nil, err
	}
	return cleanup, nil
}

func (c *SSHTunnelConfig) routeDSN(ctx context.Context, kind, dsn string) (string, func(), error) {
	if err := c.Validate(kind); err != nil {
		return "", nil, err
	}
	switch strings.ToLower(strings.TrimSpace(kind)) {
	case "mysql", "mariadb":
		return c.routeMySQL(ctx, dsn)
	default:
		return c.routePostgres(ctx, dsn)
	}
}

func (c *SSHTunnelConfig) routeMySQL(ctx context.Context, dsn string) (string, func(), error) {
	cfg, err := mysql.ParseDSN(dsn)
	if err != nil {
		return "", nil, errors.New("invalid MySQL DSN")
	}
	if cfg.Net != "tcp" && cfg.Net != "tcp4" && cfg.Net != "tcp6" {
		return "", nil, errors.New("SSH tunneling requires a TCP MySQL endpoint")
	}
	forward, err := c.Open(ctx, "mysql", cfg.Addr)
	if err != nil {
		return "", nil, err
	}
	cfg.Net, cfg.Addr = "tcp", forward.Address()
	tlsName := ""
	if cfg.TLS != nil {
		// ParseDSN already derived the correct ServerName from the original
		// address. Register its TLS settings before replacing the address.
		tlsName = "strand-ssh-" + cfg.Addr
		if err := mysql.RegisterTLSConfig(tlsName, cfg.TLS); err != nil {
			_ = forward.Close()
			return "", nil, err
		}
		cfg.TLSConfig = tlsName
	}
	return cfg.FormatDSN(), func() {
		if tlsName != "" {
			mysql.DeregisterTLSConfig(tlsName)
		}
		_ = forward.Close()
	}, nil
}

func (c *SSHTunnelConfig) routePostgres(ctx context.Context, dsn string) (string, func(), error) {
	original, err := pgconn.ParseConfig(dsn)
	if err != nil {
		return "", nil, errors.New("invalid PostgreSQL DSN")
	}
	if original.Host == "" || strings.HasPrefix(original.Host, "/") {
		return "", nil, errors.New("SSH tunneling requires a TCP PostgreSQL endpoint")
	}
	settings := pgTunnelTLS{primary: original.TLSConfig}
	for _, fallback := range original.Fallbacks {
		if fallback.Host != original.Host || fallback.Port != original.Port {
			return "", nil, errors.New("SSH tunneling requires a single PostgreSQL endpoint")
		}
		settings.fallbacks = append(settings.fallbacks, fallback.TLSConfig)
	}
	remote := net.JoinHostPort(original.Host, strconv.Itoa(int(original.Port)))
	forward, err := c.Open(ctx, "postgres", remote)
	if err != nil {
		return "", nil, err
	}
	local := forward.Address()
	host, port, err := net.SplitHostPort(local)
	if err != nil {
		_ = forward.Close()
		return "", nil, err
	}
	var routed string
	if strings.HasPrefix(dsn, "postgres://") || strings.HasPrefix(dsn, "postgresql://") {
		u, err := url.Parse(dsn)
		if err != nil {
			_ = forward.Close()
			return "", nil, errors.New("invalid PostgreSQL URL")
		}
		u.Host = local
		q := u.Query()
		q.Del("host")
		q.Del("port")
		u.RawQuery = q.Encode()
		routed = u.String()
	} else {
		routed = strings.TrimSpace(dsn) + " host=" + host + " port=" + port
	}
	check, err := pgconn.ParseConfig(routed)
	if err != nil || check.Host != host || strconv.Itoa(int(check.Port)) != port {
		_ = forward.Close()
		return "", nil, errors.New("PostgreSQL DSN could not be routed through the local SSH listener")
	}
	pgTunnelSettings.Store(local, settings)
	return routed, func() {
		pgTunnelSettings.Delete(local)
		_ = forward.Close()
	}, nil
}

func applyPGTunnelTLS(cfg *pgconn.Config) error {
	key := net.JoinHostPort(cfg.Host, strconv.Itoa(int(cfg.Port)))
	entry, ok := pgTunnelSettings.Load(key)
	if !ok {
		return nil
	}
	settings := entry.(pgTunnelTLS)
	if len(cfg.Fallbacks) != len(settings.fallbacks) {
		return errors.New("PostgreSQL tunnel configuration changed connection fallback count")
	}
	cfg.TLSConfig = settings.primary
	for i, fallback := range cfg.Fallbacks {
		if fallback.Host != cfg.Host || fallback.Port != cfg.Port {
			return errors.New("PostgreSQL tunnel configuration contains a second endpoint")
		}
		fallback.TLSConfig = settings.fallbacks[i]
	}
	return nil
}

// ParsePGConfig parses a pipeline SQL connection, preserving its remote TLS identity.
func ParsePGConfig(dsn string) (*pgx.ConnConfig, error) {
	cfg, err := pgx.ParseConfig(dsn)
	if err == nil {
		err = applyPGTunnelTLS(&cfg.Config)
	}
	return cfg, err
}

// ParsePGConnConfig is the replication-protocol equivalent of ParsePGConfig.
func ParsePGConnConfig(dsn string) (*pgconn.Config, error) {
	cfg, err := pgconn.ParseConfig(dsn)
	if err == nil {
		err = applyPGTunnelTLS(cfg)
	}
	return cfg, err
}

// OpenSQL opens a pipeline endpoint through the driver used by the pipeline.
func OpenSQL(driver, dsn string) (*sql.DB, error) {
	if driver != "pgx" {
		return sql.Open(driver, dsn)
	}
	cfg, err := ParsePGConfig(dsn)
	if err != nil {
		return nil, err
	}
	return stdlib.OpenDB(*cfg), nil
}
