package runner

import (
	"context"
	"crypto/tls"
	"database/sql"
	"errors"
	"fmt"
	"net"
	"os"
	"strconv"
	"time"

	"github.com/go-sql-driver/mysql"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"

	"strand/internal/backup"
)

// checkRuntimeTLS ensures a saved security policy is never silently dropped.
func checkRuntimeTLS(engine string, c backup.ConnSpec) error {
	if c.TLSMode == "" && c.CACertPEM == "" {
		return nil
	}
	if engine != backup.EnginePostgres && engine != backup.EngineMySQL && engine != backup.EngineMongoDB {
		return fmt.Errorf("backup and restore for %s do not yet support the saved TLS policy", engine)
	}
	_, err := c.TLSConfig()
	return err
}

func applyPGTLS(c backup.ConnSpec, cfg *pgconn.Config) error {
	if c.TLSMode == "" || c.TLSMode == "disable" {
		return nil
	}
	tlsConfig, err := c.TLSConfig()
	if err != nil {
		return err
	}
	cfg.TLSConfig = tlsConfig
	cfg.Fallbacks = nil
	return nil
}

func connectBackupPG(ctx context.Context, getenv Getenv, c backup.ConnSpec, database string, replication bool) (*pgconn.PgConn, error) {
	cfg, err := pgconn.ParseConfig(PGConnString(getenv, c, database, replication))
	if err != nil {
		return nil, err
	}
	if err := applyPGTLS(c, cfg); err != nil {
		return nil, err
	}
	return pgconn.ConnectConfig(ctx, cfg)
}

func connectBackupPGX(ctx context.Context, getenv Getenv, c backup.ConnSpec, database string) (*pgx.Conn, error) {
	cfg, err := pgx.ParseConfig(PGConnString(getenv, c, database, false))
	if err != nil {
		return nil, err
	}
	if err := applyPGTLS(c, &cfg.Config); err != nil {
		return nil, err
	}
	return pgx.ConnectConfig(ctx, cfg)
}

func addNativePGTLS(inv *Invocation, c backup.ConnSpec) error {
	if c.TLSMode == "" {
		return nil
	}
	mode := c.TLSMode
	if mode == "require" && c.CACertPEM != "" {
		mode = "verify-ca"
	}
	inv.Env = append(inv.Env, "PGSSLMODE="+mode, "PGGSSENCMODE=disable", "PGHOSTADDR=")
	if mode == "verify-ca" || mode == "verify-full" {
		ca := c.CACertPEM
		if ca == "" {
			pem, err := os.ReadFile("/etc/ssl/certs/ca-certificates.crt")
			if err != nil {
				return errors.New("native PostgreSQL tools need a CA bundle; configure a CA certificate")
			}
			ca = string(pem)
		}
		if inv.Files == nil {
			inv.Files = make(map[string]string)
		}
		inv.Files["postgres-ca.pem"] = ca
		inv.Env = append(inv.Env, "PGSSLROOTCERT={dir}/postgres-ca.pem")
	}
	if c.TLSServerName != "" && c.Host != c.TLSServerName {
		if net.ParseIP(c.Host) == nil {
			return errors.New("PostgreSQL TLS over an SSH tunnel requires a numeric local address")
		}
		inv.Env = append(inv.Env, "PGHOSTADDR="+c.Host)
		for i, arg := range inv.Args {
			if arg == "--host="+c.Host {
				inv.Args[i] = "--host=" + c.TLSServerName
			}
		}
	}
	return nil
}

func openBackupMySQL(getenv Getenv, c backup.ConnSpec) (*sql.DB, error) {
	cfg := mysql.NewConfig()
	port := c.Port
	if port == 0 {
		port = 3306
	}
	cfg.User, cfg.Passwd, cfg.Net = c.User, password(getenv, c), "tcp"
	cfg.Addr = net.JoinHostPort(c.Host, strconv.Itoa(port))
	cfg.ParseTime = true
	var err error
	cfg.TLS, err = mysqlBackupTLSConfig(c)
	if err != nil {
		return nil, err
	}
	connector, err := mysql.NewConnector(cfg)
	if err != nil {
		return nil, err
	}
	return sql.OpenDB(connector), nil
}

func mysqlBackupTLSConfig(c backup.ConnSpec) (*tls.Config, error) {
	if c.TLSMode == "" || c.TLSMode == "disable" {
		return nil, nil
	}
	return c.TLSConfig()
}

func addNativeMySQLTLS(inv *Invocation, c backup.ConnSpec, flavor MySQLDumpFlavor) error {
	if c.TLSMode == "" {
		return nil
	}
	if c.TLSMode == "verify-full" && c.TLSServerName != "" && c.Host != c.TLSServerName {
		return errors.New("the native MySQL client cannot verify the original database hostname through an SSH tunnel")
	}
	mode := c.TLSMode
	if mode == "require" && c.CACertPEM != "" {
		mode = "verify-ca"
	}
	var flags []string
	if flavor.MariaDB {
		switch mode {
		case "disable":
			flags = []string{"--skip-ssl"}
		case "verify-full":
			flags = []string{"--ssl", "--ssl-verify-server-cert"}
		default:
			return fmt.Errorf("the MariaDB client cannot enforce the requested %s policy; use verify-full", mode)
		}
	} else {
		modeNames := map[string]string{"disable": "DISABLED", "require": "REQUIRED", "verify-ca": "VERIFY_CA", "verify-full": "VERIFY_IDENTITY"}
		flags = []string{"--ssl-mode=" + modeNames[mode]}
	}
	if c.CACertPEM != "" {
		inv.Files = map[string]string{"mysql-ca.pem": c.CACertPEM}
		flags = append(flags, "--ssl-ca={dir}/mysql-ca.pem")
	} else if mode == "verify-ca" || mode == "verify-full" {
		const caPath = "/etc/ssl/certs/ca-certificates.crt"
		if _, err := os.Stat(caPath); err != nil {
			return errors.New("native MySQL tools need a CA bundle; configure a CA certificate")
		}
		flags = append(flags, "--ssl-ca="+caPath)
	}
	inv.Args = append(flags, inv.Args...)
	return nil
}

func addNativeMongoTLS(inv *Invocation, c backup.ConnSpec) error {
	if c.TLSMode == "" || c.TLSMode == "disable" {
		return nil
	}
	if c.TLSMode == "verify-full" && c.TLSServerName != "" && c.Host != c.TLSServerName {
		return errors.New("the native MongoDB tool cannot verify the original database hostname through an SSH tunnel")
	}
	inv.Args = append(inv.Args, "--ssl")
	if c.TLSMode == "require" && c.CACertPEM == "" {
		inv.Args = append(inv.Args, "--sslAllowInvalidCertificates", "--sslAllowInvalidHostnames")
		return nil
	}
	if c.TLSMode == "verify-ca" || c.TLSMode == "require" {
		inv.Args = append(inv.Args, "--sslAllowInvalidHostnames")
	}
	if c.CACertPEM != "" {
		if inv.Files == nil {
			inv.Files = make(map[string]string)
		}
		inv.Files["mongo-ca.pem"] = c.CACertPEM
		inv.Args = append(inv.Args, "--sslCAFile={dir}/mongo-ca.pem")
	}
	return nil
}

func connectBackupMongo(uri string, c backup.ConnSpec) (*mongo.Client, error) {
	opts := options.Client().ApplyURI(uri).SetServerSelectionTimeout(8 * time.Second)
	if c.TLSMode != "" && c.TLSMode != "disable" {
		policy := c
		if c.URIEnv != "" {
			// The driver sets the verification name from each actual replica
			// set or SRV address when the connection is defined by a URI.
			policy.Host, policy.TLSServerName = "", ""
		}
		tlsConfig, err := policy.TLSConfig()
		if err != nil {
			return nil, err
		}
		opts.SetTLSConfig(tlsConfig)
	}
	return mongo.Connect(opts)
}
