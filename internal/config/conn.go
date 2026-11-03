package config

import (
	"fmt"
	"net"
	"net/url"
	"os"
	"strconv"
	"strings"

	drivermysql "github.com/go-sql-driver/mysql"

	"strand/internal/exitcode"
)

// connFields là phần kết nối chung của source/target.
type connFields struct {
	kind, dsn, host, user, password, passwordEnv, database *string
	port                                                   *int
}

// resolveConnections chuẩn hóa kết nối của source và target thành DSN của go-sql-driver/mysql
// (`user:pass@tcp(host:port)/db?...`), để phần còn lại của Strand chỉ làm việc với một dạng:
//   - dsn: "mysql://user:pass@host:3306/db?tls=true" (URL, mật khẩu được URL-encode nếu có ký tự đặc biệt);
//   - hoặc host/port/user/password|password_env/database tách riêng;
//   - dạng DSN của driver (`user:pass@tcp(host:port)/db`) vẫn nhận nguyên như cũ.
func resolveConnections(cfg *Config) error {
	for _, c := range []struct {
		name string
		f    connFields
	}{
		{"source", connFields{&cfg.Source.Kind, &cfg.Source.DSN, &cfg.Source.Host, &cfg.Source.User, &cfg.Source.Password,
			&cfg.Source.PasswordEnv, &cfg.Source.Database, &cfg.Source.Port}},
		{"target", connFields{&cfg.Target.Kind, &cfg.Target.DSN, &cfg.Target.Host, &cfg.Target.User, &cfg.Target.Password,
			&cfg.Target.PasswordEnv, &cfg.Target.Database, &cfg.Target.Port}},
	} {
		if err := resolveConn(c.name, c.f); err != nil {
			return exitcode.New("invalid_connection", exitcode.InvalidSyntax, err.Error(), err)
		}
	}
	return nil
}

func resolveConn(name string, f connFields) error {
	split := *f.host != "" || *f.user != "" || *f.password != "" || *f.passwordEnv != "" || *f.port != 0 || *f.database != ""
	if !strings.EqualFold(*f.kind, "mysql") {
		if split {
			return fmt.Errorf("%s: host/port/user/password fields are supported for kind mysql only; use dsn", name)
		}
		return nil
	}
	if split && *f.dsn != "" {
		return fmt.Errorf("%s: set either dsn or host/port/user/password/database, not both", name)
	}
	if split {
		if *f.host == "" || *f.user == "" {
			return fmt.Errorf("%s: host and user are required when dsn is not set", name)
		}
		pass := *f.password
		if *f.passwordEnv != "" {
			if pass != "" {
				return fmt.Errorf("%s: set either password or password_env, not both", name)
			}
			v, ok := os.LookupEnv(*f.passwordEnv)
			if !ok {
				return fmt.Errorf("%s: password_env %s is not set in the environment", name, *f.passwordEnv)
			}
			pass = v
		}
		port := *f.port
		if port == 0 {
			port = 3306
		}
		dsn, err := buildMySQLDSN(*f.user, pass, net.JoinHostPort(*f.host, strconv.Itoa(port)), *f.database, nil)
		if err != nil {
			return fmt.Errorf("%s: %w", name, err)
		}
		*f.dsn = dsn
		return nil
	}
	if strings.HasPrefix(strings.ToLower(*f.dsn), "mysql://") {
		dsn, err := mysqlURLToDSN(*f.dsn)
		if err != nil {
			return fmt.Errorf("%s: %w", name, err)
		}
		*f.dsn = dsn
	}
	return nil
}

// mysqlURLToDSN đổi "mysql://user:pass@host[:port]/[db][?params]" sang DSN của driver.
func mysqlURLToDSN(raw string) (string, error) {
	u, err := url.Parse(raw)
	if err != nil {
		return "", fmt.Errorf("invalid mysql:// URL: %v", err)
	}
	if u.Hostname() == "" {
		return "", fmt.Errorf("invalid mysql:// URL: missing host")
	}
	port := u.Port()
	if port == "" {
		port = "3306"
	}
	user, pass := "", ""
	if u.User != nil {
		user = u.User.Username()
		pass, _ = u.User.Password()
	}
	return buildMySQLDSN(user, pass, net.JoinHostPort(u.Hostname(), port), strings.TrimPrefix(u.Path, "/"), u.Query())
}

// buildMySQLDSN dựng DSN của driver; tham số query (tls, timeout, ...) được driver kiểm như khi viết thẳng trong DSN.
func buildMySQLDSN(user, pass, addr, db string, q url.Values) (string, error) {
	base := "u@tcp(127.0.0.1:3306)/"
	if len(q) > 0 {
		base += "?" + q.Encode()
	}
	c, err := drivermysql.ParseDSN(base)
	if err != nil {
		return "", fmt.Errorf("invalid connection parameters: %v", err)
	}
	c.User, c.Passwd, c.Net, c.Addr, c.DBName = user, pass, "tcp", addr, db
	return c.FormatDSN(), nil
}

// NormalizeMySQLDSN đổi DSN dạng mysql://... sang DSN của driver; dạng khác trả nguyên. Dùng cho
// các cờ DSN trên dòng lệnh (seed, promote) để nhận cùng cú pháp với file cấu hình.
func NormalizeMySQLDSN(dsn string) (string, error) {
	if strings.HasPrefix(strings.ToLower(dsn), "mysql://") {
		return mysqlURLToDSN(dsn)
	}
	return dsn, nil
}
