package backup

import (
	"crypto/tls"
	"crypto/x509"
	"errors"
	"net/url"
	"strings"
)

func (c ConnSpec) tlsServerName() string {
	if c.TLSServerName != "" {
		return c.TLSServerName
	}
	return c.Host
}

// TLSConfig returns the configured client TLS policy. A nil policy leaves
// legacy connection defaults to the caller.
func (c ConnSpec) TLSConfig() (*tls.Config, error) {
	switch c.TLSMode {
	case "", "disable":
		if c.CACertPEM != "" {
			return nil, errors.New("an explicit encrypted TLS mode is required when configuring a backup CA")
		}
		return nil, nil
	case "require", "verify-ca", "verify-full":
	default:
		return nil, errors.New("invalid backup TLS mode")
	}
	cfg := &tls.Config{MinVersion: tls.VersionTLS12, ServerName: c.tlsServerName()}
	if c.TLSMode == "require" && c.CACertPEM == "" {
		cfg.InsecureSkipVerify = true // require encryption without authenticating the server
		return cfg, nil
	}
	if c.CACertPEM != "" {
		cfg.RootCAs = x509.NewCertPool()
		if !cfg.RootCAs.AppendCertsFromPEM([]byte(c.CACertPEM)) {
			return nil, errors.New("backup TLS CA contains no usable certificates")
		}
	}
	if c.TLSMode == "verify-ca" || c.TLSMode == "require" {
		cfg.InsecureSkipVerify = true // VerifyConnection validates the chain without checking the name.
		cfg.VerifyConnection = func(cs tls.ConnectionState) error {
			if len(cs.PeerCertificates) == 0 {
				return errors.New("database supplied no peer certificate")
			}
			opts := x509.VerifyOptions{Roots: cfg.RootCAs, Intermediates: x509.NewCertPool()}
			for _, cert := range cs.PeerCertificates[1:] {
				opts.Intermediates.AddCert(cert)
			}
			_, err := cs.PeerCertificates[0].Verify(opts)
			return err
		}
	}
	return cfg, nil
}

// MongoTLSURI rejects independent TLS settings in a URI when a saved
// connection supplies an explicit TLS policy.
func (c ConnSpec) MongoTLSURI(uri string) (string, error) {
	if c.TLSMode == "" {
		return uri, nil
	}
	base, rawQuery, _ := strings.Cut(uri, "?")
	values, err := url.ParseQuery(rawQuery)
	if err != nil {
		return "", errors.New("invalid MongoDB URI options")
	}
	for key := range values {
		key = strings.ToLower(key)
		if strings.HasPrefix(key, "tls") || strings.HasPrefix(key, "ssl") {
			return "", errors.New("MongoDB URI TLS options conflict with the saved TLS policy")
		}
	}
	if c.TLSMode == "disable" {
		values.Set("tls", "false")
		return base + "?" + values.Encode(), nil
	}
	return uri, nil
}
