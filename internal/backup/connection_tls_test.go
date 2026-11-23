package backup

import (
	"crypto/tls"
	"encoding/pem"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestBackupTLSChecksCAAndOriginalHostname(t *testing.T) {
	server := httptest.NewTLSServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}))
	defer server.Close()
	cert := server.Certificate()
	name := cert.IPAddresses[0].String()
	if len(cert.DNSNames) > 0 {
		name = cert.DNSNames[0]
	}
	ca := string(pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: cert.Raw}))
	for _, tc := range []struct {
		name, mode, serverName, ca string
		success                    bool
	}{
		{"trusted name behind forward", "verify-full", name, ca, true},
		{"wrong hostname", "verify-full", "must-not-match.example", ca, false},
		{"untrusted CA", "verify-full", name, "", false},
		{"chain only", "verify-ca", "must-not-match.example", ca, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c := ConnSpec{Host: "127.0.0.1", TLSServerName: tc.serverName, TLSMode: tc.mode, CACertPEM: tc.ca}
			policy, err := c.TLSConfig()
			if err != nil {
				t.Fatal(err)
			}
			conn, err := tls.Dial("tcp", server.Listener.Addr().String(), policy)
			if conn != nil {
				_ = conn.Close()
			}
			if (err == nil) != tc.success {
				t.Fatalf("TLS handshake error = %v, success expected = %v", err, tc.success)
			}
		})
	}
}
