package compose

import (
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

// runEntrypoint chạy entrypoint.sh với một `strand` giả ghi lại tham số của `strand web init`.
func runEntrypoint(t *testing.T, env map[string]string) (initArgs string, out string, code int) {
	t.Helper()
	dir := t.TempDir()
	log := filepath.Join(dir, "init.args")
	stub := "#!/bin/sh\nif [ \"$1 $2\" = \"web init\" ]; then echo \"$@\" > " + log + "; fi\nexit 0\n"
	if err := os.WriteFile(filepath.Join(dir, "strand"), []byte(stub), 0o755); err != nil {
		t.Fatal(err)
	}
	cmd := exec.Command("sh", "entrypoint.sh")
	cmd.Env = []string{"PATH=" + dir + ":/usr/bin:/bin", "HOME=" + dir,
		"STRAND_WEB_DATABASE_DSN=postgres://strand:x@db:5432/strand", "STRAND_WEB_SECRET_KEY=k",
		"STRAND_WEB_ADMIN_PASSWORD=admin-password"}
	for k, v := range env {
		cmd.Env = append(cmd.Env, k+"="+v)
	}
	b, err := cmd.CombinedOutput()
	if ee, ok := err.(*exec.ExitError); ok {
		code = ee.ExitCode()
	} else if err != nil {
		t.Fatal(err)
	}
	raw, _ := os.ReadFile(log)
	return string(raw), string(b), code
}

// Nghiệm thu lại (P1): chỉ đổi STRAND_WEB_BIND ra ngoài loopback không được tự phục vụ HTTP thường.
// Loopback (mặc định) vẫn chạy HTTP thường như trước; bind ngoài cần TLS hoặc chọn rõ HTTP thường.
func TestEntrypointPlainHTTPOutsideLoopbackNeedsAnExplicitChoice(t *testing.T) {
	for _, c := range []struct {
		name      string
		env       map[string]string
		wantCode  int
		wantPlain bool
	}{
		{"default loopback", nil, 0, true},
		{"loopback, empty variable as in .env.example", map[string]string{"STRAND_WEB_PUBLISH_HOST": "127.0.0.1", "STRAND_WEB_ALLOW_PLAIN_HTTP": ""}, 0, true},
		{"public bind only", map[string]string{"STRAND_WEB_PUBLISH_HOST": "0.0.0.0", "STRAND_WEB_ALLOW_PLAIN_HTTP": ""}, 2, false},
		{"public bind, explicit plain HTTP", map[string]string{"STRAND_WEB_PUBLISH_HOST": "0.0.0.0", "STRAND_WEB_ALLOW_PLAIN_HTTP": "true"}, 0, true},
		{"public bind with TLS", map[string]string{"STRAND_WEB_PUBLISH_HOST": "10.0.0.5", "STRAND_WEB_TLS_CERT": "/certs/c", "STRAND_WEB_TLS_KEY": "/certs/k"}, 0, false},
	} {
		t.Run(c.name, func(t *testing.T) {
			args, out, code := runEntrypoint(t, c.env)
			if code != c.wantCode {
				t.Fatalf("exit %d, want %d\n%s", code, c.wantCode, out)
			}
			if code != 0 {
				if !strings.Contains(out, "STRAND_WEB_ALLOW_PLAIN_HTTP=true") {
					t.Fatalf("refusal does not say how to opt in:\n%s", out)
				}
				return
			}
			if got := strings.Contains(args, "--allow-plain-http"); got != c.wantPlain {
				t.Fatalf("--allow-plain-http = %v, want %v (web init %s)", got, c.wantPlain, args)
			}
		})
	}
}
