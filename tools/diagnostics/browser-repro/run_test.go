package main

import (
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

// Exercise the actual wrapper with fake Docker/CDP/Node processes. The fixture
// itself uses real ephemeral loopback listeners; no Docker resource is touched.
func TestWrapperStopsOnlyReturnedContainerID(t *testing.T) {
	dir := t.TempDir()
	id := strings.Repeat("a", 64)
	calls := filepath.Join(dir, "docker-calls")
	scripts := map[string]string{
		"docker": `#!/bin/sh
printf '%s\n' "$*" >> "$DOCKER_CALLS"
case "$1" in
run) printf '%s\n' "$OWNED_ID" ;;
logs) printf 'DevTools listening on ws://127.0.0.1:12345/devtools/browser/fake\n' ;;
stop) test "$2" = "$OWNED_ID" || exit 99 ;;
*) exit 98 ;;
esac
`,
		"curl": "#!/bin/sh\nexit 0\n",
		"node": "#!/bin/sh\nprintf '{}\\n' > \"$3\"\n",
	}
	for name, script := range scripts {
		if err := os.WriteFile(filepath.Join(dir, name), []byte(script), 0o700); err != nil {
			t.Fatal(err)
		}
	}
	cmd := exec.Command("bash", "run.sh")
	cmd.Env = append(os.Environ(), "PATH="+dir+":"+os.Getenv("PATH"), "DOCKER_CALLS="+calls, "OWNED_ID="+id)
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("wrapper: %v\n%s", err, out)
	}
	raw, err := os.ReadFile(calls)
	if err != nil {
		t.Fatal(err)
	}
	lines := strings.Split(strings.TrimSpace(string(raw)), "\n")
	if len(lines) < 3 || !strings.HasPrefix(lines[0], "run ") || lines[len(lines)-1] != "stop "+id {
		t.Fatalf("unsafe container lifecycle: %s", raw)
	}
	for _, line := range lines {
		if strings.Contains(line, "strand-chrome-fixture") || strings.HasPrefix(line, "rm ") || (strings.HasPrefix(line, "stop ") && line != "stop "+id) {
			t.Fatalf("touched unowned container: %s", line)
		}
	}
	if !strings.Contains(lines[0], "--remote-debugging-port=0") {
		t.Fatalf("fixed CDP port: %s", lines[0])
	}
}

func TestWrapperDoesNotCleanupInvalidDockerOutput(t *testing.T) {
	dir := t.TempDir()
	calls := filepath.Join(dir, "calls")
	script := `#!/bin/sh
printf '%s\n' "$*" >> "$DOCKER_CALLS"
printf 'strand-chrome-fixture\n'
`
	if err := os.WriteFile(filepath.Join(dir, "docker"), []byte(script), 0o700); err != nil {
		t.Fatal(err)
	}
	cmd := exec.Command("bash", "run.sh")
	cmd.Env = append(os.Environ(), "PATH="+dir+":"+os.Getenv("PATH"), "DOCKER_CALLS="+calls)
	if out, err := cmd.CombinedOutput(); err == nil || !strings.Contains(string(out), "invalid container ID") {
		t.Fatalf("invalid create output accepted: %v %s", err, out)
	}
	raw, err := os.ReadFile(calls)
	if err != nil {
		t.Fatal(err)
	}
	for _, line := range strings.Split(strings.TrimSpace(string(raw)), "\n") {
		if strings.HasPrefix(line, "stop ") || strings.HasPrefix(line, "rm ") {
			t.Fatalf("cleanup used an unproven identity: %s", raw)
		}
	}
}
