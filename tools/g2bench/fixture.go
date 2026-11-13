package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"
	"time"
)

// ---------------------------------------------------------------------------
// Manifest logic của fixture: đếm + checksum từng bảng, băm chuẩn hoá toàn bộ.
// ---------------------------------------------------------------------------

type TableSum struct {
	Name     string `json:"name"`
	Rows     int64  `json:"rows"`
	Checksum string `json:"checksum"`
}

type Manifest struct {
	Engine string     `json:"engine"`
	Tables []TableSum `json:"tables"`
}

// Hash: sha256 của JSON chuẩn hoá (bảng sắp theo tên).
func (m Manifest) Hash() string {
	t := append([]TableSum(nil), m.Tables...)
	sort.Slice(t, func(i, j int) bool { return t[i].Name < t[j].Name })
	b, _ := json.Marshal(Manifest{Engine: m.Engine, Tables: t})
	h := sha256.Sum256(b)
	return hex.EncodeToString(h[:])
}

// DiffManifest liệt kê mọi khác biệt giữa mốc fixture và trạng thái đọc lại; rỗng = khớp.
func DiffManifest(want, got Manifest) []string {
	var d []string
	gm := map[string]TableSum{}
	for _, t := range got.Tables {
		gm[t.Name] = t
	}
	wm := map[string]bool{}
	for _, w := range want.Tables {
		wm[w.Name] = true
		g, ok := gm[w.Name]
		switch {
		case !ok:
			d = append(d, w.Name+": missing")
		case g.Rows != w.Rows:
			d = append(d, fmt.Sprintf("%s: rows %d != %d", w.Name, g.Rows, w.Rows))
		case g.Checksum != w.Checksum:
			d = append(d, fmt.Sprintf("%s: checksum %s != %s", w.Name, g.Checksum, w.Checksum))
		}
	}
	for _, g := range got.Tables {
		if !wm[g.Name] {
			d = append(d, g.Name+": unexpected table")
		}
	}
	return d
}

// FoldTreeHash gộp các dòng "<sha256>  <path>" (đã sort theo path) thành một hash cây thư mục.
func FoldTreeHash(lines string) (string, error) {
	lines = strings.TrimSpace(lines)
	if lines == "" {
		return "", errors.New("empty file list: datadir has no files")
	}
	for _, l := range strings.Split(lines, "\n") {
		if len(l) < 66 || l[64] != ' ' {
			return "", fmt.Errorf("malformed sha256sum line %q", l)
		}
	}
	h := sha256.Sum256([]byte(lines + "\n"))
	return hex.EncodeToString(h[:]), nil
}

// ---------------------------------------------------------------------------
// Fixture trên đĩa: <dir>/fixture.json + source.tar + target.tar
// ---------------------------------------------------------------------------

type Fixture struct {
	Created        time.Time `json:"created"`
	Engine         string    `json:"engine"`
	Params         string    `json:"params"`
	Manifest       Manifest  `json:"manifest"`
	ManifestSHA256 string    `json:"manifest_sha256"`
	SourceTar      string    `json:"source_tar_sha256"`
	SourceTree     string    `json:"source_tree_sha256"`
	TargetTar      string    `json:"target_tar_sha256"`
	TargetTree     string    `json:"target_tree_sha256"`
}

func LoadFixture(dir string) (Fixture, error) {
	var f Fixture
	b, err := os.ReadFile(filepath.Join(dir, "fixture.json"))
	if err != nil {
		return f, err
	}
	if err := json.Unmarshal(b, &f); err != nil {
		return f, err
	}
	if f.ManifestSHA256 == "" || f.ManifestSHA256 != f.Manifest.Hash() || f.SourceTree == "" || f.TargetTree == "" {
		return f, errors.New("fixture.json incomplete or manifest hash mismatch")
	}
	return f, nil
}

// ---------------------------------------------------------------------------
// Node docker: chụp / khôi phục volume dữ liệu của container
// ---------------------------------------------------------------------------

type dockerNode struct {
	Container string
	Image     string // image helper (có sh, tar, find, sha256sum, xargs)
}

func docker(ctx context.Context, args ...string) (string, error) {
	cmd := exec.CommandContext(ctx, "docker", args...)
	out, err := cmd.CombinedOutput()
	if err != nil {
		return string(out), fmt.Errorf("docker %s: %v: %s", strings.Join(args, " "), err, strings.TrimSpace(string(out)))
	}
	return string(out), nil
}

// dataMount tìm mount dữ liệu (volume hoặc bind) của container.
func (n dockerNode) dataMount(ctx context.Context) (string, error) {
	out, err := docker(ctx, "inspect", n.Container, "--format",
		`{{range .Mounts}}{{.Destination}}|{{.Type}}|{{.Name}}|{{.Source}}{{"\n"}}{{end}}`)
	if err != nil {
		return "", err
	}
	return pickDataMount(out)
}

// pickDataMount: nhận dòng "dest|type|name|source", trả nguồn mount cho -v của helper.
func pickDataMount(inspect string) (string, error) {
	for _, l := range strings.Split(strings.TrimSpace(inspect), "\n") {
		p := strings.Split(l, "|")
		if len(p) != 4 {
			continue
		}
		switch p[0] {
		case "/var/lib/mysql", "/var/lib/postgresql/data", "/var/lib/postgresql":
			if p[1] == "volume" {
				return p[2], nil
			}
			return p[3], nil
		}
	}
	return "", errors.New("container has no recognizable data mount (/var/lib/mysql or /var/lib/postgresql)")
}

func (n dockerNode) helper(ctx context.Context, mount, snapDir, script string) (string, error) {
	return docker(ctx, "run", "--rm", "--network", "none", "-v", mount+":/data", "-v", snapDir+":/snap", n.Image, "sh", "-ec", script)
}

const treeHashScript = `cd /data && LC_ALL=C find . -type f -print0 | LC_ALL=C sort -z | xargs -0 sha256sum`

func (n dockerNode) TreeHash(ctx context.Context, mount, snapDir string) (string, error) {
	out, err := n.helper(ctx, mount, snapDir, treeHashScript)
	if err != nil {
		return "", err
	}
	return FoldTreeHash(out)
}

// Snapshot: dừng container, chụp datadir thành tar + hash cây, bật lại.
func (n dockerNode) Snapshot(ctx context.Context, snapDir, name string) (tarSHA, tree string, err error) {
	mount, err := n.dataMount(ctx)
	if err != nil {
		return "", "", err
	}
	if _, err = docker(ctx, "stop", "-t", "120", n.Container); err != nil {
		return
	}
	defer func() {
		if _, e := docker(ctx, "start", n.Container); e != nil && err == nil {
			err = e
		}
	}()
	if _, err = n.helper(ctx, mount, snapDir, "cd /data && tar --numeric-owner -cpf /snap/"+name+".tar ."); err != nil {
		return
	}
	sum, err := n.helper(ctx, mount, snapDir, "sha256sum /snap/"+name+".tar")
	if err != nil {
		return
	}
	tarSHA = strings.Fields(sum)[0]
	tree, err = n.TreeHash(ctx, mount, snapDir)
	return
}

// Restore: dừng, xoá datadir, giải nén tar, đo hash cây NGAY khi container còn dừng (trước khi
// server ghi gì), rồi mới bật. Trả hash đọc được; bên gọi so với mốc fixture.
func (n dockerNode) Restore(ctx context.Context, snapDir, name string) (tree string, err error) {
	mount, err := n.dataMount(ctx)
	if err != nil {
		return "", err
	}
	if _, err = docker(ctx, "stop", "-t", "120", n.Container); err != nil {
		return
	}
	if _, err = n.helper(ctx, mount, snapDir,
		"find /data -mindepth 1 -delete && tar --numeric-owner -xpf /snap/"+name+".tar -C /data"); err != nil {
		return
	}
	tree, err = n.TreeHash(ctx, mount, snapDir)
	if err != nil {
		return
	}
	_, err = docker(ctx, "start", n.Container)
	return
}
