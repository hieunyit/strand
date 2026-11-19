package web

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"net/http"
	"os"
	"regexp"
	"strconv"
	"strings"
	"time"

	"gopkg.in/yaml.v3"
)

// Tuning: các khóa điều chỉnh tốc độ/an toàn của snapshot, sửa qua Web UI/API. Chỉ gồm khóa ĐÃ có tác dụng
// (USAGE §6.2); khóa chỉ được nhận mà chưa dùng (§6.3) cố ý không đưa vào.
// Lưu bằng cách sửa node YAML tại chỗ (giữ comment và khóa khác) rồi đi qua saveConfigContent
// (validate -> sao lưu -> ghi nguyên tử).

type tuningKind int

const (
	kInt tuningKind = iota
	kChunk
	kWindows
	kEnum
	kDuration
	kRatio
)

type tuningDef struct {
	Key     string
	Path    []string
	Kind    tuningKind
	Label   string
	Help    string
	Default string // hiển thị khi khóa vắng
	Options []string
	Min     int
	Max     int
}

var tuningDefs = []tuningDef{
	{Key: "iops_soft", Path: []string{"resources", "source", "iops", "soft"}, Kind: kInt, Min: 1, Max: 10000000, Default: "600",
		Label: "Source read limit, soft (tokens/s)",
		Help:  "Snapshot read rate on the source. Lower it to be gentler on the source. Takes effect after a restart; for an immediate change use Throttle (Graph tab)."},
	{Key: "iops_hard", Path: []string{"resources", "source", "iops", "hard"}, Kind: kInt, Min: 1, Max: 10000000, Default: "none",
		Label: "Source read limit, hard (tokens/s)",
		Help:  "Ceiling the GRACE governor never exceeds. Must be at least the soft limit. Takes effect after a restart."},
	{Key: "chunk_rows", Path: []string{"bootstrap", "logical", "chunk_rows"}, Kind: kChunk, Min: 1, Max: 10000000, Default: "auto (5000)",
		Label: "Chunk rows",
		Help:  "Rows per snapshot chunk: a number, or auto. Smaller chunks mean shorter locks and less load per read."},
	{Key: "max_parallel", Path: []string{"bootstrap", "logical", "max_parallel"}, Kind: kInt, Min: 1, Max: 32, Default: "1",
		Label: "Tables in parallel",
		Help:  "How many tables the snapshot copies at the same time (1-32). The source read limit is shared by all of them; memory and target connections grow with this number. Ignored for PostgreSQL sources. Takes effect after a restart."},
	{Key: "windows", Path: []string{"bootstrap", "logical", "windows"}, Kind: kWindows, Default: "any time",
		Label: "Snapshot windows",
		Help:  "Hours when the snapshot may run, local time of the process, comma-separated, for example 22:00-06:00 (may cross midnight). Empty = any time."},
	{Key: "algorithm", Path: []string{"governor", "algorithm"}, Kind: kEnum, Options: []string{"static", "onoff", "grace-v2"}, Default: "onoff",
		Label: "Governor algorithm",
		Help:  "static = fixed rate, no breaker; onoff = pause/resume by lag; grace-v2 = adaptive rate with breaker."},
	{Key: "lag_slo", Path: []string{"governor", "lag_slo"}, Kind: kDuration, Default: "5s",
		Label: "Lag target (SLO)",
		Help:  "Target replication lag, for example 5s. The governor slows or pauses the snapshot when lag exceeds it."},
	{Key: "breaker_max_ratio", Path: []string{"governor", "breaker", "strand_query_p99", "max_ratio"}, Kind: kRatio, Default: "off",
		Label: "Breaker: query latency ratio",
		Help:  "Open the breaker (pause the snapshot) when Strand's query latency exceeds this many times its baseline, for example 3. Must be greater than 1."},
	{Key: "replica_lag_max", Path: []string{"governor", "breaker", "replica_lag", "max"}, Kind: kDuration, Default: "off",
		Label: "Breaker: replica lag max",
		Help:  "Open the breaker when a source replica lags more than this, for example 30s."},
	{Key: "workers", Path: []string{"resources", "workers", "max"}, Kind: kInt, Min: 1, Max: 1024, Default: "8",
		Label: "Target workers",
		Help:  "Apply threads on the target (relaxed commit order; strict always uses 1). Takes effect after a restart."},
}

func tuningDefByKey(k string) *tuningDef {
	for i := range tuningDefs {
		if tuningDefs[i].Key == k {
			return &tuningDefs[i]
		}
	}
	return nil
}

var windowRE = regexp.MustCompile(`^([01]?[0-9]|2[0-3]):[0-5][0-9]-([01]?[0-9]|2[0-3]):[0-5][0-9]$`)

func validateDuration(s string) error {
	d, err := time.ParseDuration(s)
	if err != nil || d <= 0 || d > 24*time.Hour {
		return fmt.Errorf("%q is not a duration between 1ms and 24h (examples: 5s, 30s, 2m)", s)
	}
	return nil
}

func validateRatio(x float64) error {
	if math.IsNaN(x) || x <= 1 || x > 1000 {
		return fmt.Errorf("%g must be greater than 1 and at most 1000 (for example 3)", x)
	}
	return nil
}

func validateWindowList(ws []string) error {
	for _, w := range ws {
		if !windowRE.MatchString(w) {
			return fmt.Errorf("%q is not a time window; use HH:MM-HH:MM, for example 22:00-06:00", w)
		}
	}
	return nil
}

func validateAlgorithm(a string) error {
	switch a {
	case "static", "onoff", "grace-v2":
		return nil
	}
	return fmt.Errorf("%q: want static, onoff or grace-v2", a)
}

func validateIOPSPair(soft, hard int) error {
	if soft > 0 && hard > 0 && hard < soft {
		return fmt.Errorf("iops_hard (%d) must be at least iops_soft (%d)", hard, soft)
	}
	return nil
}

// parseTuningValue chuẩn hóa một giá trị nhập vào thành chuỗi hợp lệ ("" = bỏ khóa, về mặc định).
// Windows trả về chuỗi nối bằng ",".
func parseTuningValue(d *tuningDef, raw any) (string, error) {
	var s string
	switch v := raw.(type) {
	case nil:
		return "", nil
	case string:
		s = strings.TrimSpace(v)
	case json.Number:
		s = v.String()
	case float64:
		s = strconv.FormatFloat(v, 'f', -1, 64)
	case bool:
		return "", errors.New("want a value, not true/false")
	case []any:
		parts := make([]string, 0, len(v))
		for _, e := range v {
			es, ok := e.(string)
			if !ok {
				return "", errors.New("want a list of strings")
			}
			if es = strings.TrimSpace(es); es != "" {
				parts = append(parts, es)
			}
		}
		s = strings.Join(parts, ",")
	default:
		return "", errors.New("unsupported value type")
	}
	if s == "" {
		return "", nil
	}
	switch d.Kind {
	case kInt:
		n, err := strconv.Atoi(s)
		if err != nil || n < d.Min || n > d.Max {
			return "", fmt.Errorf("%q is not an integer between %d and %d", s, d.Min, d.Max)
		}
		return strconv.Itoa(n), nil
	case kChunk:
		if strings.EqualFold(s, "auto") {
			return "auto", nil
		}
		n, err := strconv.Atoi(s)
		if err != nil || n < d.Min || n > d.Max {
			return "", fmt.Errorf("%q is not auto or an integer between %d and %d", s, d.Min, d.Max)
		}
		return strconv.Itoa(n), nil
	case kWindows:
		var ws []string
		for _, w := range strings.FieldsFunc(s, func(r rune) bool { return r == ',' || r == ';' }) {
			if w = strings.TrimSpace(w); w != "" {
				ws = append(ws, w)
			}
		}
		if err := validateWindowList(ws); err != nil {
			return "", err
		}
		return strings.Join(ws, ","), nil
	case kEnum:
		if err := validateAlgorithm(s); err != nil {
			return "", err
		}
		return s, nil
	case kDuration:
		if err := validateDuration(s); err != nil {
			return "", err
		}
		return s, nil
	case kRatio:
		x, err := strconv.ParseFloat(s, 64)
		if err != nil {
			return "", fmt.Errorf("%q is not a number", s)
		}
		if err := validateRatio(x); err != nil {
			return "", err
		}
		return strconv.FormatFloat(x, 'f', -1, 64), nil
	}
	return "", errors.New("unknown field kind")
}

// ---- thao tác trên cây node YAML ----

func docMapping(doc *yaml.Node) (*yaml.Node, error) {
	if doc.Kind == yaml.DocumentNode && len(doc.Content) == 1 {
		doc = doc.Content[0]
	}
	if doc.Kind != yaml.MappingNode {
		return nil, errors.New("the config file is not a YAML mapping")
	}
	return doc, nil
}

func mapGet(m *yaml.Node, key string) *yaml.Node {
	for i := 0; i+1 < len(m.Content); i += 2 {
		if m.Content[i].Value == key {
			return m.Content[i+1]
		}
	}
	return nil
}

func lookupPath(root *yaml.Node, path []string) *yaml.Node {
	n := root
	for _, k := range path {
		if n == nil || n.Kind != yaml.MappingNode {
			return nil
		}
		n = mapGet(n, k)
	}
	return n
}

func nodeText(n *yaml.Node) string {
	switch n.Kind {
	case yaml.ScalarNode:
		if n.Tag == "!!null" {
			return ""
		}
		return n.Value
	case yaml.SequenceNode:
		parts := make([]string, 0, len(n.Content))
		for _, c := range n.Content {
			parts = append(parts, c.Value)
		}
		return strings.Join(parts, ", ")
	}
	return ""
}

func fillValueNode(n *yaml.Node, d *tuningDef, val string) {
	switch d.Kind {
	case kWindows:
		n.Kind, n.Tag, n.Value, n.Style = yaml.SequenceNode, "!!seq", "", yaml.FlowStyle
		n.Content = nil
		for _, w := range strings.Split(val, ",") {
			n.Content = append(n.Content, &yaml.Node{Kind: yaml.ScalarNode, Tag: "!!str", Value: w, Style: yaml.DoubleQuotedStyle})
		}
	case kInt:
		n.Kind, n.Tag, n.Value, n.Style, n.Content = yaml.ScalarNode, "!!int", val, 0, nil
	case kChunk:
		tag := "!!int"
		if val == "auto" {
			tag = "!!str"
		}
		n.Kind, n.Tag, n.Value, n.Style, n.Content = yaml.ScalarNode, tag, val, 0, nil
	case kRatio:
		n.Kind, n.Tag, n.Value, n.Style, n.Content = yaml.ScalarNode, "!!float", val, 0, nil
	default:
		n.Kind, n.Tag, n.Value, n.Style, n.Content = yaml.ScalarNode, "!!str", val, 0, nil
	}
}

func setPath(root *yaml.Node, d *tuningDef, val string) {
	m := root
	for _, k := range d.Path[:len(d.Path)-1] {
		next := mapGet(m, k)
		switch {
		case next == nil:
			next = &yaml.Node{Kind: yaml.MappingNode, Tag: "!!map"}
			m.Content = append(m.Content, &yaml.Node{Kind: yaml.ScalarNode, Tag: "!!str", Value: k}, next)
		case next.Kind != yaml.MappingNode: // khóa có sẵn nhưng null/rỗng: thành mapping, giữ comment
			next.Kind, next.Tag, next.Value, next.Style, next.Content = yaml.MappingNode, "!!map", "", 0, nil
		}
		m = next
	}
	last := d.Path[len(d.Path)-1]
	if n := mapGet(m, last); n != nil {
		fillValueNode(n, d, val)
		return
	}
	nn := &yaml.Node{}
	fillValueNode(nn, d, val)
	m.Content = append(m.Content, &yaml.Node{Kind: yaml.ScalarNode, Tag: "!!str", Value: last}, nn)
}

// removePath xóa khóa và dọn các mapping cha rỗng do việc xóa tạo ra.
func removePath(m *yaml.Node, path []string) {
	if m == nil || m.Kind != yaml.MappingNode {
		return
	}
	for i := 0; i+1 < len(m.Content); i += 2 {
		if m.Content[i].Value != path[0] {
			continue
		}
		if len(path) == 1 {
			m.Content = append(m.Content[:i], m.Content[i+2:]...)
			return
		}
		child := m.Content[i+1]
		removePath(child, path[1:])
		if child.Kind == yaml.MappingNode && len(child.Content) == 0 {
			m.Content = append(m.Content[:i], m.Content[i+2:]...)
		}
		return
	}
}

// TuningField: một khóa trong view/response.
type TuningField struct {
	Key     string   `json:"key"`
	Label   string   `json:"label"`
	Help    string   `json:"help"`
	Value   string   `json:"value"`   // giá trị hiện tại trong file ("" nếu vắng)
	Set     bool     `json:"set"`     // khóa có trong file
	Default string   `json:"default"` // mặc định của Strand (hiển thị)
	YAMLKey string   `json:"yaml_key"`
	Options []string `json:"options,omitempty"`
}

type TuningView struct {
	Path   string        `json:"path"`
	Fields []TuningField `json:"fields"`
	Error  string        `json:"error,omitempty"`
}

func (s *Server) tuningView(p *Pipeline) TuningView {
	v := TuningView{Path: p.ConfigPath, Fields: []TuningField{}}
	if s.db != nil {
		s.materialize(p.Name)
	}
	raw := s.getOriginalConfig(p)
	if len(raw) == 0 {
		var err error
		raw, err = os.ReadFile(p.ConfigPath)
		if err != nil {
			v.Error = "cannot read config file: " + err.Error()
			return v
		}
	}
	var doc yaml.Node
	var root *yaml.Node
	if err := yaml.Unmarshal(raw, &doc); err != nil {
		v.Error = "config is not valid YAML: " + MaskSecrets(err.Error())
	} else if root, err = docMapping(&doc); err != nil {
		v.Error = err.Error()
		root = nil
	}
	for _, d := range tuningDefs {
		f := TuningField{Key: d.Key, Label: d.Label, Help: d.Help, Default: d.Default,
			YAMLKey: strings.Join(d.Path, "."), Options: d.Options}
		if root != nil {
			if n := lookupPath(root, d.Path); n != nil {
				if t := nodeText(n); t != "" {
					f.Value, f.Set = t, true
				}
			}
		}
		v.Fields = append(v.Fields, f)
	}
	return v
}

// tuningFieldErrors: lỗi theo từng trường, trả 400.
type tuningFieldErrors map[string]string

func (e tuningFieldErrors) Error() string {
	var parts []string
	for _, d := range tuningDefs {
		if m, ok := e[d.Key]; ok {
			parts = append(parts, d.Key+": "+m)
		}
	}
	for k, m := range e {
		if tuningDefByKey(k) == nil {
			parts = append(parts, k+": "+m)
		}
	}
	return strings.Join(parts, "; ")
}

// applyTuning áp các thay đổi (khóa vắng trong req = giữ nguyên; giá trị rỗng = xóa khóa) lên YAML gốc.
// Trả YAML mới và danh sách khóa đã đổi.
func applyTuning(orig []byte, req map[string]any) ([]byte, []string, error) {
	errs := tuningFieldErrors{}
	vals := map[string]string{}
	for k, raw := range req {
		d := tuningDefByKey(k)
		if d == nil {
			errs[k] = "unknown field"
			continue
		}
		v, err := parseTuningValue(d, raw)
		if err != nil {
			errs[k] = err.Error()
			continue
		}
		vals[k] = v
	}
	var doc yaml.Node
	if err := yaml.Unmarshal(orig, &doc); err != nil {
		return nil, nil, BadRequest("the config file is not valid YAML: %s", MaskSecrets(err.Error()))
	}
	root, err := docMapping(&doc)
	if err != nil {
		return nil, nil, BadRequest("%s", err.Error())
	}
	// Kiểm tra chéo hard >= soft trên giá trị hiệu lực sau khi áp.
	eff := func(k string) int {
		if v, ok := vals[k]; ok {
			n, _ := strconv.Atoi(v)
			return n
		}
		if n := lookupPath(root, tuningDefByKey(k).Path); n != nil {
			x, _ := strconv.Atoi(n.Value)
			return x
		}
		return 0
	}
	_, hasSoft := vals["iops_soft"]
	_, hasHard := vals["iops_hard"]
	if (hasSoft || hasHard) && errs["iops_soft"] == "" && errs["iops_hard"] == "" {
		if err := validateIOPSPair(eff("iops_soft"), eff("iops_hard")); err != nil {
			errs["iops_hard"] = err.Error()
		}
	}
	if len(errs) > 0 {
		return nil, nil, errs
	}
	var changed []string
	for i := range tuningDefs {
		d := &tuningDefs[i]
		v, ok := vals[d.Key]
		if !ok {
			continue
		}
		before := ""
		if n := lookupPath(root, d.Path); n != nil {
			before = nodeText(n)
		}
		if d.Kind == kWindows {
			before = strings.ReplaceAll(before, ", ", ",")
		}
		if v == "" {
			if lookupPath(root, d.Path) != nil {
				removePath(root, d.Path)
				changed = append(changed, d.Key)
			}
			continue
		}
		if before == v {
			continue
		}
		setPath(root, d, v)
		changed = append(changed, d.Key)
	}
	if len(changed) == 0 {
		return orig, nil, nil
	}
	var buf bytes.Buffer
	enc := yaml.NewEncoder(&buf)
	enc.SetIndent(2)
	if err := enc.Encode(&doc); err != nil {
		return nil, nil, fmt.Errorf("encode yaml: %w", err)
	}
	_ = enc.Close()
	return buf.Bytes(), changed, nil
}

// ---- HTTP ----

type tuningResult struct {
	OK        bool     `json:"ok"`
	DryRun    bool     `json:"dry_run,omitempty"`
	Changed   []string `json:"changed"`
	Diff      string   `json:"diff"`
	Valid     *bool    `json:"valid,omitempty"`
	Output    string   `json:"output,omitempty"`
	Backup    string   `json:"backup,omitempty"`
	Restarted bool     `json:"restarted"`
	Message   string   `json:"message"`
}

func (s *Server) apiTuningGet(w http.ResponseWriter, r *http.Request, sess *Session) {
	p := s.reg.Get(r.PathValue("name"))
	if p == nil {
		writeErr(w, http.StatusNotFound, "unknown pipeline")
		return
	}
	writeJSON(w, http.StatusOK, s.tuningView(p))
}

func (s *Server) apiTuningSave(w http.ResponseWriter, r *http.Request, sess *Session) {
	p := s.reg.Get(r.PathValue("name"))
	if p == nil {
		writeErr(w, http.StatusNotFound, "unknown pipeline")
		return
	}
	r.Body = http.MaxBytesReader(w, r.Body, 1<<20)
	dec := json.NewDecoder(r.Body)
	dec.UseNumber()
	var req map[string]any
	if err := dec.Decode(&req); err != nil || req == nil {
		writeErr(w, http.StatusBadRequest, "invalid request body")
		return
	}
	restart, _ := req["restart"].(bool)
	dry, _ := req["dry_run"].(bool)
	force, _ := req["force"].(bool)
	delete(req, "restart")
	delete(req, "dry_run")
	delete(req, "force")

	res, err := s.saveTuning(r.Context(), p, req, restart, dry, force, s.clientIP(r))
	if err != nil {
		var fe tuningFieldErrors
		var ae *ActionError
		switch {
		case errors.As(err, &fe):
			writeJSON(w, http.StatusBadRequest, map[string]any{"error": "invalid tuning values: " + fe.Error(), "fields": fe})
		case errors.As(err, &ae):
			writeErr(w, ae.Status, ae.Msg)
		default:
			writeErr(w, http.StatusInternalServerError, err.Error())
		}
		return
	}
	writeJSON(w, http.StatusOK, res)
}

func (s *Server) saveTuning(ctx context.Context, p *Pipeline, req map[string]any, restart, dry, force bool, ip string) (*tuningResult, error) {
	s.configEditMu.Lock()
	defer s.configEditMu.Unlock()
	if p == nil || p.ConfigPath == "" {
		return nil, errors.New("pipeline has no config path")
	}
	if s.db != nil {
		s.materialize(p.Name)
	}
	orig := s.getOriginalConfig(p)
	if len(orig) == 0 {
		var err error
		orig, err = os.ReadFile(p.ConfigPath)
		if err != nil {
			return nil, fmt.Errorf("read config: %w", err)
		}
	}
	out, changed, err := applyTuning(orig, req)
	if err != nil {
		return nil, err
	}
	res := &tuningResult{OK: true, DryRun: dry, Changed: changed}
	if res.Changed == nil {
		res.Changed = []string{}
	}
	if len(changed) == 0 {
		res.Message = "No changes: the values already match the config file"
		return res, nil
	}
	oldMasked, err := MaskConfigText(orig)
	if err != nil {
		return nil, err
	}
	newMasked, err := MaskConfigText(out)
	if err != nil {
		return nil, err
	}
	res.Diff = unifiedDiff(p.ConfigPath, p.ConfigPath+" (new)", oldMasked, newMasked)
	if dry {
		vr, err := s.validateConfigContent(ctx, p, string(out))
		if err != nil {
			return nil, err
		}
		res.Valid, res.Output = &vr.Valid, vr.Output
		res.OK = vr.Valid
		res.Message = "Preview only, nothing was written"
		return res, nil
	}
	sr, err := s.saveConfigContentLocked(ctx, p, string(out), restart, force, ip)
	if err != nil {
		return nil, err
	}
	res.Backup, res.Restarted = sr.Backup, sr.Restarted
	res.Message = "Tuning saved. Read limits and most keys apply after a restart."
	if sr.Restarted {
		res.Message = "Tuning saved and the pipeline was restarted."
	}
	zero := 0
	_ = s.audit.Log(AuditEntry{IP: ip, Action: "config.tuning", Pipeline: p.Name, Result: "ok", ExitCode: &zero,
		Detail: map[string]any{"changed": changed, "backup": sr.Backup, "restarted": sr.Restarted}})
	return res, nil
}
