package web

import (
	"bytes"
	"encoding/json"
	"fmt"
	"regexp"
	"sort"
	"strings"
	"unicode/utf8"

	"gopkg.in/yaml.v3"
)

// Che / khôi phục bí mật nhưng giữ nguyên văn bản nguồn: chỉ thay đúng khoảng byte của scalar bí mật
// (xác định bằng vị trí node YAML, không bằng regex theo dòng), nên block/flow/JSON compact đều được che.

// maskTokRE: token che do MaskConfigText sinh ra hoặc do người dùng gõ lại. Dấu nháy chỉ được nuốt khi cân bằng.
var maskTokRE = regexp.MustCompile(`"\*{8}"|'\*{8}'|\*{8}`)

func isSecretYAMLKey(k string) bool { return secretKeyRE.MatchString(k) && !safeKeyRE.MatchString(k) }

type secretScalar struct {
	path     string
	isKeyed  bool
	raw      string // exact raw text from orig [off:end]
	val      string // decoded yaml string value
	style    yaml.Style
	line     int
	col      int
	flow     bool
	start    int // byte offset in original text
	end      int // byte offset in original text
	embedded []embeddedSecret
}

type embeddedSecret struct {
	ordinal int
	start   int    // relative to the decoded scalar value
	end     int    // relative to the decoded scalar value
	value   string // decoded DSN component, including its own quoting/escapes
}

type replacementRange struct {
	start       int
	end         int
	repl        string
	path        string
	expectedVal string
	isKeyed     bool
}

func lineStarts(raw []byte) []int {
	starts := []int{0}
	for i, c := range raw {
		if c == '\n' {
			starts = append(starts, i+1)
		}
	}
	return starts
}

// scalarStart đổi (line, column theo ký tự) của node thành offset byte rồi bỏ qua anchor/tag đứng trước scalar.
func scalarStart(raw []byte, starts []int, line, col int) (int, bool) {
	if line < 1 || line > len(starts) || col < 1 {
		return 0, false
	}
	off := starts[line-1]
	for i := 1; i < col; i++ {
		if off >= len(raw) || raw[off] == '\n' {
			return 0, false
		}
		_, w := utf8.DecodeRune(raw[off:])
		off += w
	}
	for off < len(raw) && (raw[off] == '&' || raw[off] == '!') {
		for off < len(raw) && raw[off] != ' ' && raw[off] != '\t' && raw[off] != '\n' {
			off++
		}
		for off < len(raw) && (raw[off] == ' ' || raw[off] == '\t') {
			off++
		}
	}
	return off, off < len(raw)
}

// scalarEnd tìm điểm kết thúc của scalar trên MỘT dòng; ok=false khi không xác định chắc chắn (nhiều dòng,
// văn bản nguồn không khớp giá trị đã giải mã...) để bên gọi fail-closed.
func scalarEnd(raw []byte, off int, n *yaml.Node, flow bool) (int, bool) {
	switch {
	case n.Style&yaml.DoubleQuotedStyle != 0:
		if raw[off] != '"' {
			return 0, false
		}
		for i := off + 1; i < len(raw) && raw[i] != '\n'; i++ {
			switch raw[i] {
			case '\\':
				i++
			case '"':
				return i + 1, true
			}
		}
		return 0, false
	case n.Style&yaml.SingleQuotedStyle != 0:
		if raw[off] != '\'' {
			return 0, false
		}
		for i := off + 1; i < len(raw) && raw[i] != '\n'; i++ {
			if raw[i] == '\'' {
				if i+1 < len(raw) && raw[i+1] == '\'' {
					i++
					continue
				}
				return i + 1, true
			}
		}
		return 0, false
	}
	i := off
	for i < len(raw) {
		c := raw[i]
		if c == '\n' || c == '\r' {
			break
		}
		if c == '#' && i > off && (raw[i-1] == ' ' || raw[i-1] == '\t') {
			break
		}
		if flow && (c == ',' || c == '}' || c == ']') {
			break
		}
		i++
	}
	end := i
	for end > off && (raw[end-1] == ' ' || raw[end-1] == '\t') {
		end--
	}
	if string(raw[off:end]) != n.Value {
		return 0, false // plain scalar nhiều dòng / có tag: không che an toàn được
	}
	return end, true
}

type kvScalarHit struct {
	valStart, end int
}

func findKVSecretsInScalar(s string) []kvScalarHit {
	var hits []kvScalarHit
	pos := 0
	for _, loc := range kvKeyRE.FindAllStringSubmatchIndex(s, -1) {
		if loc[0] < pos {
			continue
		}
		valStart := loc[1]
		end := scanKVValue(s, valStart)
		if end <= valStart {
			continue
		}
		hits = append(hits, kvScalarHit{valStart, end})
		pos = end
	}
	return hits
}

func isMaskedKVValueInScalar(value string) bool {
	return value == maskStr ||
		value == `"`+maskStr+`"` ||
		value == `'`+maskStr+`'` ||
		value == `\"`+maskStr+`\"` ||
		value == `\'`+maskStr+`\'`
}

// findEmbeddedInScalar finds secrets in a decoded scalar (URL, MySQL DSN, keyword DSN).
func findEmbeddedInScalar(s string) []embeddedSecret {
	type hit struct {
		start, end int
		val        string
	}
	var hits []hit
	for _, re := range []*regexp.Regexp{urlPassRE, dsnPassRE} {
		for _, loc := range re.FindAllStringSubmatchIndex(s, -1) {
			v := s[loc[4]:loc[5]]
			if v == "" || v == maskStr {
				continue
			}
			hits = append(hits, hit{loc[4], loc[5], v})
		}
	}
	for _, h := range findKVSecretsInScalar(s) {
		v := s[h.valStart:h.end]
		if v != "" && !isMaskedKVValueInScalar(v) {
			hits = append(hits, hit{h.valStart, h.end, v})
		}
	}
	if len(hits) == 0 {
		return nil
	}
	sort.Slice(hits, func(i, j int) bool { return hits[i].start < hits[j].start })
	var res []embeddedSecret
	lastEnd := 0
	for _, h := range hits {
		if h.start < lastEnd {
			continue // chồng lấn, bỏ qua
		}
		res = append(res, embeddedSecret{
			ordinal: len(res),
			start:   h.start,
			end:     h.end,
			value:   h.val,
		})
		lastEnd = h.end
	}
	return res
}

// findEmbeddedPlaceholdersInScalar tìm các vị trí placeholder '********' nhúng trong chuỗi scalar.
func findEmbeddedPlaceholdersInScalar(s string) []embeddedSecret {
	type hit struct {
		start, end int
		val        string
	}
	var hits []hit
	for _, re := range []*regexp.Regexp{urlPassRE, dsnPassRE} {
		for _, loc := range re.FindAllStringSubmatchIndex(s, -1) {
			v := s[loc[4]:loc[5]]
			if v == maskStr {
				hits = append(hits, hit{loc[4], loc[5], v})
			}
		}
	}
	for _, h := range findKVSecretsInScalar(s) {
		v := s[h.valStart:h.end]
		if isMaskedKVValueInScalar(v) {
			hits = append(hits, hit{h.valStart, h.end, v})
		}
	}
	if len(hits) == 0 {
		return nil
	}
	sort.Slice(hits, func(i, j int) bool { return hits[i].start < hits[j].start })
	var res []embeddedSecret
	lastEnd := 0
	for _, h := range hits {
		if h.start < lastEnd {
			continue
		}
		res = append(res, embeddedSecret{
			ordinal: len(res),
			start:   h.start,
			end:     h.end,
			value:   h.val,
		})
		lastEnd = h.end
	}
	return res
}

// collectSecrets duyệt AST của YAML để gom mọi scalar bí mật theo logical path.
func collectSecrets(raw []byte) (map[string]*secretScalar, error) {
	var doc yaml.Node
	if err := yaml.Unmarshal(raw, &doc); err != nil {
		return nil, err
	}
	starts := lineStarts(raw)
	secrets := map[string]*secretScalar{}
	var firstErr error

	var walk func(n *yaml.Node, key string, path string, inFlow bool)
	walk = func(n *yaml.Node, key string, path string, inFlow bool) {
		switch n.Kind {
		case yaml.DocumentNode:
			for _, c := range n.Content {
				walk(c, key, path, inFlow)
			}
		case yaml.SequenceNode:
			f := inFlow || (n.Style&yaml.FlowStyle != 0)
			for i, c := range n.Content {
				itemPath := fmt.Sprintf("%s[%d]", path, i)
				walk(c, key, itemPath, f)
			}
		case yaml.MappingNode:
			f := inFlow || (n.Style&yaml.FlowStyle != 0)
			for i := 0; i+1 < len(n.Content); i += 2 {
				k := n.Content[i].Value
				itemPath := fmt.Sprintf("%s/%d:%s", path, len(k), k)
				walk(n.Content[i+1], k, itemPath, f)
			}
		case yaml.AliasNode:
			if isSecretYAMLKey(key) && firstErr == nil {
				firstErr = fmt.Errorf("line %d: secret key %q uses an alias; cannot be masked safely", n.Line, key)
			}
		case yaml.ScalarNode:
			fail := func(why string) {
				if firstErr == nil {
					firstErr = fmt.Errorf("line %d: secret key %q %s; cannot be masked safely", n.Line, key, why)
				}
			}
			if isSecretYAMLKey(key) {
				if n.Value == "" {
					return
				}
				if n.Style&(yaml.LiteralStyle|yaml.FoldedStyle) != 0 {
					fail("uses a multi-line block scalar")
					return
				}
				off, ok := scalarStart(raw, starts, n.Line, n.Column)
				if !ok {
					fail("has an unlocatable value")
					return
				}
				end, ok := scalarEnd(raw, off, n, inFlow)
				if !ok {
					fail("has a multi-line or non-locatable value")
					return
				}
				v := strings.TrimSpace(n.Value)
				if strings.HasPrefix(v, "${") || n.Value == maskStr {
					return
				}
				secrets[path] = &secretScalar{
					path:    path,
					isKeyed: true,
					raw:     string(raw[off:end]),
					val:     n.Value,
					style:   n.Style,
					line:    n.Line,
					col:     n.Column,
					flow:    inFlow,
					start:   off,
					end:     end,
				}
				return
			}

			// Discover secrets in the decoded value first: raw YAML escapes and
			// multiline scalars must not hide credentials from the masker.
			embs := findEmbeddedInScalar(n.Value)
			if len(embs) == 0 {
				return
			}
			if n.Style&(yaml.LiteralStyle|yaml.FoldedStyle) != 0 {
				fail("contains an embedded secret in a multi-line block scalar")
				return
			}
			off, ok := scalarStart(raw, starts, n.Line, n.Column)
			if !ok {
				fail("has an unlocatable embedded secret")
				return
			}
			end, ok := scalarEnd(raw, off, n, inFlow)
			if !ok {
				fail("contains an embedded secret in a multi-line or non-locatable value")
				return
			}
			scalarRaw := string(raw[off:end])
			secrets[path] = &secretScalar{
				path:     path,
				isKeyed:  false,
				raw:      scalarRaw,
				val:      n.Value,
				style:    n.Style,
				line:     n.Line,
				col:      n.Column,
				flow:     inFlow,
				start:    off,
				end:      end,
				embedded: embs,
			}
		}
	}

	walk(&doc, "", "", false)
	if firstErr != nil {
		return nil, firstErr
	}
	return secrets, nil
}

// safeDoubleQuote mã hóa chuỗi thành double-quoted JSON/YAML scalar hợp lệ (không HTML escaping).
func safeDoubleQuote(s string) string {
	var buf bytes.Buffer
	enc := json.NewEncoder(&buf)
	enc.SetEscapeHTML(false)
	_ = enc.Encode(s)
	return strings.TrimRight(buf.String(), "\n")
}

// Encode a decoded scalar in its current YAML context. Never copy escapes
// from a different quote style into the edited scalar.
func encodeEmbeddedScalar(val string, style yaml.Style, flow, isJSON bool) string {
	if isJSON || style&yaml.DoubleQuotedStyle != 0 {
		return safeDoubleQuote(val)
	}
	if style&yaml.SingleQuotedStyle != 0 {
		return "'" + strings.ReplaceAll(val, "'", "''") + "'"
	}
	var parsed yaml.Node
	if (!flow || !strings.ContainsAny(val, ",{}[]")) && yaml.Unmarshal([]byte(val), &parsed) == nil &&
		len(parsed.Content) == 1 && parsed.Content[0].Kind == yaml.ScalarNode && parsed.Content[0].Tag == "!!str" && parsed.Content[0].Value == val {
		return val
	}
	return safeDoubleQuote(val)
}

func replaceEmbeddedValues(val string, hits []embeddedSecret, values []string) string {
	var b strings.Builder
	pos := 0
	for i, hit := range hits {
		b.WriteString(val[pos:hit.start])
		b.WriteString(values[i])
		pos = hit.end
	}
	b.WriteString(val[pos:])
	return b.String()
}

// isJSONText kiểm tra nhanh xem chuỗi có phải cú pháp JSON hay không.
func isJSONText(s string) bool {
	trimmed := strings.TrimSpace(s)
	if (strings.HasPrefix(trimmed, "{") && strings.HasSuffix(trimmed, "}")) ||
		(strings.HasPrefix(trimmed, "[") && strings.HasSuffix(trimmed, "]")) {
		var v any
		return json.Unmarshal([]byte(trimmed), &v) == nil
	}
	return false
}

// formatSecretReplacement định dạng lại bí mật cho phù hợp với ngữ cảnh cú pháp YAML/JSON của edited.
func formatSecretReplacement(origSec *secretScalar, editedNode *yaml.Node, editedInFlow bool, editedIsJSON bool) string {
	val := origSec.val

	if editedIsJSON {
		return safeDoubleQuote(val)
	}

	if editedInFlow {
		if origSec.style&yaml.DoubleQuotedStyle != 0 {
			return origSec.raw
		}
		if origSec.style&yaml.SingleQuotedStyle != 0 {
			return origSec.raw
		}
		// Plain scalar trong flow style: nếu chứa ký tự đặc biệt / phân cách flow, bắt buộc phải quote an toàn
		if strings.ContainsAny(val, ",{}[]:#\"'\t\n\r") || strings.HasPrefix(val, " ") || strings.HasSuffix(val, " ") {
			return safeDoubleQuote(val)
		}
		return origSec.raw
	}

	// Block style:
	if origSec.style&(yaml.DoubleQuotedStyle|yaml.SingleQuotedStyle) != 0 {
		return origSec.raw
	}
	if strings.ContainsAny(val, "\n\r#") || strings.HasPrefix(val, " ") || strings.HasSuffix(val, " ") || strings.Contains(val, ": ") {
		return safeDoubleQuote(val)
	}
	return origSec.raw
}

// MaskConfigText che giá trị bí mật của file cấu hình nhưng giữ NGUYÊN văn bản (định dạng, chú thích, dòng trống,
// flow-style, JSON compact): chỉ thay đúng khoảng văn bản của giá trị bí mật. File được đọc THÔ (chưa thay ${VAR}).
// Trả lỗi nếu file không phải YAML hợp lệ hoặc không xác định chắc chắn được vị trí bí mật (block scalar,
// scalar nhiều dòng).
func MaskConfigText(raw []byte) (string, error) {
	secrets, err := collectSecrets(raw)
	if err != nil {
		return "", err
	}

	type maskSpan struct {
		start, end int
		repl       string
	}
	var spans []maskSpan
	isJSON := isJSONText(string(raw))

	for _, sec := range secrets {
		if sec.isKeyed {
			spans = append(spans, maskSpan{start: sec.start, end: sec.end, repl: `"` + maskStr + `"`})
		} else {
			values := make([]string, len(sec.embedded))
			for i := range values {
				values[i] = maskStr
			}
			val := replaceEmbeddedValues(sec.val, sec.embedded, values)
			spans = append(spans, maskSpan{start: sec.start, end: sec.end,
				repl: encodeEmbeddedScalar(val, sec.style, sec.flow, isJSON)})
		}
	}

	if len(spans) == 0 {
		return string(raw), nil
	}

	sort.Slice(spans, func(i, j int) bool { return spans[i].start < spans[j].start })

	var b strings.Builder
	pos := 0
	for _, sp := range spans {
		if sp.start < pos {
			continue
		}
		b.Write(raw[pos:sp.start])
		b.WriteString(sp.repl)
		pos = sp.end
	}
	b.Write(raw[pos:])
	return b.String(), nil
}

// UnmaskConfigText khôi phục bí mật gốc cho mọi vị trí người dùng gửi lại với '********' (văn bản từ MaskConfigText),
// dựa trên file gốc theo danh tính đường dẫn scalar (mapping path + sequence index), không phụ thuộc vào dòng hay thứ tự.
func UnmaskConfigText(edited string, orig []byte) (string, error) {
	origSecrets, err := collectSecrets(orig)
	if err != nil {
		return "", err
	}

	var editedDoc yaml.Node
	if err := yaml.Unmarshal([]byte(edited), &editedDoc); err != nil {
		if strings.Contains(edited, maskStr) {
			return "", fmt.Errorf("cannot restore masked value ********: %w", err)
		}
		return "", err
	}

	starts := lineStarts([]byte(edited))
	isJSON := isJSONText(edited)
	var replacements []replacementRange
	var firstErr error

	var walk func(n *yaml.Node, key string, path string, inFlow bool)
	walk = func(n *yaml.Node, key string, path string, inFlow bool) {
		if firstErr != nil {
			return
		}
		switch n.Kind {
		case yaml.DocumentNode:
			for _, c := range n.Content {
				walk(c, key, path, inFlow)
			}
		case yaml.SequenceNode:
			f := inFlow || (n.Style&yaml.FlowStyle != 0)
			for i, c := range n.Content {
				itemPath := fmt.Sprintf("%s[%d]", path, i)
				walk(c, key, itemPath, f)
			}
		case yaml.MappingNode:
			f := inFlow || (n.Style&yaml.FlowStyle != 0)
			for i := 0; i+1 < len(n.Content); i += 2 {
				k := n.Content[i].Value
				itemPath := fmt.Sprintf("%s/%d:%s", path, len(k), k)
				walk(n.Content[i+1], k, itemPath, f)
			}
		case yaml.AliasNode:
			if (isSecretYAMLKey(key) || strings.Contains(n.Value, maskStr)) && firstErr == nil {
				firstErr = fmt.Errorf("line %d: secret key %q uses an alias; cannot be restored safely", n.Line, key)
			}
		case yaml.ScalarNode:
			off, ok := scalarStart([]byte(edited), starts, n.Line, n.Column)
			if !ok {
				if strings.Contains(n.Value, maskStr) {
					firstErr = fmt.Errorf("line %d: masked value has an unlocatable scalar; cannot be restored safely", n.Line)
				}
				return
			}
			end, ok := scalarEnd([]byte(edited), off, n, inFlow)
			if !ok {
				if strings.Contains(n.Value, maskStr) {
					firstErr = fmt.Errorf("line %d: masked value has a multi-line or non-locatable value", n.Line)
				}
				return
			}
			scalarRaw := edited[off:end]

			if isSecretYAMLKey(key) {
				if n.Value == maskStr {
					origSec := origSecrets[path]
					if origSec == nil || !origSec.isKeyed {
						firstErr = fmt.Errorf("line %d: cannot restore masked secret for unknown key at path %s", n.Line, path)
						return
					}
					repl := formatSecretReplacement(origSec, n, inFlow, isJSON)
					replacements = append(replacements, replacementRange{
						start:       off,
						end:         end,
						repl:        repl,
						path:        path,
						expectedVal: origSec.val,
						isKeyed:     true,
					})
				}
				return
			}

			// Non-keyed scalar: kiểm tra placeholder nhúng (DSN)
			embs := findEmbeddedPlaceholdersInScalar(n.Value)
			if len(embs) > 0 {
				origSec := origSecrets[path]
				if origSec == nil || origSec.isKeyed || len(origSec.embedded) == 0 {
					firstErr = fmt.Errorf("line %d: cannot restore masked embedded secret for path %s (not found in original file)", n.Line, path)
					return
				}
				if len(embs) != len(origSec.embedded) {
					firstErr = fmt.Errorf("line %d: ambiguous masked values ******** at path %s; count changed from %d to %d", n.Line, path, len(origSec.embedded), len(embs))
					return
				}
				values := make([]string, len(embs))
				for i := range embs {
					values[i] = origSec.embedded[i].value
				}
				val := replaceEmbeddedValues(n.Value, embs, values)
				replacements = append(replacements, replacementRange{
					start: off, end: end,
					repl: encodeEmbeddedScalar(val, n.Style, inFlow, isJSON),
					path: path, expectedVal: val,
				})
			} else if strings.Contains(scalarRaw, maskStr) {
				// Scalar không bí mật nhưng chứa placeholder ********
				firstErr = fmt.Errorf("line %d: cannot restore masked value ******** for path %s (not found in original file)", n.Line, path)
				return
			}
		}
	}

	walk(&editedDoc, "", "", false)
	if firstErr != nil {
		return "", firstErr
	}

	if len(replacements) == 0 {
		if strings.Contains(edited, maskStr) {
			return "", maskedLeft(1)
		}
		return edited, nil
	}

	sort.Slice(replacements, func(i, j int) bool { return replacements[i].start < replacements[j].start })

	var b strings.Builder
	pos := 0
	for _, r := range replacements {
		if r.start < pos {
			return "", fmt.Errorf("overlapping secret replacement at offset %d", r.start)
		}
		b.WriteString(edited[pos:r.start])
		b.WriteString(r.repl)
		pos = r.end
	}
	b.WriteString(edited[pos:])
	res := b.String()

	// Xác thực YAML kết quả và giá trị semantic của bí mật
	var checkDoc yaml.Node
	if err := yaml.Unmarshal([]byte(res), &checkDoc); err != nil {
		return "", fmt.Errorf("restored config is not valid YAML: %w", err)
	}

	// Duyệt checkDoc để đảm bảo không còn secret scalar hoặc embedded DSN nào mang giá trị '********'
	expected := make(map[string]string, len(replacements))
	for _, replacement := range replacements {
		expected[replacement.path] = replacement.expectedVal
	}
	var verifyErr error
	var verifyWalk func(n *yaml.Node, key string, path string, inFlow bool)
	verifyWalk = func(n *yaml.Node, key string, path string, inFlow bool) {
		if verifyErr != nil {
			return
		}
		switch n.Kind {
		case yaml.DocumentNode:
			for _, c := range n.Content {
				verifyWalk(c, key, path, inFlow)
			}
		case yaml.SequenceNode:
			f := inFlow || (n.Style&yaml.FlowStyle != 0)
			for i, c := range n.Content {
				verifyWalk(c, key, fmt.Sprintf("%s[%d]", path, i), f)
			}
		case yaml.MappingNode:
			f := inFlow || (n.Style&yaml.FlowStyle != 0)
			for i := 0; i+1 < len(n.Content); i += 2 {
				k := n.Content[i].Value
				verifyWalk(n.Content[i+1], k, fmt.Sprintf("%s/%d:%s", path, len(k), k), f)
			}
		case yaml.ScalarNode:
			if want, ok := expected[path]; ok {
				if n.Value != want {
					verifyErr = fmt.Errorf("line %d: restored secret changed semantic value at path %s", n.Line, path)
					return
				}
				delete(expected, path)
			}
			if isSecretYAMLKey(key) && n.Value == maskStr {
				verifyErr = maskedLeft(n.Line)
				return
			}
			if len(findEmbeddedPlaceholdersInScalar(n.Value)) > 0 {
				verifyErr = maskedLeft(n.Line)
				return
			}
		}
	}
	verifyWalk(&checkDoc, "", "", false)
	if verifyErr != nil {
		return "", verifyErr
	}
	if len(expected) != 0 {
		return "", fmt.Errorf("restored config lost a secret scalar")
	}

	return res, nil
}

func maskedLeft(line int) error {
	return fmt.Errorf("line %d: cannot restore the original secret for the masked value ******** (the line was not found in the current file); type the real value or revert the line", line)
}
