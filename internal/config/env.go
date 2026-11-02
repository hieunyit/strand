package config

import (
	"fmt"
	"os"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"strand/internal/exitcode"
)

// EnvPrefix là tiền tố biến môi trường ghi đè cấu hình (02 §1, §10).
const EnvPrefix = "STRAND_"

// envField mô tả một key cấu hình vô hướng có thể ghi đè bằng biến môi trường.
type envField struct {
	path  string // đường dẫn key YAML, ví dụ governor.lag_slo
	index []int  // chuỗi chỉ số field theo reflect, đi từ Config xuống
}

type envIndexEntry struct {
	fields map[string]envField
	err    error
}

// envIndexCache: bảng đã dựng theo kiểu struct gốc (reflect.Type -> *envIndexEntry).
var envIndexCache sync.Map

// envFieldIndex dựng (một lần cho mỗi kiểu) bảng tên biến STRAND_* -> key cấu hình bằng reflection
// trên yaml tag. Tên biến = STRAND_ + đường dẫn key viết hoa, các cấp nối bằng "_". Vì key có thể
// chứa "_" (lag_slo, max_bytes…), biến được tra theo bảng đường dẫn đã biết chứ không tách chuỗi.
func envFieldIndex(t reflect.Type) (map[string]envField, error) {
	if e, ok := envIndexCache.Load(t); ok {
		return e.(*envIndexEntry).fields, e.(*envIndexEntry).err
	}
	e := &envIndexEntry{fields: map[string]envField{}}
	e.err = collectEnvFields(t, nil, nil, e.fields)
	actual, _ := envIndexCache.LoadOrStore(t, e)
	return actual.(*envIndexEntry).fields, actual.(*envIndexEntry).err
}

func collectEnvFields(t reflect.Type, prefix []string, index []int, out map[string]envField) error {
	for i := 0; i < t.NumField(); i++ {
		f := t.Field(i)
		if !f.IsExported() {
			continue
		}
		tag := f.Tag.Get("yaml")
		name, opts, _ := strings.Cut(tag, ",")
		if name == "-" || strings.Contains(opts, "inline") {
			continue
		}
		if name == "" {
			name = strings.ToLower(f.Name)
		}
		path := append(append([]string{}, prefix...), name)
		idx := append(append([]int{}, index...), i)

		ft := f.Type
		if ft.Kind() == reflect.Pointer && ft.Elem().Kind() == reflect.Struct {
			ft = ft.Elem()
		}
		if ft.Kind() == reflect.Struct && ft != durationType {
			if err := collectEnvFields(ft, path, idx, out); err != nil {
				return err
			}
			continue
		}
		if !isEnvScalar(ft) {
			// Map và slice (tables.include, tables.rename, resources.schedule…) không ghi đè qua env.
			continue
		}
		env := EnvPrefix + strings.ToUpper(strings.Join(path, "_"))
		if prev, dup := out[env]; dup {
			return fmt.Errorf("ambiguous env override %s: matches both %s and %s", env, prev.path, strings.Join(path, "."))
		}
		out[env] = envField{path: strings.Join(path, "."), index: idx}
	}
	return nil
}

var durationType = reflect.TypeOf(time.Duration(0))

func isEnvScalar(t reflect.Type) bool {
	switch t.Kind() {
	case reflect.String, reflect.Bool,
		reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64,
		reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64,
		reflect.Float32, reflect.Float64:
		return true
	}
	return false
}

// EnvOverrideKeys trả về danh sách tên biến STRAND_* được nhận, đã sắp xếp (phục vụ tài liệu và test).
func EnvOverrideKeys() []string {
	idx, _ := envFieldIndex(reflect.TypeOf(Config{}))
	keys := make([]string, 0, len(idx))
	for k := range idx {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}

// ApplyEnvOverrides ghi đè cfg bằng các biến STRAND_* trong environ (dạng KEY=VALUE như os.Environ).
// Thứ tự theo 02 §10: YAML, rồi biến môi trường STRAND_*, rồi flag. Biến STRAND_* không khớp key
// cấu hình nào (STRAND_S3_*, STRAND_E2E_*…) bị bỏ qua, không phải lỗi. Giá trị sai kiểu trả exit 2.
func ApplyEnvOverrides(cfg *Config, environ []string) error {
	return applyEnvOverrides(cfg, environ)
}

// applyEnvOverrides là bản tổng quát cho mọi con trỏ tới struct có yaml tag.
func applyEnvOverrides(target any, environ []string) error {
	root := reflect.ValueOf(target)
	if root.Kind() != reflect.Pointer || root.Elem().Kind() != reflect.Struct {
		return fmt.Errorf("applyEnvOverrides: target must be a pointer to struct, got %T", target)
	}
	idx, err := envFieldIndex(root.Elem().Type())
	if err != nil {
		return exitcode.New("invalid_syntax", exitcode.InvalidSyntax, err.Error(), err)
	}
	// Sắp xếp để thứ tự áp và thông báo lỗi ổn định.
	envs := append([]string(nil), environ...)
	sort.Strings(envs)
	for _, kv := range envs {
		name, val, ok := strings.Cut(kv, "=")
		if !ok || !strings.HasPrefix(name, EnvPrefix) {
			continue
		}
		f, known := idx[name]
		if !known {
			continue
		}
		if err := setEnvField(root.Elem(), f.index, val); err != nil {
			return exitcode.New("invalid_syntax", exitcode.InvalidSyntax,
				fmt.Sprintf("invalid value for %s (config key %s): %v", name, f.path, err), err)
		}
	}
	return nil
}

func setEnvField(v reflect.Value, index []int, raw string) error {
	for _, i := range index {
		if v.Kind() == reflect.Pointer {
			if v.IsNil() {
				v.Set(reflect.New(v.Type().Elem()))
			}
			v = v.Elem()
		}
		v = v.Field(i)
	}
	raw = strings.TrimSpace(raw)
	if v.Type() == durationType {
		d, err := time.ParseDuration(raw)
		if err != nil {
			return err
		}
		v.SetInt(int64(d))
		return nil
	}
	switch v.Kind() {
	case reflect.String:
		v.SetString(raw)
	case reflect.Bool:
		b, err := strconv.ParseBool(raw)
		if err != nil {
			return err
		}
		v.SetBool(b)
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		n, err := strconv.ParseInt(raw, 10, v.Type().Bits())
		if err != nil {
			return err
		}
		v.SetInt(n)
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		n, err := strconv.ParseUint(raw, 10, v.Type().Bits())
		if err != nil {
			return err
		}
		v.SetUint(n)
	case reflect.Float32, reflect.Float64:
		n, err := strconv.ParseFloat(raw, v.Type().Bits())
		if err != nil {
			return err
		}
		v.SetFloat(n)
	default:
		return fmt.Errorf("unsupported field kind %s", v.Kind())
	}
	return nil
}

// applyProcessEnv áp biến môi trường của tiến trình hiện tại.
func applyProcessEnv(cfg *Config) error {
	return ApplyEnvOverrides(cfg, os.Environ())
}
