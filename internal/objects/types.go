package objects

import "sort"

// Kind là loại đối tượng.
type Kind string

const (
	KindView      Kind = "view"
	KindProcedure Kind = "procedure"
	KindFunction  Kind = "function"
	KindTrigger   Kind = "trigger"
	KindEvent     Kind = "event"
	KindPackage   Kind = "package" // MariaDB PACKAGE: không chuyển được, chỉ báo
)

// Object là một đối tượng đã chuyển sang cú pháp MySQL 8.x, sẵn sàng chạy trên target.
type Object struct {
	Kind   Kind   `json:"kind"`
	Schema string `json:"schema"` // schema ở source
	Name   string `json:"name"`
	// Trigger: bảng ở source.
	Table string `json:"table,omitempty"`
	// TargetSchema: schema trên target (khác Schema khi trigger nằm trên bảng bị tables.rename sang schema khác).
	TargetSchema string `json:"target_schema"`
	TargetName   string `json:"target_name"`
	// Stmt là CREATE đã chuyển (một câu lệnh, tên đối tượng có schema). Event luôn là DISABLE.
	Stmt string `json:"stmt"`
	// EnableStmt: ALTER EVENT ... ENABLE (chỉ event đang bật ở source).
	EnableStmt string `json:"enable_stmt,omitempty"`
	// SQLMode: sql_mode lưu cùng đối tượng ở source (đã lọc theo MySQL 8.x); rỗng = giữ mặc định của session.
	SQLMode string `json:"sql_mode,omitempty"`
	// TimeZone: time_zone lưu cùng event (rỗng/SYSTEM = không đặt).
	TimeZone string   `json:"time_zone,omitempty"`
	Deps     []string `json:"deps,omitempty"` // view: "schema.view" mà view này tham chiếu
	Warnings []string `json:"warnings,omitempty"`
	// Skipped: lý do không chuyển (trigger trên bảng bị loại, PACKAGE...). Stmt rỗng.
	Skipped string `json:"skipped,omitempty"`
}

// Qualified trả "schema.name" ở target.
func (o Object) Qualified() string { return o.TargetSchema + "." + o.TargetName }

// Label: nhãn dùng trong báo cáo.
func (o Object) Label() string { return string(o.Kind) + " " + o.Qualified() }

// Set là toàn bộ đối tượng tìm thấy ở source, đã chuyển.
type Set struct {
	Schemas []string `json:"schemas"`
	Tables  []string `json:"tables"`
	Objects []Object `json:"objects"`
	Users   *Users   `json:"users,omitempty"`
	// Notes: ghi chú (không phải cảnh báo cú pháp), ví dụ trigger bị bỏ vì bảng bị loại.
	Notes []string `json:"notes,omitempty"`
}

// Of trả các đối tượng thuộc kinds, theo thứ tự trong Set (view đã xếp theo phụ thuộc).
func (s *Set) Of(kinds ...Kind) []Object {
	var out []Object
	for _, o := range s.Objects {
		for _, k := range kinds {
			if o.Kind == k {
				out = append(out, o)
				break
			}
		}
	}
	return out
}

// Counts đếm đối tượng được chuyển (bỏ Skipped) theo loại.
func (s *Set) Counts() map[Kind]int {
	c := map[Kind]int{}
	for _, o := range s.Objects {
		if o.Skipped == "" {
			c[o.Kind]++
		}
	}
	return c
}

// AllWarnings gom cảnh báo của mọi đối tượng (kể cả bị bỏ), có nhãn đối tượng.
func (s *Set) AllWarnings() []string {
	var out []string
	for _, o := range s.Objects {
		for _, w := range o.Warnings {
			out = append(out, o.Label()+": "+w)
		}
		if o.Skipped != "" {
			out = append(out, o.Label()+": skipped: "+o.Skipped)
		}
	}
	return out
}

func sortedKeys(m map[string]bool) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}
