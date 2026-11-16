// Package rowkey chọn "khóa hàng" của một bảng nguồn: PRIMARY KEY, hoặc — khi bảng không có PK — một UNIQUE
// index mà MỌI cột đều NOT NULL (MySQL gọi là "primary key equivalent"; PostgreSQL: index của REPLICA
// IDENTITY USING INDEX). Mọi chỗ khác của Strand (writeset, snapshot theo khóa, upsert/delete, verify)
// vẫn chỉ biết "cột khóa": package này là nơi DUY NHẤT quyết định cột nào.
package rowkey

import (
	"fmt"
	"sort"
	"strings"
)

// PrimaryName là tên index của PRIMARY KEY.
const PrimaryName = "PRIMARY"

// Key là khóa hàng đã chọn.
type Key struct {
	Name    string // "PRIMARY" hoặc tên unique index
	Columns []string
}

// IsPrimary: khóa là PRIMARY KEY.
func (k Key) IsPrimary() bool { return k.Name == PrimaryName }

// Label: "PRIMARY (id)" hoặc "UNIQUE uk_email (email)" — dùng cho log/metrics/web/top.
func (k Key) Label() string {
	cols := strings.Join(k.Columns, ", ")
	if k.IsPrimary() {
		return "PRIMARY (" + cols + ")"
	}
	return "UNIQUE " + k.Name + " (" + cols + ")"
}

// Candidate là một unique index xét làm khóa.
type Candidate struct {
	Name    string
	Columns []string
	// Reason khác rỗng: index KHÔNG đủ điều kiện (và vì sao).
	Reason string
}

// Eligible: index đủ điều kiện về cấu trúc (chưa xét kiểu cột).
func (c Candidate) Eligible() bool { return c.Reason == "" }

// sortCandidates: ít cột nhất trước, rồi theo tên (xác định, không phụ thuộc thứ tự đọc từ catalog).
func sortCandidates(cs []Candidate) {
	sort.SliceStable(cs, func(i, j int) bool {
		if len(cs[i].Columns) != len(cs[j].Columns) {
			return len(cs[i].Columns) < len(cs[j].Columns)
		}
		return cs[i].Name < cs[j].Name
	})
}

// Choose chọn khóa trong số candidates đủ điều kiện cấu trúc: ít cột nhất; nếu kiểu cột của index không được
// snapshot hỗ trợ (typeOK trả lỗi) thì thử index kế tiếp; hòa thì theo tên. nil khi không có index nào; rejected
// là các index bị loại kèm lý do. typeOK nil = không xét kiểu.
func Choose(candidates []Candidate, typeOK func(cols []string) error) (key *Key, rejected []Candidate) {
	cs := append([]Candidate(nil), candidates...)
	sortCandidates(cs)
	for _, c := range cs {
		if !c.Eligible() {
			rejected = append(rejected, c)
			continue
		}
		if typeOK != nil {
			if err := typeOK(c.Columns); err != nil {
				c.Reason = "unsupported key type: " + err.Error()
				rejected = append(rejected, c)
				continue
			}
		}
		return &Key{Name: c.Name, Columns: append([]string(nil), c.Columns...)}, rejected
	}
	return nil, rejected
}

// Explain mô tả vì sao các index bị loại (cho thông báo lỗi); "" nếu không có index nào.
func Explain(rejected []Candidate) string {
	var parts []string
	for _, c := range rejected {
		parts = append(parts, fmt.Sprintf("%s (%s): %s", c.Name, strings.Join(c.Columns, ", "), c.Reason))
	}
	return strings.Join(parts, "; ")
}
