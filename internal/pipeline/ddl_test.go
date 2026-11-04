package pipeline

import (
	"reflect"
	"strings"
	"testing"

	"strand/internal/model"
)

func TestDDLNewTables(t *testing.T) {
	id := func(s, n string) model.TableID { return model.TableID{Schema: s, Table: n} }
	cases := []struct {
		ddl            string
		created, renam []model.TableID
	}{
		{"CREATE TABLE app.t1 (id INT PRIMARY KEY)", []model.TableID{id("app", "t1")}, nil},
		{"create table if not exists `app`.`t 2` (id int)", []model.TableID{id("app", "t 2")}, nil},
		{"CREATE TABLE t3 (id INT)", []model.TableID{id("def", "t3")}, nil},
		{"/* ApplicationName=x */ CREATE TABLE app.t4 LIKE app.t1", []model.TableID{id("app", "t4")}, nil},
		{"CREATE TEMPORARY TABLE app.tmp (id INT)", nil, nil},
		{"RENAME TABLE app.a TO app.b, c TO other.d", nil, []model.TableID{id("app", "b"), id("other", "d")}},
		{"ALTER TABLE app.x RENAME TO app.y", nil, []model.TableID{id("app", "y")}},
		{"ALTER TABLE x RENAME AS y", nil, []model.TableID{id("def", "y")}},
		{"ALTER TABLE app.x ADD COLUMN c INT", nil, nil},
		{"DROP TABLE app.x", nil, nil},
	}
	for _, c := range cases {
		cr, rn := ddlNewTables(c.ddl, "def")
		if !reflect.DeepEqual(cr, c.created) || !reflect.DeepEqual(rn, c.renam) {
			t.Errorf("%q: created=%v renamed=%v, want %v %v", c.ddl, cr, rn, c.created, c.renam)
		}
	}
}

// Review 4.2: bảng `user` không được bị coi là bị DDL đụng tới chỉ vì DDL nhắc `user_id`, nhưng phải
// khớp mọi cách viết tên bảng thật (không được bỏ sót).
func TestDDLMentionsTableWholeIdentifier(t *testing.T) {
	cases := []struct {
		ddl  string
		want bool
	}{
		{"ALTER TABLE orders ADD COLUMN user_id INT", false},
		{"ALTER TABLE users ADD COLUMN x INT", false},
		{"ALTER TABLE appuser ADD COLUMN x INT", false},
		{"ALTER TABLE user ADD COLUMN x INT", true},
		{"ALTER TABLE app.user ADD COLUMN x INT", true},
		{"ALTER TABLE `user` ADD COLUMN x INT", true},
		{"ALTER TABLE `app`.`user` ADD COLUMN x INT", true},
		{"DROP TABLE user", true},
		{"RENAME TABLE user_id_map TO x, user TO y", true},
		{"CREATE INDEX i ON user(id)", true},
		{"alter table APP.USER add x int", true},
	}
	for _, c := range cases {
		if got := ddlMentionsTable(strings.ToLower(c.ddl), "user"); got != c.want {
			t.Errorf("%q: got %v want %v", c.ddl, got, c.want)
		}
	}
}
