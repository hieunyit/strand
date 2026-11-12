package schemagen

import (
	"context"
	"strings"
	"testing"

	"strand/internal/model"
	"strand/internal/mysqlver/mysql57test"
)

// DDL sinh từ nguồn 5.7: SHOW CREATE TABLE không ghi COLLATE mặc định của utf8mb4 (utf8mb4_general_ci);
// mysqlToMySQL phải ghi rõ để đích 8.x không rơi về utf8mb4_0900_ai_ci.
func TestMySQL57_MySQLToMySQL_PinsCollation(t *testing.T) {
	_, db := mysql57test.Start(t, mysql57test.SourceArgs...)
	ctx := context.Background()
	for _, q := range []string{
		"CREATE DATABASE app DEFAULT CHARSET utf8mb4",
		"CREATE TABLE app.t (id INT PRIMARY KEY, name VARCHAR(20), legacy VARCHAR(20) CHARACTER SET utf8) DEFAULT CHARSET=utf8mb4",
	} {
		if _, err := db.ExecContext(ctx, q); err != nil {
			t.Fatal(err)
		}
	}
	stmts, _, err := mysqlToMySQL(ctx, Request{Source: db}, model.TableID{Schema: "app", Table: "t"})
	if err != nil {
		t.Fatalf("mysqlToMySQL on 5.7: %v", err)
	}
	ddl := stmts[len(stmts)-1]
	if !strings.Contains(ddl, "COLLATE=utf8mb4_general_ci") {
		t.Errorf("table collation not pinned:\n%s", ddl)
	}
	if !strings.Contains(stmts[0], "utf8mb4_general_ci") {
		t.Errorf("database collation: %s", stmts[0])
	}
}
