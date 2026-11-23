package validate_test

import (
	"context"
	"database/sql"
	"net"
	"net/url"
	"strings"
	"testing"

	_ "github.com/jackc/pgx/v5/stdlib"

	"strand/internal/model"
	"strand/internal/testutil"
	"strand/internal/validate"
)

func TestSQLTableOverlap(t *testing.T) {
	ctx := context.Background()
	t.Cleanup(testutil.TerminatePostgres)
	dsn := testutil.NewPostgresDSN(t)
	src, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer src.Close()
	if _, err := src.ExecContext(ctx, "CREATE ROLE strand_overlap_target LOGIN PASSWORD 'overlap-test'"); err != nil {
		t.Fatal(err)
	}
	alias, err := url.Parse(dsn)
	if err != nil {
		t.Fatal(err)
	}
	alias.Host = net.JoinHostPort("localhost", alias.Port())
	alias.User = url.UserPassword("strand_overlap_target", "overlap-test")
	dst, err := sql.Open("pgx", alias.String())
	if err != nil {
		t.Fatal(err)
	}
	defer dst.Close()
	a := model.TableID{Schema: "public", Table: "a"}
	b := model.TableID{Schema: "public", Table: "b"}
	identity := func(id model.TableID) model.TableID { return id }
	toB := func(model.TableID) model.TableID { return b }
	if err := validate.CheckTableOverlap(ctx, src, dst, model.EnginePG, []model.TableID{a}, identity); err == nil || !strings.Contains(err.Error(), "same database") {
		t.Fatalf("expected overlap rejection: %v", err)
	}
	if err := validate.CheckTableOverlap(ctx, src, dst, model.EnginePG, []model.TableID{a, b}, toB); err == nil {
		t.Fatal("mapping into another replicated source table accepted")
	}
	if err := validate.CheckTableOverlap(ctx, src, dst, model.EnginePG, []model.TableID{a}, toB); err != nil {
		t.Fatalf("disjoint table mapping rejected: %v", err)
	}
}

func TestSQLTableOverlapMySQLAcrossCredentials(t *testing.T) {
	ctx := t.Context()
	src := startMySQL(t)
	defer src.Close()
	if _, err := src.ExecContext(ctx, "CREATE USER 'strand_overlap_target'@'%' IDENTIFIED BY 'overlap-test'"); err != nil {
		t.Fatal(err)
	}
	_, port, err := net.SplitHostPort(lastMySQLAddr)
	if err != nil {
		t.Fatal(err)
	}
	dst, err := sql.Open("mysql", "strand_overlap_target:overlap-test@tcp(localhost:"+port+")/")
	if err != nil {
		t.Fatal(err)
	}
	defer dst.Close()
	table := model.TableID{Schema: "app", Table: "orders"}
	if err := validate.CheckTableOverlap(ctx, src, dst, model.EngineMySQL, []model.TableID{table}, func(id model.TableID) model.TableID { return id }); err == nil || !strings.Contains(err.Error(), "same database") {
		t.Fatalf("expected MySQL overlap rejection across credentials: %v", err)
	}
	if err := validate.CheckTableOverlap(ctx, src, dst, model.EngineMySQL, []model.TableID{table}, func(id model.TableID) model.TableID {
		return model.TableID{Schema: "copy", Table: id.Table}
	}); err != nil {
		t.Fatalf("disjoint schema was rejected: %v", err)
	}
}
