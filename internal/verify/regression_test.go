package verify_test

// Regression tests from the Gate 1 code review (2026-11-02).

import (
	"context"
	"testing"

	"strand/internal/verify"
)

// byteFetcher mimics database/sql scanning into `any`: MySQL returns []byte for
// most column types, including the PK.
type byteFetcher struct{ rows []map[string]any }

func (f byteFetcher) FetchRange(ctx context.Context, schema, table, pkCol string, low, high any) ([]map[string]any, error) {
	return f.rows, nil
}
func (f byteFetcher) FetchRowByPK(ctx context.Context, schema, table, pkCol string, pk any) (map[string]any, error) {
	for _, r := range f.rows {
		if string(r[pkCol].([]byte)) == string(pk.([]byte)) {
			return r, nil
		}
	}
	return nil, nil
}
func (f byteFetcher) GetPKRange(ctx context.Context, schema, table, pkCol string) (any, any, int64, error) {
	return int64(1), int64(2), 2, nil
}

// Verify used []byte PKs as map keys and panicked ("hash of unhashable type []uint8").
func TestVerifyByteSlicePKDoesNotPanic(t *testing.T) {
	src := byteFetcher{rows: []map[string]any{{"id": []byte("1"), "v": []byte("a")}, {"id": []byte("2"), "v": []byte("b")}}}
	dst := byteFetcher{rows: []map[string]any{{"id": []byte("1"), "v": []byte("a")}, {"id": []byte("2"), "v": []byte("X")}}}
	v := verify.NewVerifier(src, dst, nil)
	res, err := v.VerifyTable(context.Background(), verify.TableConfig{Schema: "app", Table: "t", PKColumns: []string{"id"},
		ChunkSize: 10, SampleRate: 1.0}, verify.ModeFull)
	if err != nil {
		t.Fatal(err)
	}
	if len(res.Mismatches) != 1 {
		t.Fatalf("expected 1 mismatch (id=2), got %+v", res.Mismatches)
	}
}
