package validate_test

import (
	"context"
	"testing"

	"strand/internal/typemap"
	"strand/internal/validate"
)

// spec: 02 §8, §11, §12 Unit test cho validate.CheckMySQLToPGTables

func TestCheckMySQLToPGTables_ContextCanceled(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	_, err := validate.CheckMySQLToPGTables(ctx, nil, nil, []string{"app.orders"}, typemap.DefaultOptions())
	if err == nil {
		t.Fatalf("expected context canceled error, got nil")
	}
}
