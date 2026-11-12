package promote_test

import (
	"context"
	"testing"

	"strand/internal/mysqlver/mysql57test"
	"strand/internal/promote"
)

// Fence trên nguồn MySQL 5.7 (super_read_only có từ 5.7.8) phải đặt được cờ read-only.
func TestMySQL57_SourceFence(t *testing.T) {
	_, db := mysql57test.Start(t, mysql57test.SourceArgs...)
	ctx := context.Background()
	if err := promote.NewMySQLSourceDriver(db).Fence(ctx); err != nil {
		t.Fatalf("Fence on 5.7: %v", err)
	}
	var sro int
	if err := db.QueryRowContext(ctx, "SELECT @@GLOBAL.super_read_only").Scan(&sro); err != nil || sro != 1 {
		t.Fatalf("super_read_only = %d, %v", sro, err)
	}
}
