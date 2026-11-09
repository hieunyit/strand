package sched

import (
	"context"
	"errors"
	"strand/internal/model"
	"testing"
	"time"
)

// CP-4: nhiều Txn đã nhận nhưng chưa Done không được tạo holes vượt trần khi hoàn tất lệch thứ tự.
func TestReviewCP4ReserveInflightHoles(t *testing.T) {
	cfg := DefaultConfig()
	cfg.MaxHoles = 1
	s := NewScheduler(cfg)
	defer s.Close()
	for i := 0; i < 2; i++ {
		if err := s.Admit(context.Background(), &model.Txn{}); err != nil {
			t.Fatal(err)
		}
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel()
	if err := s.Admit(ctx, &model.Txn{}); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("third admission must wait for hole budget: %v", err)
	}
	s.Done(2, nil)
	if s.Holes() > 1 {
		t.Fatalf("holes=%d", s.Holes())
	}
	ctx2, cancel2 := context.WithTimeout(context.Background(), time.Second)
	defer cancel2()
	if err := s.Admit(ctx2, &model.Txn{}); err != nil {
		t.Fatal(err)
	}
	s.Done(3, nil)
	if s.Holes() > 1 {
		t.Fatalf("holes=%d", s.Holes())
	}
	s.Done(1, nil)
	if s.Holes() != 0 || s.Lwd() != 3 {
		t.Fatalf("holes=%d lwd=%d", s.Holes(), s.Lwd())
	}
}
