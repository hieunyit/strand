package verify

import (
	"context"
	"errors"
	"reflect"
	"strconv"
	"testing"

	"strand/internal/checkpoint"
	"strand/internal/model"
)

// Phân loại một lần kiểm lại (Gate 2, 04 §5: hot row báo unverifiable_hot, không báo lệch).
func TestRecheckClassifyObs(t *testing.T) {
	cases := []struct {
		name string
		o    rowObs
		want recheckVerdict
	}{
		{"stable match", rowObs{s1: "a", d: "a", s2: "a", s1ok: true, dok: true, s2ok: true}, verdictMatch},
		{"stable, target differs", rowObs{s1: "a", d: "b", s2: "a", s1ok: true, dok: true, s2ok: true}, verdictMismatch},
		{"stable, target missing", rowObs{s1: "a", s2: "a", s1ok: true, s2ok: true}, verdictMismatch},
		{"stable absent, extra on target", rowObs{d: "a", dok: true}, verdictMismatch},
		{"absent everywhere", rowObs{}, verdictMatch},
		{"source changed", rowObs{s1: "a", d: "a", s2: "b", s1ok: true, dok: true, s2ok: true}, verdictUnstable},
		{"source deleted in window", rowObs{s1: "a", d: "a", s1ok: true, dok: true}, verdictUnstable},
		{"source inserted in window", rowObs{s2: "a", d: "a", dok: true, s2ok: true}, verdictUnstable},
		// Ảnh rỗng ("") khác với không có row: ok phân biệt hai trường hợp.
		{"empty image vs absent", rowObs{s1: "", s2: "", s1ok: true, s2ok: true}, verdictMismatch},
	}
	for _, c := range cases {
		if got := classifyObs(c.o); got != c.want {
			t.Errorf("%s: got %v want %v", c.name, got, c.want)
		}
	}
}

// fakeSide: mỗi lần đọc trả ảnh do f(lần đọc thứ n, khóa) quyết định ("" + false = không có row).
type fakeSide struct {
	n int
	f func(n int, k string) (string, bool)
}

func (s *fakeSide) read(_ context.Context, pending []string) (map[string]string, error) {
	out := map[string]string{}
	for _, k := range pending {
		if v, ok := s.f(s.n, k); ok {
			out[k] = v
		}
	}
	s.n++
	return out, nil
}

type fakeWaiter struct{ marks, waits int }

func (w *fakeWaiter) Mark(context.Context) (ApplyMark, error) { w.marks++; return ApplyMark{}, nil }
func (w *fakeWaiter) Wait(context.Context, ApplyMark) error   { w.waits++; return nil }

// Row "hot" đổi ở source giữa mọi cặp S1/S2: unverifiable_hot sau đủ số lần thử, không phải lệch. Row
// "bad" ổn định nhưng target khác: lệch thật. Row "lag" lệch ở lần đọc đầu nhưng khớp sau khi chờ
// apply: không báo gì. Mỗi lần thử chờ apply đúng một lần.
func TestRecheckSuspectsHotVsRealMismatch(t *testing.T) {
	src := &fakeSide{f: func(n int, k string) (string, bool) {
		switch k {
		case "hot":
			return "v" + strconv.Itoa(n), true // mỗi lần đọc một ảnh mới
		case "bad", "lag":
			return "x", true
		}
		return "", false
	}}
	dst := &fakeSide{f: func(n int, k string) (string, bool) {
		switch k {
		case "hot":
			return "old", true
		case "bad":
			return "y", true
		case "lag":
			return "x", true // đã apply khi kiểm lại
		}
		return "", false
	}}
	w := &fakeWaiter{}
	bad, hot, err := recheckSuspects(context.Background(), []string{"lag", "hot", "bad"}, 3, src.read, dst.read, w)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(bad, []string{"bad"}) || !reflect.DeepEqual(hot, []string{"hot"}) {
		t.Fatalf("bad=%v hot=%v, want bad=[bad] hot=[hot]", bad, hot)
	}
	if w.marks != 3 || w.waits != 3 {
		t.Fatalf("marks=%d waits=%d, want 3 each (one per attempt)", w.marks, w.waits)
	}
	if src.n != 6 || dst.n != 3 {
		t.Fatalf("source reads=%d target reads=%d, want 6 and 3", src.n, dst.n)
	}
}

// Row đổi ở lần thử đầu rồi ổn định: kết luận ở lần thử sau (khớp hoặc lệch), không phải hot.
func TestRecheckSuspectsSettlesAfterRetry(t *testing.T) {
	src := &fakeSide{f: func(n int, k string) (string, bool) {
		if n == 0 {
			return "a", true
		}
		return "b", true // S2 của lần thử 1 và mọi lần sau
	}}
	dst := &fakeSide{f: func(n int, k string) (string, bool) {
		if k == "k1" {
			return "b", true
		}
		return "c", true
	}}
	bad, hot, err := recheckSuspects(context.Background(), []string{"k1", "k2"}, 5, src.read, dst.read, nil)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(bad, []string{"k2"}) || len(hot) != 0 {
		t.Fatalf("bad=%v hot=%v, want bad=[k2] hot=[]", bad, hot)
	}
	if src.n != 4 { // lần thử 1 (2 lần đọc source) chưa kết luận, lần thử 2 kết luận cả hai khóa
		t.Fatalf("source reads=%d, want 4", src.n)
	}
}

// Lỗi chờ apply (target không theo kịp) là lỗi verify, không bị nuốt thành lệch hay hot.
func TestRecheckSuspectsWaitErrorPropagates(t *testing.T) {
	src := &fakeSide{f: func(int, string) (string, bool) { return "a", true }}
	dst := &fakeSide{f: func(int, string) (string, bool) { return "b", true }}
	boom := errors.New("target did not apply")
	_, _, err := recheckSuspects(context.Background(), []string{"k"}, 3, src.read, dst.read, errWaiter{boom})
	if !errors.Is(err, boom) {
		t.Fatalf("err=%v, want %v", err, boom)
	}
}

type errWaiter struct{ err error }

func (w errWaiter) Mark(context.Context) (ApplyMark, error) { return ApplyMark{}, nil }
func (w errWaiter) Wait(context.Context, ApplyMark) error   { return w.err }

// Điều kiện "target đã apply tới mark": PG theo lwd ≥ LSN, MySQL theo tập GTID chứa gtid_executed.
func TestApplyMarkMissing(t *testing.T) {
	pg := ApplyMark{lsn: 0x1000, text: "0/1000"}
	if miss, _ := pg.missing(model.EnginePG, checkpoint.NewPGAppliedSet(0xFFF)); miss == "" {
		t.Fatal("PG: lwd 0/FFF < mark 0/1000 must not count as applied")
	}
	if miss, _ := pg.missing(model.EnginePG, checkpoint.NewPGAppliedSet(0x1000)); miss != "" {
		t.Fatalf("PG: lwd = mark must count as applied, missing %q", miss)
	}
	if miss, _ := pg.missing(model.EnginePG, nil); miss == "" {
		t.Fatal("PG: no applied set must not count as applied")
	}
	const uuid = "3e11fa47-71ca-11e1-9e33-c80aa9429562"
	my := ApplyMark{gtid: uuid + ":1-10"}
	got, err := checkpoint.ParseMySQLGTIDSet(uuid + ":1-9")
	if err != nil {
		t.Fatal(err)
	}
	if miss, err := my.missing(model.EngineMySQL, got); err != nil || miss == "" {
		t.Fatalf("MySQL: 1-9 must not cover 1-10 (missing %q, err %v)", miss, err)
	}
	got.ParseAndAddRange(uuid + ":10")
	if miss, err := my.missing(model.EngineMySQL, got); err != nil || miss != "" {
		t.Fatalf("MySQL: 1-10 must cover 1-10 (missing %q, err %v)", miss, err)
	}
	if _, err := my.missing(model.EngineMySQL, checkpoint.NewPGAppliedSet(1)); err == nil {
		t.Fatal("MySQL mark against a PG applied set must be an error")
	}
}

func TestRecheckKeySpanAndDiffKeys(t *testing.T) {
	r, err := keySpan([]string{"5", "-3", "9223372036854775807"})
	if err != nil || r != [2]int64{-3, 9223372036854775807} {
		t.Fatalf("keySpan = %v, %v", r, err)
	}
	if _, err := keySpan([]string{"x"}); err == nil {
		t.Fatal("keySpan: non-integer key must be an error")
	}
	got := diffKeys(map[string]string{"1": "a", "2": "b", "3": "c"}, map[string]string{"1": "a", "2": "x", "4": "d"})
	if !reflect.DeepEqual(got, []string{"2", "3", "4"}) {
		t.Fatalf("diffKeys = %v", got)
	}
}
