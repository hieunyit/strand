package pipeline

import (
	"errors"
	"testing"
)

// Source nhận ALTER thứ hai giữa lúc evolve lập kế hoạch (thấy ALTER 1) và lúc so lại: phải so/áp thêm vòng
// nữa thay vì báo lệch (exit 10). Code cũ chỉ chạy evolve một lần → còn diff
// (TestP3_E2E_SchemaEvolution_{WidenAuto,AddColumnAuto}_MySQL chập chờn).
func TestEvolveUntilSettledFollowsSourceChangedMeanwhile(t *testing.T) {
	pending := 2 // số ALTER source chưa áp lên target
	evolveCalls := 0
	evolve := func(bool) (bool, error) {
		evolveCalls++
		if pending == 0 {
			return false, nil
		}
		pending = 0 // lần đầu chỉ thấy ALTER 1; ALTER 2 tới ngay sau → collect còn thấy lệch
		if evolveCalls == 1 {
			pending = 1
		}
		return true, nil
	}
	collect := func() ([]string, error) {
		if pending > 0 {
			return []string{"app.evo columns differ"}, nil
		}
		return nil, nil
	}
	diffs, err := evolveUntilSettled([]string{"app.evo columns differ"}, collect, evolve)
	if err != nil || len(diffs) != 0 {
		t.Fatalf("diffs %v err %v after %d evolve calls", diffs, err, evolveCalls)
	}
	if evolveCalls != 2 {
		t.Fatalf("evolve calls %d, want 2", evolveCalls)
	}
}

func TestEvolveUntilSettledStopsOnErrorAndBound(t *testing.T) {
	boom := errors.New("pending approval")
	if _, err := evolveUntilSettled(nil, func() ([]string, error) { return nil, nil }, func(bool) (bool, error) { return false, boom }); !errors.Is(err, boom) {
		t.Fatalf("err %v", err)
	}
	// Không có gì áp: giữ nguyên diff ban đầu (caller dừng như trước).
	d, err := evolveUntilSettled([]string{"x"}, func() ([]string, error) { t.Fatal("collect"); return nil, nil }, func(bool) (bool, error) { return false, nil })
	if err != nil || len(d) != 1 {
		t.Fatalf("d %v err %v", d, err)
	}
	// Source đổi liên tục: dừng sau maxEvolveRounds vòng, trả diff còn lại.
	n := 0
	d, err = evolveUntilSettled([]string{"x"}, func() ([]string, error) { return []string{"x"}, nil }, func(bool) (bool, error) { n++; return true, nil })
	if err != nil || len(d) != 1 || n != maxEvolveRounds {
		t.Fatalf("d %v err %v rounds %d", d, err, n)
	}
}
