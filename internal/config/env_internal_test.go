package config

import (
	"testing"
	"time"
)

// Struct thử có đủ các kiểu vô hướng mà Config hiện chưa dùng ở vị trí ghi đè được (bool, time.Duration).
type envProbe struct {
	Outer struct {
		Enabled  bool          `yaml:"enabled"`
		Interval time.Duration `yaml:"interval"`
		Count    int64         `yaml:"count"`
		Size     string        `yaml:"size"`
	} `yaml:"outer"`
	A struct {
		BC string `yaml:"b_c"`
	} `yaml:"a"`
}

func TestApplyEnvOverrides_BoolDuration(t *testing.T) {
	var p envProbe
	err := applyEnvOverrides(&p, []string{
		"STRAND_OUTER_ENABLED=true",
		"STRAND_OUTER_INTERVAL=1m30s",
		"STRAND_OUTER_COUNT=-5",
		"STRAND_OUTER_SIZE=4MiB",
		"STRAND_A_B_C=x",
	})
	if err != nil {
		t.Fatal(err)
	}
	if !p.Outer.Enabled || p.Outer.Interval != 90*time.Second || p.Outer.Count != -5 || p.Outer.Size != "4MiB" || p.A.BC != "x" {
		t.Errorf("unexpected: %+v", p)
	}
	for _, bad := range []string{"STRAND_OUTER_ENABLED=maybe", "STRAND_OUTER_INTERVAL=soon"} {
		var q envProbe
		if err := applyEnvOverrides(&q, []string{bad}); err == nil {
			t.Errorf("expected error for %s", bad)
		}
	}
}

func TestApplyEnvOverrides_AmbiguousPath(t *testing.T) {
	// a.b_c và a_b.c cùng sinh STRAND_A_B_C: phải báo lỗi thay vì đoán.
	type amb struct {
		A struct {
			BC string `yaml:"b_c"`
		} `yaml:"a"`
		AB struct {
			C string `yaml:"c"`
		} `yaml:"a_b"`
	}
	var v amb
	if err := applyEnvOverrides(&v, nil); err == nil {
		t.Fatal("expected ambiguity error")
	}
}
