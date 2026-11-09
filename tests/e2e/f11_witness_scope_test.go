package e2e

import "testing"

func TestF11WitnessScope(t *testing.T) {
	for _, tc := range []struct {
		name string
		want bool
	}{
		{"TestP2_E2E_PGSnapshotAddWhileStreaming", true},
		{"TestP2_E2E_PGSnapshotAddWhileStreaming/seed", true},
		{"TestF11_E2E_WatermarkBoundaryCrashRestart", true},
		{"TestG1_E2E_Kill9RandomCrashes", false},
		{"TestP2_E2E_PGSnapshotAddWhileStreamingExtra", false},
	} {
		if got := f11WitnessTest(tc.name); got != tc.want {
			t.Errorf("%s: got %v, want %v", tc.name, got, tc.want)
		}
	}
}
