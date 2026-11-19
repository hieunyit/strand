package snapshot

import "testing"

func TestSnapshotColumnValueBit(t *testing.T) {
	raw := []byte{0x31}
	got, ok := snapshotColumnValue(raw, "BIT").([]byte)
	if !ok || len(got) != 1 || got[0] != 0x31 {
		t.Fatalf("BIT must retain packed bytes: %#v", got)
	}
	raw[0] = 0
	if got[0] != 0x31 {
		t.Fatal("BIT aliases source buffer")
	}
	if got := snapshotColumnValue([]byte("1"), "VARCHAR"); got != "1" {
		t.Fatalf("text conversion changed: %#v", got)
	}
}
