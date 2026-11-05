package mysql

import "testing"

func TestDecodeMySQLLatin1Basics(t *testing.T) {
	cases := map[string]string{"caf\xe9": "café", "\x80": "€", "\x81": "\u0081", "\xc5ngstr\xf6m \xdf": "Ångström ß", "abc": "abc"}
	for in, want := range cases {
		if got := decodeMySQLLatin1([]byte(in)); got != want {
			t.Errorf("decode(%q) = %q, want %q", in, got, want)
		}
	}
}
