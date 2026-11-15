package web

import (
	"strings"
	"testing"
)

func TestMaskUnmaskConfigTextRoundTrip(t *testing.T) {
	orig := "a: 1\n\n" +
		"source: { kind: mysql, password: 'p w', user: u }  # c\n" +
		"dsn: \"postgres://u:pw1@h:5432/d?sslmode=prefer\"\n" +
		"t: { password: x1, dsn: \"mysql://a:b2@h/d\" }\n" +
		"password_env: SRC_PW\n" +
		"pw: ${DST_PW}\n"
	masked, err := MaskConfigText([]byte(orig))
	if err != nil {
		t.Fatal(err)
	}
	for _, leak := range []string{"p w", "pw1", "x1", "b2"} {
		if strings.Contains(masked, leak) {
			t.Errorf("leak %q:\n%s", leak, masked)
		}
	}
	for _, keep := range []string{"password_env: SRC_PW", "${DST_PW}", "# c", "\n\n"} {
		if !strings.Contains(masked, keep) {
			t.Errorf("lost %q:\n%s", keep, masked)
		}
	}
	back, err := UnmaskConfigText(masked, []byte(orig))
	if err != nil || back != orig {
		t.Fatalf("round trip lỗi (err=%v):\n%s", err, back)
	}
	// Sửa host trong DSN nhưng giữ mật khẩu che: vẫn khôi phục được theo khóa.
	edited := strings.Replace(masked, "h:5432", "newhost:5432", 1)
	back, err = UnmaskConfigText(edited, []byte(orig))
	if err != nil || !strings.Contains(back, "postgres://u:pw1@newhost:5432/d") {
		t.Fatalf("sửa host + giữ mask: err=%v\n%s", err, back)
	}
}

func TestMaskConfigTextBlockScalarSecretRejected(t *testing.T) {
	if _, err := MaskConfigText([]byte("password: |\n  line1\n  line2\n")); err == nil {
		t.Fatal("block scalar bí mật phải bị từ chối thay vì rò rỉ")
	}
}
