package mysql

// DecodeMySQLLatin1ForTest: bảng giải mã latin1 của reader (test so với MySQL).
func DecodeMySQLLatin1ForTest(b []byte) string { return decodeMySQLLatin1(b) }
