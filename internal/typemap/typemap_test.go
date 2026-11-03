package typemap_test

import (
	"math"
	"reflect"
	"strings"
	"testing"
	"time"

	"strand/internal/exitcode"
	"strand/internal/typemap"
)

// spec: 02 §8 Unit tests cho toàn bộ bảng mapping kiểu dữ liệu MySQL -> PostgreSQL

func TestMapMySQLColumn_Integers(t *testing.T) {
	opts := typemap.DefaultOptions()

	// 1. tinyint signed (-128..127) -> smallint
	t.Run("tinyint_signed", func(t *testing.T) {
		col := typemap.MySQLColumn{Name: "c1", DataType: "tinyint", ColumnType: "tinyint(4)", Unsigned: false}
		m, err := typemap.MapMySQLColumn(col, opts)
		if err != nil {
			t.Fatalf("unexpected err: %v", err)
		}
		if m.TargetType != "smallint" {
			t.Errorf("expected target smallint, got %s", m.TargetType)
		}

		// Biên dưới -128
		v, err := m.Convert(-128)
		if err != nil || v != int16(-128) {
			t.Errorf("convert -128 failed: v=%v, err=%v", v, err)
		}
		// Biên trên 127
		v, err = m.Convert(127)
		if err != nil || v != int16(127) {
			t.Errorf("convert 127 failed: v=%v, err=%v", v, err)
		}
		// Vượt biên -> lỗi
		if _, err := m.Convert(128); err == nil {
			t.Errorf("expected error on overflow 128")
		}
		if _, err := m.Convert(-129); err == nil {
			t.Errorf("expected error on underflow -129")
		}
	})

	// 2. tinyint unsigned (0..255) -> smallint
	t.Run("tinyint_unsigned", func(t *testing.T) {
		col := typemap.MySQLColumn{Name: "c2", DataType: "tinyint", ColumnType: "tinyint(3) unsigned", Unsigned: true}
		m, err := typemap.MapMySQLColumn(col, opts)
		if err != nil {
			t.Fatalf("unexpected err: %v", err)
		}
		if m.TargetType != "smallint" {
			t.Errorf("expected target smallint, got %s", m.TargetType)
		}

		v, err := m.Convert(uint64(0))
		if err != nil || v != int16(0) {
			t.Errorf("convert 0 failed: v=%v, err=%v", v, err)
		}
		v, err = m.Convert(uint64(255))
		if err != nil || v != int16(255) {
			t.Errorf("convert 255 failed: v=%v, err=%v", v, err)
		}
		if _, err := m.Convert(uint64(256)); err == nil {
			t.Errorf("expected error on overflow 256")
		}
	})

	// 3. smallint signed (-32768..32767) -> smallint
	t.Run("smallint_signed", func(t *testing.T) {
		col := typemap.MySQLColumn{Name: "c3", DataType: "smallint", ColumnType: "smallint(6)", Unsigned: false}
		m, err := typemap.MapMySQLColumn(col, opts)
		if err != nil {
			t.Fatalf("unexpected err: %v", err)
		}
		if m.TargetType != "smallint" {
			t.Errorf("expected target smallint, got %s", m.TargetType)
		}
		v, err := m.Convert(-32768)
		if err != nil || v != int16(-32768) {
			t.Errorf("convert -32768 failed: v=%v, err=%v", v, err)
		}
		v, err = m.Convert(32767)
		if err != nil || v != int16(32767) {
			t.Errorf("convert 32767 failed: v=%v, err=%v", v, err)
		}
		if _, err := m.Convert(32768); err == nil {
			t.Errorf("expected error on overflow 32768")
		}
	})

	// 4. smallint unsigned (0..65535) -> integer
	t.Run("smallint_unsigned", func(t *testing.T) {
		col := typemap.MySQLColumn{Name: "c4", DataType: "smallint", ColumnType: "smallint(5) unsigned", Unsigned: true}
		m, err := typemap.MapMySQLColumn(col, opts)
		if err != nil {
			t.Fatalf("unexpected err: %v", err)
		}
		if m.TargetType != "integer" {
			t.Errorf("expected target integer, got %s", m.TargetType)
		}
		v, err := m.Convert(uint64(65535))
		if err != nil || v != int32(65535) {
			t.Errorf("convert 65535 failed: v=%v, err=%v", v, err)
		}
		if _, err := m.Convert(uint64(65536)); err == nil {
			t.Errorf("expected error on overflow 65536")
		}
	})

	// 5. mediumint signed (-8388608..8388607) & unsigned (0..16777215) -> integer
	t.Run("mediumint", func(t *testing.T) {
		colS := typemap.MySQLColumn{Name: "c5", DataType: "mediumint", ColumnType: "mediumint(9)", Unsigned: false}
		mS, err := typemap.MapMySQLColumn(colS, opts)
		if err != nil || mS.TargetType != "integer" {
			t.Fatalf("expected target integer, got %s, err: %v", mS.TargetType, err)
		}
		v, err := mS.Convert(-8388608)
		if err != nil || v != int32(-8388608) {
			t.Errorf("convert -8388608 failed: v=%v, err=%v", v, err)
		}

		colU := typemap.MySQLColumn{Name: "c5u", DataType: "mediumint", ColumnType: "mediumint(8) unsigned", Unsigned: true}
		mU, err := typemap.MapMySQLColumn(colU, opts)
		if err != nil || mU.TargetType != "integer" {
			t.Fatalf("expected target integer, got %s, err: %v", mU.TargetType, err)
		}
		v, err = mU.Convert(uint64(16777215))
		if err != nil || v != int32(16777215) {
			t.Errorf("convert 16777215 failed: v=%v, err=%v", v, err)
		}
	})

	// 6. int signed (-2147483648..2147483647) -> integer, int unsigned (0..4294967295) -> bigint
	t.Run("int", func(t *testing.T) {
		colS := typemap.MySQLColumn{Name: "c6", DataType: "int", ColumnType: "int(11)", Unsigned: false}
		mS, err := typemap.MapMySQLColumn(colS, opts)
		if err != nil || mS.TargetType != "integer" {
			t.Fatalf("expected target integer, got %s, err: %v", mS.TargetType, err)
		}
		v, err := mS.Convert(2147483647)
		if err != nil || v != int32(2147483647) {
			t.Errorf("convert 2147483647 failed: v=%v, err=%v", v, err)
		}

		colU := typemap.MySQLColumn{Name: "c6u", DataType: "int", ColumnType: "int(10) unsigned", Unsigned: true}
		mU, err := typemap.MapMySQLColumn(colU, opts)
		if err != nil || mU.TargetType != "bigint" {
			t.Fatalf("expected target bigint, got %s, err: %v", mU.TargetType, err)
		}
		v, err = mU.Convert(uint64(4294967295))
		if err != nil || v != int64(4294967295) {
			t.Errorf("convert 4294967295 failed: v=%v, err=%v", v, err)
		}
	})

	// 7. bigint signed -> bigint
	t.Run("bigint_signed", func(t *testing.T) {
		col := typemap.MySQLColumn{Name: "c7", DataType: "bigint", ColumnType: "bigint(20)", Unsigned: false}
		m, err := typemap.MapMySQLColumn(col, opts)
		if err != nil || m.TargetType != "bigint" {
			t.Fatalf("expected target bigint, got %s, err: %v", m.TargetType, err)
		}
		v, err := m.Convert(int64(math.MaxInt64))
		if err != nil || v != int64(math.MaxInt64) {
			t.Errorf("convert MaxInt64 failed: v=%v, err=%v", v, err)
		}
	})

	// 8. bigint unsigned: numeric(20,0) mặc định; int8-checked tùy chọn
	t.Run("bigint_unsigned_numeric_default", func(t *testing.T) {
		col := typemap.MySQLColumn{Name: "c8", DataType: "bigint", ColumnType: "bigint(20) unsigned", Unsigned: true}
		m, err := typemap.MapMySQLColumn(col, opts)
		if err != nil {
			t.Fatalf("unexpected err: %v", err)
		}
		if m.TargetType != "numeric(20,0)" {
			t.Errorf("expected target numeric(20,0), got %s", m.TargetType)
		}
		v, err := m.Convert(uint64(math.MaxUint64))
		if err != nil || v != "18446744073709551615" {
			t.Errorf("convert MaxUint64 failed: v=%v, err=%v", v, err)
		}
	})

	t.Run("bigint_unsigned_int8_checked", func(t *testing.T) {
		optsInt8 := opts
		optsInt8.BigIntUnsignedPolicy = "int8-checked"
		col := typemap.MySQLColumn{Name: "c8chk", DataType: "bigint", ColumnType: "bigint(20) unsigned", Unsigned: true}
		m, err := typemap.MapMySQLColumn(col, optsInt8)
		if err != nil {
			t.Fatalf("unexpected err: %v", err)
		}
		if m.TargetType != "bigint" {
			t.Errorf("expected target bigint, got %s", m.TargetType)
		}

		// Giá trị <= 2^63-1 thành công
		v, err := m.Convert(uint64(math.MaxInt64))
		if err != nil || v != int64(math.MaxInt64) {
			t.Errorf("convert MaxInt64 with int8-checked failed: v=%v, err=%v", v, err)
		}

		// Giá trị > 2^63-1 bị từ chối với exit 3 (type_unmappable)
		_, err = m.Convert(uint64(math.MaxInt64) + 1)
		if err == nil {
			t.Fatalf("expected error for value > 2^63-1")
		}
		if exitcode.GetExitCode(err) != exitcode.Rejected {
			t.Errorf("expected exit code %d, got %d", exitcode.Rejected, exitcode.GetExitCode(err))
		}
	})
}

func TestMapMySQLColumn_Decimals(t *testing.T) {
	opts := typemap.DefaultOptions()

	col := typemap.MySQLColumn{Name: "amount", DataType: "decimal", ColumnType: "decimal(10,2)"}
	m, err := typemap.MapMySQLColumn(col, opts)
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if m.TargetType != "numeric(10,2)" {
		t.Errorf("expected numeric(10,2), got %s", m.TargetType)
	}

	testCases := []struct {
		input    any
		expected string
	}{
		{"123.45", "123.45"},
		{[]byte("99999999.99"), "99999999.99"},
		{float64(0.0), "0"},
	}
	for _, tc := range testCases {
		v, err := m.Convert(tc.input)
		if err != nil || v != tc.expected {
			t.Errorf("convert decimal %v failed: v=%v, err=%v", tc.input, v, err)
		}
	}
}

func TestMapMySQLColumn_Floats(t *testing.T) {
	opts := typemap.DefaultOptions()

	// float -> real
	colF := typemap.MySQLColumn{Name: "flt", DataType: "float", ColumnType: "float"}
	mF, err := typemap.MapMySQLColumn(colF, opts)
	if err != nil || mF.TargetType != "real" {
		t.Fatalf("expected real, got %s, err: %v", mF.TargetType, err)
	}
	vf, err := mF.Convert(float32(1.234))
	if err != nil || vf != float32(1.234) {
		t.Errorf("convert float32 failed: v=%v, err=%v", vf, err)
	}

	// double -> double precision
	colD := typemap.MySQLColumn{Name: "dbl", DataType: "double", ColumnType: "double"}
	mD, err := typemap.MapMySQLColumn(colD, opts)
	if err != nil || mD.TargetType != "double precision" {
		t.Fatalf("expected double precision, got %s, err: %v", mD.TargetType, err)
	}
	vd, err := mD.Convert(float64(12345.6789))
	if err != nil || vd != float64(12345.6789) {
		t.Errorf("convert double precision failed: v=%v, err=%v", vd, err)
	}
}

func TestMapMySQLColumn_Bit(t *testing.T) {
	opts := typemap.DefaultOptions()

	// Mặc định: bit varying
	colB := typemap.MySQLColumn{Name: "flag", DataType: "bit", ColumnType: "bit(1)"}
	mB, err := typemap.MapMySQLColumn(colB, opts)
	if err != nil || mB.TargetType != "bit varying(1)" {
		t.Fatalf("expected bit varying(1), got %s, err: %v", mB.TargetType, err)
	}
	v, err := mB.Convert("1")
	if err != nil || v != "1" {
		t.Errorf("convert bit varying failed: v=%v, err=%v", v, err)
	}

	// Tùy chọn bytea
	optsBytea := opts
	optsBytea.BitPolicy = "bytea"
	mBytea, err := typemap.MapMySQLColumn(colB, optsBytea)
	if err != nil || mBytea.TargetType != "bytea" {
		t.Fatalf("expected bytea, got %s, err: %v", mBytea.TargetType, err)
	}
}

func TestMapMySQLColumn_Strings(t *testing.T) {
	opts := typemap.DefaultOptions()

	// char(10)
	colC := typemap.MySQLColumn{Name: "code", DataType: "char", ColumnType: "char(10)"}
	mC, err := typemap.MapMySQLColumn(colC, opts)
	if err != nil || mC.TargetType != "char(10)" {
		t.Fatalf("expected char(10), got %s, err: %v", mC.TargetType, err)
	}

	// varchar(255)
	colV := typemap.MySQLColumn{Name: "name", DataType: "varchar", ColumnType: "varchar(255)"}
	mV, err := typemap.MapMySQLColumn(colV, opts)
	if err != nil || mV.TargetType != "varchar(255)" {
		t.Fatalf("expected varchar(255), got %s, err: %v", mV.TargetType, err)
	}

	// varchar với text policy
	optsText := opts
	optsText.StringPolicy = "text"
	mVText, err := typemap.MapMySQLColumn(colV, optsText)
	if err != nil || mVText.TargetType != "text" {
		t.Fatalf("expected text, got %s, err: %v", mVText.TargetType, err)
	}

	// text, tinytext, mediumtext, longtext
	colT := typemap.MySQLColumn{Name: "desc", DataType: "text", ColumnType: "text"}
	mT, err := typemap.MapMySQLColumn(colT, opts)
	if err != nil || mT.TargetType != "text" {
		t.Fatalf("expected text, got %s, err: %v", mT.TargetType, err)
	}
	vt, err := mT.Convert("hello world")
	if err != nil || vt != "hello world" {
		t.Errorf("convert text failed: v=%v, err=%v", vt, err)
	}
}

func TestMapMySQLColumn_Binary(t *testing.T) {
	opts := typemap.DefaultOptions()

	for _, dt := range []string{"binary", "varbinary", "blob", "longblob"} {
		col := typemap.MySQLColumn{Name: "raw", DataType: dt, ColumnType: dt}
		m, err := typemap.MapMySQLColumn(col, opts)
		if err != nil || m.TargetType != "bytea" {
			t.Fatalf("expected bytea for %s, got %s, err: %v", dt, m.TargetType, err)
		}
		raw := []byte{0x00, 0xFF, 0x12, 0x34}
		v, err := m.Convert(raw)
		if err != nil || !reflect.DeepEqual(v, raw) {
			t.Errorf("convert %s bytea failed: v=%v, err=%v", dt, v, err)
		}
	}
}

func TestMapMySQLColumn_Date_ZeroDatePolicies(t *testing.T) {
	col := typemap.MySQLColumn{Name: "d", DataType: "date", ColumnType: "date"}

	// 1. Policy "reject" (mặc định)
	optsReject := typemap.DefaultOptions()
	optsReject.ZeroDatePolicy = "reject"
	mReject, err := typemap.MapMySQLColumn(col, optsReject)
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	// Ngày bình thường thành công
	v, err := mReject.Convert("2026-11-03")
	if err != nil {
		t.Fatalf("convert valid date failed: %v", err)
	}
	expectedDate := time.Date(2026, 11, 3, 0, 0, 0, 0, time.UTC)
	if v != expectedDate {
		t.Errorf("expected %v, got %v", expectedDate, v)
	}

	// Zero dates phải bị từ chối
	zeroDates := []string{
		"0000-00-00",
		"0000-00-00 00:00:00",
		"2023-00-01", // tháng = 0
		"2023-01-00", // ngày = 0
	}
	for _, zd := range zeroDates {
		_, err := mReject.Convert(zd)
		if err == nil {
			t.Errorf("expected error for zero date %q with policy reject", zd)
		}
		if exitcode.GetExitCode(err) != exitcode.Rejected {
			t.Errorf("expected exitcode.Rejected for %q, got %d", zd, exitcode.GetExitCode(err))
		}
	}

	// 2. Policy "null"
	optsNull := typemap.DefaultOptions()
	optsNull.ZeroDatePolicy = "null"
	mNull, err := typemap.MapMySQLColumn(col, optsNull)
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	vNull, err := mNull.Convert("0000-00-00")
	if err != nil || vNull != nil {
		t.Errorf("expected nil for zero date with policy null, got v=%v, err=%v", vNull, err)
	}

	// 3. Policy "epoch"
	optsEpoch := typemap.DefaultOptions()
	optsEpoch.ZeroDatePolicy = "epoch"
	mEpoch, err := typemap.MapMySQLColumn(col, optsEpoch)
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	vEpoch, err := mEpoch.Convert("0000-00-00")
	if err != nil {
		t.Fatalf("convert epoch failed: %v", err)
	}
	expectedEpoch := time.Date(1970, 1, 1, 0, 0, 0, 0, time.UTC)
	if vEpoch != expectedEpoch {
		t.Errorf("expected epoch %v, got %v", expectedEpoch, vEpoch)
	}
}

func TestMapMySQLColumn_DatetimeAndTimestamp(t *testing.T) {
	opts := typemap.DefaultOptions()

	// DATETIME(6) -> timestamp(6)
	colDT := typemap.MySQLColumn{Name: "dt", DataType: "datetime", ColumnType: "datetime(6)"}
	mDT, err := typemap.MapMySQLColumn(colDT, opts)
	if err != nil || mDT.TargetType != "timestamp(6)" {
		t.Fatalf("expected timestamp(6), got %s, err: %v", mDT.TargetType, err)
	}
	vdt, err := mDT.Convert("2026-11-03 14:30:00.123456")
	if err != nil {
		t.Fatalf("convert datetime failed: %v", err)
	}
	expectedDT := time.Date(2026, 11, 3, 14, 30, 0, 123456000, time.UTC)
	if vdt != expectedDT {
		t.Errorf("expected %v, got %v", expectedDT, vdt)
	}

	// TIMESTAMP(6) -> timestamptz(6), múi giờ UTC
	colTS := typemap.MySQLColumn{Name: "ts", DataType: "timestamp", ColumnType: "timestamp(6)"}
	mTS, err := typemap.MapMySQLColumn(colTS, opts)
	if err != nil || mTS.TargetType != "timestamptz(6)" {
		t.Fatalf("expected timestamptz(6), got %s, err: %v", mTS.TargetType, err)
	}
	vts, err := mTS.Convert("2026-11-03 14:30:00.000000")
	if err != nil {
		t.Fatalf("convert timestamp failed: %v", err)
	}
	tRes, ok := vts.(time.Time)
	if !ok {
		t.Fatalf("expected time.Time, got %T", vts)
	}
	if tRes.Location() != time.UTC {
		t.Errorf("expected UTC location, got %v", tRes.Location())
	}
}

func TestMapMySQLColumn_Time(t *testing.T) {
	opts := typemap.DefaultOptions()

	// Mặc định: time
	colT := typemap.MySQLColumn{Name: "tm", DataType: "time", ColumnType: "time"}
	mT, err := typemap.MapMySQLColumn(colT, opts)
	if err != nil || mT.TargetType != "time" {
		t.Fatalf("expected time, got %s, err: %v", mT.TargetType, err)
	}
	vt, err := mT.Convert("12:34:56")
	if err != nil || vt != "12:34:56" {
		t.Errorf("convert time failed: v=%v, err=%v", vt, err)
	}

	// Tùy chọn interval
	optsInterval := opts
	optsInterval.TimePolicy = "interval"
	mTI, err := typemap.MapMySQLColumn(colT, optsInterval)
	if err != nil || mTI.TargetType != "interval" {
		t.Fatalf("expected interval, got %s, err: %v", mTI.TargetType, err)
	}
}

func TestMapMySQLColumn_Year(t *testing.T) {
	opts := typemap.DefaultOptions()

	col := typemap.MySQLColumn{Name: "yr", DataType: "year", ColumnType: "year(4)"}
	m, err := typemap.MapMySQLColumn(col, opts)
	if err != nil || m.TargetType != "smallint" {
		t.Fatalf("expected smallint, got %s, err: %v", m.TargetType, err)
	}
	v, err := m.Convert("2026")
	if err != nil || v != int16(2026) {
		t.Errorf("convert year 2026 failed: v=%v, err=%v", v, err)
	}
	v, err = m.Convert(1901)
	if err != nil || v != int16(1901) {
		t.Errorf("convert year 1901 failed: v=%v, err=%v", v, err)
	}
}

func TestMapMySQLColumn_JSON(t *testing.T) {
	opts := typemap.DefaultOptions()

	col := typemap.MySQLColumn{Name: "data", DataType: "json", ColumnType: "json"}
	m, err := typemap.MapMySQLColumn(col, opts)
	if err != nil || m.TargetType != "jsonb" {
		t.Fatalf("expected jsonb, got %s, err: %v", m.TargetType, err)
	}
	v, err := m.Convert(`{"name":"strand","version":1}`)
	if err != nil || !strings.Contains(v.(string), "strand") {
		t.Errorf("convert json failed: v=%v, err=%v", v, err)
	}
	// JSON không hợp lệ phải trả lỗi
	if _, err := m.Convert(`{invalid-json}`); err == nil {
		t.Errorf("expected error for invalid json")
	}
}

func TestMapMySQLColumn_EnumAndSet(t *testing.T) {
	opts := typemap.DefaultOptions()

	// ENUM: policy "check" -> text
	colE := typemap.MySQLColumn{
		Name:       "status",
		DataType:   "enum",
		ColumnType: "enum('pending','active','completed')",
		EnumValues: []string{"pending", "active", "completed"},
	}
	mE, err := typemap.MapMySQLColumn(colE, opts)
	if err != nil || mE.TargetType != "text" {
		t.Fatalf("expected text for enum check, got %s, err: %v", mE.TargetType, err)
	}
	ve, err := mE.Convert("active")
	if err != nil || ve != "active" {
		t.Errorf("convert valid enum failed: v=%v, err=%v", ve, err)
	}
	// Enum ngoài danh sách phải trả lỗi exit 3
	_, err = mE.Convert("unknown_status")
	if err == nil {
		t.Fatalf("expected error for invalid enum value")
	}
	if exitcode.GetExitCode(err) != exitcode.Rejected {
		t.Errorf("expected exitcode.Rejected, got %d", exitcode.GetExitCode(err))
	}

	// ENUM: policy "type"
	optsType := opts
	optsType.EnumPolicy = "type"
	mEType, err := typemap.MapMySQLColumn(colE, optsType)
	if err != nil || mEType.TargetType != "status_enum" {
		t.Fatalf("expected status_enum, got %s, err: %v", mEType.TargetType, err)
	}

	// SET -> text[]
	colS := typemap.MySQLColumn{
		Name:       "perms",
		DataType:   "set",
		ColumnType: "set('read','write','exec')",
		EnumValues: []string{"read", "write", "exec"},
	}
	mS, err := typemap.MapMySQLColumn(colS, opts)
	if err != nil || mS.TargetType != "text[]" {
		t.Fatalf("expected text[] for set, got %s, err: %v", mS.TargetType, err)
	}
	vs, err := mS.Convert("read,write")
	if err != nil {
		t.Fatalf("convert set failed: %v", err)
	}
	expectedSet := []string{"read", "write"}
	if !reflect.DeepEqual(vs, expectedSet) {
		t.Errorf("expected %v, got %v", expectedSet, vs)
	}
}

func TestMapMySQLColumn_Geometry_Unmappable(t *testing.T) {
	opts := typemap.DefaultOptions()

	geomTypes := []string{"geometry", "point", "linestring", "polygon", "multipoint", "multilinestring", "multipolygon", "geometrycollection"}
	for _, gt := range geomTypes {
		col := typemap.MySQLColumn{Name: "geom", DataType: gt, ColumnType: gt}
		_, err := typemap.MapMySQLColumn(col, opts)
		if err == nil {
			t.Fatalf("expected error for unmappable type %q", gt)
		}
		if exitcode.GetExitCode(err) != exitcode.Rejected {
			t.Errorf("expected exitcode.Rejected for %q, got %d", gt, exitcode.GetExitCode(err))
		}
	}
}
