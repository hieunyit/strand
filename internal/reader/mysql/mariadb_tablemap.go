package mysql

import (
	"encoding/binary"
	"fmt"
	"sync"

	gomysql "github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/replication"
)

// MariaDB ghi cột COMPRESSED vào TABLE_MAP bằng kiểu riêng (BLOB_COMPRESSED=140, VARCHAR_COMPRESSED=141) mà
// go-mysql không biết: decodeMeta bỏ qua meta của hai kiểu này nên meta của MỌI cột đứng sau bị lệch, và
// decodeValue báo "unsupport type". Ở đây dựng lại TABLE_MAP với kiểu BLOB/VARCHAR chuẩn và meta đúng, để
// go-mysql giải mã được giá trị thô (còn nén); reader giải nén sau (mariadb_compress.go).
const (
	mariaTypeBlobCompressed    byte = 140
	mariaTypeVarcharCompressed byte = 141
)

type fixedTableMap struct {
	src        *replication.TableMapEvent
	fixed      *replication.TableMapEvent
	compressed []int
}

var (
	fixMu    sync.Mutex
	fixCache = map[uint64]fixedTableMap{}
)

// fixMariaDBTableMap trả TABLE_MAP đã sửa (cùng con trỏ nếu không có cột nén) và chỉ số các cột nén.
// Meta thô của TABLE_MAP nằm ngay sau mảng kiểu cột trong cùng buffer của event: go-mysql gán
// ColumnType = data[pos:pos+n] nên đọc tiếp được qua cap của slice. Meta không khớp độ dài khai báo thì lỗi
// (không đoán).
func fixMariaDBTableMap(tm *replication.TableMapEvent) (*replication.TableMapEvent, []int, error) {
	has := false
	for _, t := range tm.ColumnType {
		if t == mariaTypeBlobCompressed || t == mariaTypeVarcharCompressed {
			has = true
			break
		}
	}
	if !has {
		return tm, nil, nil
	}
	fixMu.Lock()
	if c, ok := fixCache[tm.TableID]; ok && c.src == tm {
		fixMu.Unlock()
		return c.fixed, c.compressed, nil
	}
	fixMu.Unlock()

	n := int(tm.ColumnCount)
	raw := tm.ColumnType[:cap(tm.ColumnType)]
	if len(tm.ColumnType) != n || n > len(raw) {
		return nil, nil, fmt.Errorf("%s.%s: unexpected TABLE_MAP column layout", tm.Schema, tm.Table)
	}
	mlen, _, k := gomysql.LengthEncodedInt(raw[n:])
	if k == 0 || n+k+int(mlen) > len(raw) {
		return nil, nil, fmt.Errorf("%s.%s: cannot locate TABLE_MAP column metadata", tm.Schema, tm.Table)
	}
	meta := raw[n+k : n+k+int(mlen)]
	types := make([]byte, n)
	metas := make([]uint16, n)
	var comp []int
	pos := 0
	need := func(b int) error {
		if pos+b > len(meta) {
			return fmt.Errorf("%s.%s: TABLE_MAP column metadata is shorter than its column types imply", tm.Schema, tm.Table)
		}
		return nil
	}
	for i, t := range tm.ColumnType {
		types[i] = t
		switch t {
		case gomysql.MYSQL_TYPE_STRING, gomysql.MYSQL_TYPE_NEWDECIMAL:
			if err := need(2); err != nil {
				return nil, nil, err
			}
			metas[i] = uint16(meta[pos])<<8 | uint16(meta[pos+1])
			pos += 2
		case gomysql.MYSQL_TYPE_VAR_STRING, gomysql.MYSQL_TYPE_VARCHAR, gomysql.MYSQL_TYPE_BIT:
			if err := need(2); err != nil {
				return nil, nil, err
			}
			metas[i] = binary.LittleEndian.Uint16(meta[pos:])
			pos += 2
		case mariaTypeVarcharCompressed:
			if err := need(2); err != nil {
				return nil, nil, err
			}
			// meta = độ dài byte tối đa + 1 (byte header nén); MariaDB chọn tiền tố 1/2 byte theo đúng số này
			// (VARCHAR(255) latin1 nén có tiền tố 2 byte, kiểm bằng binlog thật), khớp quy tắc meta<256 của go-mysql.
			types[i], metas[i] = gomysql.MYSQL_TYPE_VARCHAR, binary.LittleEndian.Uint16(meta[pos:])
			pos += 2
			comp = append(comp, i)
		case mariaTypeBlobCompressed:
			if err := need(1); err != nil {
				return nil, nil, err
			}
			types[i], metas[i] = gomysql.MYSQL_TYPE_BLOB, uint16(meta[pos])
			pos++
			comp = append(comp, i)
		case gomysql.MYSQL_TYPE_BLOB, gomysql.MYSQL_TYPE_DOUBLE, gomysql.MYSQL_TYPE_FLOAT, gomysql.MYSQL_TYPE_GEOMETRY,
			gomysql.MYSQL_TYPE_VECTOR, gomysql.MYSQL_TYPE_JSON,
			gomysql.MYSQL_TYPE_TIME2, gomysql.MYSQL_TYPE_DATETIME2, gomysql.MYSQL_TYPE_TIMESTAMP2:
			if err := need(1); err != nil {
				return nil, nil, err
			}
			metas[i] = uint16(meta[pos])
			pos++
		}
	}
	if pos != len(meta) {
		return nil, nil, fmt.Errorf("%s.%s: TABLE_MAP column metadata has %d bytes but column types consume %d", tm.Schema, tm.Table, len(meta), pos)
	}
	fixed := *tm
	fixed.ColumnType, fixed.ColumnMeta = types, metas
	fixMu.Lock()
	if len(fixCache) > 4096 {
		fixCache = map[uint64]fixedTableMap{}
	}
	fixCache[tm.TableID] = fixedTableMap{src: tm, fixed: &fixed, compressed: comp}
	fixMu.Unlock()
	return &fixed, comp, nil
}

// MariaDBRowsDecode là RowsEventDecodeFunc của go-mysql cho nguồn MariaDB: sửa TABLE_MAP có cột COMPRESSED
// trước khi giải mã row.
func MariaDBRowsDecode(re *replication.RowsEvent, data []byte) error {
	pos, err := re.DecodeHeader(data)
	if err != nil {
		return err
	}
	fixed, _, err := fixMariaDBTableMap(re.Table)
	if err != nil {
		return err
	}
	re.Table = fixed
	return re.DecodeData(pos, data)
}

// FixMariaDBTableMap xuất cho tool/test.
func FixMariaDBTableMap(tm *replication.TableMapEvent) (*replication.TableMapEvent, []int, error) {
	return fixMariaDBTableMap(tm)
}
