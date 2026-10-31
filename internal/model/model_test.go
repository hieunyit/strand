package model

import (
	"encoding/json"
	"reflect"
	"strings"
	"testing"
)

// invariant: CP-3
// Chỉ GTID hoặc commit LSN được ghi xuống; seq không bao giờ được ghi.
// Kiểu Checkpoint không có trường seq; Txn không serialize seq.
func TestCP3_NoSeqInSerializedTxnOrCheckpoint(t *testing.T) {
	// 1. Kiểm tra cấu trúc Checkpoint không tồn tại trường seq
	cpType := reflect.TypeOf(Checkpoint{})
	for i := 0; i < cpType.NumField(); i++ {
		field := cpType.Field(i)
		fieldNameLower := strings.ToLower(field.Name)
		if strings.Contains(fieldNameLower, "seq") {
			t.Fatalf("Bất biến CP-3 vi phạm: struct Checkpoint chứa trường %s", field.Name)
		}
	}

	// 2. Kiểm tra Txn có trường seq là unexported và có json:"-"
	txnType := reflect.TypeOf(Txn{})
	seqField, found := txnType.FieldByName("seq")
	if !found {
		t.Fatalf("kỳ vọng struct Txn có trường seq trong RAM")
	}
	if seqField.PkgPath == "" {
		t.Fatalf("Bất biến CP-3 vi phạm: trường seq của Txn phải là unexported (chữ thường)")
	}
	jsonTag := seqField.Tag.Get("json")
	if jsonTag != "-" {
		t.Fatalf("Bất biến CP-3 vi phạm: trường seq của Txn phải có json:\"-\", actual: %q", jsonTag)
	}

	// 3. Serialize Txn và kiểm tra payload JSON không chứa seq
	txn := &Txn{
		SourcePos: Pos{
			Engine: EngineMySQL,
			GTID:   "3E11FA47-71CA-11E1-9E33-C80AA9429562:1-100",
		},
		SchemaVer: 1,
		Rows: []RowChange{
			{
				Table: TableID{Schema: "test", Table: "users"},
				Op:    OpInsert,
				Key:   []Value{1},
				Cols: []ColValue{
					{Name: "id", Value: 1},
					{Name: "name", Value: "alice"},
					{Name: "bio", Value: nil, Unchanged: true}, // TOAST unchanged
				},
			},
		},
		Bytes: 256,
	}
	txn.SetSeq(999999) // gán seq trong RAM

	data, err := json.Marshal(txn)
	if err != nil {
		t.Fatalf("failed to marshal Txn: %v", err)
	}
	jsonStr := string(data)
	if strings.Contains(strings.ToLower(jsonStr), "seq") {
		t.Fatalf("Bất biến CP-3 vi phạm: JSON serialized của Txn chứa 'seq': %s", jsonStr)
	}

	// 4. Serialize Checkpoint và kiểm tra payload JSON không chứa seq
	cp := &Checkpoint{
		PipelineID: "test-pipeline",
		Applied: Pos{
			Engine: EnginePG,
			LSN:    12345678,
		},
		CreatedAt: 1730419200,
	}
	cpData, err := json.Marshal(cp)
	if err != nil {
		t.Fatalf("failed to marshal Checkpoint: %v", err)
	}
	cpJsonStr := string(cpData)
	if strings.Contains(strings.ToLower(cpJsonStr), "seq") {
		t.Fatalf("Bất biến CP-3 vi phạm: JSON serialized của Checkpoint chứa 'seq': %s", cpJsonStr)
	}

	t.Logf("CP-3 ĐẠT: Txn JSON=%s", jsonStr)
	t.Logf("CP-3 ĐẠT: Checkpoint JSON=%s", cpJsonStr)
}

func TestModelBasics(t *testing.T) {
	posMySQL := Pos{Engine: EngineMySQL, GTID: "uuid:1"}
	if posMySQL.String() != "mysql:uuid:1" {
		t.Errorf("unexpected pos string: %s", posMySQL.String())
	}

	posPG := Pos{Engine: EnginePG, LSN: 0x16B2D80}
	if posPG.String() != "pg:0/16B2D80" {
		t.Errorf("unexpected pg pos string: %s", posPG.String())
	}

	tbl := TableID{Schema: "app", Table: "orders"}
	if tbl.String() != "app.orders" {
		t.Errorf("unexpected table string: %s", tbl.String())
	}

	col := ColValue{Name: "bio", Unchanged: true}
	colData, err := json.Marshal(col)
	if err != nil {
		t.Fatalf("failed to marshal ColValue: %v", err)
	}
	if !strings.Contains(string(colData), `"unchanged":true`) {
		t.Errorf("expected unchanged:true in ColValue JSON, got %s", string(colData))
	}
}
