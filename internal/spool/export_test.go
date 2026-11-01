package spool

import "strand/internal/model"

// EncodeTxnForTest xuất encodeTxn cho package spool_test.
func EncodeTxnForTest(t *model.Txn) ([]byte, error) {
	return encodeTxn(t)
}

// DecodeTxnForTest xuất decodeTxn cho package spool_test.
func DecodeTxnForTest(data []byte) (*model.Txn, error) {
	return decodeTxn(data)
}
