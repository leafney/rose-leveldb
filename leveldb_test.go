package rleveldb

import (
	"os"
	"testing"
)

func TestNewLevelDB(t *testing.T) {
	db, err := NewLevelDB("./testdb")
	if err != nil {
		os.Exit(1)
	}
	defer db.Close()

	// 事务
	tx, err := db.Begin()
	if err != nil {
		t.Log(err)
		os.Exit(1)
	}

	defer tx.Discard()

	tx.Put([]byte(""), []byte(""), nil)

	tx.Commit()

}
