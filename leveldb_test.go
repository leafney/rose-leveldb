package rleveldb

import (
	"os"
	"testing"
	"time"
)

func TestNewLevelDB(t *testing.T) {
	db, err := NewLevelDB("./testdb")
	if err != nil {
		os.Exit(1)
	}
	defer db.Close()

	// 事务
	/*
		tx, err := db.Begin()
		if err != nil {
			t.Log(err)
			os.Exit(1)
		}

		defer tx.Discard()

		tx.Put([]byte(""), []byte(""), nil)

		tx.Commit()

	*/

	//	PutEX

	db.PutExString("hello", "world", 60)
	time.Sleep(10 * time.Second)
	t.Log(db.GetExString("hello"))
	time.Sleep(20 * time.Second)
	t.Log(db.TTLString("hello"))
	time.Sleep(31 * time.Second)
	t.Log(db.GetEx([]byte("hello")))
	t.Log(db.TTL([]byte("hello")))

}
