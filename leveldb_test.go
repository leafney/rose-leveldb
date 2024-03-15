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
	/*
		db.XSetEx("hello", "world", 60)
		time.Sleep(10 * time.Second)
		t.Log(db.XGet("hello"))
		time.Sleep(20 * time.Second)
		t.Log(db.XTTL("hello"))
		time.Sleep(31 * time.Second)
		t.Log(db.XGet("hello"))
		t.Log(db.XTTL("hello"))
	*/

	//	Incr Decr
	db.XSet("sunday", "5")
	t.Log(db.XGet("sunday"))
	t.Log(db.XExpire("sunday", 30))
	time.Sleep(10 * time.Second)
	t.Log(db.XTTL("sunday"))
	t.Log(db.XIncr("sunday"))
	time.Sleep(5 * time.Second)
	t.Log(db.XTTL("sunday"))
	t.Log(db.XIncrBy("sunday", 3))
	t.Log(db.XDecrBy("sunday", 2))
	t.Log(db.XGet("sunday"))
	t.Log(db.XTTL("sunday"))
	time.Sleep(20 * time.Second)
	t.Log(db.XGet("sunday"))
}
