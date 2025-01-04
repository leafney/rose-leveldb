package rleveldb

import (
	"os"
	"testing"
	"time"
)

func TestLevelDB(t *testing.T) {
	// 初始化测试数据库
	dbPath := "./testdb"
	db, err := NewLevelDB(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		db.Close()
		os.RemoveAll(dbPath)
	}()

	t.Run("基础操作", func(t *testing.T) {
		// Set/Get 测试
		if err := db.SetS("key1", "value1"); err != nil {
			t.Fatal(err)
		}
		if val, err := db.GetS("key1"); err != nil || val != "value1" {
			t.Fatal("Set/Get failed")
		}

		// Exists 测试
		if !db.Exists("key1") {
			t.Fatal("Exists failed")
		}

		// Del 测试
		if err := db.Del("key1"); err != nil {
			t.Fatal(err)
		}
		if db.Exists("key1") {
			t.Fatal("Del failed")
		}
	})

	t.Run("过期时间操作", func(t *testing.T) {
		// XSetEx 测试
		if err := db.XSetExS("key2", "value2", time.Second*2); err != nil {
			t.Fatal(err)
		}

		// XTTL 测试
		if ttl, err := db.XTTL("key2"); err != nil || ttl <= 0 {
			t.Fatal("XTTL failed")
		}

		// 等待过期
		time.Sleep(time.Second * 3)

		// 确保 XGetS 返回 nil 而不是错误
		if val, err := db.XGetS("key2"); err != nil {
			t.Fatal("Expected no error after expiration, but got:", err)
		} else if val != "" {
			t.Fatal("Expected value to be empty after expiration, but got:", val)
		}
	})

	t.Run("计数器操作", func(t *testing.T) {
		// XIncr 测试
		val, err := db.XIncr("counter")
		if err != nil || val != 1 {
			t.Fatal("XIncr failed")
		}

		// XIncrBy 测试
		val, err = db.XIncrBy("counter", 5)
		if err != nil || val != 6 {
			t.Fatal("XIncrBy failed")
		}

		// XDecr 测试
		val, err = db.XDecr("counter")
		if err != nil || val != 5 {
			t.Fatal("XDecr failed")
		}

		// XDecrBy 测试
		val, err = db.XDecrBy("counter", 3)
		if err != nil || val != 2 {
			t.Fatal("XDecrBy failed")
		}
	})
}

func TestNewLevelDB(t *testing.T) {
	dbPath := "./testdb"
	db, err := NewLevelDB(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		db.Close()
		os.RemoveAll(dbPath)
	}()

	// db.XSetExSecS("aa", "1", 300)

	// time.Sleep(50 * time.Second)
	// b, _ := db.XTTL("aa")
	// t.Log(b)

	db.XSetS("cc","1")
	db.XExpireSec("cc",10)
	time.Sleep(8*time.Second)
	dd1,_:= db.XGetS("cc")
	t.Logf("dd1 %v",dd1)
	time.Sleep(1*time.Second)
	dd2,_:=db.XGetS("cc")
	t.Logf("dd2 %v",dd2)
	time.Sleep(1*time.Second)
	dd3,_:=db.XGetS("cc")
	t.Logf("dd3 %v",dd3)
	time.Sleep(1*time.Second)
	dd4,err4:=db.XGetS("cc")
	t.Logf("dd4 %v err %v",dd4,err4)
}
