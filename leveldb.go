package rleveldb

import (
	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/filter"
	"github.com/syndtr/goleveldb/leveldb/opt"
)

type LevelDB struct {
	db *leveldb.DB
}

func NewLevelDB(dbPath string) (*LevelDB, error) {
	db, err := leveldb.OpenFile(dbPath, nil)
	if err != nil {
		return nil, err
	}
	return &LevelDB{db}, nil
}

func NewBloomFilter(dbPath string, bits int) (*LevelDB, error) {
	o := &opt.Options{
		Filter: filter.NewBloomFilter(bits),
	}
	db, err := leveldb.OpenFile(dbPath, o)
	if err != nil {
		return nil, err
	}
	return &LevelDB{db}, nil
}

func (l *LevelDB) Get(key []byte) ([]byte, error) {
	return l.db.Get(key, nil)
}

func (l *LevelDB) GetString(key string) (string, error) {
	value, err := l.Get([]byte(key))
	if err != nil {
		return "", err
	}
	return string(value), nil
}

func (l *LevelDB) Put(key []byte, value []byte) error {
	return l.db.Put(key, value, nil)
}

func (l *LevelDB) PutString(key string, value string) error {
	return l.Put([]byte(key), []byte(value))
}

func (l *LevelDB) Delete(key []byte) error {
	return l.db.Delete(key, nil)
}

func (l *LevelDB) DeleteString(key string) error {
	return l.Delete([]byte(key))
}

func (l *LevelDB) Close() error {
	return l.db.Close()
}

func (l *LevelDB) Begin() (*leveldb.Transaction, error) {
	return l.db.OpenTransaction()
}

//func (l *LevelDB) Iterator(start []byte, end []byte) riterator {
//	iter := l.db.NewIterator(&util.Range{Start: start, Limit: end}, nil)
//	return riterator{iter}
//}
