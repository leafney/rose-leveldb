package rleveldb

import (
	"encoding/json"
	"errors"
	"github.com/syndtr/goleveldb/leveldb"
	ldberrors "github.com/syndtr/goleveldb/leveldb/errors"
	"github.com/syndtr/goleveldb/leveldb/filter"
	"github.com/syndtr/goleveldb/leveldb/opt"
	"time"
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

func (l *LevelDB) GetS(key string) (string, error) {
	value, err := l.Get([]byte(key))
	if err != nil {
		return "", err
	}
	return string(value), nil
}

func (l *LevelDB) Put(key []byte, value []byte) error {
	return l.db.Put(key, value, nil)
}

func (l *LevelDB) PutS(key string, value string) error {
	return l.Put([]byte(key), []byte(value))
}

func (l *LevelDB) Has(key []byte) (bool, error) {
	return l.db.Has(key, nil)
}

func (l *LevelDB) HasS(key string) (bool, error) {
	return l.Has([]byte(key))
}

func (l *LevelDB) Delete(key []byte) error {
	return l.db.Delete(key, nil)
}

func (l *LevelDB) DeleteS(key string) error {
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

// *******************

type CacheType struct {
	Data    []byte `json:"data"`
	Created int64  `json:"created"`
	Expire  int64  `json:"expire"`
}

func (l *LevelDB) XSetEx(key string, value string, expires int64) error {
	cache := CacheType{
		Data:    []byte(value),
		Created: time.Now().Unix(),
		Expire:  0,
	}

	if expires > 0 {
		cache.Expire = cache.Created + expires
	}

	jsonStr, err := json.Marshal(cache)
	if err != nil {
		return err
	}

	return l.db.Put([]byte(key), jsonStr, nil)
}

func (l *LevelDB) XGet(key string) (string, error) {

	data, err := l.db.Get([]byte(key), nil)
	if err != nil && !errors.Is(err, ldberrors.ErrNotFound) {
		return "", err
	}

	if len(data) == 0 {
		return "", nil
	}

	var cache CacheType
	err = json.Unmarshal(data, &cache)
	if err != nil {
		return "", err
	}

	secs := time.Now().Unix()

	if cache.Expire > 0 && cache.Expire <= secs {
		err = l.Delete([]byte(key))
		return "", err
	}

	return string(cache.Data), nil
}

func (l *LevelDB) XTTL(key string) (int64, error) {
	data, err := l.db.Get([]byte(key), nil)
	if err != nil && !errors.Is(err, ldberrors.ErrNotFound) {
		return -1, err
	}

	if len(data) == 0 {
		return -1, nil
	}

	var cache CacheType
	err = json.Unmarshal(data, &cache)
	if err != nil {
		return -1, err
	}

	secs := time.Now().Unix()
	if cache.Expire > 0 {
		if cache.Expire <= secs {
			return secs - cache.Expire, nil
		} else {
			return cache.Expire - secs, nil
		}
	}

	return -1, nil
}
