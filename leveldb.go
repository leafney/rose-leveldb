package rleveldb

import (
	"encoding/json"
	"errors"
	"github.com/syndtr/goleveldb/leveldb"
	ldberrors "github.com/syndtr/goleveldb/leveldb/errors"
	"github.com/syndtr/goleveldb/leveldb/filter"
	"github.com/syndtr/goleveldb/leveldb/opt"
	"strconv"
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

func (l *LevelDB) Set(key []byte, value []byte) error {
	return l.db.Put(key, value, nil)
}

func (l *LevelDB) SetS(key string, value string) error {
	return l.Set([]byte(key), []byte(value))
}

// Exists 检查给定的 key 是否存在
func (l *LevelDB) Exists(key []byte) (bool, error) {
	return l.db.Has(key, nil)
}

// ExistsS 检查给定的 key 是否存在
func (l *LevelDB) ExistsS(key string) (bool, error) {
	return l.Exists([]byte(key))
}

func (l *LevelDB) Del(key []byte) error {
	return l.db.Delete(key, nil)
}

func (l *LevelDB) DelS(key string) error {
	return l.Del([]byte(key))
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

// XSet 设置 key 的值
func (l *LevelDB) XSet(key string, value string) error {
	cache := CacheType{
		Data:    []byte(value),
		Created: time.Now().Unix(),
		Expire:  0,
	}

	jsonStr, err := json.Marshal(cache)
	if err != nil {
		return err
	}

	return l.db.Put([]byte(key), jsonStr, nil)
}

// XSetEx 设置 key 的值，并设置 key 的过期时间，以秒计
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

// XGet 获取指定 key 的值，如果 key 不存在时，返回空字符串
func (l *LevelDB) XGet(key string) (string, error) {
	baseKey := []byte(key)
	data, err := l.db.Get(baseKey, nil)
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
		err = l.Del(baseKey)
		return "", err
	}

	return string(cache.Data), nil
}

// XTTL 以秒为单位，返回指定 key 的剩余生存时间
// 当 key 不存在，或者 key 没有设置剩余生存时间时，命令都返回 -1
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

// XExpire 为指定的 key 设置过期时间，以秒计
func (l *LevelDB) XExpire(key string, seconds int64) error {
	baseKey := []byte(key)
	data, err := l.db.Get(baseKey, nil)
	if err != nil && !errors.Is(err, ldberrors.ErrNotFound) {
		return err
	}

	if errors.Is(err, ldberrors.ErrNotFound) {
		return nil
	}

	if len(data) == 0 {
		return nil
	}

	var cache CacheType
	err = json.Unmarshal(data, &cache)
	if err != nil {
		return err
	}

	cache.Expire = time.Now().Add(time.Duration(seconds) * time.Second).Unix()
	jsonStr, err := json.Marshal(cache)
	if err != nil {
		return err
	}

	return l.db.Put(baseKey, jsonStr, nil)
}

// XIncr 递增一
func (l *LevelDB) XIncr(key string) (int64, error) {
	return l.XIncrBy(key, 1)
}

// XIncrBy 增加给定的增量值
func (l *LevelDB) XIncrBy(key string, increment int64) (int64, error) {
	baseKey := []byte(key)
	data, err := l.db.Get(baseKey, nil)
	if err != nil && !errors.Is(err, ldberrors.ErrNotFound) {
		return 0, err
	}

	var cache CacheType
	var intValue int64
	if errors.Is(err, ldberrors.ErrNotFound) {
		intValue = 0
		cache = CacheType{
			Data:    []byte("0"),
			Created: time.Now().Unix(),
			Expire:  0,
		}
	} else {
		err = json.Unmarshal(data, &cache)
		if err != nil {
			return 0, err
		}
		intValue, err = strconv.ParseInt(string(cache.Data), 10, 64)
		if err != nil {
			return 0, err
		}
	}

	intValue += increment

	cache.Data = []byte(strconv.FormatInt(intValue, 10))
	jsonStr, err := json.Marshal(cache)
	if err != nil {
		return 0, err
	}

	err = l.db.Put(baseKey, jsonStr, nil)
	if err != nil {
		return 0, nil
	}
	return intValue, nil
}

// XDecr 递减一
func (l *LevelDB) XDecr(key string) (int64, error) {
	return l.XDecrBy(key, 1)
}

// XDecrBy 减去给定的减量值
func (l *LevelDB) XDecrBy(key string, decrement int64) (int64, error) {
	baseKey := []byte(key)
	data, err := l.db.Get(baseKey, nil)
	if err != nil && !errors.Is(err, ldberrors.ErrNotFound) {
		return 0, err
	}

	var cache CacheType
	var intValue int64
	if errors.Is(err, ldberrors.ErrNotFound) {
		intValue = 0
		cache = CacheType{
			Data:    []byte("0"),
			Created: time.Now().Unix(),
			Expire:  0,
		}
	} else {
		err = json.Unmarshal(data, &cache)
		if err != nil {
			return 0, err
		}
		intValue, err = strconv.ParseInt(string(cache.Data), 10, 64)
		if err != nil {
			return 0, err
		}
	}

	intValue -= decrement

	cache.Data = []byte(strconv.FormatInt(intValue, 10))
	jsonStr, err := json.Marshal(cache)
	if err != nil {
		return 0, err
	}

	err = l.db.Put(baseKey, jsonStr, nil)
	if err != nil {
		return 0, nil
	}
	return intValue, nil
}
