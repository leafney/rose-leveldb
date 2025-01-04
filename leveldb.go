package rleveldb

import (
	"bytes"
	"encoding/gob"
	"strconv"
	"time"

	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/filter"
	"github.com/syndtr/goleveldb/leveldb/opt"
)

// LevelDB 结构体封装了 goleveldb 的基本操作
type LevelDB struct {
	db *leveldb.DB
}

// NewLevelDB 创建一个新的 LevelDB 实例
// 示例：
//   db, err := NewLevelDB("./data")
//   if err != nil {
//       log.Fatal(err)
//   }
//   defer db.Close()
func NewLevelDB(dbPath string) (*LevelDB, error) {
	db, err := leveldb.OpenFile(dbPath, nil)
	if err != nil {
		return nil, err
	}
	return &LevelDB{db}, nil
}

// NewBloomFilter 创建一个带布隆过滤器的 LevelDB 实例
// bits 参数指定布隆过滤器的大小，通常设置为 10 即可
// 示例：
//   db, err := NewBloomFilter("./data", 10)
//   if err != nil {
//       log.Fatal(err)
//   }
//   defer db.Close()
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

// Get 获取指定key的值
// 示例：
//   value, err := db.Get("key")
//   if err != nil {
//       log.Fatal(err)
//   }
//   fmt.Printf("值: %s\n", value)
func (l *LevelDB) Get(key string) ([]byte, error) {
	return l.db.Get([]byte(key), nil)
}

// GetS 获取指定key的字符串值
// 示例：
//   value, err := db.GetS("key")
//   if err != nil {
//       log.Fatal(err)
//   }
//   fmt.Printf("字符串值: %s\n", value)
func (l *LevelDB) GetS(key string) (string, error) {
	value, err := l.Get(key)
	if err != nil {
		return "", err
	}
	return string(value), nil
}

// Set 设置key的值
// 示例：
//   err := db.Set("key", []byte("value"))
//   if err != nil {
//       log.Fatal(err)
//   }
func (l *LevelDB) Set(key string, value []byte) error {
	return l.db.Put([]byte(key), value, nil)
}

// SetS 设置key的字符串值
// 示例：
//   err := db.SetS("key", "value")
//   if err != nil {
//       log.Fatal(err)
//   }
func (l *LevelDB) SetS(key string, value string) error {
	return l.Set(key, []byte(value))
}

// Exists 检查key是否存在
// 示例：
//   if db.Exists("key") {
//       fmt.Println("key存在")
//   } else {
//       fmt.Println("key不存在")
//   }
func (l *LevelDB) Exists(key string) bool {
	exists, err := l.db.Has([]byte(key), nil)
	if err != nil {
		return false
	}
	return exists
}

// Del 删除指定的key
// 示例：
//   err := db.Del("key")
//   if err != nil {
//       log.Fatal(err)
//   }
func (l *LevelDB) Del(key string) error {
	return l.db.Delete([]byte(key), nil)
}

// Close 关闭数据库连接
// 示例：
//   defer db.Close()
func (l *LevelDB) Close() error {
	return l.db.Close()
}

// *******************

// CacheType 定义缓存数据结构
type CacheType struct {
	Data    []byte
	Created int64
	Expire  int64
}

// XGet 获取带过期时间的缓存数据
// 当数据过期时会自动删除并返回nil
// 示例：
//   value, err := db.XGet("key")
//   if err != nil {
//       log.Fatal(err)
//   }
//   if value == nil {
//       fmt.Println("key不存在或已过期")
//   } else {
//       fmt.Printf("值: %s\n", value)
//   }
func (l *LevelDB) XGet(key string) ([]byte, error) {
	data, err := l.db.Get([]byte(key), nil)
	if err != nil {
		return nil, err
	}

	var cache CacheType
	decoder := gob.NewDecoder(bytes.NewReader(data))
	if err := decoder.Decode(&cache); err != nil {
		return nil, err
	}

	// 检查是否过期
	if cache.Expire > 0 && cache.Expire <= time.Now().Unix() {
		l.Del(key) // 删除过期数据
		return nil, nil
	}

	return cache.Data, nil
}

// XGetS 获取带过期时间的字符串数据
// 示例：
//   value, err := db.XGetS("key")
//   if err != nil {
//       log.Fatal(err)
//   }
//   if value == "" {
//       fmt.Println("key不存在或已过期")
//   } else {
//       fmt.Printf("字符串值: %s\n", value)
//   }
func (l *LevelDB) XGetS(key string) (string, error) {
	data, err := l.XGet(key)
	if err != nil {
		return "", err
	}
	if data == nil {
		return "", nil
	}
	return string(data), nil
}

// XSet 设置带过期时间的缓存数据
// 示例：
//   err := db.XSet("key", []byte("value"))
//   if err != nil {
//       log.Fatal(err)
//   }
func (l *LevelDB) XSet(key string, value []byte) error {
	cache := CacheType{
		Data:    value,
		Created: time.Now().Unix(),
		Expire:  0,
	}

	var buf bytes.Buffer
	encoder := gob.NewEncoder(&buf)
	if err := encoder.Encode(cache); err != nil {
		return err
	}

	return l.db.Put([]byte(key), buf.Bytes(), nil)
}

// XSetS 设置带过期时间的字符串数据
// 示例：
//   err := db.XSetS("key", "value")
//   if err != nil {
//       log.Fatal(err)
//   }
func (l *LevelDB) XSetS(key string, value string) error {
	return l.XSet(key, []byte(value))
}

// XSetEx 设置带过期时间的缓存数据 （使用 time.Duration）
// 示例：
//   err := db.XSetEx("key", []byte("value"), time.Hour)
//   if err != nil {
//       log.Fatal(err)
//   }
func (l *LevelDB) XSetEx(key string, value []byte, expires time.Duration) error {
	cache := CacheType{
		Data:    value,
		Created: time.Now().Unix(),
		Expire:  time.Now().Add(expires).Unix(),
	}

	var buf bytes.Buffer
	encoder := gob.NewEncoder(&buf)
	if err := encoder.Encode(cache); err != nil {
		return err
	}

	return l.db.Put([]byte(key), buf.Bytes(), nil)
}

// XSetExS 设置带过期时间的字符串数据 （使用 time.Duration）
// 示例：
//   err := db.XSetExS("key", "value", time.Hour)
//   if err != nil {
//       log.Fatal(err)
//   }
func (l *LevelDB) XSetExS(key string, value string, expires time.Duration) error {
	return l.XSetEx(key, []byte(value), expires)
}

// XSetExSec 设置带过期时间的缓存数据（使用秒数）
// 示例：
//   err := db.XSetExSec("key", []byte("value"), 3600)
//   if err != nil {
//       log.Fatal(err)
//   }
func (l *LevelDB) XSetExSec(key string, value []byte, seconds int64) error {
	return l.XSetEx(key, value, time.Duration(seconds)*time.Second)
}

// XSetExSecS 设置带过期时间的字符串数据（使用秒数）
// 示例：
//   err := db.XSetExSecS("key", "value", 3600)
//   if err != nil {
//       log.Fatal(err)
//   }
func (l *LevelDB) XSetExSecS(key string, value string, seconds int64) error {
	return l.XSetExSec(key, []byte(value), seconds)
}

// XTTL 返回key的剩余生存时间(秒)
// 返回值说明：
//   -2: key不存在（包括已过期的情况）
//   -1: key存在但未设置过期时间
//   >=0: key的剩余生存时间(秒)
// 示例：
//   ttl, err := db.XTTL("key")
//   if err != nil {
//       log.Fatal(err)
//   }
//   switch ttl {
//   case -2:
//       fmt.Println("key不存在")
//   case -1:
//       fmt.Println("key未设置过期时间")
//   default:
//       fmt.Printf("剩余生存时间: %d秒\n", ttl)
//   }
func (l *LevelDB) XTTL(key string) (int64, error) {
	data, err := l.db.Get([]byte(key), nil)
	if err != nil {
		if err == leveldb.ErrNotFound {
			return -2, nil // key 不存在
		}
		return -2, err // 其他错误情况
	}

	var cache CacheType
	decoder := gob.NewDecoder(bytes.NewReader(data))
	if err := decoder.Decode(&cache); err != nil {
		return -2, err
	}

	// key 存在但未设置过期时间
	if cache.Expire == 0 {
		return -1, nil
	}

	// 计算剩余生存时间
	ttl := cache.Expire - time.Now().Unix()
	if ttl < 0 {
		// 已过期，删除数据并返回 -2
		l.Del(key)
		return -2, nil
	}

	return ttl, nil
}

// XExpire 设置key的过期时间
// 示例：
//   err := db.XExpire("key", time.Hour)
//   if err != nil {
//       log.Fatal(err)
//   }
func (l *LevelDB) XExpire(key string, expires time.Duration) error {
	return l.XExpireAt(key, time.Now().Add(expires))
}

// XExpireSec 设置key的过期时间(秒)
// 示例：
//   err := db.XExpireSec("key", 3600)
//   if err != nil {
//       log.Fatal(err)
//   }
func (l *LevelDB) XExpireSec(key string, seconds int64) error {
	return l.XExpire(key, time.Duration(seconds)*time.Second)
}

// XExpireAt 设置key的过期时间点
// 示例：
//   err := db.XExpireAt("key", time.Now().Add(time.Hour))
//   if err != nil {
//       log.Fatal(err)
//   }
func (l *LevelDB) XExpireAt(key string, tm time.Time) error {
	data, err := l.db.Get([]byte(key), nil)
	if err != nil {
		return err
	}

	var cache CacheType
	decoder := gob.NewDecoder(bytes.NewReader(data))
	if err := decoder.Decode(&cache); err != nil {
		return err
	}

	cache.Expire = tm.Unix()

	var buf bytes.Buffer
	encoder := gob.NewEncoder(&buf)
	if err := encoder.Encode(cache); err != nil {
		return err
	}

	return l.db.Put([]byte(key), buf.Bytes(), nil)
}

// XIncr 将key中存储的数字值加1
// 示例：
//   value, err := db.XIncr("counter")
//   if err != nil {
//       log.Fatal(err)
//   }
//   fmt.Printf("新值: %d\n", value)
func (l *LevelDB) XIncr(key string) (int64, error) {
	return l.XIncrBy(key, 1)
}

// XIncrBy 将key中存储的数字值增加指定的值
// 示例：
//   value, err := db.XIncrBy("counter", 10)
//   if err != nil {
//       log.Fatal(err)
//   }
//   fmt.Printf("新值: %d\n", value)
func (l *LevelDB) XIncrBy(key string, increment int64) (int64, error) {
	data, err := l.db.Get([]byte(key), nil)
	var cache CacheType
	var value int64

	if err == nil {
		// key存在，解析当前值
		decoder := gob.NewDecoder(bytes.NewReader(data))
		if err := decoder.Decode(&cache); err != nil {
			return 0, err
		}
		value, err = strconv.ParseInt(string(cache.Data), 10, 64)
		if err != nil {
			return 0, err
		}
	} else {
		// key不存在，初始化为0
		cache = CacheType{
			Created: time.Now().Unix(),
			Expire:  0,
		}
		value = 0
	}

	// 增加值
	value += increment
	cache.Data = []byte(strconv.FormatInt(value, 10))

	// 保存新值
	var buf bytes.Buffer
	encoder := gob.NewEncoder(&buf)
	if err := encoder.Encode(cache); err != nil {
		return 0, err
	}

	if err := l.db.Put([]byte(key), buf.Bytes(), nil); err != nil {
		return 0, err
	}

	return value, nil
}

// XDecr 将key中存储的数字值减1
// 示例：
//   value, err := db.XDecr("counter")
//   if err != nil {
//       log.Fatal(err)
//   }
//   fmt.Printf("新值: %d\n", value)
func (l *LevelDB) XDecr(key string) (int64, error) {
	return l.XDecrBy(key, 1)
}

// XDecrBy 将key中存储的数字值减少指定的值
// 示例：
//   value, err := db.XDecrBy("counter", 10)
//   if err != nil {
//       log.Fatal(err)
//   }
//   fmt.Printf("新值: %d\n", value)
func (l *LevelDB) XDecrBy(key string, decrement int64) (int64, error) {
	return l.XIncrBy(key, -decrement)
}
