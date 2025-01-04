# rose-leveldb

基于 [syndtr/goleveldb](https://github.com/syndtr/goleveldb) 的高级封装，提供类 Redis 的接口和功能。

## 特性

- 支持数据过期时间
- 支持计数器操作
- 支持布隆过滤器
- 简单易用的 API

## 安装

```bash
go get github.com/leafney/rose-leveldb
```

## 快速开始

```go
db, err := rleveldb.NewLevelDB("./data")
if err != nil {
    log.Fatal(err)
}
defer db.Close()

// 设置数据
db.SetS("key", "value")
// 设置带过期时间的数据
db.XSetExS("key", "value", time.Hour)
// 计数器操作
count,_ := db.XIncr("counter")
```

## API 文档

### 基础操作

- `NewLevelDB(dbPath string) (*LevelDB, error)` - 创建数据库实例
- `NewBloomFilter(dbPath string, bits int) (*LevelDB, error)` - 创建带布隆过滤器的数据库实例
- `Close() error` - 关闭数据库连接
- `Get(key string) ([]byte, error)` - 获取数据
- `GetS(key string) (string, error)` - 获取字符串数据
- `Set(key string, value []byte) error` - 设置数据
- `SetS(key string, value string) error` - 设置字符串数据
- `Exists(key string) bool` - 检查 key 是否存在
- `Del(key string) error` - 删除数据

### 缓存操作

- `XGet(key string) ([]byte, error)` - 获取带过期时间的数据
- `XGetS(key string) (string, error)` - 获取带过期时间的字符串数据
- `XSet(key string, value []byte) error` - 设置数据
- `XSetS(key string, value string) error` - 设置字符串数据
- `XSetEx(key string, value []byte, expires time.Duration) error` - 设置带过期时间的数据
- `XSetExS(key string, value string, expires time.Duration) error` - 设置带过期时间的字符串数据
- `XSetExSec(key string, value []byte,seconds int64) error` - 设置带过期时间的缓存数据（使用秒数）
- `XSetExSecS(key string,value string,seconds int64) error` - 设置带过期时间的字符串数据（使用秒数）
- `XTTL(key string) (int64, error)` - 获取剩余生存时间
- `XExpire(key string, expires time.Duration) error` - 设置过期时间
- `XExpireSec(key string, seconds int64) error` - 设置过期时间(秒)
- `XExpireAt(key string, tm time.Time) error` - 设置过期时间点

### 计数器操作

- `XIncr(key string) (int64, error)` - 将 key 中存储的数字值加 1
- `XIncrBy(key string, increment int64) (int64, error)` - 将 key 中存储的数字值增加指定的值
- `XDecr(key string) (int64, error)` - 将 key 中存储的数字值减 1
- `XDecrBy(key string, decrement int64) (int64, error)` - 将 key 中存储的数字值减少指定的值
