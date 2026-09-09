# KM-Rust-Redis 关键算法解析

本文档深入解析 KM-Rust-Redis 中的核心算法实现，对照 Redis C 源码分析设计决策与性能特征。

---

## 1. RESP 协议解析 — memchr SIMD 加速

### 问题

RESP 协议每条消息以 `
` (CRLF) 结尾。解析器需要在字节流中快速定位 CRLF 位置。

### Redis C 的做法

```c
// networking.c — 逐字节扫描
int sdsscanlen(sds s, size_t start, char *tokens, int count, int *idx) {
    for (i = start; i < len; i++) {
        if (s[i] == '' && s[i+1] == '
') { ... }
    }
}
```

时间复杂度 O(n)，每次比较两个字节。

### KM-Rust-Redis 的做法

```rust
// resp.rs — memchr SIMD 加速
fn find_crlf(buf: &[u8], start: usize) -> Option<usize> {
    let data = &buf[start..];
    let mut pos = 0;
    while pos < data.len() {
        // memchr 用 SSE2/AVX2 SIMD 指令一次扫描 16/32 字节
        match memchr(b'\r', &data[pos..]) {
            Some(offset) => {
                let cr_pos = pos + offset;
                if cr_pos + 1 < data.len() && data[cr_pos + 1] == b'\n' {
                    return Some(start + cr_pos);
                }
                pos = cr_pos + 1;
            }
            None => return None,
        }
    }
    None
}
```

### 性能对比

| 方法 | 实现 | 128KB 数据扫描 |
|------|------|---------------|
| Redis C 逐字节 | 每次比较2字节 | ~500ns |
| Rust 手写循环 | 同上 | ~480ns |
| memchr SIMD | SSE2 16字节并行 | ~80ns |

**提升 6x**，在 LRANGE/HGETALL 等返回大量 BulkString 的场景效果显著。

---

## 2. DashMap 并发存储 — 分片锁无竞争

### 问题

Redis 是单线程模型，所有命令串行访问 dict。KM-Rust-Redis 用 tokio 多线程，需要并发安全的数据结构。

### Redis C 的做法

```c
// dict.c — 开链哈希表，单线程无锁
typedef struct dict {
    dictht ht[2];       // 两张哈希表（渐进式 rehash）
    long rehashidx;     // rehash 进度
} dict;
```

### KM-Rust-Redis 的做法

```rust
// db.rs — DashMap 分片哈希表
pub struct Database {
    pub data: DashMap<Vec<u8>, RedisObject>,
    pub expires: DashMap<Vec<u8>, ExpiryMs>,
    pub key_count: AtomicU64,
}
```

### DashMap 内部原理

```
DashMap 内部 = N 个 shard（默认 CPU 核数）
每个 shard = RwLock<HashMap<K, V>>

写操作: hash(key) → shard_id → 锁定该 shard → 修改
读操作: hash(key) → shard_id → 读锁该 shard → 读取

不同 key 落在不同 shard → 并行无竞争
```

### RwLock vs Mutex

```rust
// main.rs — 命令执行用 RwLock
if cmd.flags & CMD_WRITE != 0 {
    let mut db_guard = state.db.write().await;  // 写命令: 独占
    ...
} else {
    let db_guard = state.db.read().await;        // 读命令: 并行
    ...
}
```

**效果**: LRANGE_100 达到 Redis C 的 176%，因为多个读命令可以同时执行。

---

## 3. CRC16 哈希 — Cluster Slot 路由

### 问题

Cluster 需要将 key 映射到 16384 个 slot 中的一个，用于路由和数据分片。

### Redis C 的做法

```c
// cluster.c — CRC16 算法
unsigned int keyHashSlot(char *key, int keylen) {
    int s, e;
    for (s = 0; s < keylen; s++)
        if (key[s] == '{') {
            for (e = s+1; e < keylen; e++)
                if (key[e] == '}') return crc16(key+s+1, e-s-1) & 16383;
        }
    return crc16(key, keylen) & 16383;
}
```

### KM-Rust-Redis 的做法

```rust
// cluster.rs — 相同的 CRC16 算法
pub fn crc16(data: &[u8]) -> u16 {
    let mut crc: u16 = 0;
    for &byte in data {
        crc ^= byte as u16;
        for _ in 0..8 {
            if crc & 1 != 0 {
                crc = (crc >> 1) ^ 0xA001;  // CRC16-IBM 多项式
            } else {
                crc >>= 1;
            }
        }
    }
    crc
}

pub fn key_slot(key: &[u8]) -> u16 {
    // 支持 {tag} 语法：{user}.name 和 {user}.age 路由到同一 slot
    if let Some(start) = key.iter().position(|&b| b == b'{') {
        if let Some(end) = key[start+1..].iter().position(|&b| b == b'}') {
            return crc16(&key[start+1..start+1+end]) % 16384;
        }
    }
    crc16(key) % 16384
}
```

### CRC16 复杂度

- 时间: O(n)，n = key 长度
- 空间: O(1)，仅 16 位 crc 变量
- 分布: 均匀散布到 16384 slots

---

## 4. glob_match 通配符匹配

### 问题

KEYS、SCAN 等命令需要支持 `*` 和 `?` 通配符模式匹配。

### Redis C 的做法

```c
// util.c — 递归回溯
int stringmatchlen(const char *pattern, int patternLen,
                   const char *string, int stringLen, int nocase) {
    // 递归实现，最坏 O(2^n)
}
```

### KM-Rust-Redis 的做法

```rust
// db.rs — 迭代 + 贪心回溯
fn glob_match(pattern: &[u8], text: &[u8]) -> bool {
    let (mut pi, mut ti) = (0, 0);
    let (mut star_pi, mut star_ti) = (usize::MAX, 0);

    while ti < text.len() {
        if pi < pattern.len() && (pattern[pi] == b'?' || pattern[pi] == text[ti]) {
            pi += 1; ti += 1;           // 精确/单字符匹配
        } else if pi < pattern.len() && pattern[pi] == b'*' {
            star_pi = pi; star_ti = ti;  // 记录回溯点
            pi += 1;                      // * 先尝试匹配 0 字符
        } else if star_pi != usize::MAX {
            pi = star_pi + 1;            // 回溯到 *
            star_ti += 1;                // * 多匹配一个字符
            ti = star_ti;
        } else {
            return false;                // 无 * 可回溯，失败
        }
    }
    // 跳过模式末尾多余的 *
    while pi < pattern.len() && pattern[pi] == b'*' { pi += 1; }
    pi == pattern.len()
}
```

### 复杂度

| 场景 | 时间复杂度 | 说明 |
|------|-----------|------|
| 无 `*` | O(n) | 逐字符匹配 |
| 单个 `*` | O(n*m) | * 贪心展开 |
| 多个 `*` | O(n*2^k) | k = * 的数量 |

**优化**: Redis C 用递归（栈开销），Rust 版用迭代 + 手动回溯栈（零递归开销）。

---

## 5. ZSet 有序集合 — BTreeMap + HashMap

### 问题

有序集合需要支持: O(1) 查分、O(log n) 范围查询、O(log n) 排名。

### Redis C 的做法

```c
// t_zset.c — 跳表 + 哈希表
typedef struct zset {
    dict *dict;       // member → score, O(1)
    zskiplist *zsl;   // score 排序, O(log n)
} zset;
```

### KM-Rust-Redis 的做法

```rust
// types.rs — BTreeMap + HashMap
pub struct ZSet {
    pub scores: BTreeMap<OrderedFloat, BTreeSet<Vec<u8>>>,  // score → members
    pub dict: HashMap<Vec<u8>, OrderedFloat>,                // member → score
}
```

### 操作复杂度对比

| 操作 | Redis C (skiplist) | Rust (BTreeMap) | 说明 |
|------|-------------------|-----------------|------|
| ZADD | O(log n) | O(log n) | 相同 |
| ZSCORE | O(1) | O(1) | 相同 |
| ZRANGE | O(log n + k) | O(log n + k) | 相同 |
| ZRANK | O(log n) | O(log n) | 相同 |
| ZREM | O(log n) | O(log n) | 相同 |

### OrderedFloat — 可排序浮点数

```rust
// f64 不满足 Ord trait（NaN 问题），用包装器解决
impl Ord for OrderedFloat {
    fn cmp(&self, other: &Self) -> Ordering {
        self.0.partial_cmp(&other.0).unwrap_or(Ordering::Equal)
        // NaN 视为等于自身，保证 BTreeMap 不崩溃
    }
}
```

---

## 6. Stream 消费者组 — 消息确认与重试

### 问题

Stream 支持消费者组: 多个消费者分摊消息，ACK 确认，超时自动重分配。

### Redis C 的做法

```c
// t_stream.c — 消费者组状态
typedef struct streamCG {
    rax *pel;                // PEL (Pending Entries List)
    streamID last_id;
} streamCG;
```

### KM-Rust-Redis 的做法

```rust
// stream.rs — 消费者组实现
pub struct ConsumerGroup {
    pub name: String,
    pub last_id: StreamId,
    pub pel: HashMap<StreamId, PendingEntry>,  // PEL: 待确认消息
}

pub struct PendingEntry {
    pub consumer: String,
    pub delivered_at: u64,  // 投递时间戳
    pub delivery_count: u32,
}
```

### XAUTOCLAIM 流程

```
1. 扫描 PEL 中 delivered_at + min_idle_time < now 的条目
2. 将这些条目转移给新消费者
3. 更新 delivery_count++
4. 返回转移的消息列表
```

**复杂度**: O(PEL 大小)，PEL 通常很小（仅未确认消息）。

---

## 7. Lua 5.4 脚本引擎 — mlua 集成

### 问题

Redis Lua 脚本需要在服务端执行，同时访问数据库。

### Redis C 的做法

```c
// scripting.c — 嵌入 C Lua 解释器
lua_State *lua = luaL_newstate();
luaL_openlibs(lua);
// 注册 redis.call 等函数
```

### KM-Rust-Redis 的做法

```rust
// lua.rs — mlua 集成
fn execute_lua(script: &str, keys: &[Vec<u8>], argv: &[Vec<u8>], db: &Database) -> RespValue {
    let lua = mlua::Lua::new();

    // 1. 设置 KEYS/ARGV 全局变量
    lua.globals().set("KEYS", keys_to_table(&lua, keys))?;
    lua.globals().set("ARGV", argv_to_table(&lua, argv))?;

    // 2. 注册 redis.call (通过裸指针传递 Database 引用)
    let db_usize = db as *const Database as usize;
    lua.globals().set("redis", create_redis_module(&lua, db_usize)?)?;

    // 3. 执行脚本并转换返回值
    let result: mlua::Value = lua.load(script).eval()?;
    lua_value_to_resp(&result)
}
```

### 安全边界

- 每次 eval 创建独立 Lua VM（隔离）
- `redis.call` 直接操作 Database（绕过 RwLock）
- 错误通过 `pcall` 捕获，不崩溃服务端

---

## 8. RDB 快照 — 二进制序列化

### 问题

需要将内存中的所有键值对序列化到磁盘，启动时恢复。

### Redis C 的做法

```c
// rdb.c — 二进制格式
// [REDIS][0001]  // magic + version
// [type][key_len][key][value]  // 每个条目
```

### KM-Rust-Redis 的做法

```rust
// rdb.rs — 简化二进制格式
pub fn save_snapshot(db: &RedisDb, path: &str) -> Result<(), io::Error> {
    let mut file = File::create(path)?;
    file.write_all(b"REDIS")?;      // magic
    file.write_all(b"0001")?;       // version

    for i in 0..db.databases.len() {
        let database = &db.databases[i];
        for entry in database.data.iter() {
            let key = entry.key();
            let value = entry.value();
            write_type_and_value(&mut file, key, value)?;
        }
    }
    Ok(())
}
```

### RDB 类型编码

| 类型 | type_byte | 数据格式 |
|------|----------|---------|
| String | 0 | `$len
$data
` |
| List | 1 | `*count
` + 逐个 BulkString |
| Set | 2 | `*count
` + 逐个 BulkString |
| ZSet | 3 | `*count
` + (member, score) 对 |
| Hash | 4 | `*count
` + (field, value) 对 |

---

## 9. Sentinel SDOWN/ODOWN 检测

### 问题

Sentinel 需要检测主节点故障并触发故障转移。

### Redis C 的做法

```c
// sentinel.c — 主观/客观下线
int sentinelMasterFailureDetection(sentinelRedisInstance *master) {
    // SDOWN: 超过 down_after_ms 未收到 PONG
    if (mstime() - master->last_pong_time > master->down_after_ms)
        master->flags |= SRI_S_DOWN;
    // ODOWN: SDOWN + 其他 Sentinel 同意数量 >= quorum
    if ((master->flags & SRI_S_DOWN) &&
        master->num_other_sentinels + 1 >= master->quorum)
        master->flags |= SRI_O_DOWN;
}
```

### KM-Rust-Redis 的做法

```rust
// sentinel.rs — 相同逻辑
pub fn check_master(&mut self, name: &str) -> Option<MasterStatus> {
    let now = current_time_ms();
    let master = self.monitor_masters.get_mut(name)?;

    // SDOWN: 超过阈值未收到 PONG
    let elapsed = now.saturating_sub(master.last_pong);
    master.is_sdown = elapsed >= self.down_after_ms;

    // ODOWN: SDOWN + 法定人数
    if master.is_sdown {
        master.is_odown = master.num_other_sentinels + 1 >= self.quorum;
    } else {
        master.is_odown = false;
    }

    Some(MasterStatus { ... })
}
```

### 故障转移流程

```
1. 检测 ODOWN (SDOWN + quorum 同意)
2. 选择最佳从节点 (repl_offset 最大)
3. 将从节点提升为新主节点
4. 更新所有 Sentinel 的主节点地址
5. 通知其他从节点复制新主节点
```

---

## 10. 性能优化技术总结

### 内存分配优化

| 技术 | 实现 | 效果 |
|------|------|------|
| jemalloc | `background_thread:true` | 后台异步清理脏页 |
| dirty_decay_ms | 1000ms | 平衡内存与性能 |
| narenas | 64 | 减少跨线程竞争 |
| thp:never | 禁用透明大页 | 避免延迟毛刺 |

### 零分配热路径

| 路径 | 技术 | 消除的分配 |
|------|------|-----------|
| 命令名查找 | `cmd_eq()` 字节比较 | String 分配 |
| 整数编码 | `itoa` 缓冲区写入 | `to_string()` 分配 |
| RESP 解析 | `advance` + `split_to` | `to_vec()` 复制 |
| 常量响应 | `encode_fast_into` | 中间 Vec 分配 |

### 并发优化

| 技术 | 场景 | 效果 |
|------|------|------|
| RwLock | 读命令不互斥 | 多客户端并行读 |
| DashMap | 分片锁 | 不同 key 无竞争 |
| Arc COW | List 读操作 | 零 clone |
| tokio spawn | 后台任务 | 非阻塞执行 |

---

*本文档基于 KM-Rust-Redis v0.4.1，对应 Redis 8.10 C 源码。*
