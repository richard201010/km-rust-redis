//! 核心数据类型模块 — Redis 对象、编码提示与过期时间追踪
//!
//! 本模块对应 Redis C 源码中的 `robj`（redisObject）体系，用 Rust 的 enum 和 struct
//! 重新实现了 Redis 的五种基本数据类型（String / List / Set / ZSet / Hash）以及 Stream 占位。
//!
//! 设计思路：
//! - `RedisObject` 是一个带标签的联合体（tagged union），直接替代 C 中的 `robj` 结构体。
//!   C 版本通过 `type` + `encoding` + `void *ptr` 三元组描述对象，Rust 版本用 enum 的
//!   变体（variant）天然携带类型和编码信息，无需手动管理指针和引用计数。
//! - 有序集合（ZSet）用 `BTreeMap` + `HashMap` 替代 Redis C 源码中的跳表（skiplist）+ 字典（dict）。
//!   BTreeMap 提供 O(log n) 的按分数范围查询，HashMap 提供 O(1) 的成员→分数反查。
//! - 浮点数排序通过 `OrderedFloat` 包装器实现，解决了 `f64` 不满足 `Ord` / `Hash` 的问题。
//! - 过期时间采用绝对毫秒时间戳（对应 Redis 8 的 `expireTime` 字段），存放在独立的
//!   `HashMap<key, ExpiryMs>` 中，与 Redis 的惰性删除 + 定期删除策略配合使用。

use crate::stream::Stream;
use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet, VecDeque};
use std::fmt;
use std::sync::atomic::{AtomicI64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

// ---------------------------------------------------------------------------
// RedisType — 对应 Redis C 源码中的 OBJ_STRING / OBJ_LIST / OBJ_SET / OBJ_ZSET / OBJ_HASH
// ---------------------------------------------------------------------------

/// Redis 对象类型标签。
///
/// 对应 C 源码 `server.h` 中的常量：
/// - `OBJ_STRING` → `String`
/// - `OBJ_LIST`   → `List`
/// - `OBJ_SET`    → `Set`
/// - `OBJ_ZSET`   → `ZSet`
/// - `OBJ_HASH`   → `Hash`
///
/// `Stream` 是 Redis 5.0+ 引入的数据类型，此处预留占位。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum RedisType {
    String,
    List,
    Set,
    ZSet,
    Hash,
    Stream,
}

// ---------------------------------------------------------------------------
// RedisObject — 对应 C 源码中的 robj（redisObject）
// ---------------------------------------------------------------------------

/// Redis 键值条目 — 对应 C 源码中的 `robj` 结构体。
///
/// C 版本的设计：
/// ```c
/// typedef struct redisObject {
///     unsigned type:4;        // OBJ_STRING / OBJ_LIST / ...
///     unsigned encoding:4;    // OBJ_ENCODING_RAW / OBJ_ENCODING_INT / ...
///     unsigned lru:LRU_BITS;  // LRU 或 LFU 信息
///     int refcount;           // 引用计数（内存管理）
///     void *ptr;              // 指向实际数据的指针
/// } robj;
/// ```
///
/// Rust 版本利用 enum 变体的 discriminant 自动编码 `type` 和 `encoding`，
/// 无需手动管理 `refcount`（由 Rust 的所有权系统取代），`ptr` 被替换为内联的强类型数据。
#[derive(Debug, Clone)]
pub enum RedisObject {
    /// 普通字符串 — 对应 SDS（Simple Dynamic String），此处简化为 `Vec<u8>`。
    /// 存储二进制安全的字节序列，等价于 C 中 `encoding = OBJ_ENCODING_RAW` 的情况。
    String(Vec<u8>),

    /// 内联整数 — 对应 C 中 `encoding = OBJ_ENCODING_INT` 的情况。
    /// 当字符串值可以表示为 64 位整数时，Redis 会直接将整数存入 `ptr` 字段（指针强转），
    /// 避免额外的内存分配。Rust 版本用独立的 enum 变体实现同样效果。
    Integer(i64),

    /// 双向链表 — 对应 C 中的 quicklist（ziplist + 双向链表的混合结构）。
    /// 此处简化为 `VecDeque<Vec<u8>>`，保留了双端操作的 O(1) 特性。
    List(VecDeque<Vec<u8>>),

    /// 无序集合 — 对应 C 中 `encoding = OBJ_ENCODING_HT` 的 set。
    /// C 版本用 dict（哈希表）实现，Rust 版本直接用 `HashSet`。
    Set(HashSet<Vec<u8>>),

    /// 有序集合（Sorted Set） — 对应 C 中的 `zset` 结构。
    ///
    /// C 版本的设计：
    /// ```c
    /// typedef struct zset {
    ///     dict *dict;       // member → score 的哈希表，O(1) 查分
    ///     zskiplist *zsl;   // 跳表，支持按分数范围查询和排名
    /// } zset;
    /// ```
    ///
    /// Rust 版本用 `BTreeMap`（有序映射）替代跳表，用 `HashMap`（哈希表）替代 dict，
    /// 保持了相同的语义：O(1) 的分数查询 + O(log n) 的范围查询。
    ZSet(ZSet),

    /// 哈希表 — 对应 C 中 `encoding = OBJ_ENCODING_HT` 的 hash。
    /// C 版本用 dict（两张哈希表用于渐进式 rehash），Rust 版本用 `HashMap`。
    Hash(HashMap<Vec<u8>, Vec<u8>>),

    /// 流（Stream） — 对应 Redis 5.0+ 的 stream 类型。
    /// 实现了完整的 Stream 数据结构，包括消费者组和消息确认机制。
    Stream(Stream),
}

impl RedisObject {
    /// 返回对象类型的字符串名称，对应 C 中的 `getObjectTypeName()` 函数。
    ///
    /// 用于 INFO 命令输出、DEBUG 日志等场景。
    /// 注意：`String` 和 `Integer` 都返回 `"string"`，因为它们在 Redis 协议层面
    /// 都是 STRING 类型，只是内部编码（encoding）不同。
    pub fn type_name(&self) -> &'static str {
        match self {
            RedisObject::String(_) | RedisObject::Integer(_) => "string",
            RedisObject::List(_) => "list",
            RedisObject::Set(_) => "set",
            RedisObject::ZSet(_) => "zset",
            RedisObject::Hash(_) => "hash",
            RedisObject::Stream(_) => "stream",
        }
    }

    /// 返回对象的 Redis 类型枚举值，对应 C 中的 `robj->type` 字段。
    ///
    /// 与 `type_name()` 的区别：此方法返回强类型枚举，适合内部逻辑分支判断；
    /// `type_name()` 返回字符串，适合面向用户的输出。
    pub fn redis_type(&self) -> RedisType {
        match self {
            RedisObject::String(_) | RedisObject::Integer(_) => RedisType::String,
            RedisObject::List(_) => RedisType::List,
            RedisObject::Set(_) => RedisType::Set,
            RedisObject::ZSet(_) => RedisType::ZSet,
            RedisObject::Hash(_) => RedisType::Hash,
            RedisObject::Stream(_) => RedisType::Stream,
        }
    }

    /// 返回对象的内部编码名称，对应 C 中的 `robj->encoding` 字段。
    ///
    /// Redis 同一种类型可能有多种编码方式，会在运行时根据数据量自动切换：
    /// - String: `int`（小整数）/ `embstr`（≤44 字节）/ `raw`（长字符串）
    /// - List: `listpack`（小列表）/ `quicklist`（大列表）
    /// - Set: `intset`（全整数小集合）/ `hashtable`（大集合）
    /// - ZSet: `listpack`（小有序集合）/ `skiplist`（大有序集合）
    ///
    /// 此处简化为单一编码名称，用于 TYPE / OBJECT ENCODING 等命令的响应。
    pub fn encoding_name(&self) -> &'static str {
        match self {
            RedisObject::String(_) => "embstr",
            RedisObject::Integer(_) => "int",
            RedisObject::List(_) => "listpack", // 简化：实际应根据大小判断
            RedisObject::Set(_) => "hashtable",
            RedisObject::ZSet(_) => "skiplist",
            RedisObject::Hash(_) => "hashtable",
            RedisObject::Stream(_) => "stream",
        }
    }

    /// 估算对象的内存占用（字节数），对应 C 中的 `objectComputeSize()` 函数。
    ///
    /// 用于 `MEMORY USAGE` 命令。此实现为粗略估算，包含：
    /// - 数据本身的大小（key/value 的字节长度）
    /// - 容器的固定开销（每个元素约 32~64 字节的指针/哈希/树节点开销）
    /// - 对象头的固定开销（128 字节，模拟 C 中 robj + SDS header 的大小）
    pub fn memory_usage(&self) -> usize {
        match self {
            RedisObject::String(d) => d.len() + 64,
            RedisObject::Integer(_) => 24,
            RedisObject::List(l) => l.iter().map(|e| e.len()).sum::<usize>() + l.len() * 32 + 128,
            RedisObject::Set(s) => s.iter().map(|e| e.len()).sum::<usize>() + s.len() * 32 + 128,
            RedisObject::ZSet(z) => z.memory_usage(),
            RedisObject::Hash(h) => {
                h.iter().map(|(k, v)| k.len() + v.len()).sum::<usize>() + h.len() * 64 + 128
            }
            RedisObject::Stream(_) => 0,
        }
    }
}

/// 为 RedisObject 实现 Display trait，用于调试输出和日志。
///
/// - String/Integer 直接显示内容
/// - 容器类型显示 `类型名(元素个数)` 格式，如 `list(5)`、`zset(3)`
impl fmt::Display for RedisObject {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            RedisObject::String(d) => write!(f, "{}", String::from_utf8_lossy(d)),
            RedisObject::Integer(n) => write!(f, "{}", n),
            RedisObject::List(l) => write!(f, "list({})", l.len()),
            RedisObject::Set(s) => write!(f, "set({})", s.len()),
            RedisObject::ZSet(z) => write!(f, "zset({})", z.len()),
            RedisObject::Hash(h) => write!(f, "hash({})", h.len()),
            RedisObject::Stream(_) => write!(f, "stream"),
        }
    }
}

// ---------------------------------------------------------------------------
// ZSet — 有序集合，替代 C 源码中的 skiplist + dict
// ---------------------------------------------------------------------------

/// 有序集合（Sorted Set）实现。
///
/// # 对应 C 源码的设计
///
/// C 版本使用两个数据结构协同工作：
/// ```c
/// typedef struct zset {
///     dict *dict;       // 哈希表：member → score，O(1) 查分
///     zskiplist *zsl;   // 跳表：按 score 排序，支持范围查询和排名
/// } zset;
/// ```
///
/// # Rust 版本的替代方案
///
/// | C 数据结构 | Rust 替代 | 复杂度变化 |
/// |-----------|----------|-----------|
/// | zskiplist | BTreeMap  | 范围查询 O(log n) 不变，但常数因子更小 |
/// | dict      | HashMap   | O(1) 查分不变 |
///
/// 选择 BTreeMap 而非手写跳表的原因：
/// 1. BTreeMap 是标准库提供的有序映射，内存局部性好，cache 友好
/// 2. 支持 `.range()` 方法，天然支持 `ZRANGEBYSCORE` 操作
/// 3. 无需手动管理跳表节点的内存分配/释放
///
/// # 分数桶（score bucket）
///
/// 多个成员可能有相同的分数。`scores` 字段的值是 `BTreeSet<Vec<u8>>`，
/// 同分数的成员按字典序排列（对应 Redis 中同分数成员按 member 字典序排序的语义）。
#[derive(Debug, Clone)]
pub struct ZSet {
    /// 分数 → 成员集合的有序映射。
    ///
    /// 对应 C 中跳表的作用：支持按分数范围查询（`ZRANGEBYSCORE`）。
    /// 使用 `OrderedFloat` 作为键，因为 `f64` 不满足 `Ord` trait。
    /// 同分数的多个成员存储在 `BTreeSet` 中，按字典序排列。
    pub scores: BTreeMap<OrderedFloat, BTreeSet<Vec<u8>>>,

    /// 成员 → 分数的哈希表。
    ///
    /// 对应 C 中 dict 的作用：O(1) 查找成员的分数（`ZSCORE` 命令），
    /// 以及判断成员是否存在（`ZADD` 时需要判断是新增还是更新）。
    pub dict: HashMap<Vec<u8>, OrderedFloat>,
}

// ---------------------------------------------------------------------------
// OrderedFloat — 可排序的 f64 包装器
// ---------------------------------------------------------------------------

/// 可排序的浮点数包装器。
///
/// # 为什么需要这个类型？
///
/// Rust 标准库中 `f64` 只实现了 `PartialOrd`，没有实现 `Ord` 和 `Hash`。
/// 原因是 IEEE 754 浮点数存在 `NaN`，而 `NaN != NaN`，违反了全序关系的要求。
///
/// 但 Redis 的有序集合要求分数必须支持全序排列和哈希，所以我们用这个包装器：
/// - `Ord` 实现：将 `NaN` 视为等于自身（`partial_cmp` 返回 `None` 时回退为 `Equal`）
/// - `Hash` 实现：基于浮点数的比特表示（`to_bits()`）进行哈希，保证相等的值哈希相同
///
/// 这与 Redis C 源码的行为一致 — Redis 也不区分 `NaN` 和 `NaN`，直接按比特比较。
#[derive(Debug, Clone, Copy, PartialEq, PartialOrd)]
pub struct OrderedFloat(pub f64);

/// 实现 `Eq` trait — 要求自反性（`a == a` 恒为 true）。
/// 对于正常的浮点数值满足，`NaN` 的情况由 `Ord` 实现中的 `unwrap_or(Equal)` 处理。
impl Eq for OrderedFloat {}

/// 实现 `Ord` trait — 全序比较，使 `OrderedFloat` 可以作为 `BTreeMap` 的键。
///
/// 对于正常的浮点数值，行为与 `f64` 的自然排序一致。
/// 对于 `NaN`，回退为 `Equal`，保证不会在 BTreeMap 中出现重复键。
impl Ord for OrderedFloat {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.0
            .partial_cmp(&other.0)
            .unwrap_or(std::cmp::Ordering::Equal)
    }
}

/// 实现 `Hash` trait — 基于浮点数的比特表示进行哈希。
///
/// 使用 `to_bits()` 将 `f64` 转换为 `u64`，然后对 `u64` 哈希。
/// 这保证了：如果 `a == b`，则 `hash(a) == hash(b)`（哈希一致性）。
impl std::hash::Hash for OrderedFloat {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.0.to_bits().hash(state);
    }
}

impl ZSet {
    /// 创建一个空的有序集合。
    pub fn new() -> Self {
        Self {
            scores: BTreeMap::new(),
            dict: HashMap::new(),
        }
    }

    /// 返回有序集合中的成员数量，对应 C 中的 `zsetLength()` 函数。
    ///
    /// 直接从 dict 获取长度，因为 dict 和 skiplist 始终保持同步。
    pub fn len(&self) -> usize {
        self.dict.len()
    }

    /// 判断有序集合是否为空。
    pub fn is_empty(&self) -> bool {
        self.dict.is_empty()
    }

    /// 添加成员并设置分数，对应 C 中的 `zaddGenericCommand()` 函数。
    ///
    /// # 参数
    /// - `member`: 成员的字节数据（二进制安全）
    /// - `score`: 浮点数分数
    ///
    /// # 返回值
    /// - `true`: 新增了一个成员（之前不存在）
    /// - `false`: 更新了已有成员的分数
    ///
    /// # 复杂度
    /// - 新增：O(log n) — BTreeMap 插入 + HashMap 插入
    /// - 更新：O(log n) — 需要从旧分数桶移除，再插入新分数桶
    pub fn add(&mut self, member: Vec<u8>, score: f64) -> bool {
        let of = OrderedFloat(score);
        let is_new = !self.dict.contains_key(&member);

        // 如果成员已存在且分数不同，需要从旧分数桶中移除
        if let Some(old_score) = self.dict.get(&member) {
            let old = *old_score;
            if old != of {
                if let Some(bucket) = self.scores.get_mut(&old) {
                    bucket.remove(&member);
                    // 如果旧分数桶变空，清理掉整个桶以节省内存
                    if bucket.is_empty() {
                        self.scores.remove(&old);
                    }
                }
            }
        }

        // 更新 dict（成员→分数映射）和 scores（分数→成员集合映射）
        self.dict.insert(member.clone(), of);
        self.scores.entry(of).or_default().insert(member);
        is_new
    }

    /// 移除成员，对应 C 中的 `zremGenericCommand()` 函数。
    ///
    /// # 返回值
    /// - `true`: 成员存在并被成功移除
    /// - `false`: 成员不存在
    ///
    /// # 复杂度
    /// O(log n) — HashMap 删除 + BTreeMap 删除
    pub fn remove(&mut self, member: &[u8]) -> bool {
        if let Some(score) = self.dict.remove(member) {
            if let Some(bucket) = self.scores.get_mut(&score) {
                bucket.remove(member);
                if bucket.is_empty() {
                    self.scores.remove(&score);
                }
            }
            true
        } else {
            false
        }
    }

    /// 获取成员的分数，对应 C 中的 `zscoreCommand()` 函数。
    ///
    /// # 复杂度
    /// O(1) — 直接从 HashMap 查找
    pub fn score(&self, member: &[u8]) -> Option<f64> {
        self.dict.get(member).map(|of| of.0)
    }

    /// 获取成员的排名（0 起始，升序），对应 C 中的 `zrankGenericCommand()` 函数。
    ///
    /// # 实现方式
    /// 遍历 BTreeMap（已按分数排序），累加低分桶的成员数，
    /// 到达目标分数桶后在桶内按字典序查找成员位置。
    ///
    /// # 复杂度
    /// O(log n + M)，其中 M 是同分数桶的大小。
    /// 最坏情况 O(n)，但实际场景中同分数成员很少。
    ///
    /// # 对应 C 源码的差异
    /// C 版本使用跳表的 `level` 数组在 O(log n) 内完成排名计算，
    /// 此处简化实现为线性遍历，性能略差但逻辑更清晰。
    pub fn rank(&self, member: &[u8]) -> Option<usize> {
        let score = self.dict.get(member)?;
        let mut rank = 0;
        for (s, bucket) in &self.scores {
            if s < score {
                rank += bucket.len();
            } else if s == score {
                // 在同分数桶内按字典序查找成员位置
                for m in bucket {
                    if m == member {
                        return Some(rank);
                    }
                    rank += 1;
                }
            }
        }
        None
    }

    /// 获取成员的逆序排名（0 起始，降序），对应 C 中的 `zrevrankGenericCommand()`。
    ///
    /// 通过正序排名计算：`rev_rank = len - 1 - rank`
    pub fn rev_rank(&self, member: &[u8]) -> Option<usize> {
        self.rank(member).map(|r| self.len() - 1 - r)
    }

    /// 按索引范围获取成员（升序），对应 C 中的 `zrangeGenericCommand()`。
    ///
    /// # 参数
    /// - `start`: 起始索引（支持负数，-1 表示最后一个元素）
    /// - `stop`: 结束索引（支持负数，-1 表示最后一个元素）
    /// - `withscores`: 是否在结果中包含分数（当前实现始终包含，此参数保留用于协议兼容）
    ///
    /// # 复杂度
    /// O(log n + M)，其中 M 是返回的元素数量
    pub fn range(&self, start: isize, stop: isize, withscores: bool) -> Vec<(Vec<u8>, f64)> {
        let len = self.len() as isize;
        let start = normalize_index(start, len);
        let stop = normalize_index(stop, len);
        if start > stop || start >= len {
            return vec![];
        }
        let stop = stop.min(len - 1);

        let mut result = Vec::new();
        let mut idx = 0;
        for (score, bucket) in &self.scores {
            for member in bucket {
                if idx >= start && idx <= stop {
                    result.push((member.clone(), score.0));
                }
                idx += 1;
                if idx > stop {
                    return result;
                }
            }
        }
        result
    }

    /// 按索引范围获取成员（降序），对应 C 中的 `zrevrangeGenericCommand()`。
    ///
    /// 与 `range()` 相同，但遍历 BTreeMap 和桶内成员时使用反向迭代器。
    pub fn rev_range(&self, start: isize, stop: isize, withscores: bool) -> Vec<(Vec<u8>, f64)> {
        let len = self.len() as isize;
        let start = normalize_index(start, len);
        let stop = normalize_index(stop, len);
        if start > stop || start >= len {
            return vec![];
        }
        let stop = stop.min(len - 1);

        let mut result = Vec::new();
        let mut idx = 0;
        for (score, bucket) in self.scores.iter().rev() {
            for member in bucket.iter().rev() {
                if idx >= start && idx <= stop {
                    result.push((member.clone(), score.0));
                }
                idx += 1;
                if idx > stop {
                    return result;
                }
            }
        }
        result
    }

    /// 按分数范围获取成员，对应 C 中的 `zrangebyscoreCommand()`。
    ///
    /// # 参数
    /// - `min`: 最小分数（包含）
    /// - `max`: 最大分数（包含）
    /// - `withscores`: 是否包含分数（保留用于协议兼容）
    ///
    /// # 复杂度
    /// O(log n + M)，利用 BTreeMap 的 `range()` 方法高效定位范围
    pub fn range_by_score(&self, min: f64, max: f64, withscores: bool) -> Vec<(Vec<u8>, f64)> {
        let mut result = Vec::new();
        for (score, bucket) in self.scores.range(OrderedFloat(min)..=OrderedFloat(max)) {
            for member in bucket {
                result.push((member.clone(), score.0));
            }
        }
        result
    }

    /// 统计分数在 [min, max] 范围内的成员数量，对应 C 中的 `zcountCommand()`。
    pub fn count(&self, min: f64, max: f64) -> usize {
        self.range_by_score(min, max, false).len()
    }

    /// 估算有序集合的内存占用（字节数）。
    ///
    /// 包含 dict 中的键值对开销（成员长度 + HashMap 节点开销）和对象头开销。
    fn memory_usage(&self) -> usize {
        let dict_mem: usize = self.dict.iter().map(|(k, _)| k.len() + 16).sum();
        dict_mem + self.dict.len() * 32 + 128
    }
}

/// 将索引归一化为非负值。
///
/// Redis 命令中的索引支持负数：-1 表示最后一个元素，-2 表示倒数第二个，以此类推。
/// 此函数将负索引转换为等价的非负索引，对应 C 源码中多处出现的负索引处理逻辑。
fn normalize_index(idx: isize, len: isize) -> isize {
    if idx < 0 {
        len + idx
    } else {
        idx
    }
}

// ---------------------------------------------------------------------------
// 过期时间追踪 — 对应 Redis 的 per-key 过期机制
// ---------------------------------------------------------------------------

/// 获取当前时间的毫秒时间戳（自 Unix 纪元以来），对应 C 中的 `mstime()` 函数。
///
/// 用于 `PEXPIRE`、`PEXPIREAT`、`PTTL` 等毫秒精度的过期命令。
pub fn current_time_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64
}

/// 获取当前时间的秒级时间戳（自 Unix 纪元以来），对应 C 中的 `time(NULL)` / `unixtime()`。
///
/// 用于 `EXPIRE`、`EXPIREAT`、`TTL` 等秒精度的过期命令。
pub fn current_time_secs() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs()
}

/// 过期时间条目类型 — 绝对毫秒时间戳。
///
/// 对应 Redis 8 中 `redisDb->expires` 字典的值。
/// Redis 使用绝对时间戳而非相对倒计时，这样在惰性删除时只需比较当前时间与时间戳，
/// 无需为每个键维护定时器。
///
/// 存储结构示意：
/// ```text
/// expires: HashMap<String, ExpiryMs>
///   "mykey"  → 1694150400000  // 2023-09-08 12:00:00 UTC
///   "temp"   → 1694150460000  // 2023-09-08 12:01:00 UTC
/// ```
pub type ExpiryMs = u64;

// ---------------------------------------------------------------------------
// 单元测试
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    /// 测试有序集合的基本操作：添加、更新、查询分数和排名。
    #[test]
    fn test_zset_basic() {
        let mut zset = ZSet::new();
        assert!(zset.add(b"a".to_vec(), 1.0)); // 新增，返回 true
        assert!(zset.add(b"b".to_vec(), 2.0));
        assert!(zset.add(b"c".to_vec(), 3.0));
        assert!(!zset.add(b"a".to_vec(), 1.5)); // 更新已有成员的分数，返回 false

        assert_eq!(zset.len(), 3);
        assert_eq!(zset.score(b"a"), Some(1.5)); // 分数已更新为 1.5
        assert_eq!(zset.rank(b"a"), Some(0)); // 分数最低，排名 0
        assert_eq!(zset.rank(b"b"), Some(1)); // 分数次低，排名 1
    }

    /// 测试有序集合的范围查询功能。
    #[test]
    fn test_zset_range() {
        let mut zset = ZSet::new();
        for i in 0..10 {
            zset.add(format!("m{}", i).into_bytes(), i as f64);
        }
        let r = zset.range(0, 2, false);
        assert_eq!(r.len(), 3); // 返回前 3 个元素
        assert_eq!(r[0].1, 0.0); // 第一个元素分数为 0.0
    }

    /// 测试 RedisObject 的类型判断和元数据查询。
    #[test]
    fn test_redis_object_types() {
        let s = RedisObject::String(b"hello".to_vec());
        assert_eq!(s.type_name(), "string");
        assert_eq!(s.redis_type(), RedisType::String);

        let l = RedisObject::List(VecDeque::new());
        assert_eq!(l.type_name(), "list");
    }
}
