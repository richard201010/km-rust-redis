//! 数据库层 — 多数据库存储与过期管理。
//! 对应 Redis 8 源码中的 redisDb：每个数据库拥有一个字典(dict)和一个过期时间表(expires)。
//! 本模块实现了：
//!   - `Database`: 单个 Redis 数据库（默认编号 0–15），支持 KV 存储、过期时间管理、惰性/主动过期
//!   - `RedisDb`: 多数据库管理器，维护 16 个 Database 实例，支持 SELECT 切换
//!   - `glob_match`: 简易通配符匹配函数，支持 `*`（匹配任意字符序列）和 `?`（匹配单个字符）

use dashmap::DashMap;
use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use tokio::sync::RwLock;

use crate::types::{current_time_ms, current_time_secs, ExpiryMs, RedisObject, ZSet};

// ---------------------------------------------------------------------------
// Database — 单个 Redis 数据库实例
// ---------------------------------------------------------------------------

/// 单个 Redis 数据库（默认编号 0–15，可通过 `databases` 配置项调整）。
/// 对应 Redis 源码 `redis.h` 中的 `redisDb` 结构体。
///
/// 内部使用 `DashMap`（并发安全的分片哈希表）存储 KV 数据和过期时间，
/// 无需外部锁即可在多线程环境下安全读写。
pub struct Database {
    /// 数据库编号（0–15），对应 Redis 的 `SELECT` 命令选择的数据库索引
    pub id: u8,
    /// 键 → RedisObject 的映射表，对应 Redis 的 `redisDb.dict`
    /// 使用 DashMap 实现并发安全，无需额外加锁
    pub data: DashMap<Vec<u8>, RedisObject>,
    /// 键 → 绝对过期时间戳（毫秒），对应 Redis 的 `redisDb.expires`
    /// 存储的是过期的绝对时间戳（epoch 毫秒），而非相对时长
    pub expires: DashMap<Vec<u8>, ExpiryMs>,
    /// 当前数据库中的键数量（近似值），用于 DBSIZE 命令快速返回
    /// 使用原子操作维护，惰性过期可能导致此值略大于实际有效键数
    pub key_count: AtomicU64,
}

impl Database {
    /// 创建一个新的空数据库实例。
    ///
    /// # 参数
    /// - `id`: 数据库编号，通常为 0–15
    ///
    /// # 对应 Redis 行为
    /// 类似 Redis 启动时调用 `initServer()` 初始化 16 个 `redisDb` 实例
    pub fn new(id: u8) -> Self {
        Self {
            id,
            data: DashMap::new(),
            expires: DashMap::new(),
            key_count: AtomicU64::new(0),
        }
    }

    /// 获取键对应的值（带惰性过期检查）。
    ///
    /// 在读取前先检查键是否已过期，如果过期则立即删除并返回 `None`。
    /// 这就是 Redis 的**惰性过期（lazy expiry）**策略：不在定时器中主动清除，
    /// 而是在访问时才检查并删除过期键。
    ///
    /// # 参数
    /// - `key`: 要查询的键（字节切片）
    ///
    /// # 返回
    /// - `Some(RedisObject)`: 键存在且未过期
    /// - `None`: 键不存在或已过期
    pub fn get(&self, key: &[u8]) -> Option<RedisObject> {
        // 惰性过期：先检查是否过期，过期则立即删除
        if self.is_expired(key) {
            self.delete(key);
            return None;
        }
        self.data.get(key).map(|v| v.value().clone())
    }

    /// 设置键值对，可选设置过期时间（从当前时刻起的毫秒数）。
    ///
    /// 对应 Redis 的 `SET key value [PX milliseconds]` 命令。
    /// 如果键已存在则覆盖（包括其过期时间）；如果键是新插入的，则增加键计数。
    ///
    /// # 参数
    /// - `key`: 键（字节向量）
    /// - `value`: 值（`RedisObject`，支持 String/Integer/List/Hash/Set/ZSet 等类型）
    /// - `expire_ms`: 可选的过期时长（毫秒），`None` 表示不设过期时间
    pub fn set(&self, key: Vec<u8>, value: RedisObject, expire_ms: Option<u64>) {
        let exists = self.data.contains_key(&key);
        self.data.insert(key.clone(), value);
        if let Some(ms) = expire_ms {
            // 计算绝对过期时间 = 当前时间 + 相对过期时长
            self.expires.insert(key.clone(), current_time_ms() + ms);
        } else {
            // 无过期时间时，移除可能存在的旧过期记录
            self.expires.remove(&key);
        }
        if !exists {
            // 新键插入，增加键计数
            self.key_count.fetch_add(1, Ordering::Relaxed);
        }
    }

    /// 为已有键设置相对过期时间（从当前时刻起的毫秒数）。
    ///
    /// 对应 Redis 的 `PEXPIRE key milliseconds` 命令。
    /// 只有当键存在时才设置成功。
    ///
    /// # 参数
    /// - `key`: 目标键
    /// - `ms`: 过期时长（毫秒）
    ///
    /// # 返回
    /// - `true`: 设置成功（键存在）
    /// - `false`: 键不存在，无法设置
    pub fn set_expire(&self, key: &[u8], ms: u64) -> bool {
        if self.data.contains_key(key) {
            self.expires.insert(key.to_vec(), current_time_ms() + ms);
            true
        } else {
            false
        }
    }

    /// 为已有键设置绝对过期时间戳（毫秒级 Unix 时间戳）。
    ///
    /// 对应 Redis 的 `PEXPIREAT key milliseconds-timestamp` 命令。
    ///
    /// # 参数
    /// - `key`: 目标键
    /// - `abs_ms`: 绝对过期时间戳（epoch 毫秒）
    ///
    /// # 返回
    /// - `true`: 设置成功
    /// - `false`: 键不存在
    pub fn set_expire_at(&self, key: &[u8], abs_ms: u64) -> bool {
        if self.data.contains_key(key) {
            self.expires.insert(key.to_vec(), abs_ms);
            true
        } else {
            false
        }
    }

    /// 获取键的剩余生存时间（毫秒）。
    ///
    /// 对应 Redis 的 `PTTL key` 命令。
    /// 返回值遵循 Redis 协议规范：
    /// - 正数：剩余生存时间（毫秒）
    /// - `-1`：键存在但没有设置过期时间
    /// - `-2`：键不存在或已过期
    ///
    /// # 参数
    /// - `key`: 目标键
    pub fn pttl(&self, key: &[u8]) -> i64 {
        // 键不存在，返回 -2
        if !self.data.contains_key(key) {
            return -2;
        }
        // 惰性过期检查：已过期则删除并返回 -2
        if self.is_expired(key) {
            self.delete(key);
            return -2;
        }
        match self.expires.get(key) {
            Some(exp) => {
                let now = current_time_ms();
                if *exp <= now {
                    0 // 已到期但尚未被惰性清除（理论上不应走到这里）
                } else {
                    (*exp - now) as i64 // 返回剩余毫秒数
                }
            }
            None => -1, // 键存在但无过期时间
        }
    }

    /// 获取键的剩余生存时间（秒）。
    ///
    /// 对应 Redis 的 `TTL key` 命令。
    /// 内部调用 `pttl()` 获取毫秒值后向上取整转换为秒。
    /// 返回值规则同 `pttl()`，但单位为秒。
    ///
    /// # 参数
    /// - `key`: 目标键
    pub fn ttl(&self, key: &[u8]) -> i64 {
        let pttl = self.pttl(key);
        if pttl < 0 {
            pttl // -1 或 -2 原样返回
        } else {
            (pttl + 999) / 1000 // 向上取整：哪怕只剩 1ms 也算 1 秒
        }
    }

    /// 移除键的过期时间，使其变为持久化键。
    ///
    /// 对应 Redis 的 `PERSIST key` 命令。
    ///
    /// # 返回
    /// - `true`: 成功移除过期时间
    /// - `false`: 键没有设置过期时间（或不存在）
    pub fn persist(&self, key: &[u8]) -> bool {
        self.expires.remove(key).is_some()
    }

    /// 删除键及其关联的过期时间。
    ///
    /// 对应 Redis 的 `DEL key [key ...]` 命令（单键版本）。
    /// 同时从 `data` 和 `expires` 两个映射表中移除，并更新键计数。
    ///
    /// # 返回
    /// - `true`: 键存在且已被删除
    /// - `false`: 键不存在
    pub fn delete(&self, key: &[u8]) -> bool {
        self.expires.remove(key);
        let existed = self.data.remove(key).is_some();
        if existed {
            self.key_count.fetch_sub(1, Ordering::Relaxed);
        }
        existed
    }

    /// 检查键是否存在（带惰性过期检查）。
    ///
    /// 对应 Redis 的 `EXISTS key` 命令。
    /// 在检查前先执行惰性过期，确保返回结果准确。
    ///
    /// # 返回
    /// - `true`: 键存在且未过期
    /// - `false`: 键不存在或已过期
    pub fn exists(&self, key: &[u8]) -> bool {
        if self.is_expired(key) {
            self.delete(key);
            false
        } else {
            self.data.contains_key(key)
        }
    }

    /// 检查键是否已过期（内部辅助方法）。
    ///
    /// 从 `expires` 表中读取键的过期时间戳，与当前时间比较。
    /// 不执行删除操作，仅做判断 — 删除由调用方负责。
    ///
    /// # 返回
    /// - `true`: 键已过期（或刚好到期）
    /// - `false`: 键未过期或没有设置过期时间
    fn is_expired(&self, key: &[u8]) -> bool {
        if let Some(exp) = self.expires.get(key) {
            current_time_ms() >= *exp
        } else {
            false
        }
    }

    /// 获取键存储的值类型名称。
    ///
    /// 对应 Redis 的 `TYPE key` 命令。
    /// 返回值为 Redis 类型字符串，如 `"string"`、`"list"`、`"hash"`、`"set"`、`"zset"` 等。
    /// 带惰性过期检查：如果键已过期则返回 `None`。
    ///
    /// # 返回
    /// - `Some(&'static str)`: 类型名称字符串
    /// - `None`: 键不存在或已过期
    pub fn key_type(&self, key: &[u8]) -> Option<&'static str> {
        if self.is_expired(key) {
            self.delete(key);
            return None;
        }
        self.data.get(key).map(|v| v.value().type_name())
    }

    /// 重命名键（将 old 重命名为 new）。
    ///
    /// 对应 Redis 的 `RENAME key newkey` 命令。
    /// 同时迁移键的值和过期时间。如果 old 键不存在则操作失败。
    ///
    /// # 参数
    /// - `old`: 原键名
    /// - `new`: 新键名
    ///
    /// # 返回
    /// - `true`: 重命名成功
    /// - `false`: 原键不存在
    pub fn rename(&self, old: &[u8], new: Vec<u8>) -> bool {
        if let Some((_, val)) = self.data.remove(old) {
            // 迁移过期时间：先从旧键移除，再插入到新键
            let expire = self.expires.remove(old).map(|(_, e)| e);
            self.data.insert(new.clone(), val);
            if let Some(exp) = expire {
                self.expires.insert(new, exp);
            }
            true
        } else {
            false
        }
    }

    /// 仅在新键不存在时重命名（Rename if Not eXists）。
    ///
    /// 对应 Redis 的 `RENAMENX key newkey` 命令。
    /// 原子性地检查新键是否存在，不存在才执行重命名。
    ///
    /// # 返回
    /// - `true`: 重命名成功（new 之前不存在）
    /// - `false`: new 已存在或 old 不存在
    pub fn renamenx(&self, old: &[u8], new: &[u8]) -> bool {
        if self.data.contains_key(new) {
            false
        } else {
            self.rename(old, new.to_vec())
        }
    }

    /// 返回匹配给定通配符模式的所有键。
    ///
    /// 对应 Redis 的 `KEYS pattern` 命令。
    /// 支持 `*`（匹配任意字符序列）和 `?`（匹配单个字符）。
    /// 遍历过程中跳过已过期的键（但不主动删除它们，避免遍历时修改集合）。
    ///
    /// **注意**：`KEYS` 在生产环境中应谨慎使用，数据量大时可能阻塞服务。
    ///
    /// # 参数
    /// - `pattern`: 通配符模式（字节切片）
    ///
    /// # 返回
    /// 匹配的键列表
    pub fn keys(&self, pattern: &[u8]) -> Vec<Vec<u8>> {
        let mut result = Vec::new();
        let now = current_time_ms();
        for entry in self.data.iter() {
            let key = entry.key();
            // 跳过已过期的键（但不在此处删除，避免并发修改问题）
            if let Some(exp) = self.expires.get(key) {
                if now >= *exp {
                    continue;
                }
            }
            // `*` 模式匹配所有键；其他模式使用 glob_match 通配符匹配
            if pattern == b"*" || glob_match(pattern, key) {
                result.push(key.clone());
            }
        }
        result
    }

    /// 返回当前数据库中的键数量（近似值）。
    ///
    /// 对应 Redis 的 `DBSIZE` 命令。
    /// 由于采用惰性过期策略，某些已过期但尚未被访问的键仍会被计入，
    /// 因此返回值可能略大于实际有效键数。
    pub fn dbsize(&self) -> u64 {
        self.key_count.load(Ordering::Relaxed)
    }

    /// 清空当前数据库中的所有键和过期时间。
    ///
    /// 对应 Redis 的 `FLUSHDB` 命令。
    /// 同时清空 `data`、`expires` 两个映射表，并重置键计数为 0。
    pub fn flush(&self) {
        self.data.clear();
        self.expires.clear();
        self.key_count.store(0, Ordering::Relaxed);
    }

    /// 随机返回一个键名。
    ///
    /// 对应 Redis 的 `RANDOMKEY` 命令。
    /// 收集所有键到临时 Vec 后随机选取一个索引。
    ///
    /// # 返回
    /// - `Some(Vec<u8>)`: 随机选取的键
    /// - `None`: 数据库为空
    pub fn randomkey(&self) -> Option<Vec<u8>> {
        let keys: Vec<_> = self.data.iter().map(|e| e.key().clone()).collect();
        if keys.is_empty() {
            return None;
        }
        let idx = (rand::random::<u64>() as usize) % keys.len();
        Some(keys[idx].clone())
    }

    /// 获取键对应的 RedisObject（带惰性过期检查）。
    ///
    /// 等价于 `get()` 方法的别名，用于需要明确表达"获取对象"语义的场景。
    pub fn get_object(&self, key: &[u8]) -> Option<RedisObject> {
        self.get(key)
    }

    /// 获取键对应的 RedisObject 的可变引用。
    ///
    /// 返回 DashMap 的 `RefMut`，允许调用方直接修改值的内容（如向 List 追加元素）。
    /// 带惰性过期检查：已过期的键会被删除并返回 `None`。
    ///
    /// **注意**：调用方持有 `RefMut` 期间，该键所在的 DashMap 分片会被锁定，
    /// 应尽快释放以避免阻塞其他线程。
    pub fn get_object_mut(
        &self,
        key: &[u8],
    ) -> Option<dashmap::mapref::one::RefMut<'_, Vec<u8>, RedisObject>> {
        if self.is_expired(key) {
            self.delete(key);
            return None;
        }
        self.data.get_mut(key)
    }

    /// 主动过期：抽样检查并删除已过期的键。
    ///
    /// 对应 Redis 的**主动过期（active expiry）**策略，由定时任务（cron）周期性调用。
    /// 算法类似 Redis 的 `activeExpireCycle()`：
    ///   1. 从 `expires` 表中抽样最多 20 个已过期的键
    ///   2. 逐一检查并删除确认过期的键
    ///   3. 如果执行时间超过 `target_ms` 则提前终止，避免长时间阻塞
    ///
    /// # 参数
    /// - `target_ms`: 本次主动过期的最大执行时间（毫秒），超时则停止
    ///
    /// # 返回
    /// 本次实际删除的过期键数量
    pub fn active_expire(&self, target_ms: u64) -> usize {
        let now = current_time_ms();
        let mut expired = 0;
        let start = std::time::Instant::now();

        // 从 expires 表中抽样已过期的键（最多 20 个）
        // 对应 Redis 的 ACTIVE_EXPIRE_CYCLE_LOOKUPS_PER_LOOP 常量
        let keys: Vec<Vec<u8>> = self
            .expires
            .iter()
            .filter(|e| now >= *e.value())
            .take(20)
            .map(|e| e.key().clone())
            .collect();

        for key in keys {
            if self.is_expired(&key) {
                self.delete(&key);
                expired += 1;
            }
            // 超时保护：执行时间超过目标时间则提前退出
            if start.elapsed().as_millis() as u64 >= target_ms {
                break;
            }
        }
        expired
    }
}

// ---------------------------------------------------------------------------
// RedisDb — 多数据库管理器（对应 Redis 服务器全局状态）
// ---------------------------------------------------------------------------

/// 多数据库管理器，维护多个 `Database` 实例。
///
/// 对应 Redis 服务器中的 `redisServer.db` 数组（默认 16 个数据库）。
/// 通过 `SELECT` 命令切换当前使用的数据库。
///
/// 在 Redis 中，客户端连接时会绑定一个默认数据库（db 0），
/// `SELECT` 命令可以切换到其他数据库，但不同数据库之间的键是完全隔离的。
pub struct RedisDb {
    /// 所有数据库实例的集合，索引即为数据库编号（0–15）
    pub databases: Vec<Database>,
    /// 当前选中的数据库编号，对应客户端的 `SELECT` 状态
    pub selected_db: u8,
}

impl RedisDb {
    /// 创建包含指定数量数据库的管理器。
    ///
    /// # 参数
    /// - `num_dbs`: 数据库数量，Redis 默认为 16（可通过 `databases` 配置项调整）
    ///
    /// # 对应 Redis 行为
    /// 类似 Redis 启动时在 `initServer()` 中创建 `server.db` 数组
    pub fn new(num_dbs: u8) -> Self {
        let databases = (0..num_dbs).map(Database::new).collect();
        Self {
            databases,
            selected_db: 0, // 默认选中 db 0
        }
    }

    /// 切换当前数据库。
    ///
    /// 对应 Redis 的 `SELECT index` 命令。
    /// 如果索引超出范围则返回错误。
    ///
    /// # 参数
    /// - `db`: 目标数据库编号
    ///
    /// # 返回
    /// - `Ok(())`: 切换成功
    /// - `Err(String)`: 索引无效（超出数据库数量范围）
    pub fn select(&mut self, db: u8) -> Result<(), String> {
        if db as usize >= self.databases.len() {
            return Err(format!("ERR invalid DB index {}", db));
        }
        self.selected_db = db;
        Ok(())
    }

    /// 获取当前选中数据库的不可变引用。
    ///
    /// 所有不涉及跨数据库的操作都通过此方法获取当前数据库后执行。
    pub fn current(&self) -> &Database {
        &self.databases[self.selected_db as usize]
    }

    /// 获取当前选中数据库的可变引用。
    ///
    /// 需要修改当前数据库（如 SET/DEL 等写操作）时使用。
    pub fn current_mut(&mut self) -> &mut Database {
        &mut self.databases[self.selected_db as usize]
    }

    /// 按编号获取指定数据库的不可变引用。
    ///
    /// 用于跨数据库操作场景（如 `SWAPDB` 命令）。
    ///
    /// # 参数
    /// - `id`: 数据库编号
    pub fn get_db(&self, id: u8) -> &Database {
        &self.databases[id as usize]
    }
}

// ---------------------------------------------------------------------------
// glob_match — 简易通配符匹配函数
// ---------------------------------------------------------------------------

/// 简易通配符模式匹配算法，支持两个通配符：
/// - `*`：匹配任意长度的字符序列（包括空序列）
/// - `?`：匹配恰好一个任意字符
///
/// 采用**贪心回溯算法**实现，使用两个指针分别遍历模式和文本，
/// 遇到 `*` 时记录回溯位置，失配时回溯到最近的 `*` 处重新尝试。
///
/// 该算法时间复杂度为 O(m*n)（m、n 分别为模式和文本长度），
/// 对于 Redis `KEYS` 命令中的常见短模式来说性能足够。
///
/// # 参数
/// - `pattern`: 通配符模式（字节切片）
/// - `text`: 待匹配的文本（字节切片）
///
/// # 返回
/// - `true`: 文本匹配模式
/// - `false`: 不匹配
///
/// # 示例
/// ```
/// assert!(glob_match(b"*", b"anything"));      // * 匹配一切
/// assert!(glob_match(b"h?llo", b"hello"));     // ? 匹配单个字符
/// assert!(!glob_match(b"h?llo", b"hllo"));     // ? 必须匹配一个字符
/// assert!(glob_match(b"h*llo", b"heeeello"));  // * 匹配多个字符
/// ```
fn glob_match(pattern: &[u8], text: &[u8]) -> bool {
    // pi: 模式指针, ti: 文本指针
    let (mut pi, mut ti) = (0, 0);
    // star_pi, star_ti: 记录最近一次遇到 '*' 时的位置，用于回溯
    // 初始化为 usize::MAX 表示尚未遇到 '*'
    let (mut star_pi, mut star_ti) = (usize::MAX, 0);

    while ti < text.len() {
        if pi < pattern.len() && (pattern[pi] == b'?' || pattern[pi] == text[ti]) {
            // 当前字符匹配（精确匹配或 '?' 通配），两个指针同时前进
            pi += 1;
            ti += 1;
        } else if pi < pattern.len() && pattern[pi] == b'*' {
            // 遇到 '*'：记录回溯位置，模式指针前进（'*' 先尝试匹配 0 个字符）
            star_pi = pi;
            star_ti = ti;
            pi += 1;
        } else if star_pi != usize::MAX {
            // 失配但之前遇到过 '*'：回溯到 '*' 位置，让 '*' 多匹配一个字符
            pi = star_pi + 1; // 模式指针回到 '*' 之后
            star_ti += 1; // '*' 多吞噬一个文本字符
            ti = star_ti; // 文本指针回溯到新位置
        } else {
            // 失配且无 '*' 可回溯：匹配失败
            return false;
        }
    }
    // 文本已遍历完，跳过模式末尾多余的 '*'
    while pi < pattern.len() && pattern[pi] == b'*' {
        pi += 1;
    }
    // 只有模式也完全遍历完才算匹配成功
    pi == pattern.len()
}

// ---------------------------------------------------------------------------
// 单元测试
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    /// 测试基本的 SET/GET 操作
    #[test]
    fn test_database_set_get() {
        let db = Database::new(0);
        db.set(
            b"key".to_vec(),
            RedisObject::String(b"value".to_vec()),
            None,
        );
        let val = db.get(b"key").unwrap();
        match val {
            RedisObject::String(d) => assert_eq!(d, b"value"),
            _ => panic!("Expected string"),
        }
    }

    /// 测试删除操作：删除后键应不存在
    #[test]
    fn test_database_delete() {
        let db = Database::new(0);
        db.set(b"k".to_vec(), RedisObject::Integer(42), None);
        assert!(db.delete(b"k"));
        assert!(!db.exists(b"k"));
    }

    /// 测试通配符匹配：`*`、`?` 以及组合模式
    #[test]
    fn test_glob_match() {
        assert!(glob_match(b"*", b"anything")); // * 匹配任意字符串
        assert!(glob_match(b"h?llo", b"hello")); // ? 匹配单个字符
        assert!(!glob_match(b"h?llo", b"hllo")); // ? 不能跳过字符
        assert!(glob_match(b"h*llo", b"heeeello")); // * 匹配多个字符
    }

    /// 测试多数据库隔离：不同数据库中的同名键互不影响
    #[test]
    fn test_multi_db() {
        let mut rdb = RedisDb::new(16);
        // 在 db 0 中设置 k=1
        rdb.current_mut()
            .set(b"k".to_vec(), RedisObject::Integer(1), None);
        // 切换到 db 1，设置 k=2
        rdb.select(1).unwrap();
        rdb.current_mut()
            .set(b"k".to_vec(), RedisObject::Integer(2), None);

        // 切回 db 0，验证 k 的值仍为 1
        rdb.select(0).unwrap();
        match rdb.current().get(b"k").unwrap() {
            RedisObject::Integer(n) => assert_eq!(n, 1),
            _ => panic!(),
        }
        // 切到 db 1，验证 k 的值为 2
        rdb.select(1).unwrap();
        match rdb.current().get(b"k").unwrap() {
            RedisObject::Integer(n) => assert_eq!(n, 2),
            _ => panic!(),
        }
    }
}
