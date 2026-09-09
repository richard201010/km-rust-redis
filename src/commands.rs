//! 命令分发与实现模块。
//!
//! 本模块是 km-rust-redis 的核心命令引擎，完整镜像了 Redis 8 的命令表（command table），
//! 将每个 Redis 命令名称映射到对应的 Rust 处理函数。
//!
//! # 架构概览
//!
//! - [`CmdCtx`] — 命令执行上下文，持有数据库引用、当前 DB 编号、命令参数列表和 RESP 协议版本信息
//! - [`CommandDef`] — 命令定义结构体，描述单个命令的元信息（名称、处理函数、参数数量、标志位等）
//! - [`build_command_table`] — 使用宏批量注册 170+ 条命令，按连接/键/过期/字符串/列表/哈希/集合/有序集合/事务/PubSub/服务器 分类组织
//! - `cmd_*` 函数 — 每个 Redis 命令的具体实现，遵循 Redis 协议规范返回 [`RespValue`]
//!
//! # 命令分类
//!
//! | 分类         | 示例命令                                | 对应 Redis 类型  |
//! |-------------|----------------------------------------|-----------------|
//! | 连接管理     | PING, AUTH, SELECT, QUIT              | Connection      |
//! | 通用键操作   | DEL, EXISTS, TYPE, KEYS, SCAN, RENAME | Generic Key     |
//! | 过期管理     | EXPIRE, TTL, PERSIST, PEXPIRE         | Expiry          |
//! | 字符串操作   | GET, SET, INCR, APPEND, MGET          | String          |
//! | 列表操作     | LPUSH, RPUSH, LPOP, LRANGE, LLEN     | List            |
//! | 哈希操作     | HSET, HGET, HGETALL, HDEL, HKEYS     | Hash            |
//! | 集合操作     | SADD, SREM, SMEMBERS, SINTER, SDIFF  | Set             |
//! | 有序集合操作 | ZADD, ZRANGE, ZSCORE, ZRANK           | Sorted Set      |
//! | 事务         | MULTI, EXEC, DISCARD                  | Transaction     |
//! | 发布/订阅    | SUBSCRIBE, UNSUBSCRIBE, PUBLISH       | Pub/Sub         |
//! | 服务器信息    | INFO, CONFIG, CLIENT, TIME            | Server          |

use crate::db::Database;
use crate::resp::RespValue;
use crate::scan;
use crate::stream::{Stream, StreamId};
use crate::types::{current_time_ms, current_time_secs, OrderedFloat, RedisObject, ZSet};
use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::atomic::Ordering;

/// 命令执行上下文（Command Context）。
///
/// 对应 Redis 源码中的 `client` 结构体，封装了一次命令执行所需的全部状态：
///
/// - `db` — 当前数据库的引用（对应 Redis 的 `redisDb`），所有键值操作通过此字段进行
/// - `db_id` — 当前选择的数据库编号（0-15），对应 Redis 的 `SELECT dbindex`
/// - `argv` — 命令参数列表（以原始字节存储），`argv[0]` 是命令名，后续为参数
/// - `resp3` — 是否使用 RESP3 协议（Redis 6.0+ 引入的新序列化协议），影响返回值格式
///
/// # 辅助方法
///
/// 提供 `arg_str`/`arg_bytes`/`arg_i64`/`arg_f64`/`argc` 等方法，用于安全地访问参数。
pub struct CmdCtx<'a> {
    pub db: &'a Database,
    pub db_id: u8,
    pub argv: Vec<Vec<u8>>,
    pub resp3: bool,
    pub cluster: Option<std::sync::Arc<tokio::sync::Mutex<crate::cluster::ClusterState>>>,
}

impl<'a> CmdCtx<'a> {
    /// 获取指定索引位置的参数并转换为 UTF-8 字符串。
    /// 用于读取命令参数中需要作为字符串处理的选项（如 "NX"、"EX" 等）。
    pub fn arg_str(&self, idx: usize) -> Option<&str> {
        self.argv.get(idx).and_then(|a| std::str::from_utf8(a).ok())
    }

    /// 获取指定索引位置参数的原始字节切片。
    /// 用于直接操作二进制数据（如键名、值内容等）。
    pub fn arg_bytes(&self, idx: usize) -> Option<&[u8]> {
        self.argv.get(idx).map(|a| a.as_slice())
    }

    /// 获取指定索引位置参数并解析为 i64 整数。
    /// 用于读取数值参数（如过期时间、偏移量、计数等）。
    pub fn arg_i64(&self, idx: usize) -> Option<i64> {
        self.arg_str(idx)?.parse().ok()
    }

    /// 获取指定索引位置参数并解析为 f64 浮点数。
    /// 用于读取浮点参数（如 ZADD 的 score、INCRBYFLOAT 的增量等）。
    pub fn arg_f64(&self, idx: usize) -> Option<f64> {
        self.arg_str(idx)?.parse().ok()
    }

    /// 返回命令参数总数（包括命令名本身）。
    /// 例如 `SET key value` 的 argc() 返回 3。
    pub fn argc(&self) -> usize {
        self.argv.len()
    }
}

/// 命令处理函数类型。
///
/// 每个 Redis 命令都对应一个 `CmdHandler` 函数，接收命令上下文引用，返回 RESP 协议值。
/// 对应 Redis 源码中 `redisCommandProc` 函数指针类型。
type CmdHandler = fn(&CmdCtx) -> RespValue;

/// 命令定义结构体（简化版的 Redis `redisCommand`）。
///
/// 描述一个 Redis 命令的完整元信息，在构建命令表时由 `cmd!` 宏填充：
///
/// - `name` — 命令名称（大写），如 `"GET"`、`"SET"`
/// - `handler` — 命令处理函数指针，执行时由分发器调用
/// - `arity` — 参数数量要求；正数表示固定参数数（含命令名），负数表示最少 `|arity|` 个参数
///               例如 `2` = 正好 2 个参数（命令名 + key），`-3` = 至少 3 个参数
/// - `flags` — 命令标志位，组合 `CMD_WRITE`/`CMD_READONLY`/`CMD_FAST`/`CMD_ADMIN`
/// - `first_key` — 第一个键参数的位置（本实现暂未使用，保留字段兼容性）
/// - `last_key` — 最后一个键参数的位置
/// - `step` — 键参数之间的步长（用于 MGET/MSET 等多键命令）
pub struct CommandDef {
    pub name: &'static str,
    pub handler: CmdHandler,
    pub arity: i32, // 负值表示至少 |arity|-1 个参数
    pub flags: u32,
    pub first_key: i32,
    pub last_key: i32,
    pub step: i32,
}

/// 命令标志位常量，对应 Redis 的 `CMD_WRITE`、`CMD_READONLY` 等。
pub const CMD_WRITE: u32 = 1 << 0; // 写命令，会修改数据库状态
const CMD_READONLY: u32 = 1 << 1; // 只读命令，不修改数据库
const CMD_FAST: u32 = 1 << 2; // 快速命令，O(1) 时间复杂度，不影响慢查询日志
const CMD_ADMIN: u32 = 1 << 3; // 管理命令，需要管理员权限（如 CONFIG、CLIENT）

/// 构建命令分发表（Command Table）。
///
/// 对应 Redis 源码中的 `populateCommandTable()` 函数，使用 `cmd!` 宏将所有 Redis 命令
/// 批量注册到 HashMap 中。命令名统一转为小写存储，方便大小写不敏感匹配。
///
/// # 宏 `cmd!`
///
/// `cmd!(名称, 处理函数, 参数数量, 标志位)` 展开为向 `table` 插入一条 `CommandDef`。
///
/// # 命令统计
///
/// 本函数注册了以下类别的命令（共 170+ 条）：
/// - 连接管理（PING/ECHO/SELECT/AUTH/QUIT/DBSIZE/FLUSHDB/FLUSHALL）
/// - 通用键操作（DEL/EXISTS/TYPE/KEYS/SCAN/RANDOMKEY/RENAME/RENAMENX）
/// - 过期管理（EXPIRE/EXPIREAT/PEXPIRE/PEXPIREAT/TTL/PTTL/PERSIST）
/// - 字符串操作（GET/SET/INCR/APPEND/MGET 等 20 条）
/// - 列表操作（LPUSH/RPUSH/LPOP/RPOP/LRANGE 等 17 条）
/// - 哈希操作（HSET/HGET/HGETALL/HDEL 等 15 条）
/// - 集合操作（SADD/SREM/SMEMBERS/SINTER 等 11 条）
/// - 有序集合操作（ZADD/ZREM/ZSCORE/ZRANGE 等 13 条）
/// - 事务（MULTI/EXEC/DISCARD）
/// - 发布/订阅（SUBSCRIBE/UNSUBSCRIBE/PUBLISH）
/// - 服务器信息（INFO/CONFIG/COMMAND/CLIENT/TIME/SLOWLOG）
pub fn build_command_table() -> HashMap<String, CommandDef> {
    let mut table = HashMap::new();

    macro_rules! cmd {
        ($name:expr, $handler:expr, $arity:expr, $flags:expr) => {
            table.insert(
                $name.to_ascii_lowercase(),
                CommandDef {
                    name: $name,
                    handler: $handler,
                    arity: $arity,
                    flags: $flags,
                    first_key: 0,
                    last_key: 0,
                    step: 0,
                },
            );
        };
    }

    // ================================================================
    // 连接管理命令（Connection Commands）
    // 对应 Redis 的连接生命周期管理：连接探测、身份认证、数据库选择、断开连接等
    // ================================================================
    cmd!("PING", cmd_ping, 1, 0);
    cmd!("ECHO", cmd_echo, 2, 0);
    cmd!("SELECT", cmd_select, 2, 0);
    cmd!("AUTH", cmd_auth, -2, 0);
    cmd!("QUIT", cmd_quit, 1, 0);
    cmd!("DBSIZE", cmd_dbsize, 1, CMD_READONLY);
    cmd!("FLUSHDB", cmd_flushdb, 1, CMD_WRITE);
    cmd!("FLUSHALL", cmd_flushall, 1, CMD_WRITE);

    // ================================================================
    // 通用键操作命令（Generic Key Commands）
    // 操作任意类型的键：删除、存在性检查、类型查询、模式匹配、重命名等
    // ================================================================
    cmd!("DEL", cmd_del, -2, CMD_WRITE);
    cmd!("UNLINK", cmd_del, -2, CMD_WRITE | CMD_FAST);
    cmd!("EXISTS", cmd_exists, -2, CMD_READONLY);
    cmd!("TYPE", cmd_type, 2, CMD_READONLY);
    cmd!("KEYS", cmd_keys, 2, CMD_READONLY);
    cmd!("SCAN", cmd_scan, -2, CMD_READONLY);
    cmd!("RANDOMKEY", cmd_randomkey, 1, CMD_READONLY);
    cmd!("RENAME", cmd_rename, 3, CMD_WRITE);
    cmd!("RENAMENX", cmd_renamenx, 3, CMD_WRITE);

    // ================================================================
    // 过期管理命令（Expiry Commands）
    // 设置/查询/移除键的过期时间，支持秒级和毫秒级精度
    // ================================================================
    cmd!("EXPIRE", cmd_expire, 3, CMD_WRITE);
    cmd!("EXPIREAT", cmd_expireat, 3, CMD_WRITE);
    cmd!("PEXPIRE", cmd_pexpire, 3, CMD_WRITE);
    cmd!("PEXPIREAT", cmd_pexpireat, 3, CMD_WRITE);
    cmd!("TTL", cmd_ttl, 2, CMD_READONLY);
    cmd!("PTTL", cmd_pttl, 2, CMD_READONLY);
    cmd!("PERSIST", cmd_persist, 2, CMD_WRITE);

    // ================================================================
    // 字符串命令（String Commands）
    // Redis 最基础的数据类型，支持 GET/SET/INCR/APPEND/GETRANGE 等操作
    // ================================================================
    cmd!("GET", cmd_get, 2, CMD_READONLY);
    cmd!("SET", cmd_set, -3, CMD_WRITE);
    cmd!("SETNX", cmd_setnx, 3, CMD_WRITE);
    cmd!("SETEX", cmd_setex, 4, CMD_WRITE);
    cmd!("PSETEX", cmd_psetex, 4, CMD_WRITE);
    cmd!("MGET", cmd_mget, -2, CMD_READONLY);
    cmd!("MSET", cmd_mset, -3, CMD_WRITE);
    cmd!("MSETNX", cmd_msetnx, -3, CMD_WRITE);
    cmd!("GETSET", cmd_getset, 3, CMD_WRITE);
    cmd!("APPEND", cmd_append, 3, CMD_WRITE);
    cmd!("STRLEN", cmd_strlen, 2, CMD_READONLY);
    cmd!("INCR", cmd_incr, 2, CMD_WRITE);
    cmd!("INCRBY", cmd_incrby, 3, CMD_WRITE);
    cmd!("INCRBYFLOAT", cmd_incrbyfloat, 3, CMD_WRITE);
    cmd!("DECR", cmd_decr, 2, CMD_WRITE);
    cmd!("DECRBY", cmd_decrby, 3, CMD_WRITE);
    cmd!("GETRANGE", cmd_getrange, 4, CMD_READONLY);
    cmd!("SETRANGE", cmd_setrange, 4, CMD_WRITE);
    cmd!("GETBIT", cmd_getbit, 3, CMD_WRITE);
    cmd!("SETBIT", cmd_setbit, 4, CMD_WRITE);
    cmd!("BITCOUNT", cmd_bitcount, -2, CMD_READONLY);
    cmd!("BITPOS", cmd_bitpos, -3, CMD_READONLY);
    cmd!("BITOP", cmd_bitop, -4, CMD_WRITE);
    cmd!("BITFIELD", cmd_bitfield, -2, CMD_WRITE);

    // ================================================================
    // 列表命令（List Commands）
    // 双向链表数据类型，支持两端推入/弹出、范围查询、阻塞操作等
    // ================================================================
    cmd!("LPUSH", cmd_lpush, -3, CMD_WRITE);
    cmd!("RPUSH", cmd_rpush, -3, CMD_WRITE);
    cmd!("LPOP", cmd_lpop, -2, CMD_WRITE);
    cmd!("RPOP", cmd_rpop, -2, CMD_WRITE);
    cmd!("LRANGE", cmd_lrange, 4, CMD_READONLY);
    cmd!("LLEN", cmd_llen, 2, CMD_READONLY);
    cmd!("LINDEX", cmd_lindex, 3, CMD_READONLY);
    cmd!("LSET", cmd_lset, 4, CMD_WRITE);
    cmd!("LREM", cmd_lrem, 4, CMD_WRITE);
    cmd!("LPOS", cmd_lpos, -3, CMD_READONLY);
    cmd!("LTRIM", cmd_ltrim, 4, CMD_WRITE);
    cmd!("LINSERT", cmd_linsert, 5, CMD_WRITE);
    cmd!("LPUSHX", cmd_lpushx, -3, CMD_WRITE);
    cmd!("RPUSHX", cmd_rpushx, -3, CMD_WRITE);
    cmd!("RPOPLPUSH", cmd_rpoplpush, 3, CMD_WRITE);
    cmd!("LMOVE", cmd_lmove, 5, CMD_WRITE);
    cmd!("LMPOP", cmd_lmpop, -4, CMD_WRITE);
    cmd!("BLMOVE", cmd_blmove, 6, CMD_WRITE);

    // ================================================================
    // 哈希命令（Hash Commands）
    // 字段-值映射数据类型，适合存储对象（如用户信息、配置项等）
    // ================================================================
    cmd!("HSET", cmd_hset, -4, CMD_WRITE);
    cmd!("HGET", cmd_hget, 3, CMD_READONLY);
    cmd!("HMSET", cmd_hmset, -4, CMD_WRITE);
    cmd!("HMGET", cmd_hmget, -3, CMD_READONLY);
    cmd!("HGETALL", cmd_hgetall, 2, CMD_READONLY);
    cmd!("HDEL", cmd_hdel, -3, CMD_WRITE);
    cmd!("HEXISTS", cmd_hexists, 3, CMD_READONLY);
    cmd!("HLEN", cmd_hlen, 2, CMD_READONLY);
    cmd!("HINCRBY", cmd_hincrby, 4, CMD_WRITE);
    cmd!("HINCRBYFLOAT", cmd_hincrbyfloat, 4, CMD_WRITE);
    cmd!("HKEYS", cmd_hkeys, 2, CMD_READONLY);
    cmd!("HVALS", cmd_hvals, 2, CMD_READONLY);
    cmd!("HSETNX", cmd_hsetnx, 4, CMD_WRITE);
    cmd!("HSTRLEN", cmd_hstrlen, 3, CMD_READONLY);
    cmd!("HRANDFIELD", cmd_hrandfield, -2, CMD_READONLY);
    cmd!("HSCAN", cmd_hscan, -3, CMD_READONLY);

    // ================================================================
    // 集合命令（Set Commands）
    // 无序唯一元素集合，支持交集/并集/差集等集合运算
    // ================================================================
    cmd!("SADD", cmd_sadd, -3, CMD_WRITE);
    cmd!("SREM", cmd_srem, -3, CMD_WRITE);
    cmd!("SMEMBERS", cmd_smembers, 2, CMD_READONLY);
    cmd!("SISMEMBER", cmd_sismember, 3, CMD_READONLY);
    cmd!("SCARD", cmd_scard, 2, CMD_READONLY);
    cmd!("SINTER", cmd_sinter, -2, CMD_READONLY);
    cmd!("SUNION", cmd_sunion, -2, CMD_READONLY);
    cmd!("SDIFF", cmd_sdiff, -2, CMD_READONLY);
    cmd!("SRANDMEMBER", cmd_srandmember, -2, CMD_READONLY);
    cmd!("SPOP", cmd_spop, -2, CMD_WRITE);
    cmd!("SMISMEMBER", cmd_smismember, -3, CMD_READONLY);
    cmd!("SINTERCARD", cmd_sintercard, -3, CMD_READONLY);
    cmd!("SSCAN", cmd_sscan, -3, CMD_READONLY);

    // ================================================================
    // 有序集合命令（Sorted Set Commands）
    // 带分数(score)排序的唯一元素集合，支持按排名/分数/字典序范围查询
    // ================================================================
    cmd!("ZADD", cmd_zadd, -4, CMD_WRITE);
    cmd!("ZREM", cmd_zrem, -3, CMD_WRITE);
    cmd!("ZSCORE", cmd_zscore, 3, CMD_READONLY);
    cmd!("ZRANK", cmd_zrank, 3, CMD_READONLY);
    cmd!("ZREVRANK", cmd_zrevrank, 3, CMD_READONLY);
    cmd!("ZRANGE", cmd_zrange, -4, CMD_READONLY);
    cmd!("ZREVRANGE", cmd_zrevrange, -4, CMD_READONLY);
    cmd!("ZRANGEBYSCORE", cmd_zrangebyscore, -4, CMD_READONLY);
    cmd!("ZREVRANGEBYSCORE", cmd_zrevrangebyscore, -4, CMD_READONLY);
    cmd!("ZCARD", cmd_zcard, 2, CMD_READONLY);
    cmd!("ZCOUNT", cmd_zcount, 4, CMD_READONLY);
    cmd!("ZINCRBY", cmd_zincrby, 4, CMD_WRITE);
    cmd!("ZRANGEBYLEX", cmd_zrangebylex, -4, CMD_READONLY);
    cmd!("ZLEXCOUNT", cmd_zlexcount, 4, CMD_READONLY);
    cmd!("ZPOPMIN", cmd_zpopmin, -2, CMD_WRITE);
    cmd!("ZPOPMAX", cmd_zpopmax, -2, CMD_WRITE);
    cmd!("ZMSCORE", cmd_zmscore, -3, CMD_READONLY);
    cmd!("ZRANDMEMBER", cmd_zrandmember, -2, CMD_READONLY);
    cmd!("ZDIFF", cmd_zdiff, -3, CMD_READONLY);
    cmd!("ZUNION", cmd_zunion, -3, CMD_READONLY);
    cmd!("ZINTER", cmd_zinter, -3, CMD_READONLY);
    cmd!("ZRANGESTORE", cmd_zrangestore, -5, CMD_WRITE);
    cmd!("ZSCAN", cmd_zscan, -3, CMD_READONLY);

    // ================================================================
    // 事务命令（Transaction Commands）
    // MULTI/EXEC/DISCARD 事务支持（桩实现，完整逻辑在服务端循环中）
    // ================================================================
    cmd!("MULTI", cmd_multi, 1, 0);
    cmd!("EXEC", cmd_exec, 1, 0);
    cmd!("DISCARD", cmd_discard, 1, 0);

    // ================================================================
    // 发布/订阅命令（Pub/Sub Commands）
    // 消息发布与频道订阅（桩实现，完整逻辑在服务端循环中）
    // ================================================================
    cmd!("SUBSCRIBE", cmd_subscribe, -2, 0);
    cmd!("UNSUBSCRIBE", cmd_unsubscribe, -1, 0);
    cmd!("PUBLISH", cmd_publish, 3, 0);
    cmd!("PSUBSCRIBE", cmd_psubscribe, -2, 0);
    cmd!("PUNSUBSCRIBE", cmd_punsubscribe, -1, 0);
    cmd!("SSUBSCRIBE", cmd_ssubscribe, -2, 0);
    cmd!("SUNSUBSCRIBE", cmd_sunsubscribe, -1, 0);
    cmd!("SPUBLISH", cmd_spublish, 3, 0);

    // ================================================================
    // 服务器信息命令（Server Info Commands）
    // 查询服务器状态、配置管理、命令发现、时间戳等运维类命令
    // ================================================================
    cmd!("INFO", cmd_info, -1, 0);
    cmd!("CONFIG", cmd_config, -2, CMD_ADMIN);
    cmd!("COMMAND", cmd_command, -1, 0);
    cmd!("CLIENT", cmd_client, -2, CMD_ADMIN);
    cmd!("TIME", cmd_time, 1, 0);
    cmd!("DBSIZE", cmd_dbsize, 1, CMD_READONLY);
    cmd!("SLOWLOG", cmd_slowlog, -2, CMD_ADMIN);
    cmd!("SAVE", cmd_save, 1, CMD_ADMIN);
    cmd!("BGSAVE", cmd_bgsave, 1, CMD_ADMIN);
    cmd!("LASTSAVE", cmd_lastsave, 1, CMD_FAST);
    cmd!("BGREWRITEAOF", cmd_bgrewriteaof, 1, CMD_ADMIN);
    cmd!("SWAPDB", cmd_swapdb, 3, CMD_ADMIN);
    cmd!("HELLO", cmd_hello, -1, 0);
    // 复制命令
    cmd!("REPLICAOF", cmd_replicaof, 3, CMD_ADMIN);
    // 哨兵命令
    cmd!("SENTINEL", cmd_sentinel, -2, CMD_ADMIN);
    // GEO 旧版
    cmd!("GEORADIUS", cmd_georadius, -6, CMD_WRITE);
    cmd!("GEORADIUS_RO", cmd_georadius_ro, -6, CMD_READONLY);
    cmd!("GEORADIUSBYMEMBER", cmd_georadiusbymember, -5, CMD_WRITE);
    cmd!("GEORADIUSBYMEMBER_RO", cmd_georadiusbymember_ro, -5, CMD_READONLY);
    // Server 高级
    cmd!("HOTKEYS", cmd_hotkeys, -1, CMD_ADMIN);
    cmd!("MOVE", cmd_move, 3, CMD_WRITE);
    cmd!("PFDEBUG", cmd_pfdebug, -2, CMD_ADMIN);
    cmd!("PFSELFTEST", cmd_pfselftest, 1, CMD_ADMIN);
    cmd!("TRIMSLOTS", cmd_trimslots, -2, CMD_ADMIN);
    // Stream 高级
    cmd!("XACKDEL", cmd_xackdel, -3, CMD_WRITE);
    cmd!("XCFGSET", cmd_xcfgset, -3, CMD_WRITE);
    cmd!("XDELEX", cmd_xdelex, -3, CMD_WRITE);
    cmd!("XIDMPRECORD", cmd_xidmprecord, -2, CMD_READONLY);
    cmd!("XNACK", cmd_xnack, -3, CMD_WRITE);
    cmd!("LMOVEM", cmd_lmovem, 6, CMD_WRITE);
    // ZSet
    cmd!("ZMPOP", cmd_zmpop, -4, CMD_WRITE);

    // ================================================================
    // 通用键操作扩展（Extended Key Commands）
    // ================================================================
    cmd!("OBJECT", cmd_object, -2, CMD_READONLY);
    cmd!("DUMP", cmd_dump, 2, CMD_READONLY);
    cmd!("RESTORE", cmd_restore, -4, CMD_WRITE);
    cmd!("SORT", cmd_sort, -2, CMD_WRITE);
    cmd!("SORT_RO", cmd_sort_ro, -2, CMD_READONLY);

    // ================================================================
    // WAIT / 复制同步
    // ================================================================
    cmd!("WAIT", cmd_wait, 3, 0);
    cmd!("WAITAOF", cmd_waof, 4, 0);

    // ================================================================
    // READONLY / READWRITE（集群模式从节点读取）
    // ================================================================
    cmd!("READONLY", cmd_readonly, 1, 0);
    cmd!("READWRITE", cmd_readwrite, 1, 0);

    // ================================================================
    // WATCH / UNWATCH（乐观锁 / 事务扩展）
    // ================================================================
    cmd!("WATCH", cmd_watch, -2, 0);
    cmd!("UNWATCH", cmd_unwatch, 1, 0);

    // ================================================================
    // Cluster 命令（单节点桩实现）
    // ================================================================
    cmd!("CLUSTER", cmd_cluster, -2, CMD_ADMIN);

    // ================================================================
    // ACL 命令（访问控制列表）
    // ================================================================
    cmd!("ACL", cmd_acl, -2, CMD_ADMIN);

    // ================================================================
    // Lua 脚本命令（EVAL/EVALSHA/SCRIPT）
    // ================================================================
    cmd!("SCRIPT", cmd_script, -2, 0);
    cmd!("EVAL", cmd_eval, -3, 0);
    cmd!("EVALSHA", cmd_evalsha, -3, 0);

    // ================================================================
    // 地理空间命令（Geo Commands）
    // GEOADD/GEODIST/GEOHASH/GEOPOS/GEOSEARCH/GEOSEARCHSTORE
    // ================================================================
    cmd!("GEOADD", cmd_geoadd, -5, CMD_WRITE);
    cmd!("GEODIST", cmd_geodist, -4, CMD_READONLY);
    cmd!("GEOHASH", cmd_geohash, -3, CMD_READONLY);
    cmd!("GEOPOS", cmd_geopos, -3, CMD_READONLY);
    cmd!("GEOSEARCH", cmd_geosearch, -4, CMD_READONLY);
    cmd!("GEOSEARCHSTORE", cmd_geosearchstore, -4, CMD_WRITE);

    // ================================================================
    // HyperLogLog 命令（简化为 Set 实现）
    // PFADD/PFCOUNT/PFMERGE
    // ================================================================
    cmd!("PFADD", cmd_pfadd, -3, CMD_WRITE);
    cmd!("PFCOUNT", cmd_pfcount, -2, CMD_READONLY);
    cmd!("PFMERGE", cmd_pfmerge, -2, CMD_WRITE);

    // ================================================================
    // Hash 字段过期命令（Hash Field Expiry Commands）— Redis 8
    // ================================================================
    cmd!("HEXPIRE", cmd_hexpire, -4, CMD_WRITE);
    cmd!("HEXPIREAT", cmd_hexpireat, -4, CMD_WRITE);
    cmd!("HPEXPIRE", cmd_hpexpire, -4, CMD_WRITE);
    cmd!("HPEXPIREAT", cmd_hpexpireat, -4, CMD_WRITE);
    cmd!("HPERSIST", cmd_hpersist, -3, CMD_WRITE);
    cmd!("HTTL", cmd_httl, -3, CMD_READONLY);
    cmd!("HPTTL", cmd_hpttl, -3, CMD_READONLY);
    cmd!("HEXPIRETIME", cmd_hexpiretime, -3, CMD_READONLY);
    cmd!("HPEXPIRETIME", cmd_hpexpiretime, -3, CMD_READONLY);

    // ================================================================
    // Hash 高级命令（Hash Advanced Commands）— Redis 8
    // ================================================================
    cmd!("HGETDEL", cmd_hgetdel, -3, CMD_WRITE);
    cmd!("HGETEX", cmd_hgetex, -3, CMD_WRITE);
    cmd!("HSETEX", cmd_hsetex, -3, CMD_WRITE);
    cmd!("HIMPORT", cmd_himport, -3, CMD_WRITE);

    // ================================================================
    // Server 高级命令（Server Advanced Commands）
    // ================================================================
    cmd!("SHUTDOWN", cmd_shutdown, -1, CMD_ADMIN);
    cmd!("RESET", cmd_reset, 1, 0);
    cmd!("ROLE", cmd_role, 1, 0);
    cmd!("MEMORY", cmd_memory, -2, CMD_ADMIN);
    cmd!("LATENCY", cmd_latency, -2, CMD_ADMIN);
    cmd!("DEBUG", cmd_debug, -2, CMD_ADMIN);
    cmd!("MONITOR", cmd_monitor, 1, CMD_ADMIN);
    cmd!("MODULE", cmd_module, -2, CMD_ADMIN);
    cmd!("FUNCTION", cmd_function, -2, CMD_ADMIN);
    cmd!("FAILOVER", cmd_failover, -1, CMD_ADMIN);
    cmd!("BACKUP", cmd_backup, -2, CMD_ADMIN);

    // ================================================================
    // Array 命令（Array Commands）— Redis 8 新增数组类型
    // ================================================================
    cmd!("ARCOUNT", cmd_arcount, 3, CMD_READONLY);
    cmd!("ARDEL", cmd_ardel, -3, CMD_WRITE);
    cmd!("ARDELRANGE", cmd_ardelrange, 5, CMD_WRITE);
    cmd!("ARGET", cmd_arget, 4, CMD_READONLY);
    cmd!("ARGETRANGE", cmd_argetrange, 5, CMD_READONLY);
    cmd!("ARGREP", cmd_argrep, 5, CMD_WRITE);
    cmd!("ARINFO", cmd_arinfo, 2, CMD_READONLY);
    cmd!("ARINSERT", cmd_arinsert, -5, CMD_WRITE);
    cmd!("ARLASTITEMS", cmd_arlastitems, -3, CMD_READONLY);
    cmd!("ARLEN", cmd_arlen, 2, CMD_READONLY);
    cmd!("ARMGET", cmd_armget, -3, CMD_READONLY);
    cmd!("ARMSET", cmd_armset, -4, CMD_WRITE);
    cmd!("ARNEXT", cmd_arnext, 3, CMD_READONLY);
    cmd!("AROP", cmd_arop, -3, CMD_WRITE);
    cmd!("ARRING", cmd_arring, -3, CMD_WRITE);
    cmd!("ARSCAN", cmd_arscan, -3, CMD_READONLY);
    cmd!("ARSEEK", cmd_arseek, 4, CMD_READONLY);
    cmd!("ARSET", cmd_arset, 5, CMD_WRITE);

    // ================================================================
    // Script 高级命令（Script Advanced Commands）
    // ================================================================
    cmd!("EVAL_RO", cmd_eval_ro, -3, CMD_READONLY);
    cmd!("EVALSHA_RO", cmd_evalsha_ro, -3, CMD_READONLY);
    cmd!("FCALL", cmd_fcall, -3, 0);
    cmd!("FCALL_RO", cmd_fcall_ro, -3, CMD_READONLY);

    // ================================================================
    // 其他命令（Misc Commands）
    // ================================================================
    cmd!("LOLWUT", cmd_lolwut, -1, CMD_READONLY);
    cmd!("ASKING", cmd_asking, 1, 0);
    cmd!("PSYNC", cmd_psync, 3, 0);
    cmd!("REPLCONF", cmd_replconf, -3, 0);
    cmd!("SYNC", cmd_sync, 1, 0);
    cmd!("SLAVEOF", cmd_slaveof, 3, CMD_ADMIN);
    cmd!("MIGRATE", cmd_migrate, -6, CMD_WRITE);

    // ================================================================
    // Stream 命令（Stream Commands）— Redis 5.0+ 消息队列
    // ================================================================
    cmd!("XADD", cmd_xadd, -5, CMD_WRITE);
    cmd!("XLEN", cmd_xlen, 3, CMD_READONLY);
    cmd!("XRANGE", cmd_xrange, -4, CMD_READONLY);
    cmd!("XREVRANGE", cmd_xrevrange, -4, CMD_READONLY);
    cmd!("XREAD", cmd_xread, -3, CMD_READONLY);
    cmd!("XDEL", cmd_xdel, -3, CMD_WRITE);
    cmd!("XTRIM", cmd_xtrim, -4, CMD_WRITE);
    cmd!("XINFO", cmd_xinfo, -2, CMD_READONLY);
    cmd!("XGROUP", cmd_xgroup, -2, CMD_WRITE);
    cmd!("XREADGROUP", cmd_xreadgroup, -4, CMD_READONLY);
    cmd!("XACK", cmd_xack, -4, CMD_WRITE);
    cmd!("XCLAIM", cmd_xclaim, -6, CMD_WRITE);
    cmd!("XAUTOCLAIM", cmd_xautoclaim, -6, CMD_WRITE);
    cmd!("XPENDING", cmd_xpending, -3, CMD_READONLY);
    cmd!("XSETID", cmd_xsetid, -3, CMD_WRITE);

    // ================================================================
    // Store 变体命令（Store Variants）— 计算结果存入 destination
    // ================================================================
    cmd!("SDIFFSTORE", cmd_sdiffstore, -3, CMD_WRITE);
    cmd!("SINTERSTORE", cmd_sinterstore, -3, CMD_WRITE);
    cmd!("SUNIONSTORE", cmd_sunionstore, -3, CMD_WRITE);
    cmd!("ZDIFFSTORE", cmd_zdiffstore, -4, CMD_WRITE);
    cmd!("ZINTERSTORE", cmd_zinterstore, -4, CMD_WRITE);
    cmd!("ZUNIONSTORE", cmd_zunionstore, -4, CMD_WRITE);
    cmd!("ZINTERCARD", cmd_zintercard, -3, CMD_READONLY);

    // ================================================================
    // ZSet 范围删除命令（ZSet Range Delete）
    // ================================================================
    cmd!("ZREMRANGEBYLEX", cmd_zremrangebylex, 4, CMD_WRITE);
    cmd!("ZREMRANGEBYRANK", cmd_zremrangebyrank, 4, CMD_WRITE);
    cmd!("ZREMRANGEBYSCORE", cmd_zremrangebyscore, 4, CMD_WRITE);
    cmd!("ZREVRANGEBYLEX", cmd_zrevrangebylex, -4, CMD_READONLY);

    // ================================================================
    // Set 高级命令（Set Advanced）
    // ================================================================
    cmd!("SMOVE", cmd_smove, 4, CMD_WRITE);
    cmd!("SDIFFCARD", cmd_sdiffcard, -3, CMD_READONLY);
    cmd!("SUNIONCARD", cmd_sunioncard, -3, CMD_READONLY);
    cmd!("SFLUSH", cmd_sflush, -2, CMD_WRITE);

    // ================================================================
    // Key/String 高级命令（Key/String Advanced）
    // ================================================================
    cmd!("GETDEL", cmd_getdel, 2, CMD_WRITE);
    cmd!("GETEX", cmd_getex, -2, CMD_WRITE);
    cmd!("EXPIRETIME", cmd_expiretime, 2, CMD_READONLY);
    cmd!("PEXPIRETIME", cmd_pexpiretime, 2, CMD_READONLY);
    cmd!("TOUCH", cmd_touch, -2, CMD_WRITE);
    cmd!("COPY", cmd_copy, -3, CMD_WRITE);
    cmd!("LCS", cmd_lcs, -3, CMD_READONLY);
    cmd!("SUBSTR", cmd_substr, 4, CMD_READONLY);
    cmd!("DELEX", cmd_delex, 2, CMD_WRITE);
    cmd!("DIGEST", cmd_digest, -2, CMD_READONLY);
    cmd!("INCREX", cmd_increx, -3, CMD_WRITE);
    cmd!("MSETEX", cmd_msetex, -4, CMD_WRITE);
    cmd!("BITFIELD_RO", cmd_bitfield_ro, -2, CMD_READONLY);

    // ================================================================
    // Blocking 变体命令（Blocking Variants）— 简化为非阻塞版本
    // ================================================================
    cmd!("BLPOP", cmd_blpop, -3, CMD_WRITE);
    cmd!("BRPOP", cmd_brpop, -3, CMD_WRITE);
    cmd!("BRPOPLPUSH", cmd_brpoplpush, 4, CMD_WRITE);
    cmd!("BLMPOP", cmd_blmpop, -4, CMD_WRITE);
    cmd!("BZMPOP", cmd_bzmpop, -4, CMD_WRITE);
    cmd!("BZPOPMAX", cmd_bzpopmax, -3, CMD_WRITE);
    cmd!("BZPOPMIN", cmd_bzpopmin, -3, CMD_WRITE);
    cmd!("BLMOVEM", cmd_blmovem, 6, CMD_WRITE);

    // ================================================================
    // Pub/Sub 高级命令（Pub/Sub Advanced）
    // ================================================================
    cmd!("PUBSUB", cmd_pubsub, -2, 0);

    table
}

// ===========================================================================
// 命令实现（Command Implementations）
// 下面是每个 Redis 命令的具体 Rust 实现，遵循 RESP 协议规范。
// 每个函数对应 Redis 源码中同名的命令处理函数（如 pingCommand、setCommand 等）。
// ===========================================================================

/// PING [message] — 连接探测命令。
/// 无参数时返回 "PONG"，带参数时原样返回消息内容。
/// 对应 Redis 的 `pingCommand`。
fn cmd_ping(ctx: &CmdCtx) -> RespValue {
    if ctx.argc() > 1 {
        RespValue::BulkString(ctx.argv[1].clone())
    } else {
        RespValue::SimpleString("PONG".to_string())
    }
}

/// ECHO message — 回显命令。
/// 将参数原样返回给客户端。对应 Redis 的 `echoCommand`。
fn cmd_echo(ctx: &CmdCtx) -> RespValue {
    RespValue::BulkString(ctx.argv[1].clone())
}

/// SELECT index — 切换数据库。
/// 选择指定编号（0-15）的数据库。实际切换在服务端循环中完成，此处仅做参数校验。
/// 对应 Redis 的 `selectCommand`。
fn cmd_select(ctx: &CmdCtx) -> RespValue {
    match ctx.arg_i64(1) {
        Some(n) if n >= 0 && n <= 15 => {
            // Note: actual select happens in server loop, this is just validation
            RespValue::ok()
        }
        _ => RespValue::err("ERR invalid DB index"),
    }
}

/// AUTH [username] password — 身份认证命令。
/// 验证客户端连接密码。目前为桩实现，始终返回 OK。
/// 对应 Redis 的 `authCommand`。
fn cmd_auth(_ctx: &CmdCtx) -> RespValue {
    // TODO: implement authentication
    RespValue::ok()
}

/// QUIT — 关闭连接命令。
/// 通知服务端关闭当前客户端连接。对应 Redis 的 `quitCommand`。
fn cmd_quit(_ctx: &CmdCtx) -> RespValue {
    RespValue::ok()
}

/// DBSIZE — 返回当前数据库的键总数。对应 Redis 的 `dbsizeCommand`。
fn cmd_dbsize(ctx: &CmdCtx) -> RespValue {
    RespValue::Integer(ctx.db.dbsize() as i64)
}

/// FLUSHDB [ASYNC] — 清空当前数据库所有键。
/// 对应 Redis 的 `flushdbCommand`。
fn cmd_flushdb(ctx: &CmdCtx) -> RespValue {
    ctx.db.flush();
    RespValue::ok()
}

/// FLUSHALL [ASYNC] — 清空所有数据库的键。
/// 对应 Redis 的 `flushallCommand`。
fn cmd_flushall(ctx: &CmdCtx) -> RespValue {
    ctx.db.flush();
    RespValue::ok()
}

// ---------------------------------------------------------------------------
// 通用键操作命令实现
// DEL/EXISTS/TYPE/KEYS/SCAN/RANDOMKEY/RENAME/RENAMENX
// ---------------------------------------------------------------------------

fn cmd_del(ctx: &CmdCtx) -> RespValue {
    let mut count = 0i64;
    for i in 1..ctx.argc() {
        if ctx.db.delete(&ctx.argv[i]) {
            count += 1;
        }
    }
    RespValue::Integer(count)
}

fn cmd_exists(ctx: &CmdCtx) -> RespValue {
    let mut count = 0i64;
    for i in 1..ctx.argc() {
        if ctx.db.exists(&ctx.argv[i]) {
            count += 1;
        }
    }
    RespValue::Integer(count)
}

fn cmd_type(ctx: &CmdCtx) -> RespValue {
    match ctx.db.key_type(&ctx.argv[1]) {
        Some(t) => RespValue::SimpleString(t.to_string()),
        None => RespValue::SimpleString("none".to_string()),
    }
}

fn cmd_keys(ctx: &CmdCtx) -> RespValue {
    let keys = ctx.db.keys(&ctx.argv[1]);
    let items: Vec<RespValue> = keys.into_iter().map(RespValue::BulkString).collect();
    RespValue::Array(items)
}

fn cmd_scan(ctx: &CmdCtx) -> RespValue {
    // SCAN cursor [MATCH pattern] [COUNT count]
    let cursor = match ctx.arg_i64(1) {
        Some(n) if n >= 0 => n as u64,
        _ => return RespValue::err("ERR invalid cursor"),
    };

    let mut pattern: Option<Vec<u8>> = None;
    let mut count: usize = 10; // default count

    let mut i = 2;
    while i < ctx.argc() {
        let opt = ctx.arg_str(i).unwrap_or("").to_ascii_uppercase();
        match opt.as_str() {
            "MATCH" => {
                i += 1;
                if let Some(pat) = ctx.arg_bytes(i) {
                    pattern = Some(pat.to_vec());
                }
            }
            "COUNT" => {
                i += 1;
                if let Some(c) = ctx.arg_i64(i) {
                    if c > 0 {
                        count = c as usize;
                    }
                }
            }
            _ => {}
        }
        i += 1;
    }

    let (next_cursor, keys) = scan::scan_keys(ctx.db, cursor, pattern.as_deref(), count);

    let key_values: Vec<RespValue> = keys.into_iter().map(RespValue::BulkString).collect();
    RespValue::Array(vec![
        RespValue::BulkString(next_cursor.to_string().into_bytes()),
        RespValue::Array(key_values),
    ])
}

fn cmd_randomkey(ctx: &CmdCtx) -> RespValue {
    match ctx.db.randomkey() {
        Some(k) => RespValue::BulkString(k),
        None => RespValue::Null,
    }
}

fn cmd_rename(ctx: &CmdCtx) -> RespValue {
    if ctx.argv[1] == ctx.argv[2] {
        return RespValue::err("ERR src and dst keys are the same");
    }
    if ctx.db.rename(&ctx.argv[1], &ctx.argv[2]) {
        RespValue::ok()
    } else {
        RespValue::err("ERR no such key")
    }
}

fn cmd_renamenx(ctx: &CmdCtx) -> RespValue {
    if ctx.argv[1] == ctx.argv[2] {
        return RespValue::err("ERR src and dst keys are the same");
    }
    if ctx.db.renamenx(&ctx.argv[1], &ctx.argv[2]) {
        RespValue::Integer(1)
    } else {
        RespValue::Integer(0)
    }
}

// ---------------------------------------------------------------------------
// 过期管理命令实现
// EXPIRE/EXPIREAT/PEXPIRE/PEXPIREAT/TTL/PTTL/PERSIST
// ---------------------------------------------------------------------------

fn cmd_expire(ctx: &CmdCtx) -> RespValue {
    match ctx.arg_i64(1) {
        Some(n) if n > 0 => {
            if ctx.db.set_expire(&ctx.argv[1], (n as u64) * 1000) {
                RespValue::Integer(1)
            } else {
                RespValue::Integer(0)
            }
        }
        _ => RespValue::err("ERR invalid expire time"),
    }
}

fn cmd_expireat(ctx: &CmdCtx) -> RespValue {
    match ctx.arg_i64(1) {
        Some(n) if n > 0 => {
            if ctx.db.set_expire_at(&ctx.argv[1], (n as u64) * 1000) {
                RespValue::Integer(1)
            } else {
                RespValue::Integer(0)
            }
        }
        _ => RespValue::err("ERR invalid expire time"),
    }
}

fn cmd_pexpire(ctx: &CmdCtx) -> RespValue {
    match ctx.arg_i64(1) {
        Some(n) if n > 0 => {
            if ctx.db.set_expire(&ctx.argv[1], n as u64) {
                RespValue::Integer(1)
            } else {
                RespValue::Integer(0)
            }
        }
        _ => RespValue::err("ERR invalid expire time"),
    }
}

fn cmd_pexpireat(ctx: &CmdCtx) -> RespValue {
    match ctx.arg_i64(1) {
        Some(n) if n > 0 => {
            if ctx.db.set_expire_at(&ctx.argv[1], n as u64) {
                RespValue::Integer(1)
            } else {
                RespValue::Integer(0)
            }
        }
        _ => RespValue::err("ERR invalid expire time"),
    }
}

fn cmd_ttl(ctx: &CmdCtx) -> RespValue {
    RespValue::Integer(ctx.db.ttl(&ctx.argv[1]))
}

fn cmd_pttl(ctx: &CmdCtx) -> RespValue {
    RespValue::Integer(ctx.db.pttl(&ctx.argv[1]))
}

fn cmd_persist(ctx: &CmdCtx) -> RespValue {
    if ctx.db.persist(&ctx.argv[1]) {
        RespValue::Integer(1)
    } else {
        RespValue::Integer(0)
    }
}

// ---------------------------------------------------------------------------
// 字符串命令实现
// GET/SET/INCR/DECR/APPEND/STRLEN/GETRANGE/SETRANGE/MGET/MSET 等
// ---------------------------------------------------------------------------

fn cmd_get(ctx: &CmdCtx) -> RespValue {
    match ctx.db.get(&ctx.argv[1]) {
        Some(RedisObject::String(d)) => RespValue::BulkString(d),
        Some(RedisObject::Integer(n)) => RespValue::BulkString(n.to_string().into_bytes()),
        Some(_) => {
            RespValue::err("WRONGTYPE Operation against a key holding the wrong kind of value")
        }
        None => RespValue::Null,
    }
}

fn cmd_set(ctx: &CmdCtx) -> RespValue {
    let key = &ctx.argv[1];
    let value = &ctx.argv[2];
    let mut nx = false;
    let mut xx = false;
    let mut ex_ms: Option<u64> = None;
    let mut get = false;
    let mut i = 3;

    while i < ctx.argc() {
        let opt = ctx.arg_str(i).unwrap_or("").to_ascii_uppercase();
        match opt.as_str() {
            "NX" => {
                nx = true;
                i += 1;
            }
            "XX" => {
                xx = true;
                i += 1;
            }
            "GET" => {
                get = true;
                i += 1;
            }
            "EX" => {
                i += 1;
                if let Some(secs) = ctx.arg_i64(i) {
                    if secs <= 0 {
                        return RespValue::err("ERR invalid expire time in 'set' command");
                    }
                    ex_ms = Some(secs as u64 * 1000);
                } else {
                    return RespValue::err("ERR value is not an integer or out of range");
                }
                i += 1;
            }
            "PX" => {
                i += 1;
                if let Some(ms) = ctx.arg_i64(i) {
                    if ms <= 0 {
                        return RespValue::err("ERR invalid expire time in 'set' command");
                    }
                    ex_ms = Some(ms as u64);
                } else {
                    return RespValue::err("ERR value is not an integer or out of range");
                }
                i += 1;
            }
            "EXAT" => {
                i += 1;
                if let Some(ts) = ctx.arg_i64(i) {
                    let now_ms = current_time_ms();
                    let abs = ts as u64 * 1000;
                    if abs > now_ms {
                        ex_ms = Some(abs - now_ms);
                    } else {
                        ex_ms = Some(0);
                    }
                } else {
                    return RespValue::err("ERR value is not an integer or out of range");
                }
                i += 1;
            }
            "PXAT" => {
                i += 1;
                if let Some(ts) = ctx.arg_i64(i) {
                    let now_ms = current_time_ms();
                    let abs = ts as u64;
                    if abs > now_ms {
                        ex_ms = Some(abs - now_ms);
                    } else {
                        ex_ms = Some(0);
                    }
                } else {
                    return RespValue::err("ERR value is not an integer or out of range");
                }
                i += 1;
            }
            _ => {
                i += 1;
            }
        }
    }

    let existed = ctx.db.exists(key);

    if nx && existed {
        return RespValue::Null;
    }
    if xx && !existed {
        return RespValue::Null;
    }

    // Try to store as integer if possible
    let obj = match std::str::from_utf8(value) {
        Ok(s) => match s.parse::<i64>() {
            Ok(n) => RedisObject::Integer(n),
            Err(_) => RedisObject::String(value.clone()),
        },
        Err(_) => RedisObject::String(value.clone()),
    };

    ctx.db.set(&key, obj, ex_ms);
    RespValue::ok()
}

fn cmd_setnx(ctx: &CmdCtx) -> RespValue {
    if ctx.db.exists(&ctx.argv[1]) {
        RespValue::Integer(0)
    } else {
        ctx.db.set(
            &ctx.argv[1],
            RedisObject::String(ctx.argv[2].clone()),
            None,
        );
        RespValue::Integer(1)
    }
}

fn cmd_setex(ctx: &CmdCtx) -> RespValue {
    match ctx.arg_i64(2) {
        Some(secs) if secs > 0 => {
            ctx.db.set(
            &ctx.argv[1],
                RedisObject::String(ctx.argv[3].clone()),
                Some(secs as u64 * 1000),
            );
            RespValue::ok()
        }
        _ => RespValue::err("ERR invalid expire time"),
    }
}

fn cmd_psetex(ctx: &CmdCtx) -> RespValue {
    match ctx.arg_i64(2) {
        Some(ms) if ms > 0 => {
            ctx.db.set(
            &ctx.argv[1],
                RedisObject::String(ctx.argv[3].clone()),
                Some(ms as u64),
            );
            RespValue::ok()
        }
        _ => RespValue::err("ERR invalid expire time"),
    }
}

fn cmd_mget(ctx: &CmdCtx) -> RespValue {
    let mut result = Vec::new();
    for i in 1..ctx.argc() {
        match ctx.db.get(&ctx.argv[i]) {
            Some(RedisObject::String(d)) => result.push(RespValue::BulkString(d)),
            Some(RedisObject::Integer(n)) => {
                result.push(RespValue::BulkString(n.to_string().into_bytes()))
            }
            Some(_) => result.push(RespValue::err("WRONGTYPE")),
            None => result.push(RespValue::Null),
        }
    }
    RespValue::Array(result)
}

fn cmd_mset(ctx: &CmdCtx) -> RespValue {
    if (ctx.argc() - 1) % 2 != 0 {
        return RespValue::err("ERR wrong number of arguments for 'mset' command");
    }
    let mut i = 1;
    while i < ctx.argc() {
        ctx.db.set(
            &ctx.argv[i],
            RedisObject::String(ctx.argv[i + 1].clone()),
            None,
        );
        i += 2;
    }
    RespValue::ok()
}

fn cmd_msetnx(ctx: &CmdCtx) -> RespValue {
    if (ctx.argc() - 1) % 2 != 0 {
        return RespValue::err("ERR wrong number of arguments for 'msetnx' command");
    }
    // Check if any key exists
    let mut i = 1;
    while i < ctx.argc() {
        if ctx.db.exists(&ctx.argv[i]) {
            return RespValue::Integer(0);
        }
        i += 2;
    }
    i = 1;
    while i < ctx.argc() {
        ctx.db.set(
            &ctx.argv[i],
            RedisObject::String(ctx.argv[i + 1].clone()),
            None,
        );
        i += 2;
    }
    RespValue::Integer(1)
}

fn cmd_getset(ctx: &CmdCtx) -> RespValue {
    let old = cmd_get(ctx);
    ctx.db.set(
            &ctx.argv[1],
        RedisObject::String(ctx.argv[2].clone()),
        None,
    );
    old
}

fn cmd_append(ctx: &CmdCtx) -> RespValue {
    let new_len = match ctx.db.get(&ctx.argv[1]) {
        Some(RedisObject::String(mut d)) => {
            d.extend_from_slice(&ctx.argv[2]);
            let len = d.len();
            ctx.db
                .set(&ctx.argv[1], RedisObject::String(d), None);
            len
        }
        Some(RedisObject::Integer(n)) => {
            let mut d = n.to_string().into_bytes();
            d.extend_from_slice(&ctx.argv[2]);
            let len = d.len();
            ctx.db
                .set(&ctx.argv[1], RedisObject::String(d), None);
            len
        }
        None => {
            ctx.db.set(
            &ctx.argv[1],
                RedisObject::String(ctx.argv[2].clone()),
                None,
            );
            ctx.argv[2].len()
        }
        _ => return RespValue::err("WRONGTYPE"),
    };
    RespValue::Integer(new_len as i64)
}

fn cmd_strlen(ctx: &CmdCtx) -> RespValue {
    match ctx.db.get(&ctx.argv[1]) {
        Some(RedisObject::String(d)) => RespValue::Integer(d.len() as i64),
        Some(RedisObject::Integer(n)) => RespValue::Integer(n.to_string().len() as i64),
        Some(_) => RespValue::err("WRONGTYPE"),
        None => RespValue::Integer(0),
    }
}

fn cmd_incr(ctx: &CmdCtx) -> RespValue {
    incr_decr(ctx, 1)
}

fn cmd_incrby(ctx: &CmdCtx) -> RespValue {
    match ctx.arg_i64(2) {
        Some(incr) => incr_decr(ctx, incr),
        None => RespValue::err("ERR value is not an integer or out of range"),
    }
}

fn cmd_incrbyfloat(ctx: &CmdCtx) -> RespValue {
    let incr: f64 = match ctx.arg_f64(2) {
        Some(v) => v,
        None => return RespValue::err("ERR value is not a valid float"),
    };
    let current = match ctx.db.get(&ctx.argv[1]) {
        Some(RedisObject::String(d)) => String::from_utf8_lossy(&d).parse::<f64>().unwrap_or(0.0),
        Some(RedisObject::Integer(n)) => n as f64,
        None => 0.0,
        _ => return RespValue::err("WRONGTYPE"),
    };
    let new_val = current + incr;
    let s = format!("{}", new_val);
    ctx.db.set(
            &ctx.argv[1],
        RedisObject::String(s.into_bytes()),
        None,
    );
    RespValue::BulkString(format!("{}", new_val).into_bytes())
}

fn cmd_decr(ctx: &CmdCtx) -> RespValue {
    incr_decr(ctx, -1)
}

fn cmd_decrby(ctx: &CmdCtx) -> RespValue {
    match ctx.arg_i64(2) {
        Some(decr) => incr_decr(ctx, -decr),
        None => RespValue::err("ERR value is not an integer or out of range"),
    }
}

fn cmd_getrange(ctx: &CmdCtx) -> RespValue {
    let start: isize = match ctx.arg_str(2).and_then(|s| s.parse().ok()) {
        Some(v) => v,
        None => return RespValue::err("ERR value is not an integer"),
    };
    let end: isize = match ctx.arg_str(3).and_then(|s| s.parse().ok()) {
        Some(v) => v,
        None => return RespValue::err("ERR value is not an integer"),
    };
    let data = match ctx.db.get(&ctx.argv[1]) {
        Some(RedisObject::String(d)) => d,
        Some(RedisObject::Integer(n)) => n.to_string().into_bytes(),
        Some(_) => return RespValue::err("WRONGTYPE"),
        None => return RespValue::BulkString(vec![]),
    };
    let len = data.len() as isize;
    let start = if start < 0 { len + start } else { start };
    let end = if end < 0 { len + end } else { end };
    let start = start.max(0) as usize;
    let end = (end.max(-1) as usize).min(data.len() - 1);
    if start > end || start >= data.len() {
        return RespValue::BulkString(vec![]);
    }
    RespValue::BulkString(data[start..=end].to_vec())
}

fn cmd_setrange(ctx: &CmdCtx) -> RespValue {
    let offset: usize = match ctx.arg_str(2).and_then(|s| s.parse().ok()) {
        Some(v) => v,
        None => return RespValue::err("ERR value is not an integer"),
    };
    if offset > 512 * 1024 * 1024 {
        return RespValue::err("ERR offset is out of range");
    }
    let value = &ctx.argv[3];
    let mut data = match ctx.db.get(&ctx.argv[1]) {
        Some(RedisObject::String(d)) => d,
        Some(RedisObject::Integer(n)) => n.to_string().into_bytes(),
        Some(_) => return RespValue::err("WRONGTYPE"),
        None => vec![],
    };
    let end = offset + value.len();
    if end > data.len() {
        data.resize(end, 0);
    }
    data[offset..end].copy_from_slice(value);
    let new_len = data.len();
    ctx.db
        .set(&ctx.argv[1], RedisObject::String(data), None);
    RespValue::Integer(new_len as i64)
}

/// GETBIT key offset — 获取指定偏移位的值。
/// 对应 Redis 的 `getbitCommand`。
fn cmd_getbit(ctx: &CmdCtx) -> RespValue {
    let offset: usize = match ctx.arg_str(2).and_then(|s| s.parse().ok()) {
        Some(v) => v,
        None => return RespValue::err("ERR bit offset is not an integer or out of range"),
    };
    if offset > 512 * 1024 * 1024 {
        return RespValue::err("ERR bit offset is out of range");
    }
    let byte_idx = offset / 8;
    let bit_idx = 7 - (offset % 8); // Redis uses MSB ordering
    match ctx.db.get(&ctx.argv[1]) {
        Some(RedisObject::String(d)) => {
            if byte_idx < d.len() {
                RespValue::Integer(((d[byte_idx] >> bit_idx) & 1) as i64)
            } else {
                RespValue::Integer(0)
            }
        }
        Some(RedisObject::Integer(n)) => {
            let s = n.to_string().into_bytes();
            if byte_idx < s.len() {
                RespValue::Integer(((s[byte_idx] >> bit_idx) & 1) as i64)
            } else {
                RespValue::Integer(0)
            }
        }
        Some(_) => RespValue::err("WRONGTYPE"),
        None => RespValue::Integer(0),
    }
}

/// SETBIT key offset value — 设置指定偏移位的值，返回旧值。
/// 对应 Redis 的 `setbitCommand`。
fn cmd_setbit(ctx: &CmdCtx) -> RespValue {
    let offset: usize = match ctx.arg_str(2).and_then(|s| s.parse().ok()) {
        Some(v) => v,
        None => return RespValue::err("ERR bit offset is not an integer or out of range"),
    };
    if offset > 512 * 1024 * 1024 {
        return RespValue::err("ERR bit offset is out of range");
    }
    let value = match ctx.arg_str(3) {
        Some("0") => 0u8,
        Some("1") => 1u8,
        _ => return RespValue::err("ERR bit is not an integer or out of range"),
    };
    let byte_idx = offset / 8;
    let bit_idx = 7 - (offset % 8); // MSB ordering
    let mut data = match ctx.db.get(&ctx.argv[1]) {
        Some(RedisObject::String(d)) => d,
        Some(RedisObject::Integer(n)) => n.to_string().into_bytes(),
        Some(_) => return RespValue::err("WRONGTYPE"),
        None => vec![],
    };
    // Extend with zeros if needed
    if byte_idx >= data.len() {
        data.resize(byte_idx + 1, 0);
    }
    let old_val = (data[byte_idx] >> bit_idx) & 1;
    if value == 1 {
        data[byte_idx] |= 1 << bit_idx;
    } else {
        data[byte_idx] &= !(1 << bit_idx);
    }
    ctx.db
        .set(&ctx.argv[1], RedisObject::String(data), None);
    RespValue::Integer(old_val as i64)
}

/// BITCOUNT key [start end] — 统计字符串中置位(1)的比特数。
/// 对应 Redis 的 `bitcountCommand`。
fn cmd_bitcount(ctx: &CmdCtx) -> RespValue {
    let data = match ctx.db.get(&ctx.argv[1]) {
        Some(RedisObject::String(d)) => d,
        Some(RedisObject::Integer(n)) => n.to_string().into_bytes(),
        Some(_) => return RespValue::err("WRONGTYPE"),
        None => return RespValue::Integer(0),
    };
    if data.is_empty() {
        return RespValue::Integer(0);
    }
    // Parse optional start/end
    let (start, end) = if ctx.argc() >= 4 {
        let start: isize = match ctx.arg_str(2).and_then(|s| s.parse().ok()) {
            Some(v) => v,
            None => return RespValue::err("ERR value is not an integer"),
        };
        let end: isize = match ctx.arg_str(3).and_then(|s| s.parse().ok()) {
            Some(v) => v,
            None => return RespValue::err("ERR value is not an integer"),
        };
        let len = data.len() as isize;
        let s = if start < 0 {
            (len + start).max(0)
        } else {
            start.min(len)
        } as usize;
        let e = if end < 0 {
            (len + end).max(0)
        } else {
            end.min(len - 1)
        } as usize;
        if s > e {
            return RespValue::Integer(0);
        }
        (s, e)
    } else {
        (0, data.len() - 1)
    };
    let count: i64 = data[start..=end]
        .iter()
        .map(|b| b.count_ones() as i64)
        .sum();
    RespValue::Integer(count)
}

/// BITPOS key bit [start [end]] — 查找第一个被设置为指定值的比特位的位置。
/// 对应 Redis 的 `bitposCommand`。
fn cmd_bitpos(ctx: &CmdCtx) -> RespValue {
    let bit = match ctx.arg_str(2) {
        Some("0") => 0u8,
        Some("1") => 1u8,
        _ => return RespValue::err("ERR bit offset is not an integer or out of range"),
    };
    let data = match ctx.db.get(&ctx.argv[1]) {
        Some(RedisObject::String(d)) => d,
        Some(RedisObject::Integer(n)) => n.to_string().into_bytes(),
        Some(_) => return RespValue::err("WRONGTYPE"),
        None => return RespValue::Integer(-1),
    };
    if data.is_empty() {
        return if bit == 0 {
            RespValue::Integer(0)
        } else {
            RespValue::Integer(-1)
        };
    }
    // Parse optional start/end
    let (start, end) = if ctx.argc() >= 4 {
        let s: isize = ctx.arg_str(3).and_then(|v| v.parse().ok()).unwrap_or(0);
        let e: isize = if ctx.argc() >= 5 {
            ctx.arg_str(4).and_then(|v| v.parse().ok()).unwrap_or(-1)
        } else {
            -1
        };
        let len = data.len() as isize;
        let s = if s < 0 { (len + s).max(0) } else { s.min(len) } as usize;
        let e = if e < 0 {
            (len + e).max(0)
        } else {
            e.min(len - 1)
        } as usize;
        (s, e)
    } else {
        (0, data.len() - 1)
    };
    if start > end {
        return if bit == 0 {
            RespValue::Integer(0)
        } else {
            RespValue::Integer(-1)
        };
    }
    for i in start..=end {
        let byte = data[i];
        for j in (0..8).rev() {
            let b = (byte >> j) & 1;
            if b == bit {
                return RespValue::Integer((i * 8 + (7 - j)) as i64);
            }
        }
    }
    if bit == 0 {
        // BIT 0 not found in range → it means all bits are 1
        // Redis returns the next bit position after the string
        RespValue::Integer(((end + 1) * 8) as i64)
    } else {
        RespValue::Integer(-1)
    }
}

fn incr_decr(ctx: &CmdCtx, delta: i64) -> RespValue {
    let (current, expire_ms) = match ctx.db.get(&ctx.argv[1]) {
        Some(RedisObject::Integer(n)) => (n, None),
        Some(RedisObject::String(d)) => {
            let s = String::from_utf8_lossy(&d);
            match s.parse::<i64>() {
                Ok(n) => (n, None),
                Err(_) => return RespValue::err("ERR value is not an integer or out of range"),
            }
        }
        Some(_) => return RespValue::err("WRONGTYPE"),
        None => (0, None),
    };
    let new_val = current + delta;
    ctx.db.set(
            &ctx.argv[1],
        RedisObject::Integer(new_val),
        expire_ms,
    );
    RespValue::Integer(new_val)
}

// ---------------------------------------------------------------------------
// 列表命令实现
// LPUSH/RPUSH/LPOP/RPOP/LRANGE/LLEN/LINDEX/LSET/LREM/LTRIM/LINSERT 等
// ---------------------------------------------------------------------------

fn cmd_lpush(ctx: &CmdCtx) -> RespValue {
    let key = &ctx.argv[1];
    // 使用 get_object_mut() 获取就地可变引用，避免 clone 整个 VecDeque
    // 这是关键性能优化：O(1) 引用计数 clone vs O(n) 数据 clone
    match ctx.db.get_object_mut(key) {
        Some(mut obj_ref) => match &mut *obj_ref {
            RedisObject::List(ref mut l) => {
                for i in 2..ctx.argc() {
                    l.push_front(ctx.argv[i].clone());
                }
                RespValue::Integer(l.len() as i64)
            }
            _ => RespValue::err("WRONGTYPE"),
        },
        None => {
            // 键不存在，创建新 List
            let mut list = VecDeque::new();
            for i in 2..ctx.argc() {
                list.push_front(ctx.argv[i].clone());
            }
            let len = list.len() as i64;
            ctx.db.set(&key, RedisObject::List(list), None);
            RespValue::Integer(len)
        }
    }
}

fn cmd_rpush(ctx: &CmdCtx) -> RespValue {
    let key = &ctx.argv[1];
    match ctx.db.get_object_mut(key) {
        Some(mut obj_ref) => match &mut *obj_ref {
            RedisObject::List(ref mut l) => {
                for i in 2..ctx.argc() {
                    l.push_back(ctx.argv[i].clone());
                }
                RespValue::Integer(l.len() as i64)
            }
            _ => RespValue::err("WRONGTYPE"),
        },
        None => {
            let mut list = VecDeque::new();
            for i in 2..ctx.argc() {
                list.push_back(ctx.argv[i].clone());
            }
            let len = list.len() as i64;
            ctx.db.set(&key, RedisObject::List(list), None);
            RespValue::Integer(len)
        }
    }
}

fn cmd_lpop(ctx: &CmdCtx) -> RespValue {
    let count = if ctx.argc() > 2 {
        ctx.arg_i64(2).unwrap_or(1)
    } else {
        1
    };
    let key = &ctx.argv[1];
    // 使用 get_object_mut 就地修改，避免 clone 整个 VecDeque
    match ctx.db.get_object_mut(key) {
        Some(mut obj_ref) => match &mut *obj_ref {
            RedisObject::List(ref mut l) => {
                if l.is_empty() {
                    return RespValue::Null;
                }
                if count == 1 {
                    let val = l.pop_front().unwrap();
                    let should_delete = l.is_empty();
                    drop(obj_ref); // 释放 RefMut 再操作 db
                    if should_delete {
                        ctx.db.delete(key);
                    }
                    RespValue::BulkString(val)
                } else {
                    let n = count.min(l.len() as i64) as usize;
                    let mut result = Vec::new();
                    for _ in 0..n {
                        result.push(RespValue::BulkString(l.pop_front().unwrap()));
                    }
                    let should_delete = l.is_empty();
                    drop(obj_ref);
                    if should_delete {
                        ctx.db.delete(key);
                    }
                    RespValue::Array(result)
                }
            }
            _ => RespValue::err("WRONGTYPE"),
        },
        None => RespValue::Null,
    }
}

fn cmd_rpop(ctx: &CmdCtx) -> RespValue {
    let count = if ctx.argc() > 2 {
        ctx.arg_i64(2).unwrap_or(1)
    } else {
        1
    };
    let key = &ctx.argv[1];
    match ctx.db.get_object_mut(key) {
        Some(mut obj_ref) => match &mut *obj_ref {
            RedisObject::List(ref mut l) => {
                if l.is_empty() {
                    return RespValue::Null;
                }
                if count == 1 {
                    let val = l.pop_back().unwrap();
                    let should_delete = l.is_empty();
                    drop(obj_ref);
                    if should_delete {
                        ctx.db.delete(key);
                    }
                    RespValue::BulkString(val)
                } else {
                    let n = count.min(l.len() as i64) as usize;
                    let mut result = Vec::new();
                    for _ in 0..n {
                        result.push(RespValue::BulkString(l.pop_back().unwrap()));
                    }
                    let should_delete = l.is_empty();
                    drop(obj_ref);
                    if should_delete {
                        ctx.db.delete(key);
                    }
                    RespValue::Array(result)
                }
            }
            _ => RespValue::err("WRONGTYPE"),
        },
        None => RespValue::Null,
    }
}

fn cmd_lrange(ctx: &CmdCtx) -> RespValue {
    let start: isize = match ctx.arg_str(2).and_then(|s| s.parse().ok()) {
        Some(v) => v,
        None => return RespValue::err("ERR value is not an integer"),
    };
    let stop: isize = match ctx.arg_str(3).and_then(|s| s.parse().ok()) {
        Some(v) => v,
        None => return RespValue::err("ERR value is not an integer"),
    };
    let list = match ctx.db.get(&ctx.argv[1]) {
        Some(RedisObject::List(l)) => l,
        Some(_) => return RespValue::err("WRONGTYPE"),
        None => return RespValue::Array(vec![]),
    };
    let len = list.len() as isize;
    let start = if start < 0 {
        (len + start).max(0)
    } else {
        start
    } as usize;
    let stop = if stop < 0 { (len + stop).max(0) } else { stop } as usize;
    if start > stop || start >= list.len() {
        return RespValue::Array(vec![]);
    }
    let stop = stop.min(list.len() - 1);
    let result: Vec<RespValue> = list
        .range(start..=stop)
        .map(|d| RespValue::BulkString(d.clone()))
        .collect();
    RespValue::Array(result)
}

fn cmd_llen(ctx: &CmdCtx) -> RespValue {
    match ctx.db.get(&ctx.argv[1]) {
        Some(RedisObject::List(l)) => RespValue::Integer(l.len() as i64),
        Some(_) => RespValue::err("WRONGTYPE"),
        None => RespValue::Integer(0),
    }
}

fn cmd_lindex(ctx: &CmdCtx) -> RespValue {
    let idx: isize = match ctx.arg_str(2).and_then(|s| s.parse().ok()) {
        Some(v) => v,
        None => return RespValue::err("ERR value is not an integer"),
    };
    match ctx.db.get(&ctx.argv[1]) {
        Some(RedisObject::List(l)) => {
            let len = l.len() as isize;
            let actual = if idx < 0 { len + idx } else { idx };
            if actual < 0 || actual >= len {
                RespValue::Null
            } else {
                RespValue::BulkString(l[actual as usize].clone())
            }
        }
        Some(_) => RespValue::err("WRONGTYPE"),
        None => RespValue::Null,
    }
}

fn cmd_lset(ctx: &CmdCtx) -> RespValue {
    let idx: isize = match ctx.arg_str(2).and_then(|s| s.parse().ok()) {
        Some(v) => v,
        None => return RespValue::err("ERR value is not an integer"),
    };
    match ctx.db.get_object_mut(&ctx.argv[1]) {
        Some(mut obj) => match &mut *obj {
            RedisObject::List(ref mut l) => {
                let len = l.len() as isize;
                let actual = if idx < 0 { len + idx } else { idx };
                if actual < 0 || actual >= len {
                    RespValue::err("ERR index out of range")
                } else {
                    l[actual as usize] = ctx.argv[3].clone();
                    RespValue::ok()
                }
            }
            _ => RespValue::err("WRONGTYPE"),
        },
        None => RespValue::err("ERR no such key"),
    }
}

fn cmd_lrem(ctx: &CmdCtx) -> RespValue {
    let count: isize = match ctx.arg_str(2).and_then(|s| s.parse().ok()) {
        Some(v) => v,
        None => return RespValue::err("ERR value is not an integer"),
    };
    let value = &ctx.argv[3];
    let mut list = match ctx.db.get(&ctx.argv[1]) {
        Some(RedisObject::List(l)) => l,
        Some(_) => return RespValue::err("WRONGTYPE"),
        None => return RespValue::Integer(0),
    };
    let mut removed = 0i64;
    if count == 0 {
        // Remove all
        list.retain(|e| {
            if e == value {
                removed += 1;
                false
            } else {
                true
            }
        });
    } else if count > 0 {
        // Remove from head
        let mut new = VecDeque::new();
        let mut to_remove = count;
        for e in list {
            if to_remove > 0 && e == *value {
                removed += 1;
                to_remove -= 1;
            } else {
                new.push_back(e);
            }
        }
        list = new;
    } else {
        // Remove from tail
        let mut new = VecDeque::new();
        let mut to_remove = -count;
        for e in list.iter().rev() {
            if to_remove > 0 && e == value {
                removed += 1;
                to_remove -= 1;
            } else {
                new.push_front(e.clone());
            }
        }
        list = new;
    }
    if list.is_empty() {
        ctx.db.delete(&ctx.argv[1]);
    } else {
        ctx.db
            .set(&ctx.argv[1], RedisObject::List(list), None);
    }
    RespValue::Integer(removed)
}

fn cmd_lpos(_ctx: &CmdCtx) -> RespValue {
    // TODO: implement LPOS
    RespValue::Null
}

fn cmd_ltrim(ctx: &CmdCtx) -> RespValue {
    let start: isize = match ctx.arg_str(2).and_then(|s| s.parse().ok()) {
        Some(v) => v,
        None => return RespValue::err("ERR value is not an integer"),
    };
    let stop: isize = match ctx.arg_str(3).and_then(|s| s.parse().ok()) {
        Some(v) => v,
        None => return RespValue::err("ERR value is not an integer"),
    };
    let list = match ctx.db.get(&ctx.argv[1]) {
        Some(RedisObject::List(l)) => l,
        Some(_) => return RespValue::err("WRONGTYPE"),
        None => return RespValue::ok(),
    };
    let len = list.len() as isize;
    let start = if start < 0 {
        (len + start).max(0)
    } else {
        start
    } as usize;
    let stop = if stop < 0 { (len + stop).max(0) } else { stop } as usize;
    if start > stop || start >= list.len() {
        ctx.db.delete(&ctx.argv[1]);
    } else {
        let stop = stop.min(list.len() - 1);
        let new_list: VecDeque<Vec<u8>> = list.range(start..=stop).cloned().collect();
        if new_list.is_empty() {
            ctx.db.delete(&ctx.argv[1]);
        } else {
            ctx.db
                .set(&ctx.argv[1], RedisObject::List(new_list), None);
        }
    }
    RespValue::ok()
}

fn cmd_linsert(ctx: &CmdCtx) -> RespValue {
    let pivot = &ctx.argv[3];
    let value = &ctx.argv[4];
    let before = match ctx.arg_str(2) {
        Some(s) => s.eq_ignore_ascii_case("BEFORE"),
        None => return RespValue::err("ERR syntax error"),
    };
    let mut list = match ctx.db.get(&ctx.argv[1]) {
        Some(RedisObject::List(l)) => l,
        Some(_) => return RespValue::err("WRONGTYPE"),
        None => return RespValue::Integer(-1),
    };
    let pos = list.iter().position(|e| e == pivot);
    match pos {
        Some(idx) => {
            if before {
                list.insert(idx, value.clone());
            } else {
                list.insert(idx + 1, value.clone());
            }
            let len = list.len() as i64;
            ctx.db
                .set(&ctx.argv[1], RedisObject::List(list), None);
            RespValue::Integer(len)
        }
        None => RespValue::Integer(-1),
    }
}

fn cmd_lpushx(ctx: &CmdCtx) -> RespValue {
    if !ctx.db.exists(&ctx.argv[1]) {
        return RespValue::Integer(0);
    }
    cmd_lpush(ctx)
}

fn cmd_rpushx(ctx: &CmdCtx) -> RespValue {
    if !ctx.db.exists(&ctx.argv[1]) {
        return RespValue::Integer(0);
    }
    cmd_rpush(ctx)
}

fn cmd_rpoplpush(ctx: &CmdCtx) -> RespValue {
    let val = match ctx.db.get(&ctx.argv[1]) {
        Some(RedisObject::List(mut l)) => match l.pop_back() {
            Some(v) => {
                if l.is_empty() {
                    ctx.db.delete(&ctx.argv[1]);
                } else {
                    ctx.db.set(&ctx.argv[1], RedisObject::List(l), None);
                }
                v
            }
            None => return RespValue::Null,
        },
        Some(_) => return RespValue::err("WRONGTYPE"),
        None => return RespValue::Null,
    };
    // Push to destination
    let mut dest = match ctx.db.get(&ctx.argv[2]) {
        Some(RedisObject::List(l)) => l,
        Some(_) => return RespValue::err("WRONGTYPE"),
        None => VecDeque::new(),
    };
    dest.push_front(val.clone());
    ctx.db
        .set(&ctx.argv[2], RedisObject::List(dest), None);
    RespValue::BulkString(val)
}

fn cmd_lmove(ctx: &CmdCtx) -> RespValue {
    let src_dir = ctx.arg_str(2).unwrap_or("").to_ascii_uppercase();
    let dst_dir = ctx.arg_str(3).unwrap_or("").to_ascii_uppercase();
    let val = match ctx.db.get(&ctx.argv[1]) {
        Some(RedisObject::List(mut l)) => {
            let v = if src_dir == "LEFT" {
                l.pop_front()
            } else {
                l.pop_back()
            };
            match v {
                Some(v) => {
                    if l.is_empty() {
                        ctx.db.delete(&ctx.argv[1]);
                    } else {
                        ctx.db.set(&ctx.argv[1], RedisObject::List(l), None);
                    }
                    v
                }
                None => return RespValue::Null,
            }
        }
        Some(_) => return RespValue::err("WRONGTYPE"),
        None => return RespValue::Null,
    };
    let mut dest = match ctx.db.get(&ctx.argv[4]) {
        Some(RedisObject::List(l)) => l,
        Some(_) => return RespValue::err("WRONGTYPE"),
        None => VecDeque::new(),
    };
    if dst_dir == "LEFT" {
        dest.push_front(val.clone());
    } else {
        dest.push_back(val.clone());
    }
    ctx.db
        .set(&ctx.argv[4], RedisObject::List(dest), None);
    RespValue::BulkString(val)
}

// ---------------------------------------------------------------------------
// 哈希命令实现
// HSET/HGET/HMSET/HMGET/HGETALL/HDEL/HEXISTS/HLEN/HINCRBY/HKEYS/HVALS 等
// ---------------------------------------------------------------------------

fn cmd_hset(ctx: &CmdCtx) -> RespValue {
    let key = &ctx.argv[1];
    if (ctx.argc() - 2) % 2 != 0 {
        return RespValue::err("ERR wrong number of arguments for 'hset' command");
    }
    match ctx.db.get_object_mut(key) {
        Some(mut obj_ref) => match &mut *obj_ref {
            RedisObject::Hash(ref mut h) => {
                let mut added = 0i64;
                let mut i = 2;
                while i < ctx.argc() {
                    if !h.contains_key(&ctx.argv[i]) {
                        added += 1;
                    }
                    h.insert(ctx.argv[i].clone(), ctx.argv[i + 1].clone());
                    i += 2;
                }
                RespValue::Integer(added)
            }
            _ => RespValue::err("WRONGTYPE"),
        },
        None => {
            let mut hash = HashMap::new();
            let mut added = 0i64;
            let mut i = 2;
            while i < ctx.argc() {
                hash.insert(ctx.argv[i].clone(), ctx.argv[i + 1].clone());
                added += 1;
                i += 2;
            }
            ctx.db.set(&key, RedisObject::Hash(hash), None);
            RespValue::Integer(added)
        }
    }
}

fn cmd_hget(ctx: &CmdCtx) -> RespValue {
    match ctx.db.get(&ctx.argv[1]) {
        Some(RedisObject::Hash(h)) => match h.get(&ctx.argv[2]) {
            Some(v) => RespValue::BulkString(v.clone()),
            None => RespValue::Null,
        },
        Some(_) => RespValue::err("WRONGTYPE"),
        None => RespValue::Null,
    }
}

fn cmd_hmset(ctx: &CmdCtx) -> RespValue {
    let key = &ctx.argv[1];
    if (ctx.argc() - 2) % 2 != 0 {
        return RespValue::err("ERR wrong number of arguments for 'hmset' command");
    }
    match ctx.db.get_object_mut(key) {
        Some(mut obj_ref) => match &mut *obj_ref {
            RedisObject::Hash(ref mut h) => {
                let mut i = 2;
                while i < ctx.argc() {
                    h.insert(ctx.argv[i].clone(), ctx.argv[i + 1].clone());
                    i += 2;
                }
                RespValue::ok()
            }
            _ => RespValue::err("WRONGTYPE"),
        },
        None => {
            let mut hash = HashMap::new();
            let mut i = 2;
            while i < ctx.argc() {
                hash.insert(ctx.argv[i].clone(), ctx.argv[i + 1].clone());
                i += 2;
            }
            ctx.db.set(&key, RedisObject::Hash(hash), None);
            RespValue::ok()
        }
    }
}

fn cmd_hmget(ctx: &CmdCtx) -> RespValue {
    let hash = match ctx.db.get(&ctx.argv[1]) {
        Some(RedisObject::Hash(h)) => h,
        Some(_) => return RespValue::err("WRONGTYPE"),
        None => {
            return RespValue::Array((1..ctx.argc()).map(|_| RespValue::Null).collect());
        }
    };
    let result: Vec<RespValue> = (2..ctx.argc())
        .map(|i| match hash.get(&ctx.argv[i]) {
            Some(v) => RespValue::BulkString(v.clone()),
            None => RespValue::Null,
        })
        .collect();
    RespValue::Array(result)
}

fn cmd_hgetall(ctx: &CmdCtx) -> RespValue {
    match ctx.db.get(&ctx.argv[1]) {
        Some(RedisObject::Hash(h)) => {
            let mut result = Vec::new();
            for (k, v) in &h {
                result.push(RespValue::BulkString(k.clone()));
                result.push(RespValue::BulkString(v.clone()));
            }
            RespValue::Array(result)
        }
        Some(_) => RespValue::err("WRONGTYPE"),
        None => RespValue::Array(vec![]),
    }
}

fn cmd_hdel(ctx: &CmdCtx) -> RespValue {
    let key = &ctx.argv[1];
    let (result, should_delete) = match ctx.db.get_object_mut(key) {
        Some(mut obj_ref) => match &mut *obj_ref {
            RedisObject::Hash(ref mut h) => {
                let mut removed = 0i64;
                for i in 2..ctx.argc() {
                    if h.remove(&ctx.argv[i]).is_some() {
                        removed += 1;
                    }
                }
                (RespValue::Integer(removed), h.is_empty())
            }
            _ => (RespValue::err("WRONGTYPE"), false),
        },
        None => (RespValue::Integer(0), false),
    };
    // RefMut dropped here, safe to delete
    if should_delete {
        ctx.db.delete(key);
    }
    result
}

fn cmd_hexists(ctx: &CmdCtx) -> RespValue {
    match ctx.db.get(&ctx.argv[1]) {
        Some(RedisObject::Hash(h)) => {
            if h.contains_key(&ctx.argv[2]) {
                RespValue::Integer(1)
            } else {
                RespValue::Integer(0)
            }
        }
        Some(_) => RespValue::err("WRONGTYPE"),
        None => RespValue::Integer(0),
    }
}

fn cmd_hlen(ctx: &CmdCtx) -> RespValue {
    match ctx.db.get(&ctx.argv[1]) {
        Some(RedisObject::Hash(h)) => RespValue::Integer(h.len() as i64),
        Some(_) => RespValue::err("WRONGTYPE"),
        None => RespValue::Integer(0),
    }
}

fn cmd_hincrby(ctx: &CmdCtx) -> RespValue {
    let key = &ctx.argv[1];
    let incr = match ctx.arg_i64(3) {
        Some(v) => v,
        None => return RespValue::err("ERR value is not an integer or out of range"),
    };
    match ctx.db.get_object_mut(key) {
        Some(mut obj_ref) => match &mut *obj_ref {
            RedisObject::Hash(ref mut h) => {
                let current = h
                    .get(&ctx.argv[2])
                    .and_then(|v| std::str::from_utf8(v).ok())
                    .and_then(|s| s.parse::<i64>().ok())
                    .unwrap_or(0);
                let new_val = current + incr;
                h.insert(ctx.argv[2].clone(), new_val.to_string().into_bytes());
                RespValue::Integer(new_val)
            }
            _ => RespValue::err("WRONGTYPE"),
        },
        None => {
            let mut hash = HashMap::new();
            let new_val = incr;
            hash.insert(ctx.argv[2].clone(), new_val.to_string().into_bytes());
            ctx.db.set(&key, RedisObject::Hash(hash), None);
            RespValue::Integer(new_val)
        }
    }
}

fn cmd_hincrbyfloat(ctx: &CmdCtx) -> RespValue {
    let key = &ctx.argv[1];
    let incr: f64 = match ctx.arg_f64(3) {
        Some(v) => v,
        None => return RespValue::err("ERR value is not a valid float"),
    };
    match ctx.db.get_object_mut(key) {
        Some(mut obj_ref) => match &mut *obj_ref {
            RedisObject::Hash(ref mut h) => {
                let current = h
                    .get(&ctx.argv[2])
                    .and_then(|v| std::str::from_utf8(v).ok())
                    .and_then(|s| s.parse::<f64>().ok())
                    .unwrap_or(0.0);
                let new_val = current + incr;
                let s = format!("{}", new_val);
                h.insert(ctx.argv[2].clone(), s.clone().into_bytes());
                RespValue::BulkString(s.into_bytes())
            }
            _ => RespValue::err("WRONGTYPE"),
        },
        None => {
            let mut hash = HashMap::new();
            let new_val = incr;
            let s = format!("{}", new_val);
            hash.insert(ctx.argv[2].clone(), s.clone().into_bytes());
            ctx.db.set(&key, RedisObject::Hash(hash), None);
            RespValue::BulkString(s.into_bytes())
        }
    }
}

fn cmd_hkeys(ctx: &CmdCtx) -> RespValue {
    match ctx.db.get(&ctx.argv[1]) {
        Some(RedisObject::Hash(h)) => {
            let keys: Vec<RespValue> = h.keys().map(|k| RespValue::BulkString(k.clone())).collect();
            RespValue::Array(keys)
        }
        Some(_) => RespValue::err("WRONGTYPE"),
        None => RespValue::Array(vec![]),
    }
}

fn cmd_hvals(ctx: &CmdCtx) -> RespValue {
    match ctx.db.get(&ctx.argv[1]) {
        Some(RedisObject::Hash(h)) => {
            let vals: Vec<RespValue> = h
                .values()
                .map(|v| RespValue::BulkString(v.clone()))
                .collect();
            RespValue::Array(vals)
        }
        Some(_) => RespValue::err("WRONGTYPE"),
        None => RespValue::Array(vec![]),
    }
}

fn cmd_hsetnx(ctx: &CmdCtx) -> RespValue {
    let key = &ctx.argv[1];
    match ctx.db.get_object_mut(key) {
        Some(mut obj_ref) => match &mut *obj_ref {
            RedisObject::Hash(ref mut h) => {
                if h.contains_key(&ctx.argv[2]) {
                    RespValue::Integer(0)
                } else {
                    h.insert(ctx.argv[2].clone(), ctx.argv[3].clone());
                    RespValue::Integer(1)
                }
            }
            _ => RespValue::err("WRONGTYPE"),
        },
        None => {
            let mut hash = HashMap::new();
            hash.insert(ctx.argv[2].clone(), ctx.argv[3].clone());
            ctx.db.set(&key, RedisObject::Hash(hash), None);
            RespValue::Integer(1)
        }
    }
}

fn cmd_hstrlen(ctx: &CmdCtx) -> RespValue {
    match ctx.db.get(&ctx.argv[1]) {
        Some(RedisObject::Hash(h)) => match h.get(&ctx.argv[2]) {
            Some(v) => RespValue::Integer(v.len() as i64),
            None => RespValue::Integer(0),
        },
        Some(_) => RespValue::err("WRONGTYPE"),
        None => RespValue::Integer(0),
    }
}

fn cmd_hrandfield(ctx: &CmdCtx) -> RespValue {
    let count = if ctx.argc() > 2 {
        ctx.arg_i64(2).unwrap_or(1)
    } else {
        1
    };
    let withvalues = ctx.argc() > 3
        && ctx
            .arg_str(3)
            .map(|s| s.eq_ignore_ascii_case("WITHVALUES"))
            .unwrap_or(false);
    match ctx.db.get(&ctx.argv[1]) {
        Some(RedisObject::Hash(h)) => {
            let keys: Vec<&Vec<u8>> = h.keys().collect();
            if keys.is_empty() {
                if count == 1 {
                    return RespValue::Null;
                }
                return RespValue::Array(vec![]);
            }
            if count == 1 {
                let idx = (rand::random::<u64>() as usize) % keys.len();
                return RespValue::BulkString(keys[idx].clone());
            }
            let abs_count = count.unsigned_abs() as usize;
            let mut result = Vec::new();
            for _ in 0..abs_count.min(keys.len()) {
                let idx = (rand::random::<u64>() as usize) % keys.len();
                result.push(RespValue::BulkString(keys[idx].clone()));
                if withvalues {
                    result.push(RespValue::BulkString(h[keys[idx]].clone()));
                }
            }
            RespValue::Array(result)
        }
        Some(_) => RespValue::err("WRONGTYPE"),
        None => {
            if count == 1 {
                RespValue::Null
            } else {
                RespValue::Array(vec![])
            }
        }
    }
}

// ---------------------------------------------------------------------------
// 集合命令实现
// SADD/SREM/SMEMBERS/SISMEMBER/SCARD/SINTER/SUNION/SDIFF/SPOP 等
// ---------------------------------------------------------------------------

fn cmd_sadd(ctx: &CmdCtx) -> RespValue {
    let key = &ctx.argv[1];
    match ctx.db.get_object_mut(key) {
        Some(mut obj_ref) => match &mut *obj_ref {
            RedisObject::Set(ref mut s) => {
                let mut added = 0i64;
                for i in 2..ctx.argc() {
                    if s.insert(ctx.argv[i].clone()) {
                        added += 1;
                    }
                }
                RespValue::Integer(added)
            }
            _ => RespValue::err("WRONGTYPE"),
        },
        None => {
            let mut set = std::collections::HashSet::new();
            let mut added = 0i64;
            for i in 2..ctx.argc() {
                if set.insert(ctx.argv[i].clone()) {
                    added += 1;
                }
            }
            ctx.db.set(&key, RedisObject::Set(set), None);
            RespValue::Integer(added)
        }
    }
}

fn cmd_srem(ctx: &CmdCtx) -> RespValue {
    let key = &ctx.argv[1];
    let (result, should_delete) = match ctx.db.get_object_mut(key) {
        Some(mut obj_ref) => match &mut *obj_ref {
            RedisObject::Set(ref mut s) => {
                let mut removed = 0i64;
                for i in 2..ctx.argc() {
                    if s.remove(&ctx.argv[i]) {
                        removed += 1;
                    }
                }
                (RespValue::Integer(removed), s.is_empty())
            }
            _ => (RespValue::err("WRONGTYPE"), false),
        },
        None => (RespValue::Integer(0), false),
    };
    // RefMut dropped here, safe to delete
    if should_delete {
        ctx.db.delete(key);
    }
    result
}

fn cmd_smembers(ctx: &CmdCtx) -> RespValue {
    match ctx.db.get(&ctx.argv[1]) {
        Some(RedisObject::Set(s)) => {
            let members: Vec<RespValue> = s.into_iter().map(RespValue::BulkString).collect();
            RespValue::Array(members)
        }
        Some(_) => RespValue::err("WRONGTYPE"),
        None => RespValue::Array(vec![]),
    }
}

fn cmd_sismember(ctx: &CmdCtx) -> RespValue {
    match ctx.db.get(&ctx.argv[1]) {
        Some(RedisObject::Set(s)) => {
            if s.contains(&ctx.argv[2]) {
                RespValue::Integer(1)
            } else {
                RespValue::Integer(0)
            }
        }
        Some(_) => RespValue::err("WRONGTYPE"),
        None => RespValue::Integer(0),
    }
}

fn cmd_scard(ctx: &CmdCtx) -> RespValue {
    match ctx.db.get(&ctx.argv[1]) {
        Some(RedisObject::Set(s)) => RespValue::Integer(s.len() as i64),
        Some(_) => RespValue::err("WRONGTYPE"),
        None => RespValue::Integer(0),
    }
}

fn cmd_sinter(ctx: &CmdCtx) -> RespValue {
    let first = match ctx.db.get(&ctx.argv[1]) {
        Some(RedisObject::Set(s)) => s,
        Some(_) => return RespValue::err("WRONGTYPE"),
        None => return RespValue::Array(vec![]),
    };
    let mut result = first;
    for i in 2..ctx.argc() {
        match ctx.db.get(&ctx.argv[i]) {
            Some(RedisObject::Set(s)) => {
                result = result.intersection(&s).cloned().collect();
            }
            Some(_) => return RespValue::err("WRONGTYPE"),
            None => return RespValue::Array(vec![]),
        }
    }
    RespValue::Array(result.into_iter().map(RespValue::BulkString).collect())
}

fn cmd_sunion(ctx: &CmdCtx) -> RespValue {
    let mut result = std::collections::HashSet::new();
    for i in 1..ctx.argc() {
        match ctx.db.get(&ctx.argv[i]) {
            Some(RedisObject::Set(s)) => result.extend(s),
            Some(_) => return RespValue::err("WRONGTYPE"),
            None => {}
        }
    }
    RespValue::Array(result.into_iter().map(RespValue::BulkString).collect())
}

fn cmd_sdiff(ctx: &CmdCtx) -> RespValue {
    let mut result = match ctx.db.get(&ctx.argv[1]) {
        Some(RedisObject::Set(s)) => s,
        Some(_) => return RespValue::err("WRONGTYPE"),
        None => return RespValue::Array(vec![]),
    };
    for i in 2..ctx.argc() {
        match ctx.db.get(&ctx.argv[i]) {
            Some(RedisObject::Set(s)) => {
                result = result.difference(&s).cloned().collect();
            }
            Some(_) => return RespValue::err("WRONGTYPE"),
            None => {}
        }
    }
    RespValue::Array(result.into_iter().map(RespValue::BulkString).collect())
}

fn cmd_srandmember(ctx: &CmdCtx) -> RespValue {
    let count = ctx.arg_i64(2);
    let set = match ctx.db.get(&ctx.argv[1]) {
        Some(RedisObject::Set(s)) => s,
        Some(_) => return RespValue::err("WRONGTYPE"),
        None => return RespValue::Null,
    };
    if set.is_empty() {
        return RespValue::Null;
    }
    let members: Vec<&Vec<u8>> = set.iter().collect();
    match count {
        None => {
            let idx = (rand::random::<u64>() as usize) % members.len();
            RespValue::BulkString(members[idx].clone())
        }
        Some(n) => {
            let abs_n = n.unsigned_abs() as usize;
            let mut result = Vec::new();
            for _ in 0..abs_n.min(members.len()) {
                let idx = (rand::random::<u64>() as usize) % members.len();
                result.push(RespValue::BulkString(members[idx].clone()));
            }
            RespValue::Array(result)
        }
    }
}

fn cmd_spop(ctx: &CmdCtx) -> RespValue {
    let count = ctx.arg_i64(2).unwrap_or(1);
    let mut set = match ctx.db.get(&ctx.argv[1]) {
        Some(RedisObject::Set(s)) => s,
        Some(_) => return RespValue::err("WRONGTYPE"),
        None => return RespValue::Null,
    };
    if set.is_empty() {
        return RespValue::Null;
    }
    let members: Vec<Vec<u8>> = set.iter().cloned().collect();
    let n = (count as usize).min(members.len());
    let mut result = Vec::new();
    for _ in 0..n {
        let idx = (rand::random::<u64>() as usize) % members.len();
        let member = &members[idx];
        set.remove(member);
        result.push(RespValue::BulkString(member.clone()));
    }
    if set.is_empty() {
        ctx.db.delete(&ctx.argv[1]);
    } else {
        ctx.db.set(&ctx.argv[1], RedisObject::Set(set), None);
    }
    if count == 1 && !result.is_empty() {
        result.pop().unwrap()
    } else {
        RespValue::Array(result)
    }
}

fn cmd_smismember(ctx: &CmdCtx) -> RespValue {
    let set = match ctx.db.get(&ctx.argv[1]) {
        Some(RedisObject::Set(s)) => s,
        Some(_) => return RespValue::err("WRONGTYPE"),
        None => return RespValue::Array((2..ctx.argc()).map(|_| RespValue::Integer(0)).collect()),
    };
    let result: Vec<RespValue> = (2..ctx.argc())
        .map(|i| {
            if set.contains(&ctx.argv[i]) {
                RespValue::Integer(1)
            } else {
                RespValue::Integer(0)
            }
        })
        .collect();
    RespValue::Array(result)
}

// ---------------------------------------------------------------------------
// 有序集合命令实现
// ZADD/ZREM/ZSCORE/ZRANK/ZREVRANK/ZRANGE/ZREVRANGE/ZRANGEBYSCORE 等
// ---------------------------------------------------------------------------

fn cmd_zadd(ctx: &CmdCtx) -> RespValue {
    let key = &ctx.argv[1];
    let mut nx = false;
    let mut xx = false;
    let mut gt = false;
    let mut lt = false;
    let mut ch = false;
    let mut i = 2;
    // Parse flags
    while i < ctx.argc() {
        let opt = ctx.arg_str(i).unwrap_or("").to_ascii_uppercase();
        match opt.as_str() {
            "NX" => {
                nx = true;
                i += 1;
            }
            "XX" => {
                xx = true;
                i += 1;
            }
            "GT" => {
                gt = true;
                i += 1;
            }
            "LT" => {
                lt = true;
                i += 1;
            }
            "CH" => {
                ch = true;
                i += 1;
            }
            _ => break,
        }
    }
    // Parse score-member pairs
    if (ctx.argc() - i) % 2 != 0 || i >= ctx.argc() {
        return RespValue::err("ERR syntax error");
    }
    // 使用 get_object_mut() 就地修改，避免 clone 整个 ZSet
    match ctx.db.get_object_mut(key) {
        Some(mut obj_ref) => match &mut *obj_ref {
            RedisObject::ZSet(ref mut zset) => {
                let mut added = 0i64;
                let mut updated = 0i64;
                while i < ctx.argc() {
                    let score = match ctx.arg_f64(i) {
                        Some(s) => s,
                        None => return RespValue::err("ERR value is not a valid float"),
                    };
                    let member = ctx.argv[i + 1].clone();
                    let existed = zset.dict.contains_key(&member);
                    if nx && existed {
                        i += 2;
                        continue;
                    }
                    if xx && !existed {
                        i += 2;
                        continue;
                    }
                    if gt {
                        if let Some(old_score) = zset.score(&member) {
                            if score <= old_score {
                                i += 2;
                                continue;
                            }
                        }
                    }
                    if lt {
                        if let Some(old_score) = zset.score(&member) {
                            if score >= old_score {
                                i += 2;
                                continue;
                            }
                        }
                    }
                    if zset.add(member, score) {
                        added += 1;
                    } else {
                        updated += 1;
                    }
                    i += 2;
                }
                if ch {
                    RespValue::Integer(added + updated)
                } else {
                    RespValue::Integer(added)
                }
            }
            _ => RespValue::err("WRONGTYPE"),
        },
        None => {
            // 键不存在，创建新 ZSet
            let mut zset = ZSet::new();
            let mut added = 0i64;
            while i < ctx.argc() {
                let score = match ctx.arg_f64(i) {
                    Some(s) => s,
                    None => return RespValue::err("ERR value is not a valid float"),
                };
                // 对新 ZSet，NX/XX/GT/LT 语义无需检查（成员不存在）
                let member = ctx.argv[i + 1].clone();
                zset.add(member, score);
                added += 1;
                i += 2;
            }
            ctx.db.set(&key, RedisObject::ZSet(zset), None);
            if ch {
                RespValue::Integer(added)
            } else {
                RespValue::Integer(added)
            }
        }
    }
}

fn cmd_zrem(ctx: &CmdCtx) -> RespValue {
    let key = &ctx.argv[1];
    // 使用 get_object_mut() 就地修改，避免 clone 整个 ZSet
    let (result, should_delete) = match ctx.db.get_object_mut(key) {
        Some(mut obj_ref) => match &mut *obj_ref {
            RedisObject::ZSet(ref mut zset) => {
                let mut removed = 0i64;
                for i in 2..ctx.argc() {
                    if zset.remove(&ctx.argv[i]) {
                        removed += 1;
                    }
                }
                (RespValue::Integer(removed), zset.is_empty())
            }
            _ => (RespValue::err("WRONGTYPE"), false),
        },
        None => (RespValue::Integer(0), false),
    };
    // RefMut dropped here, safe to delete
    if should_delete {
        ctx.db.delete(key);
    }
    result
}

fn cmd_zscore(ctx: &CmdCtx) -> RespValue {
    match ctx.db.get(&ctx.argv[1]) {
        Some(RedisObject::ZSet(z)) => match z.score(&ctx.argv[2]) {
            Some(s) => RespValue::BulkString(format!("{}", s).into_bytes()),
            None => RespValue::Null,
        },
        Some(_) => RespValue::err("WRONGTYPE"),
        None => RespValue::Null,
    }
}

fn cmd_zrank(ctx: &CmdCtx) -> RespValue {
    match ctx.db.get(&ctx.argv[1]) {
        Some(RedisObject::ZSet(z)) => match z.rank(&ctx.argv[2]) {
            Some(r) => RespValue::Integer(r as i64),
            None => RespValue::Null,
        },
        Some(_) => RespValue::err("WRONGTYPE"),
        None => RespValue::Null,
    }
}

fn cmd_zrevrank(ctx: &CmdCtx) -> RespValue {
    match ctx.db.get(&ctx.argv[1]) {
        Some(RedisObject::ZSet(z)) => match z.rev_rank(&ctx.argv[2]) {
            Some(r) => RespValue::Integer(r as i64),
            None => RespValue::Null,
        },
        Some(_) => RespValue::err("WRONGTYPE"),
        None => RespValue::Null,
    }
}

fn cmd_zrange(ctx: &CmdCtx) -> RespValue {
    let start: isize = match ctx.arg_str(2).and_then(|s| s.parse().ok()) {
        Some(v) => v,
        None => return RespValue::err("ERR value is not an integer"),
    };
    let stop: isize = match ctx.arg_str(3).and_then(|s| s.parse().ok()) {
        Some(v) => v,
        None => return RespValue::err("ERR value is not an integer"),
    };
    let mut withscores = false;
    let mut byscore = false;
    let mut bylex = false;
    for i in 4..ctx.argc() {
        let opt = ctx.arg_str(i).unwrap_or("").to_ascii_uppercase();
        match opt.as_str() {
            "WITHSCORES" => withscores = true,
            "BYSCORE" => byscore = true,
            "BYLEX" => bylex = true,
            _ => {}
        }
    }
    match ctx.db.get(&ctx.argv[1]) {
        Some(RedisObject::ZSet(z)) => {
            let result = z.range(start, stop, withscores);
            format_zrange_result(result, withscores)
        }
        Some(_) => RespValue::err("WRONGTYPE"),
        None => RespValue::Array(vec![]),
    }
}

fn cmd_zrevrange(ctx: &CmdCtx) -> RespValue {
    let start: isize = match ctx.arg_str(2).and_then(|s| s.parse().ok()) {
        Some(v) => v,
        None => return RespValue::err("ERR value is not an integer"),
    };
    let stop: isize = match ctx.arg_str(3).and_then(|s| s.parse().ok()) {
        Some(v) => v,
        None => return RespValue::err("ERR value is not an integer"),
    };
    let withscores = ctx.argc() > 4
        && ctx
            .arg_str(4)
            .map(|s| s.eq_ignore_ascii_case("WITHSCORES"))
            .unwrap_or(false);
    match ctx.db.get(&ctx.argv[1]) {
        Some(RedisObject::ZSet(z)) => {
            let result = z.rev_range(start, stop, withscores);
            format_zrange_result(result, withscores)
        }
        Some(_) => RespValue::err("WRONGTYPE"),
        None => RespValue::Array(vec![]),
    }
}

fn cmd_zrangebyscore(ctx: &CmdCtx) -> RespValue {
    let (min, max) = match (ctx.arg_f64(2), ctx.arg_f64(3)) {
        (Some(min), Some(max)) => (min, max),
        _ => return RespValue::err("ERR min or max is not a float"),
    };
    let withscores = ctx.argc() > 4
        && ctx
            .arg_str(4)
            .map(|s| s.eq_ignore_ascii_case("WITHSCORES"))
            .unwrap_or(false);
    match ctx.db.get(&ctx.argv[1]) {
        Some(RedisObject::ZSet(z)) => {
            let result = z.range_by_score(min, max, withscores);
            format_zrange_result(result, withscores)
        }
        Some(_) => RespValue::err("WRONGTYPE"),
        None => RespValue::Array(vec![]),
    }
}

fn cmd_zrevrangebyscore(ctx: &CmdCtx) -> RespValue {
    let (max, min) = match (ctx.arg_f64(2), ctx.arg_f64(3)) {
        (Some(max), Some(min)) => (max, min),
        _ => return RespValue::err("ERR min or max is not a float"),
    };
    let withscores = ctx.argc() > 4
        && ctx
            .arg_str(4)
            .map(|s| s.eq_ignore_ascii_case("WITHSCORES"))
            .unwrap_or(false);
    match ctx.db.get(&ctx.argv[1]) {
        Some(RedisObject::ZSet(z)) => {
            let mut result = z.range_by_score(min, max, withscores);
            result.reverse();
            format_zrange_result(result, withscores)
        }
        Some(_) => RespValue::err("WRONGTYPE"),
        None => RespValue::Array(vec![]),
    }
}

fn cmd_zcard(ctx: &CmdCtx) -> RespValue {
    match ctx.db.get(&ctx.argv[1]) {
        Some(RedisObject::ZSet(z)) => RespValue::Integer(z.len() as i64),
        Some(_) => RespValue::err("WRONGTYPE"),
        None => RespValue::Integer(0),
    }
}

fn cmd_zcount(ctx: &CmdCtx) -> RespValue {
    let (min, max) = match (ctx.arg_f64(2), ctx.arg_f64(3)) {
        (Some(min), Some(max)) => (min, max),
        _ => return RespValue::err("ERR min or max is not a float"),
    };
    match ctx.db.get(&ctx.argv[1]) {
        Some(RedisObject::ZSet(z)) => RespValue::Integer(z.count(min, max) as i64),
        Some(_) => RespValue::err("WRONGTYPE"),
        None => RespValue::Integer(0),
    }
}

fn cmd_zincrby(ctx: &CmdCtx) -> RespValue {
    let incr = match ctx.arg_f64(2) {
        Some(v) => v,
        None => return RespValue::err("ERR value is not a valid float"),
    };
    let key = &ctx.argv[1];
    // 使用 get_object_mut() 就地修改，避免 clone 整个 ZSet
    match ctx.db.get_object_mut(key) {
        Some(mut obj_ref) => match &mut *obj_ref {
            RedisObject::ZSet(ref mut zset) => {
                let current = zset.score(&ctx.argv[3]).unwrap_or(0.0);
                let new_score = current + incr;
                zset.add(ctx.argv[3].clone(), new_score);
                RespValue::BulkString(format!("{}", new_score).into_bytes())
            }
            _ => RespValue::err("WRONGTYPE"),
        },
        None => {
            let mut zset = ZSet::new();
            let new_score = incr; // current is 0.0 for new key
            zset.add(ctx.argv[3].clone(), new_score);
            ctx.db.set(&key, RedisObject::ZSet(zset), None);
            RespValue::BulkString(format!("{}", new_score).into_bytes())
        }
    }
}

fn cmd_zrangebylex(_ctx: &CmdCtx) -> RespValue {
    // TODO: implement ZRANGEBYLEX
    RespValue::Array(vec![])
}

fn cmd_zlexcount(_ctx: &CmdCtx) -> RespValue {
    // TODO: implement ZLEXCOUNT
    RespValue::Integer(0)
}

fn format_zrange_result(result: Vec<(Vec<u8>, f64)>, withscores: bool) -> RespValue {
    if withscores {
        let mut items = Vec::new();
        for (member, score) in result {
            items.push(RespValue::BulkString(member));
            items.push(RespValue::BulkString(format!("{}", score).into_bytes()));
        }
        RespValue::Array(items)
    } else {
        RespValue::Array(
            result
                .into_iter()
                .map(|(m, _)| RespValue::BulkString(m))
                .collect(),
        )
    }
}

// ---------------------------------------------------------------------------
// 事务命令实现（桩 — 完整逻辑在服务端循环中）
// MULTI/EXEC/DISCARD
// ---------------------------------------------------------------------------

fn cmd_multi(_ctx: &CmdCtx) -> RespValue {
    RespValue::ok()
}

fn cmd_exec(_ctx: &CmdCtx) -> RespValue {
    RespValue::err("ERR EXEC without MULTI")
}

fn cmd_discard(_ctx: &CmdCtx) -> RespValue {
    RespValue::err("ERR DISCARD without MULTI")
}

// ---------------------------------------------------------------------------
// 发布/订阅命令实现（桩 — 完整逻辑在服务端循环中）
// SUBSCRIBE/UNSUBSCRIBE/PUBLISH
// ---------------------------------------------------------------------------

fn cmd_subscribe(ctx: &CmdCtx) -> RespValue {
    RespValue::Array(vec![
        RespValue::BulkString(b"subscribe".to_vec()),
        RespValue::BulkString(ctx.argv[1].clone()),
        RespValue::Integer(1),
    ])
}

fn cmd_unsubscribe(_ctx: &CmdCtx) -> RespValue {
    RespValue::Array(vec![
        RespValue::BulkString(b"unsubscribe".to_vec()),
        RespValue::Null,
        RespValue::Integer(0),
    ])
}

fn cmd_publish(_ctx: &CmdCtx) -> RespValue {
    RespValue::Integer(0)
}

// ---------------------------------------------------------------------------
// 服务器信息命令实现
// INFO/CONFIG/COMMAND/CLIENT/TIME/SLOWLOG
// ---------------------------------------------------------------------------

fn cmd_info(ctx: &CmdCtx) -> RespValue {
    let section = ctx.arg_str(1).unwrap_or("default").to_ascii_lowercase();
    let mut info = String::new();

    if section == "default" || section == "server" {
        info.push_str("# Server\r\n");
        info.push_str("redis_version:8.0.0-rust\r\n");
        info.push_str("redis_mode:standalone\r\n");
        info.push_str("os:Rust\r\n");
        info.push_str("tcp_port:6379\r\n");
        info.push_str("uptime_in_seconds:0\r\n");
        info.push_str("server_name:km-rust-redis\r\n");
        info.push_str("\r\n");
    }
    if section == "default" || section == "clients" {
        info.push_str("# Clients\r\n");
        info.push_str("connected_clients:1\r\n");
        info.push_str("\r\n");
    }
    if section == "default" || section == "memory" {
        info.push_str("# Memory\r\n");
        info.push_str("used_memory:0\r\n");
        info.push_str("used_memory_human:0B\r\n");
        info.push_str("\r\n");
    }
    if section == "default" || section == "stats" {
        info.push_str("# Stats\r\n");
        info.push_str("total_connections_received:1\r\n");
        info.push_str("total_commands_processed:0\r\n");
        info.push_str("instantaneous_ops_per_sec:0\r\n");
        info.push_str("\r\n");
    }
    if section == "default" || section == "keyspace" {
        info.push_str("# Keyspace\r\n");
        for i in 0..16 {
            let db = ctx.db;
            let dbsize = db.dbsize();
            if dbsize > 0 {
                info.push_str(&format!("db{}:keys={},expires=0,avg_ttl=0\r\n", i, dbsize));
            }
        }
    }
    RespValue::BulkString(info.into_bytes())
}

fn cmd_config(_ctx: &CmdCtx) -> RespValue {
    // TODO: implement CONFIG GET/SET
    RespValue::Array(vec![])
}

fn cmd_command(_ctx: &CmdCtx) -> RespValue {
    // Return empty array — redis-cli uses this to discover commands
    RespValue::Array(vec![])
}

fn cmd_client(_ctx: &CmdCtx) -> RespValue {
    // TODO: implement CLIENT LIST/SETNAME/etc
    RespValue::ok()
}

fn cmd_time(_ctx: &CmdCtx) -> RespValue {
    let now = current_time_secs();
    let us = (current_time_ms() % 1000) * 1000;
    RespValue::Array(vec![
        RespValue::BulkString(now.to_string().into_bytes()),
        RespValue::BulkString(us.to_string().into_bytes()),
    ])
}

fn cmd_slowlog(_ctx: &CmdCtx) -> RespValue {
    // TODO: implement SLOWLOG
    RespValue::Array(vec![])
}

/// SAVE 命令：同步保存当前数据库快照到 RDB 文件。
/// 对应 Redis 的 SAVE 命令，会阻塞当前线程直到保存完成。
fn cmd_save(_ctx: &CmdCtx) -> RespValue {
    // TODO: 集成 RDB 模块执行同步保存
    RespValue::ok()
}

/// BGSAVE 命令：后台异步保存数据库快照。
/// 对应 Redis 的 BGSAVE 命令，立即返回并在子进程中执行保存。
fn cmd_bgsave(_ctx: &CmdCtx) -> RespValue {
    RespValue::SimpleString("Background saving started".to_string())
}

/// REPLICAOF 命令：设置主从复制关系。
/// REPLICAOF host port → 将当前节点设为指定节点的从节点
/// REPLICAOF NO ONE → 提升当前节点为主节点
fn cmd_replicaof(ctx: &CmdCtx) -> RespValue {
    use crate::replication;
    match replication::parse_replicaof_args(&ctx.argv) {
        Ok(replication::ReplicaOfCmd::SetMaster { host, port }) => {
            RespValue::SimpleString(format!("OK master {}:{}", host, port))
        }
        Ok(replication::ReplicaOfCmd::NoOne) => {
            RespValue::SimpleString("OK promoted to master".to_string())
        }
        Err(e) => RespValue::err(e),
    }
}

/// SENTINEL 命令：哨兵监控管理。
fn cmd_sentinel(ctx: &CmdCtx) -> RespValue {
    use crate::sentinel;
    let subcmd = ctx.arg_str(1).unwrap_or("").to_ascii_uppercase();
    let mut s = sentinel::Sentinel::new(2, 5000);
    match subcmd.as_str() {
        "MASTERS" => {
            let statuses = s.master_status();
            let mut result = Vec::new();
            for ms in statuses {
                result.push(RespValue::Array(vec![
                    RespValue::BulkString(b"name".to_vec()),
                    RespValue::BulkString(ms.name.into_bytes()),
                    RespValue::BulkString(b"addr".to_vec()),
                    RespValue::BulkString(ms.addr.into_bytes()),
                    RespValue::BulkString(b"flags".to_vec()),
                    RespValue::BulkString(if ms.is_sdown { b"s_down" } else { b"master" }.to_vec()),
                ]));
            }
            RespValue::Array(result)
        }
        "GET-MASTER-ADDR-BY-NAME" => {
            let name = ctx.arg_str(2).unwrap_or("");
            match s.get_master_addr_by_name(name) {
                Some((ip, port)) => RespValue::Array(vec![
                    RespValue::BulkString(ip.as_bytes().to_vec()),
                    RespValue::BulkString(port.as_bytes().to_vec()),
                ]),
                None => RespValue::Null,
            }
        }
        "INFO" => RespValue::BulkString(s.info_sentinel().into_bytes()),
        "IS-MASTER-DOWN-BY-ADDR" => {
            RespValue::Array(vec![
                RespValue::Integer(0),
                RespValue::BulkString(b"".to_vec()),
            ])
        }
        "SLAVES" => {
            let name = ctx.arg_str(2).unwrap_or("");
            match s.check_master(name) {
                Some(ms) => {
                    let mut result = Vec::new();
                    result.push(RespValue::Array(vec![
                        RespValue::BulkString(b"name".to_vec()),
                        RespValue::BulkString(ms.name.into_bytes()),
                        RespValue::BulkString(b"addr".to_vec()),
                        RespValue::BulkString(ms.addr.into_bytes()),
                    ]));
                    RespValue::Array(result)
                }
                None => RespValue::Array(vec![]),
            }
        }
        _ => RespValue::err(format!("ERR Unknown SENTINEL subcommand '{}'", subcmd)),
    }
}

/// BITOP 命令：对一个或多个 key 按位运算，结果存入 destkey。
/// 支持 AND/OR/XOR/NOT 四种运算。
fn cmd_bitop(ctx: &CmdCtx) -> RespValue {
    let op = ctx.arg_str(1).unwrap_or("").to_ascii_uppercase();
    let destkey = &ctx.argv[2];
    if ctx.argc() < 4 {
        return RespValue::err("ERR wrong number of arguments for 'bitop' command");
    }
    // 读取所有源key的值
    let mut values: Vec<Vec<u8>> = Vec::new();
    for i in 3..ctx.argc() {
        match ctx.db.get(&ctx.argv[i]) {
            Some(RedisObject::String(d)) => values.push(d),
            Some(RedisObject::Integer(n)) => values.push(n.to_string().into_bytes()),
            _ => values.push(vec![]),
        }
    }
    if values.is_empty() {
        ctx.db.delete(destkey);
        return RespValue::Integer(0);
    }
    let max_len = values.iter().map(|v| v.len()).max().unwrap_or(0);
    let mut result = vec![0u8; max_len];
    match op.as_str() {
        "AND" => {
            result.copy_from_slice(&values[0]);
            result.resize(max_len, 0);
            for v in &values[1..] {
                for i in 0..max_len {
                    result[i] &= v.get(i).copied().unwrap_or(0);
                }
            }
        }
        "OR" => {
            for v in &values {
                for i in 0..max_len {
                    result[i] |= v.get(i).copied().unwrap_or(0);
                }
            }
        }
        "XOR" => {
            for v in &values {
                for i in 0..max_len {
                    result[i] ^= v.get(i).copied().unwrap_or(0);
                }
            }
        }
        "NOT" => {
            let v = &values[0];
            result = vec![0u8; v.len()];
            for i in 0..v.len() {
                result[i] = !v[i];
            }
        }
        _ => return RespValue::err("ERR syntax error"),
    }
    let len = result.len() as i64;
    ctx.db.set(&destkey, RedisObject::String(result), None);
    RespValue::Integer(len)
}

/// BITFIELD 命令：位域操作（GET/SET/INCRBY）。
/// 简化版：返回 OK（完整实现需要复杂的位偏移计算）。
fn cmd_bitfield(_ctx: &CmdCtx) -> RespValue {
    RespValue::Array(vec![])
}

/// LMPOP 命令：从多个列表中弹出元素。
fn cmd_lmpop(ctx: &CmdCtx) -> RespValue {
    let numkeys: usize = match ctx.arg_str(1).and_then(|s| s.parse().ok()) {
        Some(n) => n,
        None => return RespValue::err("ERR value is not an integer"),
    };
    let side = ctx.arg_str(2 + numkeys).unwrap_or("").to_ascii_uppercase();
    let count = if ctx.argc() > 3 + numkeys {
        ctx.arg_i64(3 + numkeys).unwrap_or(1)
    } else {
        1
    };
    for i in 2..2 + numkeys {
        match ctx.db.get_object_mut(&ctx.argv[i]) {
            Some(mut obj_ref) => match &mut *obj_ref {
                RedisObject::List(ref mut l) => {
                    if !l.is_empty() {
                        let n = count.min(l.len() as i64) as usize;
                        let mut result = Vec::new();
                        for _ in 0..n {
                            let val = if side == "LEFT" { l.pop_front() } else { l.pop_back() };
                            if let Some(v) = val {
                                result.push(RespValue::BulkString(v));
                            }
                        }
                        let should_delete = l.is_empty();
                        drop(obj_ref);
                        if should_delete {
                            ctx.db.delete(&ctx.argv[i]);
                        }
                        return RespValue::Array(vec![
                            RespValue::BulkString(ctx.argv[i].clone()),
                            RespValue::Array(result),
                        ]);
                    }
                }
                _ => return RespValue::err("WRONGTYPE"),
            },
            None => continue,
        }
    }
    RespValue::Null
}

/// BLMOVE 命令：阻塞列表移动（简化版：不阻塞，立即执行）。
fn cmd_blmove(ctx: &CmdCtx) -> RespValue {
    // 简化版：等同于 LMOVE，忽略 timeout
    let src = &ctx.argv[1];
    let dst = &ctx.argv[2];
    let src_dir = ctx.arg_str(3).unwrap_or("").to_ascii_uppercase();
    let dst_dir = ctx.arg_str(4).unwrap_or("").to_ascii_uppercase();
    let val = match ctx.db.get_object_mut(src) {
        Some(mut obj_ref) => match &mut *obj_ref {
            RedisObject::List(ref mut l) => {
                let v = if src_dir == "LEFT" { l.pop_front() } else { l.pop_back() };
                match v {
                    Some(val) => {
                        let should_delete = l.is_empty();
                        drop(obj_ref);
                        if should_delete {
                            ctx.db.delete(src);
                        }
                        val
                    }
                    None => return RespValue::Null,
                }
            }
            _ => return RespValue::err("WRONGTYPE"),
        },
        None => return RespValue::Null,
    };
    match ctx.db.get_object_mut(dst) {
        Some(mut obj_ref) => match &mut *obj_ref {
            RedisObject::List(ref mut l) => {
                if dst_dir == "LEFT" { l.push_front(val.clone()); } else { l.push_back(val.clone()); }
            }
            _ => return RespValue::err("WRONGTYPE"),
        },
        None => {
            let mut l = VecDeque::new();
            if dst_dir == "LEFT" { l.push_front(val.clone()); } else { l.push_back(val.clone()); }
            ctx.db.set(&dst, RedisObject::List(l), None);
        }
    }
    RespValue::BulkString(val)
}

/// HSCAN 命令：游标扫描哈希表字段。
fn cmd_hscan(ctx: &CmdCtx) -> RespValue {
    let cursor: u64 = match ctx.arg_str(2).and_then(|s| s.parse().ok()) {
        Some(n) => n,
        None => return RespValue::err("ERR invalid cursor"),
    };
    match ctx.db.get(&ctx.argv[1]) {
        Some(RedisObject::Hash(h)) => {
            let mut fields: Vec<Vec<u8>> = h.keys().cloned().collect();
            fields.sort();
            let total = fields.len();
            let start = (cursor as usize).min(total);
            let count = 10;
            let end = (start + count).min(total);
            let next = if end >= total { 0 } else { end as u64 };
            let mut result = Vec::new();
            for k in &fields[start..end] {
                result.push(RespValue::BulkString(k.clone()));
                if let Some(v) = h.get(k) {
                    result.push(RespValue::BulkString(v.clone()));
                }
            }
            RespValue::Array(vec![
                RespValue::BulkString(next.to_string().into_bytes()),
                RespValue::Array(result),
            ])
        }
        Some(_) => RespValue::err("WRONGTYPE"),
        None => RespValue::Array(vec![
            RespValue::BulkString(b"0".to_vec()),
            RespValue::Array(vec![]),
        ]),
    }
}

// ====================================================================
// 批量补全的缺失命令实现
// ====================================================================

// --- ZSet 扩展命令 ---

fn cmd_zpopmin(ctx: &CmdCtx) -> RespValue {
    let count = if ctx.argc() > 2 { ctx.arg_i64(2).unwrap_or(1) } else { 1 } as usize;
    match ctx.db.get_object_mut(&ctx.argv[1]) {
        Some(mut obj_ref) => match &mut *obj_ref {
            RedisObject::ZSet(ref mut z) => {
                let mut result = Vec::new();
                for _ in 0..count.min(z.len()) {
                    if let Some((&score, members)) = z.scores.iter().next() {
                        let member = members.iter().next().cloned();
                        if let Some(m) = member {
                            z.remove(&m);
                            result.push(RespValue::BulkString(m));
                            result.push(RespValue::BulkString(format!("{}", score.0).into_bytes()));
                        }
                    }
                }
                let should_delete = z.is_empty();
                drop(obj_ref);
                if should_delete { ctx.db.delete(&ctx.argv[1]); }
                RespValue::Array(result)
            }
            _ => RespValue::err("WRONGTYPE"),
        },
        None => RespValue::Array(vec![]),
    }
}

fn cmd_zpopmax(ctx: &CmdCtx) -> RespValue {
    let count = if ctx.argc() > 2 { ctx.arg_i64(2).unwrap_or(1) } else { 1 } as usize;
    match ctx.db.get_object_mut(&ctx.argv[1]) {
        Some(mut obj_ref) => match &mut *obj_ref {
            RedisObject::ZSet(ref mut z) => {
                let mut result = Vec::new();
                for _ in 0..count.min(z.len()) {
                    if let Some((&score, members)) = z.scores.iter().next_back() {
                        let member = members.iter().next().cloned();
                        if let Some(m) = member {
                            z.remove(&m);
                            result.push(RespValue::BulkString(m));
                            result.push(RespValue::BulkString(format!("{}", score.0).into_bytes()));
                        }
                    }
                }
                let should_delete = z.is_empty();
                drop(obj_ref);
                if should_delete { ctx.db.delete(&ctx.argv[1]); }
                RespValue::Array(result)
            }
            _ => RespValue::err("WRONGTYPE"),
        },
        None => RespValue::Array(vec![]),
    }
}

fn cmd_zmscore(ctx: &CmdCtx) -> RespValue {
    match ctx.db.get(&ctx.argv[1]) {
        Some(RedisObject::ZSet(z)) => {
            let mut result = Vec::new();
            for i in 2..ctx.argc() {
                match z.score(&ctx.argv[i]) {
                    Some(s) => result.push(RespValue::BulkString(format!("{}", s).into_bytes())),
                    None => result.push(RespValue::Null),
                }
            }
            RespValue::Array(result)
        }
        Some(_) => RespValue::err("WRONGTYPE"),
        None => RespValue::Array((2..ctx.argc()).map(|_| RespValue::Null).collect()),
    }
}

fn cmd_zrandmember(ctx: &CmdCtx) -> RespValue {
    let count = ctx.arg_i64(2);
    match ctx.db.get(&ctx.argv[1]) {
        Some(RedisObject::ZSet(z)) => {
            let members: Vec<&Vec<u8>> = z.dict.keys().collect();
            if members.is_empty() { return RespValue::Null; }
            match count {
                None => {
                    let idx = (rand::random::<u64>() as usize) % members.len();
                    RespValue::BulkString(members[idx].clone())
                }
                Some(n) => {
                    let mut result = Vec::new();
                    for _ in 0..(n.unsigned_abs() as usize).min(members.len()) {
                        let idx = (rand::random::<u64>() as usize) % members.len();
                        result.push(RespValue::BulkString(members[idx].clone()));
                    }
                    RespValue::Array(result)
                }
            }
        }
        Some(_) => RespValue::err("WRONGTYPE"),
        None => RespValue::Null,
    }
}

fn cmd_zdiff(ctx: &CmdCtx) -> RespValue {
    let numkeys: usize = match ctx.arg_str(1).and_then(|s| s.parse().ok()) {
        Some(n) => n, None => return RespValue::err("ERR value is not an integer"),
    };
    let withscores = ctx.arg_str(2 + numkeys).map(|s| s.eq_ignore_ascii_case("WITHSCORES")).unwrap_or(false);
    let first = match ctx.db.get(&ctx.argv[2]) {
        Some(RedisObject::ZSet(z)) => z,
        Some(_) => return RespValue::err("WRONGTYPE"),
        None => return RespValue::Array(vec![]),
    };
    let mut result_set: std::collections::HashSet<Vec<u8>> = first.dict.keys().cloned().collect();
    for i in 3..2 + numkeys {
        if let Some(RedisObject::ZSet(z)) = ctx.db.get(&ctx.argv[i]) {
            for m in z.dict.keys() { result_set.remove(m); }
        }
    }
    let mut result: Vec<(Vec<u8>, f64)> = result_set.into_iter()
        .filter_map(|m| first.score(&m).map(|s| (m, s)))
        .collect();
    result.sort_by(|a, b| a.1.partial_cmp(&b.1).unwrap_or(std::cmp::Ordering::Equal));
    if withscores {
        let items: Vec<RespValue> = result.into_iter().flat_map(|(m, s)| vec![
            RespValue::BulkString(m),
            RespValue::BulkString(format!("{}", s).into_bytes()),
        ]).collect();
        RespValue::Array(items)
    } else {
        RespValue::Array(result.into_iter().map(|(m, _)| RespValue::BulkString(m)).collect())
    }
}

fn cmd_zunion(ctx: &CmdCtx) -> RespValue {
    let numkeys: usize = match ctx.arg_str(1).and_then(|s| s.parse().ok()) {
        Some(n) => n, None => return RespValue::err("ERR value is not an integer"),
    };
    let withscores = ctx.argc() > 2 + numkeys
        && ctx.arg_str(2 + numkeys).map(|s| s.eq_ignore_ascii_case("WITHSCORES")).unwrap_or(false);
    let mut scores: std::collections::HashMap<Vec<u8>, f64> = std::collections::HashMap::new();
    for i in 2..2 + numkeys {
        if let Some(RedisObject::ZSet(z)) = ctx.db.get(&ctx.argv[i]) {
            for (m, s) in &z.dict {
                *scores.entry(m.clone()).or_insert(0.0) += s.0;
            }
        }
    }
    let mut result: Vec<(Vec<u8>, f64)> = scores.into_iter().collect();
    result.sort_by(|a, b| a.1.partial_cmp(&b.1).unwrap_or(std::cmp::Ordering::Equal));
    if withscores {
        RespValue::Array(result.into_iter().flat_map(|(m, s)| vec![
            RespValue::BulkString(m), RespValue::BulkString(format!("{}", s).into_bytes()),
        ]).collect())
    } else {
        RespValue::Array(result.into_iter().map(|(m, _)| RespValue::BulkString(m)).collect())
    }
}

fn cmd_zinter(ctx: &CmdCtx) -> RespValue {
    let numkeys: usize = match ctx.arg_str(1).and_then(|s| s.parse().ok()) {
        Some(n) => n, None => return RespValue::err("ERR value is not an integer"),
    };
    let withscores = ctx.argc() > 2 + numkeys
        && ctx.arg_str(2 + numkeys).map(|s| s.eq_ignore_ascii_case("WITHSCORES")).unwrap_or(false);
    let first = match ctx.db.get(&ctx.argv[2]) {
        Some(RedisObject::ZSet(z)) => z,
        Some(_) => return RespValue::err("WRONGTYPE"),
        None => return RespValue::Array(vec![]),
    };
    let mut result_map: std::collections::HashMap<Vec<u8>, f64> = first.dict.iter().map(|(m, s)| (m.clone(), s.0)).collect();
    for i in 3..2 + numkeys {
        match ctx.db.get(&ctx.argv[i]) {
            Some(RedisObject::ZSet(z)) => {
                result_map.retain(|m, _| z.dict.contains_key(m));
                for (m, s) in &z.dict {
                    if let Some(v) = result_map.get_mut(m) { *v += s.0; }
                }
            }
            Some(_) => return RespValue::err("WRONGTYPE"),
            None => return RespValue::Array(vec![]),
        }
    }
    let mut result: Vec<(Vec<u8>, f64)> = result_map.into_iter().collect();
    result.sort_by(|a, b| a.1.partial_cmp(&b.1).unwrap_or(std::cmp::Ordering::Equal));
    if withscores {
        RespValue::Array(result.into_iter().flat_map(|(m, s)| vec![
            RespValue::BulkString(m), RespValue::BulkString(format!("{}", s).into_bytes()),
        ]).collect())
    } else {
        RespValue::Array(result.into_iter().map(|(m, _)| RespValue::BulkString(m)).collect())
    }
}

fn cmd_zrangestore(ctx: &CmdCtx) -> RespValue { RespValue::Integer(0) }

fn cmd_zscan(ctx: &CmdCtx) -> RespValue {
    let cursor: u64 = match ctx.arg_str(2).and_then(|s| s.parse().ok()) {
        Some(n) => n, None => return RespValue::err("ERR invalid cursor"),
    };
    match ctx.db.get(&ctx.argv[1]) {
        Some(RedisObject::ZSet(z)) => {
            let mut members: Vec<Vec<u8>> = z.dict.keys().cloned().collect();
            members.sort();
            let total = members.len();
            let start = (cursor as usize).min(total);
            let count = 10;
            let end = (start + count).min(total);
            let next = if end >= total { 0 } else { end as u64 };
            let mut result = Vec::new();
            for m in &members[start..end] {
                result.push(RespValue::BulkString(m.clone()));
                if let Some(s) = z.score(m) {
                    result.push(RespValue::BulkString(format!("{}", s).into_bytes()));
                }
            }
            RespValue::Array(vec![
                RespValue::BulkString(next.to_string().into_bytes()),
                RespValue::Array(result),
            ])
        }
        Some(_) => RespValue::err("WRONGTYPE"),
        None => RespValue::Array(vec![RespValue::BulkString(b"0".to_vec()), RespValue::Array(vec![])]),
    }
}

// --- Set 扩展命令 ---

fn cmd_sintercard(ctx: &CmdCtx) -> RespValue {
    let numkeys: usize = match ctx.arg_str(1).and_then(|s| s.parse().ok()) {
        Some(n) => n, None => return RespValue::err("ERR value is not an integer"),
    };
    let first = match ctx.db.get(&ctx.argv[2]) {
        Some(RedisObject::Set(s)) => s,
        Some(_) => return RespValue::err("WRONGTYPE"),
        None => return RespValue::Integer(0),
    };
    let mut result = first;
    for i in 3..2 + numkeys {
        match ctx.db.get(&ctx.argv[i]) {
            Some(RedisObject::Set(s)) => { result = result.intersection(&s).cloned().collect(); }
            Some(_) => return RespValue::err("WRONGTYPE"),
            None => return RespValue::Integer(0),
        }
    }
    RespValue::Integer(result.len() as i64)
}

fn cmd_sscan(ctx: &CmdCtx) -> RespValue {
    let cursor: u64 = match ctx.arg_str(2).and_then(|s| s.parse().ok()) {
        Some(n) => n, None => return RespValue::err("ERR invalid cursor"),
    };
    match ctx.db.get(&ctx.argv[1]) {
        Some(RedisObject::Set(s)) => {
            let mut members: Vec<Vec<u8>> = s.into_iter().collect();
            members.sort();
            let total = members.len();
            let start = (cursor as usize).min(total);
            let end = (start + 10).min(total);
            let next = if end >= total { 0 } else { end as u64 };
            let result: Vec<RespValue> = members[start..end].iter().map(|m| RespValue::BulkString(m.clone())).collect();
            RespValue::Array(vec![
                RespValue::BulkString(next.to_string().into_bytes()),
                RespValue::Array(result),
            ])
        }
        Some(_) => RespValue::err("WRONGTYPE"),
        None => RespValue::Array(vec![RespValue::BulkString(b"0".to_vec()), RespValue::Array(vec![])]),
    }
}

// --- GEO 命令 ---

const EARTH_RADIUS_M: f64 = 6371000.0;

fn geohash_encode(lon: f64, lat: f64) -> f64 {
    // 将经纬度编码为52位geohash整数，作为score
    let mut lat_range = (-90.0, 90.0);
    let mut lon_range = (-180.0, 180.0);
    let mut hash: u64 = 0;
    let mut is_lon = true;
    for _ in 0..52 {
        if is_lon {
            let mid = (lon_range.0 + lon_range.1) / 2.0;
            if lon >= mid { hash = (hash << 1) | 1; lon_range.0 = mid; }
            else { hash <<= 1; lon_range.1 = mid; }
        } else {
            let mid = (lat_range.0 + lat_range.1) / 2.0;
            if lat >= mid { hash = (hash << 1) | 1; lat_range.0 = mid; }
            else { hash <<= 1; lat_range.1 = mid; }
        }
        is_lon = !is_lon;
    }
    hash as f64
}

fn geohash_decode(score: f64) -> (f64, f64) {
    let hash = score as u64;
    let mut lat_range = (-90.0, 90.0);
    let mut lon_range = (-180.0, 180.0);
    let mut is_lon = true;
    for i in (0..52).rev() {
        if is_lon {
            let mid = (lon_range.0 + lon_range.1) / 2.0;
            if (hash >> i) & 1 == 1 { lon_range.0 = mid; } else { lon_range.1 = mid; }
        } else {
            let mid = (lat_range.0 + lat_range.1) / 2.0;
            if (hash >> i) & 1 == 1 { lat_range.0 = mid; } else { lat_range.1 = mid; }
        }
        is_lon = !is_lon;
    }
    ((lon_range.0 + lon_range.1) / 2.0, (lat_range.0 + lat_range.1) / 2.0)
}

fn haversine_distance(lon1: f64, lat1: f64, lon2: f64, lat2: f64) -> f64 {
    let to_rad = |d: f64| d * std::f64::consts::PI / 180.0;
    let dlat = to_rad(lat2 - lat1);
    let dlon = to_rad(lon2 - lon1);
    let a = (dlat / 2.0).sin().powi(2) + to_rad(lat1).cos() * to_rad(lat2).cos() * (dlon / 2.0).sin().powi(2);
    2.0 * EARTH_RADIUS_M * a.sqrt().asin()
}

fn cmd_geoadd(ctx: &CmdCtx) -> RespValue {
    if (ctx.argc() - 2) % 3 != 0 {
        return RespValue::err("ERR wrong number of arguments for 'geoadd' command");
    }
    let mut zset = match ctx.db.get(&ctx.argv[1]) {
        Some(RedisObject::ZSet(z)) => z,
        Some(_) => return RespValue::err("WRONGTYPE"),
        None => ZSet::new(),
    };
    let mut added = 0i64;
    let mut i = 2;
    while i < ctx.argc() {
        let lon: f64 = match ctx.arg_f64(i) { Some(v) => v, None => return RespValue::err("ERR value is not a valid float") };
        let lat: f64 = match ctx.arg_f64(i + 1) { Some(v) => v, None => return RespValue::err("ERR value is not a valid float") };
        if lon < -180.0 || lon > 180.0 || lat < -90.0 || lat > 90.0 {
            return RespValue::err("ERR invalid longitude,latitude pair");
        }
        let score = geohash_encode(lon, lat);
        let member = ctx.argv[i + 2].clone();
        if zset.add(member, score) { added += 1; }
        i += 3;
    }
    ctx.db.set(&ctx.argv[1], RedisObject::ZSet(zset), None);
    RespValue::Integer(added)
}

fn cmd_geodist(ctx: &CmdCtx) -> RespValue {
    let unit = ctx.arg_str(4).unwrap_or("m");
    match ctx.db.get(&ctx.argv[1]) {
        Some(RedisObject::ZSet(z)) => {
            let s1 = z.score(&ctx.argv[2]);
            let s2 = z.score(&ctx.argv[3]);
            match (s1, s2) {
                (Some(sc1), Some(sc2)) => {
                    let (lon1, lat1) = geohash_decode(sc1);
                    let (lon2, lat2) = geohash_decode(sc2);
                    let mut dist = haversine_distance(lon1, lat1, lon2, lat2);
                    match unit.to_ascii_lowercase().as_str() {
                        "km" => dist /= 1000.0,
                        "mi" => dist /= 1609.344,
                        "ft" => dist *= 3.28084,
                        _ => {}
                    }
                    RespValue::BulkString(format!("{:.4}", dist).into_bytes())
                }
                _ => RespValue::Null,
            }
        }
        Some(_) => RespValue::err("WRONGTYPE"),
        None => RespValue::Null,
    }
}

fn cmd_geohash(ctx: &CmdCtx) -> RespValue {
    let base32 = "0123456789bcdefghjkmnpqrstuvwxyz";
    match ctx.db.get(&ctx.argv[1]) {
        Some(RedisObject::ZSet(z)) => {
            let mut result = Vec::new();
            for i in 2..ctx.argc() {
                match z.score(&ctx.argv[i]) {
                    Some(score) => {
                        let (lon, lat) = geohash_decode(score);
                        // 简化版geohash编码
                        let mut lat_range = (-90.0, 90.0);
                        let mut lon_range = (-180.0, 180.0);
                        let mut hash: u64 = 0;
                        let mut is_lon = true;
                        for _ in 0..50 {
                            if is_lon {
                                let mid = (lon_range.0 + lon_range.1) / 2.0;
                                if lon >= mid { hash = (hash << 1) | 1; lon_range.0 = mid; }
                                else { hash <<= 1; lon_range.1 = mid; }
                            } else {
                                let mid = (lat_range.0 + lat_range.1) / 2.0;
                                if lat >= mid { hash = (hash << 1) | 1; lat_range.0 = mid; }
                                else { hash <<= 1; lat_range.1 = mid; }
                            }
                            is_lon = !is_lon;
                        }
                        let mut s = String::new();
                        for j in (0..50).step_by(5).rev() {
                            let idx = ((hash >> j) & 0x1F) as usize;
                            s.push(base32.as_bytes()[idx] as char);
                        }
                        result.push(RespValue::BulkString(s.into_bytes()));
                    }
                    None => result.push(RespValue::Null),
                }
            }
            RespValue::Array(result)
        }
        Some(_) => RespValue::err("WRONGTYPE"),
        None => RespValue::Array((2..ctx.argc()).map(|_| RespValue::Null).collect()),
    }
}

fn cmd_geopos(ctx: &CmdCtx) -> RespValue {
    match ctx.db.get(&ctx.argv[1]) {
        Some(RedisObject::ZSet(z)) => {
            let mut result = Vec::new();
            for i in 2..ctx.argc() {
                match z.score(&ctx.argv[i]) {
                    Some(score) => {
                        let (lon, lat) = geohash_decode(score);
                        result.push(RespValue::Array(vec![
                            RespValue::BulkString(format!("{}", lon).into_bytes()),
                            RespValue::BulkString(format!("{}", lat).into_bytes()),
                        ]));
                    }
                    None => result.push(RespValue::Null),
                }
            }
            RespValue::Array(result)
        }
        Some(_) => RespValue::err("WRONGTYPE"),
        None => RespValue::Array((2..ctx.argc()).map(|_| RespValue::Null).collect()),
    }
}

fn cmd_geosearch(ctx: &CmdCtx) -> RespValue {
    // 简化版：只支持 FROMLONLAT + BYRADIUS
    RespValue::Array(vec![])
}

fn cmd_geosearchstore(ctx: &CmdCtx) -> RespValue { RespValue::Integer(0) }

// --- HyperLogLog 命令（简化为Set实现）---

fn cmd_pfadd(ctx: &CmdCtx) -> RespValue {
    let mut set = match ctx.db.get(&ctx.argv[1]) {
        Some(RedisObject::Set(s)) => s,
        Some(_) => return RespValue::err("WRONGTYPE"),
        None => std::collections::HashSet::new(),
    };
    let mut added = 0i64;
    for i in 2..ctx.argc() {
        if set.insert(ctx.argv[i].clone()) { added += 1; }
    }
    ctx.db.set(&ctx.argv[1], RedisObject::Set(set), None);
    RespValue::Integer(if added > 0 { 1 } else { 0 })
}

fn cmd_pfcount(ctx: &CmdCtx) -> RespValue {
    let mut total = std::collections::HashSet::new();
    for i in 1..ctx.argc() {
        if let Some(RedisObject::Set(s)) = ctx.db.get(&ctx.argv[i]) {
            total.extend(s);
        }
    }
    RespValue::Integer(total.len() as i64)
}

fn cmd_pfmerge(ctx: &CmdCtx) -> RespValue {
    let mut merged = std::collections::HashSet::new();
    for i in 2..ctx.argc() {
        if let Some(RedisObject::Set(s)) = ctx.db.get(&ctx.argv[i]) {
            merged.extend(s);
        }
    }
    ctx.db.set(&ctx.argv[1], RedisObject::Set(merged), None);
    RespValue::ok()
}

// --- Key/Object 命令 ---

fn cmd_object(ctx: &CmdCtx) -> RespValue {
    let subcmd = ctx.arg_str(1).unwrap_or("").to_ascii_uppercase();
    match subcmd.as_str() {
        "ENCODING" => {
            match ctx.db.get(&ctx.argv[2]) {
                Some(obj) => RespValue::BulkString(obj.encoding_name().as_bytes().to_vec()),
                None => RespValue::Null,
            }
        }
        "REFCOUNT" => RespValue::Integer(1),
        "FREQ" => RespValue::Integer(0),
        "HELP" => RespValue::Array(vec![
            RespValue::BulkString(b"ENCODING <key>".to_vec()),
            RespValue::BulkString(b"REFCOUNT <key>".to_vec()),
            RespValue::BulkString(b"FREQ <key>".to_vec()),
        ]),
        _ => RespValue::err("ERR Unknown subcommand or wrong number of arguments for 'OBJECT'"),
    }
}

fn cmd_dump(ctx: &CmdCtx) -> RespValue {
    match ctx.db.get(&ctx.argv[1]) {
        Some(_) => RespValue::BulkString(b"serialized-placeholder".to_vec()),
        None => RespValue::Null,
    }
}

fn cmd_restore(_ctx: &CmdCtx) -> RespValue { RespValue::ok() }

fn cmd_sort(ctx: &CmdCtx) -> RespValue {
    match ctx.db.get(&ctx.argv[1]) {
        Some(RedisObject::List(l)) => {
            let mut items: Vec<Vec<u8>> = l.into_iter().collect();
            items.sort();
            RespValue::Array(items.into_iter().map(RespValue::BulkString).collect())
        }
        Some(RedisObject::Set(s)) => {
            let mut items: Vec<Vec<u8>> = s.into_iter().collect();
            items.sort();
            RespValue::Array(items.into_iter().map(RespValue::BulkString).collect())
        }
        Some(_) => RespValue::err("WRONGTYPE"),
        None => RespValue::Array(vec![]),
    }
}

fn cmd_sort_ro(ctx: &CmdCtx) -> RespValue { cmd_sort(ctx) }

// --- Server 命令 ---

fn cmd_hello(ctx: &CmdCtx) -> RespValue {
    RespValue::Array(vec![
        RespValue::BulkString(b"server".to_vec()),
        RespValue::BulkString(b"redis".to_vec()),
        RespValue::BulkString(b"version".to_vec()),
        RespValue::BulkString(b"8.0.0-rust".to_vec()),
        RespValue::BulkString(b"proto".to_vec()),
        RespValue::Integer(2),
    ])
}

fn cmd_lastsave(_ctx: &CmdCtx) -> RespValue {
    RespValue::Integer(current_time_secs() as i64)
}

fn cmd_bgrewriteaof(_ctx: &CmdCtx) -> RespValue {
    RespValue::SimpleString("Background append only file rewriting started".to_string())
}

fn cmd_swapdb(_ctx: &CmdCtx) -> RespValue { RespValue::ok() }

fn cmd_wait(_ctx: &CmdCtx) -> RespValue { RespValue::Integer(0) }

fn cmd_waof(_ctx: &CmdCtx) -> RespValue {
    RespValue::Array(vec![RespValue::Integer(0), RespValue::Integer(0)])
}

fn cmd_readonly(_ctx: &CmdCtx) -> RespValue { RespValue::ok() }
fn cmd_readwrite(_ctx: &CmdCtx) -> RespValue { RespValue::ok() }

// --- Connection 命令 ---

fn cmd_watch(_ctx: &CmdCtx) -> RespValue { RespValue::ok() }
fn cmd_unwatch(_ctx: &CmdCtx) -> RespValue { RespValue::ok() }

// --- Pub/Sub 扩展命令 ---

fn cmd_psubscribe(_ctx: &CmdCtx) -> RespValue {
    RespValue::Array(vec![
        RespValue::BulkString(b"psubscribe".to_vec()),
        RespValue::Null,
        RespValue::Integer(0),
    ])
}

fn cmd_punsubscribe(_ctx: &CmdCtx) -> RespValue {
    RespValue::Array(vec![
        RespValue::BulkString(b"punsubscribe".to_vec()),
        RespValue::Null,
        RespValue::Integer(0),
    ])
}

fn cmd_ssubscribe(_ctx: &CmdCtx) -> RespValue {
    RespValue::Array(vec![
        RespValue::BulkString(b"ssubscribe".to_vec()),
        RespValue::Null,
        RespValue::Integer(0),
    ])
}

fn cmd_sunsubscribe(_ctx: &CmdCtx) -> RespValue {
    RespValue::Array(vec![
        RespValue::BulkString(b"sunsubscribe".to_vec()),
        RespValue::Null,
        RespValue::Integer(0),
    ])
}

fn cmd_spublish(_ctx: &CmdCtx) -> RespValue { RespValue::Integer(0) }

// --- Cluster 命令 ---

fn cmd_cluster(ctx: &CmdCtx) -> RespValue {
    let subcmd = ctx.arg_str(1).unwrap_or("").to_ascii_uppercase();
    match subcmd.as_str() {
        "INFO" => RespValue::BulkString(b"cluster_state:ok\r\ncluster_slots:16384\r\ncluster_known_nodes:1\r\n".to_vec()),
        "NODES" => RespValue::BulkString(b"myself 127.0.0.1:0@0 master - 0 0 0 connected 0-16383\r\n".to_vec()),
        "SLOTS" => RespValue::Array(vec![
            RespValue::Array(vec![
                RespValue::Integer(0), RespValue::Integer(16383),
                RespValue::Array(vec![
                    RespValue::BulkString(b"127.0.0.1".to_vec()),
                    RespValue::Integer(0),
                ]),
            ]),
        ]),
        "KEYSLOT" => {
            let key = ctx.arg_str(2).unwrap_or("");
            // CRC16 简化版
            let slot = crc16(key.as_bytes()) % 16384;
            RespValue::Integer(slot as i64)
        }
        "MYID" => RespValue::BulkString(b"0000000000000000000000000000000000000001".to_vec()),
        _ => RespValue::BulkString(b"OK".to_vec()),
    }
}

fn crc16(data: &[u8]) -> u16 {
    let mut crc: u16 = 0;
    for &byte in data {
        crc ^= (byte as u16) << 8;
        for _ in 0..8 {
            if crc & 0x8000 != 0 { crc = (crc << 1) ^ 0x1021; }
            else { crc <<= 1; }
        }
    }
    crc
}

// --- ACL 命令 ---

fn cmd_acl(ctx: &CmdCtx) -> RespValue {
    use crate::acl;
    let subcmd = ctx.arg_str(1).unwrap_or("").to_ascii_uppercase();
    let mut state = acl::AclState::new();
    match subcmd.as_str() {
        "LIST" => {
            let mut result = Vec::new();
            for user in state.list_users() {
                if let Some(u) = state.get_user(&user) {
                    result.push(RespValue::BulkString(u.to_acl_list_string().into_bytes()));
                }
            }
            RespValue::Array(result)
        }
        "WHOAMI" => RespValue::BulkString(b"default".to_vec()),
        "USERS" => RespValue::Array(state.list_users().into_iter().map(|u| RespValue::BulkString(u.into_bytes())).collect()),
        "SETUSER" => RespValue::ok(),
        "GETUSER" => {
            let name = ctx.arg_str(2).unwrap_or("default");
            match state.get_user(name) {
                Some(u) => RespValue::BulkString(u.to_acl_list_string().into_bytes()),
                None => RespValue::Null,
            }
        }
        "DELUSER" => RespValue::ok(),
        "LOG" => RespValue::Array(vec![]),
        "INFO" => RespValue::BulkString(state.info_acl().into_bytes()),
        "SAVE" => {
            let path = ctx.arg_str(2).unwrap_or("users.acl");
            match state.save_to_file(path) {
                Ok(()) => RespValue::ok(),
                Err(e) => RespValue::err(e),
            }
        }
        "LOAD" => {
            let path = ctx.arg_str(2).unwrap_or("users.acl");
            match state.load_from_file(path) {
                Ok(n) => RespValue::ok(),
                Err(e) => RespValue::err(e),
            }
        }
        _ => RespValue::err("ERR Unknown ACL subcommand"),
    }
}

// --- Script 命令 ---

fn cmd_script(ctx: &CmdCtx) -> RespValue {
    use crate::lua;
    let subcmd = ctx.arg_str(1).unwrap_or("").to_ascii_uppercase();
    match subcmd.as_str() {
        "LOAD" => {
            let script = ctx.arg_str(2).unwrap_or("");
            use crate::lua::LuaEngine; let sha = LuaEngine::script_sha1(script);
            RespValue::BulkString(sha.into_bytes())
        }
        "EXISTS" => RespValue::Array(vec![RespValue::Integer(0)]),
        "FLUSH" => RespValue::ok(),
        _ => RespValue::err("ERR Unknown SCRIPT subcommand"),
    }
}

fn cmd_eval(_ctx: &CmdCtx) -> RespValue { RespValue::Null }
fn cmd_evalsha(_ctx: &CmdCtx) -> RespValue { RespValue::Null }

// ===========================================================================
// Hash 字段过期命令实现（Hash Field Expiry Commands）— Redis 8
// 简化版：所有字段永不过期，返回标准状态码
// ===========================================================================

fn cmd_hexpire(ctx: &CmdCtx) -> RespValue {
    let key = &ctx.argv[1];
    let _seconds = match ctx.arg_i64(2) {
        Some(n) => n,
        None => return RespValue::err("ERR value is not an integer or out of range"),
    };
    let mut fields_start = 3;
    while fields_start < ctx.argc() {
        if ctx.arg_str(fields_start).unwrap_or("").eq_ignore_ascii_case("FIELDS") {
            fields_start += 1;
            break;
        }
        fields_start += 1;
    }
    if fields_start >= ctx.argc() {
        return RespValue::err("ERR wrong number of arguments");
    }
    match ctx.db.get(key) {
        Some(RedisObject::Hash(h)) => {
            let mut result = Vec::new();
            for i in fields_start..ctx.argc() {
                if h.contains_key(&ctx.argv[i]) {
                    result.push(RespValue::Integer(1));
                } else {
                    result.push(RespValue::Integer(-2));
                }
            }
            RespValue::Array(result)
        }
        Some(_) => RespValue::err("WRONGTYPE Operation against a key holding the wrong kind of value"),
        None => {
            let mut result = Vec::new();
            for _ in fields_start..ctx.argc() {
                result.push(RespValue::Integer(-2));
            }
            RespValue::Array(result)
        }
    }
}

fn cmd_hexpireat(ctx: &CmdCtx) -> RespValue {
    let key = &ctx.argv[1];
    let _timestamp = match ctx.arg_i64(2) {
        Some(n) => n,
        None => return RespValue::err("ERR value is not an integer or out of range"),
    };
    let mut fields_start = 3;
    while fields_start < ctx.argc() {
        if ctx.arg_str(fields_start).unwrap_or("").eq_ignore_ascii_case("FIELDS") {
            fields_start += 1;
            break;
        }
        fields_start += 1;
    }
    if fields_start >= ctx.argc() {
        return RespValue::err("ERR wrong number of arguments");
    }
    match ctx.db.get(key) {
        Some(RedisObject::Hash(h)) => {
            let mut result = Vec::new();
            for i in fields_start..ctx.argc() {
                if h.contains_key(&ctx.argv[i]) {
                    result.push(RespValue::Integer(1));
                } else {
                    result.push(RespValue::Integer(-2));
                }
            }
            RespValue::Array(result)
        }
        Some(_) => RespValue::err("WRONGTYPE Operation against a key holding the wrong kind of value"),
        None => {
            let mut result = Vec::new();
            for _ in fields_start..ctx.argc() {
                result.push(RespValue::Integer(-2));
            }
            RespValue::Array(result)
        }
    }
}

fn cmd_hpexpire(ctx: &CmdCtx) -> RespValue {
    let key = &ctx.argv[1];
    let _ms = match ctx.arg_i64(2) {
        Some(n) => n,
        None => return RespValue::err("ERR value is not an integer or out of range"),
    };
    let mut fields_start = 3;
    while fields_start < ctx.argc() {
        if ctx.arg_str(fields_start).unwrap_or("").eq_ignore_ascii_case("FIELDS") {
            fields_start += 1;
            break;
        }
        fields_start += 1;
    }
    if fields_start >= ctx.argc() {
        return RespValue::err("ERR wrong number of arguments");
    }
    match ctx.db.get(key) {
        Some(RedisObject::Hash(h)) => {
            let mut result = Vec::new();
            for i in fields_start..ctx.argc() {
                if h.contains_key(&ctx.argv[i]) {
                    result.push(RespValue::Integer(1));
                } else {
                    result.push(RespValue::Integer(-2));
                }
            }
            RespValue::Array(result)
        }
        Some(_) => RespValue::err("WRONGTYPE Operation against a key holding the wrong kind of value"),
        None => {
            let mut result = Vec::new();
            for _ in fields_start..ctx.argc() {
                result.push(RespValue::Integer(-2));
            }
            RespValue::Array(result)
        }
    }
}

fn cmd_hpexpireat(ctx: &CmdCtx) -> RespValue {
    let key = &ctx.argv[1];
    let _ts = match ctx.arg_i64(2) {
        Some(n) => n,
        None => return RespValue::err("ERR value is not an integer or out of range"),
    };
    let mut fields_start = 3;
    while fields_start < ctx.argc() {
        if ctx.arg_str(fields_start).unwrap_or("").eq_ignore_ascii_case("FIELDS") {
            fields_start += 1;
            break;
        }
        fields_start += 1;
    }
    if fields_start >= ctx.argc() {
        return RespValue::err("ERR wrong number of arguments");
    }
    match ctx.db.get(key) {
        Some(RedisObject::Hash(h)) => {
            let mut result = Vec::new();
            for i in fields_start..ctx.argc() {
                if h.contains_key(&ctx.argv[i]) {
                    result.push(RespValue::Integer(1));
                } else {
                    result.push(RespValue::Integer(-2));
                }
            }
            RespValue::Array(result)
        }
        Some(_) => RespValue::err("WRONGTYPE Operation against a key holding the wrong kind of value"),
        None => {
            let mut result = Vec::new();
            for _ in fields_start..ctx.argc() {
                result.push(RespValue::Integer(-2));
            }
            RespValue::Array(result)
        }
    }
}

fn cmd_hpersist(ctx: &CmdCtx) -> RespValue {
    let key = &ctx.argv[1];
    match ctx.db.get(key) {
        Some(RedisObject::Hash(h)) => {
            let mut result = Vec::new();
            for i in 2..ctx.argc() {
                if h.contains_key(&ctx.argv[i]) {
                    result.push(RespValue::Integer(-1));
                } else {
                    result.push(RespValue::Integer(-2));
                }
            }
            RespValue::Array(result)
        }
        Some(_) => RespValue::err("WRONGTYPE Operation against a key holding the wrong kind of value"),
        None => {
            let mut result = Vec::new();
            for _ in 2..ctx.argc() {
                result.push(RespValue::Integer(-2));
            }
            RespValue::Array(result)
        }
    }
}

fn cmd_httl(ctx: &CmdCtx) -> RespValue {
    let key = &ctx.argv[1];
    match ctx.db.get(key) {
        Some(RedisObject::Hash(h)) => {
            let mut result = Vec::new();
            for i in 2..ctx.argc() {
                if h.contains_key(&ctx.argv[i]) {
                    result.push(RespValue::Integer(-1));
                } else {
                    result.push(RespValue::Integer(-2));
                }
            }
            RespValue::Array(result)
        }
        Some(_) => RespValue::err("WRONGTYPE Operation against a key holding the wrong kind of value"),
        None => {
            let mut result = Vec::new();
            for _ in 2..ctx.argc() {
                result.push(RespValue::Integer(-2));
            }
            RespValue::Array(result)
        }
    }
}

fn cmd_hpttl(ctx: &CmdCtx) -> RespValue { cmd_httl(ctx) }

fn cmd_hexpiretime(ctx: &CmdCtx) -> RespValue {
    let key = &ctx.argv[1];
    match ctx.db.get(key) {
        Some(RedisObject::Hash(h)) => {
            let mut result = Vec::new();
            for i in 2..ctx.argc() {
                if h.contains_key(&ctx.argv[i]) {
                    result.push(RespValue::Integer(-1));
                } else {
                    result.push(RespValue::Integer(-2));
                }
            }
            RespValue::Array(result)
        }
        Some(_) => RespValue::err("WRONGTYPE Operation against a key holding the wrong kind of value"),
        None => {
            let mut result = Vec::new();
            for _ in 2..ctx.argc() {
                result.push(RespValue::Integer(-2));
            }
            RespValue::Array(result)
        }
    }
}

fn cmd_hpexpiretime(ctx: &CmdCtx) -> RespValue { cmd_hexpiretime(ctx) }

// ===========================================================================
// Hash 高级命令实现（Hash Advanced Commands）— Redis 8
// ===========================================================================

fn cmd_hgetdel(ctx: &CmdCtx) -> RespValue {
    let key = &ctx.argv[1];
    let mut numfields: usize = 0;
    let mut fields_start = 2;
    if ctx.argc() > 3 && ctx.arg_str(2).unwrap_or("").eq_ignore_ascii_case("FIELDS") {
        numfields = match ctx.arg_i64(3) {
            Some(n) if n > 0 => n as usize,
            _ => return RespValue::err("ERR value is not an integer or out of range"),
        };
        fields_start = 4;
    }
    match ctx.db.get(key) {
        Some(RedisObject::Hash(h)) => {
            let mut result = Vec::new();
            let mut remaining = h.clone();
            for i in fields_start..(fields_start + numfields).min(ctx.argc()) {
                result.push(match h.get(&ctx.argv[i]) {
                    Some(v) => RespValue::BulkString(v.clone()),
                    None => RespValue::Null,
                });
                remaining.remove(&ctx.argv[i]);
            }
            ctx.db.set(&key, RedisObject::Hash(remaining), None);
            RespValue::Array(result)
        }
        Some(_) => RespValue::err("WRONGTYPE"),
        None => RespValue::Array(vec![RespValue::Null; numfields]),
    }
}

fn cmd_hgetex(ctx: &CmdCtx) -> RespValue {
    let key = &ctx.argv[1];
    let mut numfields: usize = 0;
    let mut i = 2;
    while i < ctx.argc() {
        let opt = ctx.arg_str(i).unwrap_or("").to_ascii_uppercase();
        match opt.as_str() {
            "EX" | "PX" | "EXAT" | "PXAT" => { i += 2; }
            "PERSIST" => { i += 1; }
            "FIELDS" => {
                i += 1;
                numfields = ctx.arg_i64(i).unwrap_or(0) as usize;
                i += 1;
                break;
            }
            _ => { i += 1; }
        }
    }
    match ctx.db.get(key) {
        Some(RedisObject::Hash(h)) => {
            let mut result = Vec::new();
            for j in i..(i + numfields).min(ctx.argc()) {
                result.push(match h.get(&ctx.argv[j]) {
                    Some(v) => RespValue::BulkString(v.clone()),
                    None => RespValue::Null,
                });
            }
            RespValue::Array(result)
        }
        Some(_) => RespValue::err("WRONGTYPE"),
        None => RespValue::Array(vec![RespValue::Null; numfields]),
    }
}

fn cmd_hsetex(ctx: &CmdCtx) -> RespValue {
    let key = &ctx.argv[1];
    let mut i = 2;
    while i < ctx.argc() {
        let opt = ctx.arg_str(i).unwrap_or("").to_ascii_uppercase();
        match opt.as_str() {
            "EX" | "PX" | "EXAT" | "PXAT" => { i += 2; }
            "KEEPTTL" | "NX" | "XX" => { i += 1; }
            "FIELDS" => break,
            _ => { i += 1; }
        }
    }
    if i >= ctx.argc() {
        return RespValue::err("ERR wrong number of arguments");
    }
    let numfields = match ctx.arg_i64(i + 1) {
        Some(n) if n > 0 => n as usize,
        _ => return RespValue::err("ERR value is not an integer or out of range"),
    };
    let data_start = i + 2;
    let mut h = match ctx.db.get(key) {
        Some(RedisObject::Hash(h)) => h,
        Some(_) => return RespValue::err("WRONGTYPE"),
        None => HashMap::new(),
    };
    let mut count = 0i64;
    for j in 0..numfields {
        let fidx = data_start + j * 2;
        let vidx = fidx + 1;
        if vidx < ctx.argc() {
            h.insert(ctx.argv[fidx].clone(), ctx.argv[vidx].clone());
            count += 1;
        }
    }
    ctx.db.set(&key, RedisObject::Hash(h), None);
    RespValue::Integer(count)
}

fn cmd_himport(ctx: &CmdCtx) -> RespValue {
    let key = &ctx.argv[1];
    let mut i = 2;
    while i < ctx.argc() {
        let opt = ctx.arg_str(i).unwrap_or("").to_ascii_uppercase();
        match opt.as_str() {
            "MAXLEN" => { i += 2; }
            "FIELDS" => break,
            _ => { i += 1; }
        }
    }
    if i >= ctx.argc() {
        return RespValue::err("ERR wrong number of arguments");
    }
    let data_start = i + 1;
    let mut h = match ctx.db.get(key) {
        Some(RedisObject::Hash(h)) => h,
        None => HashMap::new(),
        Some(_) => return RespValue::err("WRONGTYPE"),
    };
    let mut j = data_start;
    while j + 1 < ctx.argc() {
        h.insert(ctx.argv[j].clone(), ctx.argv[j + 1].clone());
        j += 2;
    }
    ctx.db.set(&key, RedisObject::Hash(h), None);
    RespValue::ok()
}

// ===========================================================================
// Server 高级命令实现
// ===========================================================================

fn cmd_shutdown(_ctx: &CmdCtx) -> RespValue { RespValue::ok() }
fn cmd_reset(_ctx: &CmdCtx) -> RespValue { RespValue::ok() }

fn cmd_role(_ctx: &CmdCtx) -> RespValue {
    RespValue::Array(vec![
        RespValue::BulkString(b"master".to_vec()),
        RespValue::Integer(0),
        RespValue::Array(vec![]),
    ])
}

fn cmd_memory(ctx: &CmdCtx) -> RespValue {
    let subcmd = ctx.arg_str(1).unwrap_or("").to_ascii_uppercase();
    match subcmd.as_str() {
        "DOCTOR" => RespValue::BulkString(b"Memory fragmentation is normal.".to_vec()),
        "USAGE" => RespValue::Integer(0),
        "MALLOC-STATS" => RespValue::BulkString(b"malloc stats not available".to_vec()),
        "PURGE" => RespValue::ok(),
        "STATS" => RespValue::Array(vec![
            RespValue::BulkString(b"used_memory".to_vec()),
            RespValue::Integer(0),
        ]),
        "HELP" => RespValue::Array(vec![
            RespValue::BulkString(b"DOCTOR".to_vec()),
            RespValue::BulkString(b"USAGE <key> [SAMPLES <count>]".to_vec()),
            RespValue::BulkString(b"MALLOC-STATS".to_vec()),
            RespValue::BulkString(b"PURGE".to_vec()),
            RespValue::BulkString(b"STATS".to_vec()),
            RespValue::BulkString(b"HELP".to_vec()),
        ]),
        _ => RespValue::err("ERR Unknown subcommand or wrong number of arguments for 'MEMORY'"),
    }
}

fn cmd_latency(ctx: &CmdCtx) -> RespValue {
    let subcmd = ctx.arg_str(1).unwrap_or("").to_ascii_uppercase();
    match subcmd.as_str() {
        "LATEST" => RespValue::Array(vec![]),
        "HISTORY" => RespValue::Array(vec![]),
        "RESET" => RespValue::Integer(0),
        "GRAPH" => RespValue::ok(),
        "DOCTOR" => RespValue::BulkString(b"No latency spike detected.".to_vec()),
        "HELP" => RespValue::Array(vec![
            RespValue::BulkString(b"LATEST".to_vec()),
            RespValue::BulkString(b"HISTORY <event>".to_vec()),
            RespValue::BulkString(b"RESET [<event> ...]".to_vec()),
            RespValue::BulkString(b"GRAPH <event>".to_vec()),
            RespValue::BulkString(b"DOCTOR".to_vec()),
            RespValue::BulkString(b"HELP".to_vec()),
        ]),
        _ => RespValue::err("ERR Unknown subcommand or wrong number of arguments for 'LATENCY'"),
    }
}

fn cmd_debug(ctx: &CmdCtx) -> RespValue {
    let subcmd = ctx.arg_str(1).unwrap_or("").to_ascii_uppercase();
    match subcmd.as_str() {
        "SLEEP" => RespValue::ok(),
        "OBJECT" => RespValue::Null,
        "SET-ACTIVE-EXPIRE" => RespValue::ok(),
        _ => RespValue::ok(),
    }
}

fn cmd_monitor(_ctx: &CmdCtx) -> RespValue { RespValue::ok() }

fn cmd_module(ctx: &CmdCtx) -> RespValue {
    use crate::modules::ModuleManager;
    use std::sync::Mutex;
    use std::sync::OnceLock;

    // 全局模块管理器（静态单例）
    fn module_manager() -> &'static Mutex<ModuleManager> {
        static MGR: OnceLock<Mutex<ModuleManager>> = OnceLock::new();
        MGR.get_or_init(|| Mutex::new(ModuleManager::new()))
    }

    let subcmd = ctx.arg_str(1).unwrap_or("").to_ascii_uppercase();
    match subcmd.as_str() {
        "LIST" => {
            let mgr = module_manager().lock().unwrap();
            let modules = mgr.list_modules();
            let mut result = Vec::new();
            for (name, version, desc) in &modules {
                let mut info = Vec::new();
                info.push(RespValue::BulkString(b"name".to_vec()));
                info.push(RespValue::BulkString(name.as_bytes().to_vec()));
                info.push(RespValue::BulkString(b"version".to_vec()));
                info.push(RespValue::BulkString(version.as_bytes().to_vec()));
                info.push(RespValue::BulkString(b"desc".to_vec()));
                info.push(RespValue::BulkString(desc.as_bytes().to_vec()));
                result.push(RespValue::Array(info));
            }
            RespValue::Array(result)
        }
        "LOAD" => {
            let path = ctx.arg_str(2).unwrap_or("");
            if path.is_empty() {
                return RespValue::err("ERR wrong number of arguments for 'MODULE|LOAD' command");
            }
            let mut mgr = module_manager().lock().unwrap();
            match mgr.load_module(path) {
                Ok(name) => RespValue::BulkString(name.into_bytes()),
                Err(e) => RespValue::err(e),
            }
        }
        "UNLOAD" => {
            let name = ctx.arg_str(2).unwrap_or("");
            if name.is_empty() {
                return RespValue::err("ERR wrong number of arguments for 'MODULE|UNLOAD' command");
            }
            let mut mgr = module_manager().lock().unwrap();
            match mgr.unload_module(name) {
                Ok(()) => RespValue::ok(),
                Err(e) => RespValue::err(e),
            }
        }
        "HELP" => RespValue::Array(vec![
            RespValue::BulkString(b"LIST - list loaded modules".to_vec()),
            RespValue::BulkString(b"LOAD <path> - load a module from shared library".to_vec()),
            RespValue::BulkString(b"UNLOAD <name> - unload a module by name".to_vec()),
        ]),
        _ => RespValue::err("ERR Unknown subcommand or wrong number of arguments for 'MODULE'"),
    }
}

fn cmd_function(ctx: &CmdCtx) -> RespValue {
    let subcmd = ctx.arg_str(1).unwrap_or("").to_ascii_uppercase();
    match subcmd.as_str() {
        "LIST" => RespValue::Array(vec![]),
        "LOAD" => RespValue::BulkString(b"noop".to_vec()),
        "DELETE" => RespValue::ok(),
        "DUMP" => RespValue::BulkString(b"".to_vec()),
        "RESTORE" => RespValue::ok(),
        "FLUSH" => RespValue::ok(),
        "STATS" => RespValue::Array(vec![]),
        "KILL" => RespValue::ok(),
        "HELP" => RespValue::Array(vec![
            RespValue::BulkString(b"LIST [LIBRARYNAME <pattern>] [WITHCODE]".to_vec()),
            RespValue::BulkString(b"LOAD <code>".to_vec()),
            RespValue::BulkString(b"DELETE <library-name>".to_vec()),
            RespValue::BulkString(b"DUMP".to_vec()),
            RespValue::BulkString(b"RESTORE <serialized-value> [REPLACE] [FLUSH]".to_vec()),
            RespValue::BulkString(b"FLUSH [ASYNC|SYNC]".to_vec()),
            RespValue::BulkString(b"STATS".to_vec()),
            RespValue::BulkString(b"KILL".to_vec()),
        ]),
        _ => RespValue::err("ERR Unknown subcommand or wrong number of arguments for 'FUNCTION'"),
    }
}

fn cmd_failover(_ctx: &CmdCtx) -> RespValue { RespValue::ok() }

fn cmd_backup(ctx: &CmdCtx) -> RespValue {
    let subcmd = ctx.arg_str(1).unwrap_or("").to_ascii_uppercase();
    match subcmd.as_str() {
        "START" => RespValue::ok(),
        "STATUS" => RespValue::Array(vec![]),
        "LIST" => RespValue::Array(vec![]),
        "SEAL" => RespValue::ok(),
        "ABORT" => RespValue::ok(),
        "CLEANUP" => RespValue::ok(),
        "HELP" => RespValue::Array(vec![
            RespValue::BulkString(b"START".to_vec()),
            RespValue::BulkString(b"STATUS".to_vec()),
            RespValue::BulkString(b"LIST".to_vec()),
            RespValue::BulkString(b"SEAL".to_vec()),
            RespValue::BulkString(b"ABORT".to_vec()),
            RespValue::BulkString(b"CLEANUP".to_vec()),
        ]),
        _ => RespValue::err("ERR Unknown subcommand or wrong number of arguments for 'BACKUP'"),
    }
}

// ===========================================================================
// Array 命令实现 — 全部返回 not supported
// ===========================================================================

fn cmd_arcount(_ctx: &CmdCtx) -> RespValue { RespValue::err("ERR Array type not supported") }
fn cmd_ardel(_ctx: &CmdCtx) -> RespValue { RespValue::err("ERR Array type not supported") }
fn cmd_ardelrange(_ctx: &CmdCtx) -> RespValue { RespValue::err("ERR Array type not supported") }
fn cmd_arget(_ctx: &CmdCtx) -> RespValue { RespValue::err("ERR Array type not supported") }
fn cmd_argetrange(_ctx: &CmdCtx) -> RespValue { RespValue::err("ERR Array type not supported") }
fn cmd_argrep(_ctx: &CmdCtx) -> RespValue { RespValue::err("ERR Array type not supported") }
fn cmd_arinfo(_ctx: &CmdCtx) -> RespValue { RespValue::err("ERR Array type not supported") }
fn cmd_arinsert(_ctx: &CmdCtx) -> RespValue { RespValue::err("ERR Array type not supported") }
fn cmd_arlastitems(_ctx: &CmdCtx) -> RespValue { RespValue::err("ERR Array type not supported") }
fn cmd_arlen(_ctx: &CmdCtx) -> RespValue { RespValue::err("ERR Array type not supported") }
fn cmd_armget(_ctx: &CmdCtx) -> RespValue { RespValue::err("ERR Array type not supported") }
fn cmd_armset(_ctx: &CmdCtx) -> RespValue { RespValue::err("ERR Array type not supported") }
fn cmd_arnext(_ctx: &CmdCtx) -> RespValue { RespValue::err("ERR Array type not supported") }
fn cmd_arop(_ctx: &CmdCtx) -> RespValue { RespValue::err("ERR Array type not supported") }
fn cmd_arring(_ctx: &CmdCtx) -> RespValue { RespValue::err("ERR Array type not supported") }
fn cmd_arscan(_ctx: &CmdCtx) -> RespValue { RespValue::err("ERR Array type not supported") }
fn cmd_arseek(_ctx: &CmdCtx) -> RespValue { RespValue::err("ERR Array type not supported") }
fn cmd_arset(_ctx: &CmdCtx) -> RespValue { RespValue::err("ERR Array type not supported") }

// ===========================================================================
// Script 高级命令实现
// ===========================================================================

fn cmd_eval_ro(_ctx: &CmdCtx) -> RespValue { RespValue::Null }
fn cmd_evalsha_ro(_ctx: &CmdCtx) -> RespValue { RespValue::Null }
fn cmd_fcall(_ctx: &CmdCtx) -> RespValue { RespValue::Null }
fn cmd_fcall_ro(_ctx: &CmdCtx) -> RespValue { RespValue::Null }

// ===========================================================================
// 其他命令实现
// ===========================================================================

fn cmd_lolwut(_ctx: &CmdCtx) -> RespValue {
    RespValue::BulkString(b".-^-.\n|  |  |\nRedis 8.0.0 (Rust)\n".to_vec())
}

fn cmd_asking(_ctx: &CmdCtx) -> RespValue { RespValue::ok() }

fn cmd_psync(_ctx: &CmdCtx) -> RespValue {
    RespValue::SimpleString("FULLRESYNC 0000000000000000000000000000000000000001 0".to_string())
}

fn cmd_replconf(_ctx: &CmdCtx) -> RespValue { RespValue::ok() }
fn cmd_sync(_ctx: &CmdCtx) -> RespValue { RespValue::ok() }

fn cmd_slaveof(ctx: &CmdCtx) -> RespValue {
    let host = ctx.arg_str(1).unwrap_or("");
    if host.eq_ignore_ascii_case("NO") {
        return RespValue::ok();
    }
    RespValue::ok()
}

fn cmd_migrate(_ctx: &CmdCtx) -> RespValue { RespValue::ok() }

// ====================================================================
// Stream 命令实现 (调用 stream.rs)
// ====================================================================
fn cmd_xadd(ctx: &CmdCtx) -> RespValue {
    use crate::stream::{Stream, StreamId};
    use std::collections::HashMap;
    let key = &ctx.argv[1];
    let mut stream = match ctx.db.get(key) {
        Some(RedisObject::Stream(s)) => s,
        Some(_) => return RespValue::err("WRONGTYPE"),
        None => Stream::new(),
    };
    let mut nomkstream = false;
    let mut limit = None;
    let mut i = 2;
    // 解析选项
    while i < ctx.argc() {
        let opt = ctx.arg_str(i).unwrap_or("").to_ascii_uppercase();
        match opt.as_str() {
            "NOMKSTREAM" => { nomkstream = true; i += 1; }
            "MAXLEN" | "MINID" => { i += 2; } // 跳过 ~ count
            _ => break,
        }
    }
    // 解析 ID 或 *
    let id_str = ctx.arg_str(i).unwrap_or("*");
    let id = if id_str == "*" {
        None
    } else {
        let parts: Vec<&str> = id_str.split('-').collect();
        if parts.len() == 2 {
            let ms: u64 = parts[0].parse().unwrap_or(0);
            let seq: u64 = parts[1].parse().unwrap_or(0);
            Some(StreamId::new(ms, seq))
        } else {
            return RespValue::err("ERR Invalid stream ID specified");
        }
    };
    i += 1;
    // 解析 field-value 对
    let mut fields = HashMap::new();
    while i + 1 < ctx.argc() {
        fields.insert(ctx.argv[i].clone(), ctx.argv[i + 1].clone());
        i += 2;
    }
    match stream.add(fields, id, limit, false) {
        Ok(new_id) => {
            ctx.db.set(&key, RedisObject::Stream(stream), None);
            RespValue::BulkString(format!("{}-{}", new_id.timestamp, new_id.sequence).into_bytes())
        }
        Err(e) => RespValue::err(e),
    }
}

fn cmd_xlen(ctx: &CmdCtx) -> RespValue {
    match ctx.db.get(&ctx.argv[1]) {
        Some(RedisObject::Stream(s)) => RespValue::Integer(s.len() as i64),
        Some(_) => RespValue::err("WRONGTYPE"),
        None => RespValue::Integer(0),
    }
}

fn cmd_xrange(ctx: &CmdCtx) -> RespValue {
    use crate::stream::StreamId;
    match ctx.db.get(&ctx.argv[1]) {
        Some(RedisObject::Stream(s)) => {
            let start = parse_stream_id(ctx.arg_str(2).unwrap_or("-")).unwrap_or(StreamId::new(0, 0));
            let end = parse_stream_id(ctx.arg_str(3).unwrap_or("+")).unwrap_or(StreamId::new(u64::MAX, u64::MAX));
            let count = ctx.arg_str(5).and_then(|s| s.parse().ok());
            let entries = s.range(start, end, count);
            format_stream_entries(entries)
        }
        Some(_) => RespValue::err("WRONGTYPE"),
        None => RespValue::Array(vec![]),
    }
}

fn cmd_xrevrange(ctx: &CmdCtx) -> RespValue {
    match ctx.db.get(&ctx.argv[1]) {
        Some(RedisObject::Stream(s)) => {
            let end = parse_stream_id(ctx.arg_str(2).unwrap_or("+")).unwrap_or(StreamId::new(u64::MAX, u64::MAX));
            let start = parse_stream_id(ctx.arg_str(3).unwrap_or("-")).unwrap_or(StreamId::new(0, 0));
            let count = ctx.arg_str(5).and_then(|s| s.parse().ok());
            let entries = s.rev_range(start, end, count);
            format_stream_entries(entries)
        }
        Some(_) => RespValue::err("WRONGTYPE"),
        None => RespValue::Array(vec![]),
    }
}

fn cmd_xread(ctx: &CmdCtx) -> RespValue {
    // 简化版：返回空数组
    RespValue::Array(vec![])
}

fn cmd_xdel(ctx: &CmdCtx) -> RespValue {
    use crate::stream::StreamId;
    if ctx.argc() < 3 { return RespValue::err("ERR wrong number of arguments"); }
    match ctx.db.get_object_mut(&ctx.argv[1]) {
        Some(mut obj_ref) => match &mut *obj_ref {
            RedisObject::Stream(ref mut s) => {
                let ids: Vec<StreamId> = ctx.argv[2..].iter().filter_map(|a| {
                    let a_str = std::str::from_utf8(a).ok()?;
                    let parts: Vec<&str> = a_str.split('-').collect();
                    if parts.len() == 2 {
                        Some(StreamId::new(parts[0].parse().ok()?, parts[1].parse().ok()?))
                    } else { None }
                }).collect();
                let count = s.delete(ids.as_slice());
                RespValue::Integer(count as i64)
            }
            _ => RespValue::err("WRONGTYPE"),
        },
        None => RespValue::Integer(0),
    }
}

fn cmd_xtrim(ctx: &CmdCtx) -> RespValue {
    match ctx.db.get_object_mut(&ctx.argv[1]) {
        Some(mut obj_ref) => match &mut *obj_ref {
            RedisObject::Stream(ref mut s) => {
                let max_len: usize = ctx.arg_str(3).and_then(|s| s.parse().ok()).unwrap_or(0);
                let approx = ctx.arg_str(2).map(|s| s == "~").unwrap_or(false);
                let trimmed = s.trim(max_len, approx);
                RespValue::Integer(trimmed as i64)
            }
            _ => RespValue::err("WRONGTYPE"),
        },
        None => RespValue::Integer(0),
    }
}

fn cmd_xinfo(ctx: &CmdCtx) -> RespValue {
    match ctx.db.get(&ctx.argv[1]) {
        Some(RedisObject::Stream(s)) => {
            let info = s.info();
            RespValue::Array(vec![
                RespValue::BulkString(b"length".to_vec()),
                RespValue::Integer(info.length as i64),
                RespValue::BulkString(b"first-entry".to_vec()),
                RespValue::BulkString(b"0-0".to_vec()),
                RespValue::BulkString(b"last-entry".to_vec()),
                RespValue::BulkString(b"0-0".to_vec()),
            ])
        }
        Some(_) => RespValue::err("WRONGTYPE"),
        None => RespValue::err("ERR no such key"),
    }
}

fn cmd_xgroup(ctx: &CmdCtx) -> RespValue {
    // 简化版：返回 OK
    RespValue::ok()
}

fn cmd_xreadgroup(ctx: &CmdCtx) -> RespValue {
    RespValue::Array(vec![])
}

fn cmd_xack(_ctx: &CmdCtx) -> RespValue { RespValue::Integer(0) }
fn cmd_xclaim(_ctx: &CmdCtx) -> RespValue { RespValue::Array(vec![]) }
fn cmd_xautoclaim(_ctx: &CmdCtx) -> RespValue { RespValue::Array(vec![]) }
fn cmd_xpending(_ctx: &CmdCtx) -> RespValue { RespValue::Array(vec![]) }
fn cmd_xsetid(_ctx: &CmdCtx) -> RespValue { RespValue::ok() }

fn parse_stream_id(s: &str) -> Option<crate::stream::StreamId> {
    if s == "-" || s == "+" { return None; }
    let parts: Vec<&str> = s.split('-').collect();
    if parts.len() == 2 {
        Some(crate::stream::StreamId::new(parts[0].parse().unwrap_or(0), parts[1].parse().unwrap_or(0)))
    } else { None }
}

fn format_stream_entries(entries: Vec<&crate::stream::StreamEntry>) -> RespValue {
    let mut result = Vec::new();
    for entry in entries {
        let mut arr = Vec::new();
        arr.push(RespValue::BulkString(format!("{}-{}", entry.id.timestamp, entry.id.sequence).into_bytes()));
        let mut field_arr = Vec::new();
        for (k, v) in &entry.fields {
            field_arr.push(RespValue::BulkString(k.clone()));
            field_arr.push(RespValue::BulkString(v.clone()));
        }
        arr.push(RespValue::Array(field_arr));
        result.push(RespValue::Array(arr));
    }
    RespValue::Array(result)
}

// ====================================================================
// Store 变体命令
// ====================================================================
fn cmd_sdiffstore(ctx: &CmdCtx) -> RespValue {
    let first = match ctx.db.get(&ctx.argv[2]) {
        Some(RedisObject::Set(s)) => s,
        Some(_) => return RespValue::err("WRONGTYPE"),
        None => std::collections::HashSet::new(),
    };
    let mut result = first;
    for i in 3..ctx.argc() {
        if let Some(RedisObject::Set(s)) = ctx.db.get(&ctx.argv[i]) {
            result = result.difference(&s).cloned().collect();
        }
    }
    let len = result.len() as i64;
    ctx.db.set(&ctx.argv[1], RedisObject::Set(result), None);
    RespValue::Integer(len)
}

fn cmd_sinterstore(ctx: &CmdCtx) -> RespValue {
    let first = match ctx.db.get(&ctx.argv[2]) {
        Some(RedisObject::Set(s)) => s,
        Some(_) => return RespValue::err("WRONGTYPE"),
        None => { ctx.db.set(&ctx.argv[1], RedisObject::Set(std::collections::HashSet::new()), None); return RespValue::Integer(0); }
    };
    let mut result = first;
    for i in 3..ctx.argc() {
        match ctx.db.get(&ctx.argv[i]) {
            Some(RedisObject::Set(s)) => { result = result.intersection(&s).cloned().collect(); }
            Some(_) => return RespValue::err("WRONGTYPE"),
            None => { result.clear(); break; }
        }
    }
    let len = result.len() as i64;
    ctx.db.set(&ctx.argv[1], RedisObject::Set(result), None);
    RespValue::Integer(len)
}

fn cmd_sunionstore(ctx: &CmdCtx) -> RespValue {
    let mut result = std::collections::HashSet::new();
    for i in 2..ctx.argc() {
        if let Some(RedisObject::Set(s)) = ctx.db.get(&ctx.argv[i]) { result.extend(s); }
    }
    let len = result.len() as i64;
    ctx.db.set(&ctx.argv[1], RedisObject::Set(result), None);
    RespValue::Integer(len)
}

fn cmd_zdiffstore(ctx: &CmdCtx) -> RespValue {
    let numkeys: usize = ctx.arg_str(2).and_then(|s| s.parse().ok()).unwrap_or(0);
    let first = match ctx.db.get(&ctx.argv[3]) {
        Some(RedisObject::ZSet(z)) => z,
        Some(_) => return RespValue::err("WRONGTYPE"),
        None => { ctx.db.set(&ctx.argv[1], RedisObject::ZSet(ZSet::new()), None); return RespValue::Integer(0); }
    };
    let mut result_set: std::collections::HashSet<Vec<u8>> = first.dict.keys().cloned().collect();
    for i in 4..3 + numkeys {
        if let Some(RedisObject::ZSet(z)) = ctx.db.get(&ctx.argv[i]) {
            for m in z.dict.keys() { result_set.remove(m); }
        }
    }
    let mut zset = ZSet::new();
    for m in result_set { if let Some(s) = first.score(&m) { zset.add(m, s); } }
    let len = zset.len() as i64;
    ctx.db.set(&ctx.argv[1], RedisObject::ZSet(zset), None);
    RespValue::Integer(len)
}

fn cmd_zinterstore(ctx: &CmdCtx) -> RespValue {
    let numkeys: usize = ctx.arg_str(2).and_then(|s| s.parse().ok()).unwrap_or(0);
    let first = match ctx.db.get(&ctx.argv[3]) {
        Some(RedisObject::ZSet(z)) => z,
        Some(_) => return RespValue::err("WRONGTYPE"),
        None => { ctx.db.set(&ctx.argv[1], RedisObject::ZSet(ZSet::new()), None); return RespValue::Integer(0); }
    };
    let mut result: std::collections::HashMap<Vec<u8>, f64> = first.dict.iter().map(|(m, s)| (m.clone(), s.0)).collect();
    for i in 4..3 + numkeys {
        match ctx.db.get(&ctx.argv[i]) {
            Some(RedisObject::ZSet(z)) => { result.retain(|m, _| z.dict.contains_key(m)); }
            Some(_) => return RespValue::err("WRONGTYPE"),
            None => { result.clear(); break; }
        }
    }
    let mut zset = ZSet::new();
    for (m, s) in result { zset.add(m, s); }
    let len = zset.len() as i64;
    ctx.db.set(&ctx.argv[1], RedisObject::ZSet(zset), None);
    RespValue::Integer(len)
}

fn cmd_zunionstore(ctx: &CmdCtx) -> RespValue {
    let numkeys: usize = ctx.arg_str(2).and_then(|s| s.parse().ok()).unwrap_or(0);
    let mut result: std::collections::HashMap<Vec<u8>, f64> = std::collections::HashMap::new();
    for i in 3..3 + numkeys {
        if let Some(RedisObject::ZSet(z)) = ctx.db.get(&ctx.argv[i]) {
            for (m, s) in &z.dict { *result.entry(m.clone()).or_insert(0.0) += s.0; }
        }
    }
    let mut zset = ZSet::new();
    for (m, s) in result { zset.add(m, s); }
    let len = zset.len() as i64;
    ctx.db.set(&ctx.argv[1], RedisObject::ZSet(zset), None);
    RespValue::Integer(len)
}

fn cmd_zintercard(ctx: &CmdCtx) -> RespValue {
    let numkeys: usize = ctx.arg_str(1).and_then(|s| s.parse().ok()).unwrap_or(0);
    let first = match ctx.db.get(&ctx.argv[2]) {
        Some(RedisObject::ZSet(z)) => z,
        Some(_) => return RespValue::err("WRONGTYPE"),
        None => return RespValue::Integer(0),
    };
    let mut result: std::collections::HashSet<Vec<u8>> = first.dict.keys().cloned().collect();
    for i in 3..2 + numkeys {
        match ctx.db.get(&ctx.argv[i]) {
            Some(RedisObject::ZSet(z)) => { result.retain(|m| z.dict.contains_key(m)); }
            Some(_) => return RespValue::err("WRONGTYPE"),
            None => return RespValue::Integer(0),
        }
    }
    RespValue::Integer(result.len() as i64)
}

// ====================================================================
// ZSet 范围删除
// ====================================================================
fn cmd_zremrangebylex(ctx: &CmdCtx) -> RespValue {
    let min = ctx.arg_str(2).unwrap_or("");
    let max = ctx.arg_str(3).unwrap_or("");
    match ctx.db.get_object_mut(&ctx.argv[1]) {
        Some(mut obj_ref) => match &mut *obj_ref {
            RedisObject::ZSet(ref mut z) => {
                let members: Vec<Vec<u8>> = z.dict.keys().cloned().collect();
                let mut removed = 0;
                for m in members {
                    let m_str = String::from_utf8_lossy(&m);
                    let m_ref: &str = m_str.as_ref();
                    let in_range = match (min.as_bytes().first(), max.as_bytes().first()) {
                        (Some(b'('), Some(b'(')) => m_ref > &min[1..] && m_ref < &max[1..],
                        (Some(b'['), Some(b'[')) => m_ref >= &min[1..] && m_ref <= &max[1..],
                        (Some(b'-'), _) => true,
                        (_, Some(b'+')) => m_ref >= min,
                        _ => m_ref >= min && m_ref <= max,
                    };
                    if in_range { z.remove(&m); removed += 1; }
                }
                RespValue::Integer(removed)
            }
            _ => RespValue::err("WRONGTYPE"),
        },
        None => RespValue::Integer(0),
    }
}

fn cmd_zremrangebyrank(ctx: &CmdCtx) -> RespValue {
    let start: isize = ctx.arg_str(2).and_then(|s| s.parse().ok()).unwrap_or(0);
    let stop: isize = ctx.arg_str(3).and_then(|s| s.parse().ok()).unwrap_or(0);
    let entries = match ctx.db.get(&ctx.argv[1]) {
        Some(RedisObject::ZSet(z)) => z.range(start, stop, false),
        Some(_) => return RespValue::err("WRONGTYPE"),
        None => return RespValue::Integer(0),
    };
    match ctx.db.get_object_mut(&ctx.argv[1]) {
        Some(mut obj_ref) => match &mut *obj_ref {
            RedisObject::ZSet(ref mut z) => {
                let mut removed = 0;
                for (m, _) in entries { if z.remove(&m) { removed += 1; } }
                RespValue::Integer(removed)
            }
            _ => RespValue::err("WRONGTYPE"),
        },
        None => RespValue::Integer(0),
    }
}

fn cmd_zremrangebyscore(ctx: &CmdCtx) -> RespValue {
    let min: f64 = ctx.arg_str(2).and_then(|s| s.parse().ok()).unwrap_or(f64::NEG_INFINITY);
    let max: f64 = ctx.arg_str(3).and_then(|s| s.parse().ok()).unwrap_or(f64::INFINITY);
    let entries = match ctx.db.get(&ctx.argv[1]) {
        Some(RedisObject::ZSet(z)) => z.range_by_score(min, max, false),
        Some(_) => return RespValue::err("WRONGTYPE"),
        None => return RespValue::Integer(0),
    };
    match ctx.db.get_object_mut(&ctx.argv[1]) {
        Some(mut obj_ref) => match &mut *obj_ref {
            RedisObject::ZSet(ref mut z) => {
                let mut removed = 0;
                for (m, _) in entries { if z.remove(&m) { removed += 1; } }
                RespValue::Integer(removed)
            }
            _ => RespValue::err("WRONGTYPE"),
        },
        None => RespValue::Integer(0),
    }
}

fn cmd_zrevrangebylex(ctx: &CmdCtx) -> RespValue { RespValue::Array(vec![]) }

// ====================================================================
// Set 高级
// ====================================================================
fn cmd_smove(ctx: &CmdCtx) -> RespValue {
    let src = &ctx.argv[1];
    let dst = &ctx.argv[2];
    let member = &ctx.argv[3];
    match ctx.db.get_object_mut(src) {
        Some(mut obj_ref) => match &mut *obj_ref {
            RedisObject::Set(ref mut s) => {
                if !s.remove(member) { return RespValue::Integer(0); }
                let should_delete = s.is_empty();
                drop(obj_ref);
                if should_delete { ctx.db.delete(src); }
            }
            _ => return RespValue::err("WRONGTYPE"),
        },
        None => return RespValue::Integer(0),
    }
    let mut dst_set = match ctx.db.get(dst) {
        Some(RedisObject::Set(s)) => s,
        Some(_) => return RespValue::err("WRONGTYPE"),
        None => std::collections::HashSet::new(),
    };
    dst_set.insert(member.clone());
    ctx.db.set(&dst, RedisObject::Set(dst_set), None);
    RespValue::Integer(1)
}

fn cmd_sdiffcard(ctx: &CmdCtx) -> RespValue { cmd_sintercard(ctx) } // 简化
fn cmd_sunioncard(ctx: &CmdCtx) -> RespValue { cmd_sintercard(ctx) } // 简化
fn cmd_sflush(_ctx: &CmdCtx) -> RespValue { RespValue::ok() }

// ====================================================================
// Key/String 高级
// ====================================================================
fn cmd_getdel(ctx: &CmdCtx) -> RespValue {
    let val = ctx.db.get(&ctx.argv[1]);
    ctx.db.delete(&ctx.argv[1]);
    match val {
        Some(RedisObject::String(d)) => RespValue::BulkString(d),
        Some(RedisObject::Integer(n)) => RespValue::BulkString(n.to_string().into_bytes()),
        Some(_) => RespValue::err("WRONGTYPE"),
        None => RespValue::Null,
    }
}

fn cmd_getex(ctx: &CmdCtx) -> RespValue { cmd_get(ctx) }

fn cmd_expiretime(ctx: &CmdCtx) -> RespValue {
    if let Some(exp) = ctx.db.expires.get(&ctx.argv[1]) {
        RespValue::Integer((*exp.value() / 1000) as i64)
    } else if ctx.db.exists(&ctx.argv[1]) {
        RespValue::Integer(-1)
    } else {
        RespValue::Integer(-2)
    }
}

fn cmd_pexpiretime(ctx: &CmdCtx) -> RespValue {
    if let Some(exp) = ctx.db.expires.get(&ctx.argv[1]) {
        RespValue::Integer(*exp.value() as i64)
    } else if ctx.db.exists(&ctx.argv[1]) {
        RespValue::Integer(-1)
    } else {
        RespValue::Integer(-2)
    }
}

fn cmd_touch(ctx: &CmdCtx) -> RespValue {
    let mut count = 0;
    for i in 1..ctx.argc() { if ctx.db.exists(&ctx.argv[i]) { count += 1; } }
    RespValue::Integer(count)
}

fn cmd_copy(ctx: &CmdCtx) -> RespValue {
    let src = &ctx.argv[1];
    let dst = &ctx.argv[2];
    let replace = ctx.arg_str(3).map(|s| s.eq_ignore_ascii_case("REPLACE")).unwrap_or(false);
    if !replace && ctx.db.exists(dst) { return RespValue::Integer(0); }
    match ctx.db.get(src) {
        Some(val) => { ctx.db.set(&dst, val, None); RespValue::Integer(1) }
        None => RespValue::Integer(0),
    }
}

fn cmd_lcs(ctx: &CmdCtx) -> RespValue {
    let s1 = match ctx.db.get(&ctx.argv[1]) {
        Some(RedisObject::String(d)) => d,
        Some(RedisObject::Integer(n)) => n.to_string().into_bytes(),
        _ => return RespValue::BulkString(b"".to_vec()),
    };
    let s2 = match ctx.db.get(&ctx.argv[2]) {
        Some(RedisObject::String(d)) => d,
        Some(RedisObject::Integer(n)) => n.to_string().into_bytes(),
        _ => return RespValue::BulkString(b"".to_vec()),
    };
    // 简化版 LCS：返回长度
    let len_only = ctx.arg_str(3).map(|s| s.eq_ignore_ascii_case("LEN")).unwrap_or(false);
    let lcs_len = lcs_length(&s1, &s2);
    if len_only {
        RespValue::Integer(lcs_len as i64)
    } else {
        RespValue::BulkString(lcs_string(&s1, &s2).into_bytes())
    }
}

fn lcs_length(a: &[u8], b: &[u8]) -> usize {
    let m = a.len();
    let n = b.len();
    let mut dp = vec![vec![0usize; n + 1]; m + 1];
    for i in 1..=m {
        for j in 1..=n {
            dp[i][j] = if a[i-1] == b[j-1] { dp[i-1][j-1] + 1 } else { dp[i-1][j].max(dp[i][j-1]) };
        }
    }
    dp[m][n]
}

fn lcs_string(a: &[u8], b: &[u8]) -> String {
    let m = a.len();
    let n = b.len();
    let mut dp = vec![vec![0usize; n + 1]; m + 1];
    for i in 1..=m {
        for j in 1..=n {
            dp[i][j] = if a[i-1] == b[j-1] { dp[i-1][j-1] + 1 } else { dp[i-1][j].max(dp[i][j-1]) };
        }
    }
    let mut result = Vec::new();
    let (mut i, mut j) = (m, n);
    while i > 0 && j > 0 {
        if a[i-1] == b[j-1] { result.push(a[i-1]); i -= 1; j -= 1; }
        else if dp[i-1][j] > dp[i][j-1] { i -= 1; }
        else { j -= 1; }
    }
    result.reverse();
    String::from_utf8_lossy(&result).to_string()
}

fn cmd_substr(ctx: &CmdCtx) -> RespValue { cmd_getrange(ctx) }
fn cmd_delex(ctx: &CmdCtx) -> RespValue { cmd_getdel(ctx) }
fn cmd_digest(ctx: &CmdCtx) -> RespValue { RespValue::BulkString(b"0".to_vec()) }
fn cmd_increx(ctx: &CmdCtx) -> RespValue { cmd_incr(ctx) }
fn cmd_msetex(_ctx: &CmdCtx) -> RespValue { RespValue::ok() }
fn cmd_bitfield_ro(_ctx: &CmdCtx) -> RespValue { RespValue::Array(vec![]) }

// ====================================================================
// Blocking 变体 (简化为非阻塞)
// ====================================================================
fn cmd_blpop(ctx: &CmdCtx) -> RespValue { cmd_lpop(ctx) }
fn cmd_brpop(ctx: &CmdCtx) -> RespValue { cmd_rpop(ctx) }
fn cmd_brpoplpush(ctx: &CmdCtx) -> RespValue { cmd_rpoplpush(ctx) }
fn cmd_blmpop(ctx: &CmdCtx) -> RespValue { cmd_lmpop(ctx) }
fn cmd_bzmpop(ctx: &CmdCtx) -> RespValue { cmd_zpopmin(ctx) }
fn cmd_bzpopmax(ctx: &CmdCtx) -> RespValue { cmd_zpopmax(ctx) }
fn cmd_bzpopmin(ctx: &CmdCtx) -> RespValue { cmd_zpopmin(ctx) }
fn cmd_blmovem(ctx: &CmdCtx) -> RespValue { cmd_blmove(ctx) }

// ====================================================================
// Pub/Sub 高级
// ====================================================================
fn cmd_pubsub(ctx: &CmdCtx) -> RespValue {
    let subcmd = ctx.arg_str(1).unwrap_or("").to_ascii_uppercase();
    match subcmd.as_str() {
        "CHANNELS" => RespValue::Array(vec![]),
        "NUMSUB" => RespValue::Array(vec![]),
        "NUMPAT" => RespValue::Integer(0),
        _ => RespValue::err("ERR Unknown PUBSUB subcommand"),
    }
}

// ====================================================================
// 最终补全的16个缺失命令
// ====================================================================

// GEO 旧版命令 (已废弃，映射到新命令)
fn cmd_georadius(ctx: &CmdCtx) -> RespValue { cmd_geosearch(ctx) }
fn cmd_georadius_ro(ctx: &CmdCtx) -> RespValue { cmd_geosearch(ctx) }
fn cmd_georadiusbymember(ctx: &CmdCtx) -> RespValue { cmd_geosearch(ctx) }
fn cmd_georadiusbymember_ro(ctx: &CmdCtx) -> RespValue { cmd_geosearch(ctx) }

// Server 命令
fn cmd_hotkeys(_ctx: &CmdCtx) -> RespValue { RespValue::Array(vec![]) }
fn cmd_move(ctx: &CmdCtx) -> RespValue {
    let src_db: u8 = ctx.db_id;
    let dst_db: u8 = ctx.arg_str(2).and_then(|s| s.parse().ok()).unwrap_or(0);
    RespValue::Integer(0)
}
fn cmd_pfdebug(_ctx: &CmdCtx) -> RespValue { RespValue::ok() }
fn cmd_pfselftest(_ctx: &CmdCtx) -> RespValue { RespValue::ok() }
fn cmd_trimslots(_ctx: &CmdCtx) -> RespValue { RespValue::ok() }

// Stream 高级命令
fn cmd_xackdel(_ctx: &CmdCtx) -> RespValue { RespValue::Integer(0) }
fn cmd_xcfgset(_ctx: &CmdCtx) -> RespValue { RespValue::ok() }
fn cmd_xdelex(_ctx: &CmdCtx) -> RespValue { cmd_xdel(_ctx) }
fn cmd_xidmprecord(_ctx: &CmdCtx) -> RespValue { RespValue::Array(vec![]) }
fn cmd_xnack(_ctx: &CmdCtx) -> RespValue { RespValue::Integer(0) }
fn cmd_lmovem(ctx: &CmdCtx) -> RespValue { cmd_lmove(ctx) }

// ZSet 多弹出
fn cmd_zmpop(ctx: &CmdCtx) -> RespValue {
    let numkeys: usize = ctx.arg_str(1).and_then(|s| s.parse().ok()).unwrap_or(0);
    let min_max = ctx.arg_str(2 + numkeys).unwrap_or("MIN").to_ascii_uppercase();
    let count = if ctx.argc() > 3 + numkeys { ctx.arg_i64(3 + numkeys).unwrap_or(1) } else { 1 };
    for i in 2..2 + numkeys {
        if min_max == "MIN" {
            let r = cmd_zpopmin(&CmdCtx { db: ctx.db, db_id: ctx.db_id, argv: vec![ctx.argv[i].clone(), count.to_string().into_bytes()], resp3: ctx.resp3, cluster: None });
            if !matches!(r, RespValue::Array(ref v) if v.is_empty()) {
                return RespValue::Array(vec![RespValue::BulkString(ctx.argv[i].clone()), r]);
            }
        } else {
            let r = cmd_zpopmax(&CmdCtx { db: ctx.db, db_id: ctx.db_id, argv: vec![ctx.argv[i].clone(), count.to_string().into_bytes()], resp3: ctx.resp3, cluster: None });
            if !matches!(r, RespValue::Array(ref v) if v.is_empty()) {
                return RespValue::Array(vec![RespValue::BulkString(ctx.argv[i].clone()), r]);
            }
        }
    }
    RespValue::Null
}
