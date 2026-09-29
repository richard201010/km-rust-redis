//! RDB 持久化模块。
//!
//! 将当前数据库状态序列化到文件，并从文件中恢复数据。
//!
//! # 文件格式
//!
//! ```text
//! [magic: 5 bytes "REDIS"]
//! [version: 4 bytes "0002"]
//! [records...]
//!   [0xF6][db_id: u8]                      // 切换到某个逻辑数据库，其后的记录属于该库
//!   [type: u8][expire_flag: u8]
//!     expire_flag == 1 时紧跟 [expire_at_ms: u64]  // 绝对过期时间戳（毫秒）
//!   [key_len: u32][key]
//!   [value_data: variable, depends on type]
//! [end_marker: 1 byte 0xFF]
//! ```
//!
//! # 各类型的值编码
//!
//! - **String / Integer**: `len(u32) + bytes`（Integer 以十进制文本存储，读回时尝试解析）
//! - **List**: `count(u32) + [len(u32) + bytes]*`
//! - **Set**: `count(u32) + [len(u32) + bytes]*`
//! - **Hash**: `count(u32) + [key_len(u32) + key + val_len(u32) + val]*`
//! - **ZSet**: `count(u32) + [member_len(u32) + member + score(f64)]*`
//! - **Stream**: 消息 + ID 序列 + 消费者组（含 PEL）
//!
//! 所有整数均为小端序。这是本项目自有的快照格式：文件头版本号为 `0002`，
//! 与真 Redis 的 RDB（变长编码 + listpack + CRC64 尾）不互相兼容，
//! 既不能被 redis-server 加载，也不能加载它产出的文件。
//!
//! # 标准 Redis RDB 的读取
//!
//! 读取端同时支持真 Redis 的标准格式（文件头 `REDIS0003` .. `REDIS0014`）：
//! 变长长度编码（6/14/32/64 位与整数、LZF 特殊编码）、操作码
//! （SELECTDB / EXPIRETIME(_MS) / RESIZEDB / AUX / IDLE / FREQ / EOF）、
//! LZF（fastlzf）解压，以及 listpack、ziplist、intset 三种容器，
//! 按 `redis-8.10/src/{rdb.c,listpack.c,ziplist.c,intset.c}` 的字节布局解析。
//! 只有写入仍然使用本项目的 `0002` 格式；`load_snapshot` 依据文件头自动分流。
//! 模块类型、stream 与 hash field TTL 等暂不支持的值类型会返回
//! `unsupported RDB type X` 错误。
//!
//! 多数据库、键的过期时间与 stream 类型都会完整落盘：SAVE 后重启不会丢 TTL，
//! 也不会把 1 号库的数据并进 0 号库。

use crate::db::{Database, RedisDb};
use crate::stream::{Consumer, ConsumerGroup, PendingEntry, Stream, StreamEntry, StreamId};
use crate::types::{current_time_ms, RedisObject, ZSet};
use std::collections::{HashMap, HashSet, VecDeque};
use std::fs::File;
use std::io::{self, BufReader, BufWriter, Read, Write};
use std::path::Path;

// RDB 文件魔数和版本
const RDB_MAGIC: &[u8; 5] = b"REDIS";
const RDB_VERSION: &[u8; 4] = b"0002";

// 类型字节常量
const TYPE_STRING: u8 = 0;
const TYPE_LIST: u8 = 1;
const TYPE_SET: u8 = 2;
const TYPE_ZSET: u8 = 3;
const TYPE_HASH: u8 = 4;
const TYPE_STREAM: u8 = 5;

// 文件级控制标记（与类型字节取值区间不重叠）
const RDB_SELECT_DB: u8 = 0xF6;
const RDB_EOF: u8 = 0xFF;

// 条目级过期标记
const EXPIRE_NONE: u8 = 0;
const EXPIRE_ABS: u8 = 1;

// ---------------------------------------------------------------------------
// 标准 Redis RDB（REDIS0003..REDIS0014）读取所需常量
// 取值与 redis-8.10/src/rdb.h 中的 RDB_OPCODE_* / RDB_TYPE_* 一一对应。
// ---------------------------------------------------------------------------

/// 标准 RDB 操作码
const S_OPCODE_IDLE: u8 = 0xF8; // LRU 空闲时间，后跟一个长度编码
const S_OPCODE_FREQ: u8 = 0xF9; // LFU 频率，后跟 1 字节
const S_OPCODE_AUX: u8 = 0xFA; // 辅助字段，两串
const S_OPCODE_RESIZEDB: u8 = 0xFB; // 库大小提示，两个长度编码
const S_OPCODE_EXPIRETIME_MS: u8 = 0xFC; // 毫秒过期时间，i64 小端
const S_OPCODE_EXPIRETIME: u8 = 0xFD; // 秒过期时间，i32 小端
const S_OPCODE_SELECTDB: u8 = 0xFE; // 切库，后跟一个长度编码
const S_OPCODE_EOF: u8 = 0xFF; // 文件结束

/// 标准 RDB 值类型
const S_TYPE_STRING: u8 = 0;
const S_TYPE_LIST: u8 = 1;
const S_TYPE_SET: u8 = 2;
const S_TYPE_ZSET: u8 = 3;
const S_TYPE_HASH: u8 = 4;
const S_TYPE_ZSET_2: u8 = 5; // ZSET，分数为 8 字节小端 double
const S_TYPE_LIST_ZIPLIST: u8 = 10;
const S_TYPE_SET_INTSET: u8 = 11;
const S_TYPE_ZSET_ZIPLIST: u8 = 12;
const S_TYPE_HASH_ZIPLIST: u8 = 13;
const S_TYPE_LIST_QUICKLIST: u8 = 14;
const S_TYPE_HASH_LISTPACK: u8 = 16;
const S_TYPE_ZSET_LISTPACK: u8 = 17;
const S_TYPE_LIST_QUICKLIST_2: u8 = 18;
const S_TYPE_SET_LISTPACK: u8 = 20;

/// 长度编码中的特殊编码（前缀 0b11），取值见 rdb.h 的 RDB_ENC_*
const S_ENC_INT8: u8 = 0;
const S_ENC_INT16: u8 = 1;
const S_ENC_INT32: u8 = 2;
const S_ENC_LZF: u8 = 3;

/// quicklist 节点容器类型（quicklist.h 的 QUICKLIST_NODE_CONTAINER_*）
const QL_CONTAINER_PLAIN: u64 = 1;
const QL_CONTAINER_PACKED: u64 = 2;

/// 预分配容量上限：损坏文件里的超大计数不应触发巨额分配。
const MAX_PREALLOC: usize = 1 << 20;

/// 从 RDB 恢复出来的单个键。
#[derive(Debug)]
pub struct LoadedKey {
    pub key: Vec<u8>,
    pub value: RedisObject,
    /// 绝对过期时间戳（毫秒）；`None` 表示该键持久不过期
    pub expire_at_ms: Option<u64>,
}

/// RDB 写入器，负责将数据库状态序列化到文件。
pub struct RdbWriter {
    writer: BufWriter<File>,
}

impl RdbWriter {
    /// 创建新的 RDB 写入器。
    pub fn new(path: &str) -> io::Result<Self> {
        let file = File::create(path)?;
        let writer = BufWriter::new(file);
        Ok(Self { writer })
    }

    /// 写入文件头（魔数 + 版本）。
    fn write_header(&mut self) -> io::Result<()> {
        self.writer.write_all(RDB_MAGIC)?;
        self.writer.write_all(RDB_VERSION)?;
        Ok(())
    }

    /// 写入 u32（小端序）。
    fn write_u32(&mut self, val: u32) -> io::Result<()> {
        self.writer.write_all(&val.to_le_bytes())
    }

    /// 写入 u64（小端序）。
    fn write_u64(&mut self, val: u64) -> io::Result<()> {
        self.writer.write_all(&val.to_le_bytes())
    }

    /// 写入 f64（小端序）。
    fn write_f64(&mut self, val: f64) -> io::Result<()> {
        self.writer.write_all(&val.to_le_bytes())
    }

    /// 写入带长度前缀的字节序列。
    fn write_bytes_with_len(&mut self, data: &[u8]) -> io::Result<()> {
        self.write_u32(data.len() as u32)?;
        self.writer.write_all(data)?;
        Ok(())
    }

    fn write_stream_id(&mut self, id: &StreamId) -> io::Result<()> {
        self.write_u64(id.timestamp)?;
        self.write_u64(id.sequence)
    }

    /// 写入一个 stream 值：消息、ID 序列与消费者组（含 PEL）。
    ///
    /// 消费者的待确认列表不单独落盘 —— 组级 PEL 已带 consumer_name，
    /// 恢复时据此重建，避免同一份数据写两遍。
    fn write_stream(&mut self, stream: &Stream) -> io::Result<()> {
        self.write_u32(stream.entries().len() as u32)?;
        for entry in stream.entries() {
            self.write_stream_id(&entry.id)?;
            self.write_u32(entry.fields.len() as u32)?;
            for (field, value) in &entry.fields {
                self.write_bytes_with_len(field)?;
                self.write_bytes_with_len(value)?;
            }
        }
        self.write_stream_id(&stream.last_id())?;
        self.write_u64(stream.entries_added())?;
        self.write_stream_id(&stream.max_deleted_entry_id())?;

        self.write_u32(stream.groups().len() as u32)?;
        for group in stream.groups().values() {
            self.write_bytes_with_len(group.name.as_bytes())?;
            self.write_stream_id(&group.last_delivered_id)?;
            self.write_u32(group.pending.len() as u32)?;
            for nack in &group.pending {
                self.write_stream_id(&nack.entry_id)?;
                self.write_u64(nack.delivery_time)?;
                self.write_u64(nack.delivery_count)?;
                self.write_bytes_with_len(nack.consumer_name.as_bytes())?;
            }
            self.write_u32(group.consumers.len() as u32)?;
            for name in group.consumers.keys() {
                self.write_bytes_with_len(name.as_bytes())?;
            }
        }
        Ok(())
    }

    /// 写入单个键值对（含其过期时间）。
    ///
    /// 类型字节与键长度必须先落盘，因此任何类型都不允许整体跳过 ——
    /// 跳过会让后续记录从键长处错位解析。
    fn write_entry(
        &mut self,
        key: &[u8],
        value: &RedisObject,
        expire_at_ms: Option<u64>,
    ) -> io::Result<()> {
        let type_byte = match value {
            RedisObject::String(_) | RedisObject::Integer(_) => TYPE_STRING,
            RedisObject::List(_) => TYPE_LIST,
            RedisObject::Set(_) => TYPE_SET,
            RedisObject::Hash(_) => TYPE_HASH,
            RedisObject::ZSet(_) => TYPE_ZSET,
            RedisObject::Stream(_) => TYPE_STREAM,
        };
        self.writer.write_all(&[type_byte])?;
        match expire_at_ms {
            Some(exp) => {
                self.writer.write_all(&[EXPIRE_ABS])?;
                self.write_u64(exp)?;
            }
            None => self.writer.write_all(&[EXPIRE_NONE])?,
        }
        self.write_bytes_with_len(key)?;

        match value {
            RedisObject::String(data) => self.write_bytes_with_len(data)?,
            RedisObject::Integer(n) => {
                // Integer 复用 String 编码，读回时按十进制解析
                self.write_bytes_with_len(&n.to_string().into_bytes())?
            }
            RedisObject::List(list) => {
                self.write_u32(list.len() as u32)?;
                for item in list {
                    self.write_bytes_with_len(item)?;
                }
            }
            RedisObject::Set(set) => {
                self.write_u32(set.len() as u32)?;
                for item in set {
                    self.write_bytes_with_len(item)?;
                }
            }
            RedisObject::Hash(hash) => {
                self.write_u32(hash.len() as u32)?;
                for (k, v) in hash {
                    self.write_bytes_with_len(k)?;
                    self.write_bytes_with_len(v)?;
                }
            }
            RedisObject::ZSet(zset) => {
                self.write_u32(zset.dict.len() as u32)?;
                for (member, score) in &zset.dict {
                    self.write_bytes_with_len(member)?;
                    self.write_f64(score.0)?;
                }
            }
            RedisObject::Stream(stream) => self.write_stream(stream)?,
        }
        Ok(())
    }

    /// 将一个逻辑数据库落盘：先写库号标记，再写库内所有未过期的键。
    fn write_database(&mut self, db: &Database) -> io::Result<()> {
        self.writer.write_all(&[RDB_SELECT_DB, db.id])?;
        for entry in db.data.iter() {
            let key = entry.key();
            let expire_at_ms = db.expires.get(key).map(|e| *e.value());
            if expire_at_ms.is_some_and(|exp| current_time_ms() >= exp) {
                continue; // 跳过已过期的键
            }
            self.write_entry(key, entry.value(), expire_at_ms)?;
        }
        Ok(())
    }

    /// 完成写入（写入结束标记）。
    fn write_footer(&mut self) -> io::Result<()> {
        self.writer.write_all(&[RDB_EOF])?;
        self.writer.flush()?;
        Ok(())
    }
}

/// 同步将当前所有数据库序列化到 RDB 文件。
///
/// # 参数
/// - `rdb`: 多数据库管理器
/// - `path`: RDB 文件路径
///
/// # 返回
/// - `Ok(())`: 保存成功
/// - `Err(e)`: IO 错误
pub fn save_snapshot(rdb: &RedisDb, path: &str) -> io::Result<()> {
    let mut writer = RdbWriter::new(path)?;
    writer.write_header()?;

    for db in &rdb.databases {
        writer.write_database(db)?;
    }

    writer.write_footer()?;
    log::info!("RDB snapshot saved to {}", path);
    Ok(())
}

/// `read_header` 的结果：文件是本项目自定义格式，还是真 Redis 的标准格式。
enum RdbFormat {
    /// 本项目自有的 `REDIS0002` 格式。
    Custom,
    /// 标准 Redis RDB，携带解析出的版本号（3..=14）。
    Standard(u32),
}

/// RDB 读取器，负责从文件中恢复数据库状态。
pub struct RdbReader {
    reader: BufReader<File>,
}

impl RdbReader {
    /// 创建新的 RDB 读取器。
    pub fn new(path: &str) -> io::Result<Self> {
        let file = File::open(path)?;
        let reader = BufReader::new(file);
        Ok(Self { reader })
    }

    /// 读取固定数量的字节。
    fn read_exact(&mut self, buf: &mut [u8]) -> io::Result<()> {
        self.reader.read_exact(buf)
    }

    /// 读取单个字节作为记录标记；文件自然结束时返回 `None`。
    fn read_tag(&mut self) -> io::Result<Option<u8>> {
        let mut buf = [0u8; 1];
        match self.reader.read_exact(&mut buf) {
            Ok(()) => Ok(Some(buf[0])),
            Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => Ok(None),
            Err(e) => Err(e),
        }
    }

    /// 读取 u32（小端序）。
    fn read_u32(&mut self) -> io::Result<u32> {
        let mut buf = [0u8; 4];
        self.read_exact(&mut buf)?;
        Ok(u32::from_le_bytes(buf))
    }

    /// 读取 u64（小端序）。
    fn read_u64(&mut self) -> io::Result<u64> {
        let mut buf = [0u8; 8];
        self.read_exact(&mut buf)?;
        Ok(u64::from_le_bytes(buf))
    }

    /// 读取 f64（小端序）。
    fn read_f64(&mut self) -> io::Result<f64> {
        let mut buf = [0u8; 8];
        self.read_exact(&mut buf)?;
        Ok(f64::from_le_bytes(buf))
    }

    /// 读取带长度前缀的字节序列。
    fn read_bytes_with_len(&mut self) -> io::Result<Vec<u8>> {
        let len = self.read_u32()? as usize;
        let mut buf = vec![0u8; len];
        self.read_exact(&mut buf)?;
        Ok(buf)
    }

    /// 读取带长度前缀的字符串。
    fn read_string(&mut self) -> io::Result<String> {
        Ok(String::from_utf8_lossy(&self.read_bytes_with_len()?).to_string())
    }

    /// 验证文件头并识别格式。
    ///
    /// - `REDIS0002` → 本项目自定义格式（`RdbFormat::Custom`）
    /// - `REDIS0003` ..= `REDIS0014` → 真 Redis 的标准格式（`RdbFormat::Standard`）
    /// - 其他 → `Unsupported RDB version` 错误
    fn read_header(&mut self) -> io::Result<RdbFormat> {
        let mut magic = [0u8; 5];
        self.read_exact(&mut magic)?;
        if &magic != RDB_MAGIC {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "Invalid RDB magic bytes",
            ));
        }
        let mut version = [0u8; 4];
        self.read_exact(&mut version)?;
        if &version == RDB_VERSION {
            return Ok(RdbFormat::Custom);
        }
        if version.iter().all(u8::is_ascii_digit) {
            if let Ok(ver) = std::str::from_utf8(&version).unwrap_or("").parse::<u32>() {
                if (3..=14).contains(&ver) {
                    return Ok(RdbFormat::Standard(ver));
                }
            }
        }
        Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!(
                "Unsupported RDB version: {}",
                String::from_utf8_lossy(&version)
            ),
        ))
    }

    fn read_stream_id(&mut self) -> io::Result<StreamId> {
        let timestamp = self.read_u64()?;
        let sequence = self.read_u64()?;
        Ok(StreamId::new(timestamp, sequence))
    }

    /// 读取一个 stream 值（类型字节已被消费）。
    fn read_stream(&mut self) -> io::Result<RedisObject> {
        let count = self.read_u32()? as usize;
        let mut entries = VecDeque::with_capacity(count);
        for _ in 0..count {
            let id = self.read_stream_id()?;
            let nfields = self.read_u32()? as usize;
            let mut fields = HashMap::with_capacity(nfields);
            for _ in 0..nfields {
                let field = self.read_bytes_with_len()?;
                let value = self.read_bytes_with_len()?;
                fields.insert(field, value);
            }
            entries.push_back(StreamEntry::new(id, fields));
        }
        let last_id = self.read_stream_id()?;
        let entries_added = self.read_u64()?;
        let max_deleted_entry_id = self.read_stream_id()?;

        let ngroups = self.read_u32()? as usize;
        let mut groups = HashMap::with_capacity(ngroups);
        for _ in 0..ngroups {
            let name = self.read_string()?;
            let last_delivered_id = self.read_stream_id()?;
            let npending = self.read_u32()? as usize;
            let mut pending = Vec::with_capacity(npending);
            let mut per_consumer: HashMap<String, Vec<PendingEntry>> = HashMap::new();
            for _ in 0..npending {
                let entry_id = self.read_stream_id()?;
                let delivery_time = self.read_u64()?;
                let delivery_count = self.read_u64()?;
                let consumer_name = self.read_string()?;
                let nack = PendingEntry {
                    entry_id,
                    consumer_name: consumer_name.clone(),
                    delivery_time,
                    delivery_count,
                };
                per_consumer
                    .entry(consumer_name)
                    .or_default()
                    .push(nack.clone());
                pending.push(nack);
            }
            let nconsumers = self.read_u32()? as usize;
            let mut consumers = HashMap::with_capacity(nconsumers);
            for _ in 0..nconsumers {
                let name = self.read_string()?;
                consumers.insert(
                    name.clone(),
                    Consumer {
                        name: name.clone(),
                        pending: per_consumer.remove(&name).unwrap_or_default(),
                    },
                );
            }
            groups.insert(
                name.clone(),
                ConsumerGroup {
                    name,
                    last_delivered_id,
                    consumers,
                    pending,
                },
            );
        }

        Ok(RedisObject::Stream(Stream::restore(
            entries,
            last_id,
            entries_added,
            max_deleted_entry_id,
            groups,
        )))
    }

    /// 读取一条键记录（类型字节已被消费）。
    fn read_entry(&mut self, type_byte: u8) -> io::Result<LoadedKey> {
        let mut flag = [0u8; 1];
        self.read_exact(&mut flag)?;
        let expire_at_ms = match flag[0] {
            EXPIRE_NONE => None,
            EXPIRE_ABS => Some(self.read_u64()?),
            other => {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!("Unknown RDB expire flag: {}", other),
                ))
            }
        };
        let key = self.read_bytes_with_len()?;
        let value = match type_byte {
            TYPE_STRING => {
                let data = self.read_bytes_with_len()?;
                // 尝试解析为整数
                match std::str::from_utf8(&data) {
                    Ok(s) => match s.parse::<i64>() {
                        Ok(n) => RedisObject::Integer(n),
                        Err(_) => RedisObject::String(data),
                    },
                    Err(_) => RedisObject::String(data),
                }
            }
            TYPE_LIST => {
                let count = self.read_u32()? as usize;
                let mut list = VecDeque::with_capacity(count);
                for _ in 0..count {
                    let item = self.read_bytes_with_len()?;
                    list.push_back(item);
                }
                RedisObject::List(list)
            }
            TYPE_SET => {
                let count = self.read_u32()? as usize;
                let mut set = HashSet::with_capacity(count);
                for _ in 0..count {
                    let item = self.read_bytes_with_len()?;
                    set.insert(item);
                }
                RedisObject::Set(set)
            }
            TYPE_HASH => {
                let count = self.read_u32()? as usize;
                let mut hash = HashMap::with_capacity(count);
                for _ in 0..count {
                    let k = self.read_bytes_with_len()?;
                    let v = self.read_bytes_with_len()?;
                    hash.insert(k, v);
                }
                RedisObject::Hash(hash)
            }
            TYPE_ZSET => {
                let count = self.read_u32()? as usize;
                let mut zset = ZSet::new();
                for _ in 0..count {
                    let member = self.read_bytes_with_len()?;
                    let score = self.read_f64()?;
                    zset.add(member, score);
                }
                RedisObject::ZSet(zset)
            }
            TYPE_STREAM => self.read_stream()?,
            other => {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!("Unknown RDB type byte: {}", other),
                ))
            }
        };

        Ok(LoadedKey {
            key,
            value,
            expire_at_ms,
        })
    }

    /// 读取本项目的自定义 `REDIS0002` 文件体，返回 库号 → 键列表。
    fn load_custom_snapshot(&mut self) -> io::Result<HashMap<u8, Vec<LoadedKey>>> {
        let mut databases: HashMap<u8, Vec<LoadedKey>> = HashMap::new();
        let mut current_db: u8 = 0;
        loop {
            let tag = match self.read_tag()? {
                Some(tag) => tag,
                None => break,
            };
            if tag == RDB_EOF {
                break;
            }
            if tag == RDB_SELECT_DB {
                let mut buf = [0u8; 1];
                self.read_exact(&mut buf)?;
                current_db = buf[0];
                continue;
            }
            let entry = self.read_entry(tag)?;
            databases.entry(current_db).or_default().push(entry);
        }
        Ok(databases)
    }

    // -----------------------------------------------------------------------
    // 标准 Redis RDB（REDIS0003..0014）读取
    // -----------------------------------------------------------------------

    /// 读取单个字节。
    fn read_u8(&mut self) -> io::Result<u8> {
        let mut buf = [0u8; 1];
        self.read_exact(&mut buf)?;
        Ok(buf[0])
    }

    /// 读取指定长度的字节；长度异常时返回错误而不是直接触发巨额分配。
    fn read_exact_vec(&mut self, len: usize) -> io::Result<Vec<u8>> {
        let mut buf = Vec::new();
        buf.try_reserve_exact(len).map_err(|_| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                format!("RDB length too large: {}", len),
            )
        })?;
        buf.resize(len, 0);
        self.read_exact(&mut buf)?;
        Ok(buf)
    }

    /// 读取标准 RDB 的长度编码（rdb.c 的 `rdbLoadLen`）。
    ///
    /// 返回 `(长度, 特殊编码)`；`Some(enc)` 表示该值是整数或 LZF 压缩串，
    /// 而不是紧跟其后的裸字节。
    fn read_len(&mut self) -> io::Result<(u64, Option<u8>)> {
        let b0 = self.read_u8()?;
        match b0 >> 6 {
            0 => Ok(((b0 & 0x3F) as u64, None)),
            1 => {
                let b1 = self.read_u8()?;
                Ok(((((b0 & 0x3F) as u64) << 8) | b1 as u64, None))
            }
            2 => {
                if b0 == 0x80 {
                    let mut buf = [0u8; 4];
                    self.read_exact(&mut buf)?;
                    Ok((u32::from_be_bytes(buf) as u64, None))
                } else if b0 == 0x81 {
                    let mut buf = [0u8; 8];
                    self.read_exact(&mut buf)?;
                    Ok((u64::from_be_bytes(buf), None))
                } else {
                    Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        format!("Unknown length encoding: {:#04x}", b0),
                    ))
                }
            }
            // 高两位 0b11：后 6 位是特殊编码类型
            _ => Ok(((b0 & 0x3F) as u64, Some(b0 & 0x3F))),
        }
    }

    /// 读取必须是普通数值的长度编码（计数、库号等）。
    fn read_count(&mut self, what: &str) -> io::Result<usize> {
        let (n, enc) = self.read_len()?;
        if enc.is_some() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("encoded length is not allowed for {}", what),
            ));
        }
        Ok(n as usize)
    }

    /// 读取标准 RDB 字符串（rdb.c 的 `rdbGenericLoadStringObject`）：
    /// 裸串、int8/int16/int32 整数编码或 LZF（fastlzf）压缩串。
    fn read_std_string(&mut self) -> io::Result<Vec<u8>> {
        let (len, enc) = self.read_len()?;
        match enc {
            None => self.read_exact_vec(len as usize),
            Some(S_ENC_INT8) => Ok(self.read_u8()?.to_string().into_bytes()),
            Some(S_ENC_INT16) => {
                let mut buf = [0u8; 2];
                self.read_exact(&mut buf)?;
                Ok(i16::from_le_bytes(buf).to_string().into_bytes())
            }
            Some(S_ENC_INT32) => {
                let mut buf = [0u8; 4];
                self.read_exact(&mut buf)?;
                Ok(i32::from_le_bytes(buf).to_string().into_bytes())
            }
            Some(S_ENC_LZF) => {
                // 布局：[0xC3][压缩长度][原始长度][压缩数据]
                let clen = self.read_count("LZF compressed length")?;
                let raw_len = self.read_count("LZF original length")?;
                let compressed = self.read_exact_vec(clen)?;
                lzf_decompress(&compressed, raw_len)
            }
            Some(other) => Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("Unknown RDB string encoding type {}", other),
            )),
        }
    }

    /// 读取 `rdbSaveDoubleValue` 写出的 double（长度前缀 + ASCII，见 rdb.c）。
    fn read_rdb_double(&mut self) -> io::Result<f64> {
        let len = self.read_u8()?;
        match len {
            253 => Ok(f64::NAN),
            254 => Ok(f64::INFINITY),
            255 => Ok(f64::NEG_INFINITY),
            _ => {
                let raw = self.read_exact_vec(len as usize)?;
                let text = String::from_utf8_lossy(&raw);
                text.parse::<f64>().map_err(|_| {
                    io::Error::new(
                        io::ErrorKind::InvalidData,
                        format!("Invalid RDB double: {}", text),
                    )
                })
            }
        }
    }

    /// 读取标准 RDB 文件体，返回 库号 → 键列表。
    fn load_standard_snapshot(&mut self, _version: u32) -> io::Result<HashMap<u8, Vec<LoadedKey>>> {
        let mut databases: HashMap<u8, Vec<LoadedKey>> = HashMap::new();
        let mut current_db: u8 = 0;
        let mut expire_at_ms: Option<u64> = None;

        loop {
            let opcode = self.read_u8()?;
            match opcode {
                S_OPCODE_EOF => break,
                S_OPCODE_SELECTDB => {
                    let (db, enc) = self.read_len()?;
                    if enc.is_some() || db > u8::MAX as u64 {
                        return Err(io::Error::new(
                            io::ErrorKind::InvalidData,
                            format!("Invalid SELECTDB database id: {}", db),
                        ));
                    }
                    current_db = db as u8;
                }
                S_OPCODE_EXPIRETIME => {
                    // 秒精度，i32 小端；值为绝对 Unix 时间戳
                    let mut buf = [0u8; 4];
                    self.read_exact(&mut buf)?;
                    let secs = i32::from_le_bytes(buf) as i64;
                    expire_at_ms = Some(secs.saturating_mul(1000).max(0) as u64);
                }
                S_OPCODE_EXPIRETIME_MS => {
                    // 毫秒精度，i64 小端
                    let mut buf = [0u8; 8];
                    self.read_exact(&mut buf)?;
                    expire_at_ms = Some((i64::from_le_bytes(buf)).max(0) as u64);
                }
                S_OPCODE_RESIZEDB => {
                    self.read_count("RESIZEDB db size")?;
                    self.read_count("RESIZEDB expires size")?;
                }
                S_OPCODE_AUX => {
                    self.read_std_string()?;
                    self.read_std_string()?;
                }
                S_OPCODE_IDLE => {
                    self.read_count("IDLE")?;
                }
                S_OPCODE_FREQ => {
                    self.read_u8()?;
                }
                // 其余 0xF0..=0xFE 都是本实现未覆盖的操作码
                // （MODULE_AUX / FUNCTION2 / KEY_META / HASH_TEMPLATE ...）
                0xF0..=0xFE => {
                    // 这些操作码后面的字节无法跳过（没有长度前缀），只能停止解析。
                    // 遵循 partial > nothing：已解析的键全部保留，只丢弃本操作码之后的内容。
                    log::error!(
                        "RDB: unsupported opcode {} — keeping {} keys already parsed, skipping the rest of the file",
                        opcode,
                        databases.values().map(|v| v.len()).sum::<usize>()
                    );
                    break;
                }
                ty => {
                    let key = self.read_std_string()?;
                    if !is_standard_type_supported(ty) {
                        // 未知类型的值同样无法跳过 → 停止解析，但保留已加载的键
                        log::error!(
                            "RDB: unsupported value type {} for key \"{}\" — keeping {} keys already parsed, skipping the rest of the file",
                            ty,
                            String::from_utf8_lossy(&key),
                            databases.values().map(|v| v.len()).sum::<usize>()
                        );
                        break;
                    }
                    let value = self.read_std_value(ty)?;
                    databases
                        .entry(current_db)
                        .or_default()
                        .push(LoadedKey {
                            key,
                            value,
                            expire_at_ms,
                        });
                    // 过期时间只作用于紧随其后的那个键
                    expire_at_ms = None;
                }
            }
        }

        let total: usize = databases.values().map(|keys| keys.len()).sum();
        log::info!(
            "Loaded {} keys across {} databases from standard RDB",
            total,
            databases.len()
        );
        Ok(databases)
    }

    /// 读取标准 RDB 的值（类型字节已被消费），映射为 `RedisObject`。
    fn read_std_value(&mut self, ty: u8) -> io::Result<RedisObject> {
        match ty {
            S_TYPE_STRING => Ok(string_to_object(self.read_std_string()?)),
            S_TYPE_LIST => {
                let n = self.read_count("list length")?;
                let mut list = VecDeque::with_capacity(n.min(MAX_PREALLOC));
                for _ in 0..n {
                    list.push_back(self.read_std_string()?);
                }
                Ok(RedisObject::List(list))
            }
            S_TYPE_SET => {
                let n = self.read_count("set length")?;
                let mut set = HashSet::with_capacity(n.min(MAX_PREALLOC));
                for _ in 0..n {
                    set.insert(self.read_std_string()?);
                }
                Ok(RedisObject::Set(set))
            }
            S_TYPE_HASH => {
                let n = self.read_count("hash length")?;
                let mut hash = HashMap::with_capacity(n.min(MAX_PREALLOC));
                for _ in 0..n {
                    let field = self.read_std_string()?;
                    let value = self.read_std_string()?;
                    hash.insert(field, value);
                }
                Ok(RedisObject::Hash(hash))
            }
            S_TYPE_ZSET | S_TYPE_ZSET_2 => {
                let n = self.read_count("zset length")?;
                let mut zset = ZSet::new();
                for _ in 0..n {
                    let member = self.read_std_string()?;
                    let score = if ty == S_TYPE_ZSET {
                        self.read_rdb_double()?
                    } else {
                        self.read_f64()?
                    };
                    if score.is_nan() {
                        return Err(io::Error::new(
                            io::ErrorKind::InvalidData,
                            "Zset with NAN score detected",
                        ));
                    }
                    zset.add(member, score);
                }
                Ok(RedisObject::ZSet(zset))
            }
            S_TYPE_LIST_ZIPLIST => Ok(list_from_entries(parse_ziplist(
                &self.read_std_string()?,
            )?)),
            S_TYPE_LIST_QUICKLIST => {
                // n 个 ziplist 节点，每个节点是一个字符串 blob
                let n = self.read_count("quicklist length")?;
                let mut list = VecDeque::new();
                for _ in 0..n {
                    let blob = self.read_std_string()?;
                    for entry in parse_ziplist(&blob)? {
                        list.push_back(entry.into_bytes());
                    }
                }
                Ok(RedisObject::List(list))
            }
            S_TYPE_LIST_QUICKLIST_2 => {
                // n 个节点，每个节点先写容器类型再写 listpack/裸串 blob
                let n = self.read_count("quicklist length")?;
                let mut list = VecDeque::new();
                for _ in 0..n {
                    let container = self.read_count("quicklist container")?;
                    let blob = self.read_std_string()?;
                    match container as u64 {
                        QL_CONTAINER_PLAIN => list.push_back(blob),
                        QL_CONTAINER_PACKED => {
                            for entry in parse_listpack(&blob)? {
                                list.push_back(entry.into_bytes());
                            }
                        }
                        other => {
                            return Err(io::Error::new(
                                io::ErrorKind::InvalidData,
                                format!("Invalid quicklist node container: {}", other),
                            ))
                        }
                    }
                }
                Ok(RedisObject::List(list))
            }
            S_TYPE_SET_INTSET => {
                let blob = self.read_std_string()?;
                let values = parse_intset(&blob)?;
                let mut set = HashSet::with_capacity(values.len().min(MAX_PREALLOC));
                for value in values {
                    set.insert(value.to_string().into_bytes());
                }
                Ok(RedisObject::Set(set))
            }
            S_TYPE_SET_LISTPACK => Ok(set_from_entries(parse_listpack(
                &self.read_std_string()?,
            )?)),
            S_TYPE_ZSET_ZIPLIST => zset_from_entries(parse_ziplist(&self.read_std_string()?)?),
            S_TYPE_ZSET_LISTPACK => zset_from_entries(parse_listpack(&self.read_std_string()?)?),
            S_TYPE_HASH_ZIPLIST => hash_from_entries(parse_ziplist(&self.read_std_string()?)?),
            S_TYPE_HASH_LISTPACK => hash_from_entries(parse_listpack(&self.read_std_string()?)?),
            other => Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("unsupported RDB type {}", other),
            )),
        }
    }
}

/// 标准 RDB 是否支持该值类型。
fn is_standard_type_supported(ty: u8) -> bool {
    matches!(
        ty,
        S_TYPE_STRING
            | S_TYPE_LIST
            | S_TYPE_SET
            | S_TYPE_ZSET
            | S_TYPE_HASH
            | S_TYPE_ZSET_2
            | S_TYPE_LIST_ZIPLIST
            | S_TYPE_SET_INTSET
            | S_TYPE_ZSET_ZIPLIST
            | S_TYPE_HASH_ZIPLIST
            | S_TYPE_LIST_QUICKLIST
            | S_TYPE_HASH_LISTPACK
            | S_TYPE_ZSET_LISTPACK
            | S_TYPE_LIST_QUICKLIST_2
            | S_TYPE_SET_LISTPACK
    )
}

/// 顶层数字串转成 `Integer`，与真 Redis 加载时的对象编码逻辑一致
/// （只有十进制规范写法才算整数，`0042` 之类保持字符串）。
fn string_to_object(data: Vec<u8>) -> RedisObject {
    if let Ok(text) = std::str::from_utf8(&data) {
        if let Ok(n) = text.parse::<i64>() {
            if n.to_string().as_bytes() == data.as_slice() {
                return RedisObject::Integer(n);
            }
        }
    }
    RedisObject::String(data)
}

/// listpack / ziplist 中的一个条目：字符串或整数。
#[derive(Debug, Clone, PartialEq)]
enum ContainerEntry {
    Str(Vec<u8>),
    Int(i64),
}

impl ContainerEntry {
    /// 转成字节序列：整数用十进制文本表示（与 Redis 的 SDS 视图一致）。
    fn into_bytes(self) -> Vec<u8> {
        match self {
            ContainerEntry::Str(s) => s,
            ContainerEntry::Int(n) => n.to_string().into_bytes(),
        }
    }

    /// 作为有序集合分数读取：整数直接转 f64，字符串按浮点文本解析。
    fn score(&self) -> io::Result<f64> {
        match self {
            ContainerEntry::Int(n) => Ok(*n as f64),
            ContainerEntry::Str(s) => std::str::from_utf8(s)
                .ok()
                .and_then(|t| t.parse::<f64>().ok())
                .ok_or_else(|| {
                    io::Error::new(
                        io::ErrorKind::InvalidData,
                        format!("Invalid zset score: {:?}", s),
                    )
                }),
        }
    }
}

/// 容器条目按字符串视图收集（列表、集合）。
fn list_from_entries(entries: Vec<ContainerEntry>) -> RedisObject {
    RedisObject::List(entries.into_iter().map(|e| e.into_bytes()).collect())
}

/// 容器条目按字符串视图收集为集合。
fn set_from_entries(entries: Vec<ContainerEntry>) -> RedisObject {
    RedisObject::Set(entries.into_iter().map(|e| e.into_bytes()).collect())
}

/// 容器条目按「字段, 值」两两配对成哈希。
fn hash_from_entries(entries: Vec<ContainerEntry>) -> io::Result<RedisObject> {
    if entries.len() % 2 != 0 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "Hash container with odd number of entries",
        ));
    }
    let mut iter = entries.into_iter();
    let mut hash = HashMap::new();
    while let Some(field) = iter.next() {
        let value = iter.next().expect("checked even length");
        hash.insert(field.into_bytes(), value.into_bytes());
    }
    Ok(RedisObject::Hash(hash))
}

/// 容器条目按「成员, 分数」两两配对成有序集合。
fn zset_from_entries(entries: Vec<ContainerEntry>) -> io::Result<RedisObject> {
    if entries.len() % 2 != 0 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "Zset container with odd number of entries",
        ));
    }
    let mut iter = entries.into_iter();
    let mut zset = ZSet::new();
    while let Some(member) = iter.next() {
        let score = iter.next().expect("checked even length");
        let score = score.score()?;
        if score.is_nan() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "Zset with NAN score detected",
            ));
        }
        zset.add(member.into_bytes(), score);
    }
    Ok(RedisObject::ZSet(zset))
}

/// listpack 条目占用的回填长度（backlen）字节数，见 listpack.c 的
/// `lpEncodeBacklenBytes`。
fn listpack_backlen_bytes(entry_len: usize) -> usize {
    if entry_len <= 127 {
        1
    } else if entry_len <= 16_383 {
        2
    } else if entry_len <= 2_097_151 {
        3
    } else if entry_len <= 268_435_455 {
        4
    } else {
        5
    }
}

/// 按补码规则把无符号值还原为有符号值（listpack.c `lpGet` 的写法）。
fn listpack_signed(uval: u64, negstart: u64, negmax: u64) -> i64 {
    if uval >= negstart {
        -((negmax - uval) as i64) - 1
    } else {
        uval as i64
    }
}

/// 解析整个 listpack（listpack.c），返回其中所有条目。
fn parse_listpack(buf: &[u8]) -> io::Result<Vec<ContainerEntry>> {
    if buf.len() < 7 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "Listpack too short",
        ));
    }
    let mut out = Vec::new();
    let mut p = 6usize; // 跳过 4 字节总长 + 2 字节数量
    loop {
        let first = *buf.get(p).ok_or_else(|| {
            io::Error::new(io::ErrorKind::InvalidData, "Listpack entry out of bounds")
        })?;
        if first == 0xFF {
            break; // LP_EOF
        }
        let (entry, content_len) = listpack_entry(buf, p)?;
        // content_len 不含回填长度，跳过回填长度即到下一条目
        p = p
            .checked_add(content_len)
            .and_then(|x| x.checked_add(listpack_backlen_bytes(content_len)))
            .ok_or_else(|| {
                io::Error::new(io::ErrorKind::InvalidData, "Listpack entry overflow")
            })?;
        if p > buf.len() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "Listpack entry out of bounds",
            ));
        }
        out.push(entry);
        if out.len() > (1usize << 28) {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "Listpack too many entries",
            ));
        }
    }
    Ok(out)
}

/// 解析 listpack 中 `p` 处的单个条目，返回条目与「编码 + 数据」的字节数
/// （不含尾部回填长度）。
fn listpack_entry(buf: &[u8], p: usize) -> io::Result<(ContainerEntry, usize)> {
    let need = |n: usize| -> io::Result<()> {
        if p + n <= buf.len() {
            Ok(())
        } else {
            Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "Listpack entry truncated",
            ))
        }
    };
    let e = buf[p];
    if e & 0x80 == 0 {
        // 00xxxxxx：7 位无符号整数，值就在编码字节里
        Ok((ContainerEntry::Int((e & 0x7F) as i64), 1))
    } else if e & 0xC0 == 0x80 {
        // 10xxxxxx：6 位长度的字符串
        let len = (e & 0x3F) as usize;
        need(1 + len)?;
        Ok((ContainerEntry::Str(buf[p + 1..p + 1 + len].to_vec()), 1 + len))
    } else if e & 0xE0 == 0xC0 {
        // 110xxxxx：13 位整数
        need(2)?;
        let uval = (((e & 0x1F) as u64) << 8) | buf[p + 1] as u64;
        Ok((
            ContainerEntry::Int(listpack_signed(uval, 1 << 12, 8191)),
            2,
        ))
    } else if e & 0xF0 == 0xE0 {
        // 1110xxxx：12 位长度的字符串
        need(2)?;
        let len = (((e & 0x0F) as usize) << 8) | buf[p + 1] as usize;
        need(2 + len)?;
        Ok((
            ContainerEntry::Str(buf[p + 2..p + 2 + len].to_vec()),
            2 + len,
        ))
    } else {
        match e {
            0xF0 => {
                // 32 位长度的字符串（小端）
                need(5)?;
                let len = u32::from_le_bytes(buf[p + 1..p + 5].try_into().unwrap()) as usize;
                need(5 + len)?;
                Ok((
                    ContainerEntry::Str(buf[p + 5..p + 5 + len].to_vec()),
                    5 + len,
                ))
            }
            0xF1 => {
                need(3)?;
                let uval = u16::from_le_bytes(buf[p + 1..p + 3].try_into().unwrap()) as u64;
                Ok((
                    ContainerEntry::Int(listpack_signed(uval, 1 << 15, u16::MAX as u64)),
                    3,
                ))
            }
            0xF2 => {
                need(4)?;
                let uval = (buf[p + 1] as u64)
                    | ((buf[p + 2] as u64) << 8)
                    | ((buf[p + 3] as u64) << 16);
                Ok((
                    ContainerEntry::Int(listpack_signed(uval, 1 << 23, 0xFF_FFFF)),
                    4,
                ))
            }
            0xF3 => {
                need(5)?;
                let uval = u32::from_le_bytes(buf[p + 1..p + 5].try_into().unwrap()) as u64;
                Ok((
                    ContainerEntry::Int(listpack_signed(uval, 1 << 31, u32::MAX as u64)),
                    5,
                ))
            }
            0xF4 => {
                need(9)?;
                let uval = u64::from_le_bytes(buf[p + 1..p + 9].try_into().unwrap());
                Ok((
                    ContainerEntry::Int(listpack_signed(uval, 1 << 63, u64::MAX)),
                    9,
                ))
            }
            other => Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("Unknown listpack encoding: {:#04x}", other),
            )),
        }
    }
}

/// 解析整个 ziplist（ziplist.c），返回其中所有条目。
fn parse_ziplist(buf: &[u8]) -> io::Result<Vec<ContainerEntry>> {
    if buf.len() < 11 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "Ziplist too short",
        ));
    }
    let mut out = Vec::new();
    let mut p = 10usize; // 4 字节总长 + 4 字节尾偏移 + 2 字节数量
    loop {
        let first = *buf.get(p).ok_or_else(|| {
            io::Error::new(io::ErrorKind::InvalidData, "Ziplist entry out of bounds")
        })?;
        if first == 0xFF {
            break; // ZIP_END
        }
        // [前一条目长度][编码][数据]
        let prevlen_size = if first < 254 { 1 } else { 5 };
        if p + prevlen_size >= buf.len() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "Ziplist prevlen out of bounds",
            ));
        }
        let epos = p + prevlen_size;
        let enc = buf[epos];
        let next = if enc < 0xC0 {
            // 字符串编码
            let (hdr, len) = match enc & 0xC0 {
                0x00 => (1usize, (enc & 0x3F) as usize),
                0x40 => (
                    2usize,
                    (((enc & 0x3F) as usize) << 8) | buf[epos + 1] as usize,
                ),
                _ => (
                    5usize,
                    u32::from_be_bytes(buf[epos + 1..epos + 5].try_into().unwrap()) as usize,
                ),
            };
            let start = epos + hdr;
            let end = start.checked_add(len).ok_or_else(|| {
                io::Error::new(io::ErrorKind::InvalidData, "Ziplist length overflow")
            })?;
            if end > buf.len() {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "Ziplist string out of bounds",
                ));
            }
            out.push(ContainerEntry::Str(buf[start..end].to_vec()));
            end
        } else {
            // 整数编码
            let (data_len, value) = match enc {
                0xC0 => {
                    (2usize, i16::from_le_bytes(buf[epos + 1..epos + 3].try_into().unwrap()) as i64)
                }
                0xD0 => (
                    4usize,
                    i32::from_le_bytes(buf[epos + 1..epos + 5].try_into().unwrap()) as i64,
                ),
                0xE0 => (
                    8usize,
                    i64::from_le_bytes(buf[epos + 1..epos + 9].try_into().unwrap()),
                ),
                0xF0 => {
                    // 24 位小端，符号扩展
                    let raw = (buf[epos + 1] as u32)
                        | ((buf[epos + 2] as u32) << 8)
                        | ((buf[epos + 3] as u32) << 16);
                    (3usize, (((raw << 8) as i32) >> 8) as i64)
                }
                0xFE => (1usize, buf[epos + 1] as i8 as i64),
                0xF1..=0xFD => (0usize, ((enc & 0x0F) as i64) - 1),
                other => {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        format!("Unknown ziplist encoding: {:#04x}", other),
                    ))
                }
            };
            if epos + 1 + data_len > buf.len() {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "Ziplist integer out of bounds",
                ));
            }
            out.push(ContainerEntry::Int(value));
            epos + 1 + data_len
        };
        if next <= p || next > buf.len() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "Ziplist entry out of bounds",
            ));
        }
        p = next;
        if out.len() > (1usize << 28) {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "Ziplist too many entries",
            ));
        }
    }
    Ok(out)
}

/// 解析 intset（intset.c）：`[encoding: u32][count: u32][数据...]`，全小端。
fn parse_intset(buf: &[u8]) -> io::Result<Vec<i64>> {
    if buf.len() < 8 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "Intset too short",
        ));
    }
    let encoding = u32::from_le_bytes(buf[0..4].try_into().unwrap());
    // 编码值就是元素宽度（sizeof(int16_t)=2 等）
    let width = match encoding {
        2 => 2usize,
        4 => 4usize,
        8 => 8usize,
        other => {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("Invalid intset encoding: {}", other),
            ))
        }
    };
    let count = u32::from_le_bytes(buf[4..8].try_into().unwrap()) as usize;
    let need = count.checked_mul(width).and_then(|n| n.checked_add(8));
    let need = need.ok_or_else(|| {
        io::Error::new(io::ErrorKind::InvalidData, "Intset length overflow")
    })?;
    if need > buf.len() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "Intset out of bounds",
        ));
    }
    let mut out = Vec::with_capacity(count.min(MAX_PREALLOC));
    for i in 0..count {
        let start = 8 + i * width;
        let value = match width {
            2 => i16::from_le_bytes(buf[start..start + 2].try_into().unwrap()) as i64,
            4 => i32::from_le_bytes(buf[start..start + 4].try_into().unwrap()) as i64,
            _ => i64::from_le_bytes(buf[start..start + 8].try_into().unwrap()),
        };
        out.push(value);
    }
    Ok(out)
}

/// LZF（fastlzf）解压，算法与 redis-8.10/src/lzf_d.c 的 `lzf_decompress`
/// 一致：控制字节 < 32 是字面量串，否则是回溯引用。
fn lzf_decompress(input: &[u8], out_len: usize) -> io::Result<Vec<u8>> {
    let invalid = || io::Error::new(io::ErrorKind::InvalidData, "Invalid LZF compressed string");
    let mut out = Vec::new();
    out.try_reserve_exact(out_len).map_err(|_| {
        io::Error::new(
            io::ErrorKind::InvalidData,
            format!("LZF output too large: {}", out_len),
        )
    })?;
    out.resize(out_len, 0);

    let mut ip = 0usize;
    let mut op = 0usize;
    while ip < input.len() {
        let ctrl = input[ip];
        ip += 1;
        if ctrl < (1 << 5) {
            // 字面量串：ctrl+1 个原始字节
            let len = ctrl as usize + 1;
            if ip + len > input.len() || op + len > out_len {
                return Err(invalid());
            }
            out[op..op + len].copy_from_slice(&input[ip..ip + len]);
            ip += len;
            op += len;
        } else {
            // 回溯引用：从输出缓冲区已有的数据里拷贝
            let mut len = (ctrl >> 5) as usize;
            if len == 7 {
                let extra = *input.get(ip).ok_or_else(invalid)? as usize;
                ip += 1;
                len += extra;
            }
            let low = *input.get(ip).ok_or_else(invalid)? as i64;
            ip += 1;
            let ref_off = op as i64 - (((ctrl & 0x1F) as i64) << 8) - 1 - low;
            if ref_off < 0 {
                return Err(invalid());
            }
            len += 2;
            if op + len > out_len {
                return Err(invalid());
            }
            // 逐字节拷贝，允许目标与源重叠
            let mut src = ref_off as usize;
            for _ in 0..len {
                out[op] = out[src];
                op += 1;
                src += 1;
            }
        }
    }
    if op != out_len {
        return Err(invalid());
    }
    Ok(out)
}

/// 从 RDB 文件加载数据，按逻辑数据库编号分组。
///
/// # 参数
/// - `path`: RDB 文件路径
///
/// # 返回
/// - `Ok(map)`: 库号 → 该库的键列表；文件不存在时为空 map
/// - `Err(e)`: 文件格式错误
pub fn load_snapshot(path: &str) -> io::Result<HashMap<u8, Vec<LoadedKey>>> {
    if !Path::new(path).exists() {
        log::info!(
            "No RDB file found at {}, starting with empty database",
            path
        );
        return Ok(HashMap::new());
    }

    let mut reader = RdbReader::new(path)?;
    let databases = match reader.read_header()? {
        RdbFormat::Custom => reader.load_custom_snapshot()?,
        RdbFormat::Standard(version) => reader.load_standard_snapshot(version)?,
    };

    let total: usize = databases.values().map(|keys| keys.len()).sum();
    log::info!(
        "Loaded {} keys across {} databases from RDB file {}",
        total,
        databases.len(),
        path
    );
    Ok(databases)
}

/// 从 RDB 文件恢复数据到 `RedisDb`，保留各库归属与键的过期时间。
pub fn restore_from_rdb(rdb: &mut RedisDb, path: &str) -> io::Result<()> {
    let databases = load_snapshot(path)?;
    for (db_id, keys) in databases {
        let db = match rdb.databases.get(db_id as usize) {
            Some(db) => db,
            None => {
                log::warn!(
                    "RDB contains database {} but only {} databases are configured; skipping it",
                    db_id,
                    rdb.databases.len()
                );
                continue;
            }
        };
        for loaded in keys {
            // set() 会清掉原有 TTL，所以绝对过期时间要单独恢复。
            db.set(&loaded.key, loaded.value, None);
            if let Some(exp) = loaded.expire_at_ms {
                db.set_expire_at(&loaded.key, exp);
            }
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    type Snapshot = HashMap<u8, Vec<LoadedKey>>;

    /// 取出某个库里指定键的值；键不存在则 panic。
    fn value_of<'a>(loaded: &'a Snapshot, db_id: u8, key: &[u8]) -> &'a RedisObject {
        loaded
            .get(&db_id)
            .and_then(|keys| keys.iter().find(|k| k.key == key))
            .map(|k| &k.value)
            .unwrap_or_else(|| panic!("key {:?} missing from db{}", String::from_utf8_lossy(key), db_id))
    }

    /// 取出某个库里指定键的过期时间。
    fn expire_of(loaded: &Snapshot, db_id: u8, key: &[u8]) -> Option<u64> {
        loaded
            .get(&db_id)
            .and_then(|keys| keys.iter().find(|k| k.key == key))
            .map(|k| k.expire_at_ms)
            .unwrap_or(None)
    }

    fn keys_in(loaded: &Snapshot, db_id: u8) -> Vec<String> {
        let mut names: Vec<String> = loaded
            .get(&db_id)
            .map(|keys| {
                keys.iter()
                    .map(|k| String::from_utf8_lossy(&k.key).to_string())
                    .collect()
            })
            .unwrap_or_default();
        names.sort();
        names
    }

    fn temp_path(name: &str) -> String {
        format!("/tmp/km_rdb_{}.rdb", name)
    }

    #[test]
    fn test_rdb_string_roundtrip() {
        let path = temp_path("string");
        let rdb = RedisDb::new(1);
        rdb.databases[0].set(b"hello", RedisObject::String(b"world".to_vec()), None);
        rdb.databases[0].set(b"number", RedisObject::Integer(42), None);

        save_snapshot(&rdb, &path).unwrap();
        let loaded = load_snapshot(&path).unwrap();
        assert_eq!(keys_in(&loaded, 0), vec!["hello", "number"]);

        match value_of(&loaded, 0, b"hello") {
            RedisObject::String(d) => assert_eq!(d, b"world"),
            _ => panic!("Expected string"),
        }
        match value_of(&loaded, 0, b"number") {
            RedisObject::Integer(n) => assert_eq!(*n, 42),
            _ => panic!("Expected integer"),
        }

        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn test_rdb_list_roundtrip() {
        let path = temp_path("list");
        let rdb = RedisDb::new(1);
        let mut list = VecDeque::new();
        list.push_back(b"a".to_vec());
        list.push_back(b"b".to_vec());
        list.push_back(b"c".to_vec());
        rdb.databases[0].set(b"mylist", RedisObject::List(list), None);

        save_snapshot(&rdb, &path).unwrap();
        let loaded = load_snapshot(&path).unwrap();
        assert_eq!(loaded.len(), 1);

        match value_of(&loaded, 0, b"mylist") {
            RedisObject::List(l) => {
                assert_eq!(l.len(), 3);
                assert_eq!(l[0], b"a");
                assert_eq!(l[2], b"c");
            }
            _ => panic!("Expected list"),
        }

        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn test_rdb_hash_roundtrip() {
        let path = temp_path("hash");
        let rdb = RedisDb::new(1);
        let mut hash = HashMap::new();
        hash.insert(b"f1".to_vec(), b"v1".to_vec());
        hash.insert(b"f2".to_vec(), b"v2".to_vec());
        rdb.databases[0].set(b"myhash", RedisObject::Hash(hash), None);

        save_snapshot(&rdb, &path).unwrap();
        let loaded = load_snapshot(&path).unwrap();

        match value_of(&loaded, 0, b"myhash") {
            RedisObject::Hash(h) => {
                assert_eq!(h.len(), 2);
                assert_eq!(h[b"f1".as_slice()], b"v1");
            }
            _ => panic!("Expected hash"),
        }

        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn test_rdb_set_roundtrip() {
        let path = temp_path("set");
        let rdb = RedisDb::new(1);
        let mut set = HashSet::new();
        set.insert(b"m1".to_vec());
        set.insert(b"m2".to_vec());
        rdb.databases[0].set(b"myset", RedisObject::Set(set), None);

        save_snapshot(&rdb, &path).unwrap();
        let loaded = load_snapshot(&path).unwrap();

        match value_of(&loaded, 0, b"myset") {
            RedisObject::Set(s) => {
                assert_eq!(s.len(), 2);
                assert!(s.contains(b"m1".as_slice()));
            }
            _ => panic!("Expected set"),
        }

        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn test_rdb_zset_roundtrip() {
        let path = temp_path("zset");
        let rdb = RedisDb::new(1);
        let mut zset = ZSet::new();
        zset.add(b"alice".to_vec(), 1.0);
        zset.add(b"bob".to_vec(), 2.5);
        rdb.databases[0].set(b"myzset", RedisObject::ZSet(zset), None);

        save_snapshot(&rdb, &path).unwrap();
        let loaded = load_snapshot(&path).unwrap();

        match value_of(&loaded, 0, b"myzset") {
            RedisObject::ZSet(z) => {
                assert_eq!(z.len(), 2);
                assert_eq!(z.score(b"alice"), Some(1.0));
                assert_eq!(z.score(b"bob"), Some(2.5));
            }
            _ => panic!("Expected zset"),
        }

        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn test_rdb_stream_roundtrip() {
        let path = temp_path("stream");
        let mut stream = Stream::new();
        let mut e1 = HashMap::new();
        e1.insert(b"name".to_vec(), b"alice".to_vec());
        let mut e2 = HashMap::new();
        e2.insert(b"name".to_vec(), b"bob".to_vec());
        e2.insert(b"age".to_vec(), b"7".to_vec());
        stream.add(e1, Some(StreamId::new(1, 1)), None, false).unwrap();
        stream.add(e2, Some(StreamId::new(2, 3)), None, false).unwrap();
        stream.create_group("g1", StreamId::new(1, 1)).unwrap();

        let rdb = RedisDb::new(1);
        rdb.databases[0].set(b"mystream", RedisObject::Stream(stream), None);

        save_snapshot(&rdb, &path).unwrap();
        let loaded = load_snapshot(&path).unwrap();

        match value_of(&loaded, 0, b"mystream") {
            RedisObject::Stream(s) => {
                assert_eq!(s.entries().len(), 2);
                assert_eq!(s.entries()[0].id, StreamId::new(1, 1));
                assert_eq!(s.entries()[1].fields[b"age".as_slice()], b"7");
                assert_eq!(s.last_id(), StreamId::new(2, 3));
                assert_eq!(s.entries_added(), 2);
                let group = s.get_group("g1").expect("group g1 lost");
                assert_eq!(group.last_delivered_id, StreamId::new(1, 1));
            }
            _ => panic!("Expected stream"),
        }

        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn test_rdb_stream_keeps_pending_entries() {
        let path = temp_path("stream_pel");
        let mut stream = Stream::new();
        let mut fields = HashMap::new();
        fields.insert(b"k".to_vec(), b"v".to_vec());
        stream.add(fields, Some(StreamId::new(5, 1)), None, false).unwrap();
        stream.create_group("g1", StreamId::new(0, 0)).unwrap();
        stream
            .read_group("g1", "c1", None, false, Some(StreamId::new(0, 0)))
            .unwrap();

        let rdb = RedisDb::new(1);
        rdb.databases[0].set(b"s", RedisObject::Stream(stream), None);
        save_snapshot(&rdb, &path).unwrap();
        let loaded = load_snapshot(&path).unwrap();

        match value_of(&loaded, 0, b"s") {
            RedisObject::Stream(s) => {
                let group = s.get_group("g1").unwrap();
                assert_eq!(group.pending.len(), 1);
                assert_eq!(group.pending[0].consumer_name, "c1");
                let consumer = group.consumers.get("c1").expect("consumer lost");
                assert_eq!(consumer.pending.len(), 1);
            }
            _ => panic!("Expected stream"),
        }

        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn test_rdb_keeps_ttl() {
        let path = temp_path("ttl");
        let rdb = RedisDb::new(1);
        rdb.databases[0].set(b"k1", RedisObject::String(b"v1".to_vec()), None);
        let expect = current_time_ms() + 60_000;
        rdb.databases[0].set_expire_at(b"k1", expect);
        rdb.databases[0].set(b"k2", RedisObject::String(b"v2".to_vec()), Some(30_000));

        save_snapshot(&rdb, &path).unwrap();
        let loaded = load_snapshot(&path).unwrap();
        assert_eq!(expire_of(&loaded, 0, b"k1"), Some(expect));
        assert!(expire_of(&loaded, 0, b"k2").is_some());

        let mut restored = RedisDb::new(1);
        restore_from_rdb(&mut restored, &path).unwrap();
        assert!(restored.databases[0].pttl(b"k1") > 50_000);
        assert!(restored.databases[0].pttl(b"k2") > 20_000);

        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn test_rdb_expired_keys_are_dropped() {
        let path = temp_path("expired");
        let rdb = RedisDb::new(1);
        rdb.databases[0].set(b"alive", RedisObject::String(b"1".to_vec()), None);
        rdb.databases[0].set(b"dead", RedisObject::String(b"2".to_vec()), None);
        rdb.databases[0].set_expire_at(b"dead", current_time_ms() - 1);

        save_snapshot(&rdb, &path).unwrap();
        let loaded = load_snapshot(&path).unwrap();
        assert_eq!(keys_in(&loaded, 0), vec!["alive"]);

        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn test_rdb_keeps_databases_separate() {
        let path = temp_path("multidb");
        let rdb = RedisDb::new(3);
        rdb.databases[0].set(b"shared", RedisObject::String(b"db0".to_vec()), None);
        rdb.databases[1].set(b"only1", RedisObject::String(b"db1".to_vec()), None);
        rdb.databases[2].set(b"only2", RedisObject::Integer(7), None);

        save_snapshot(&rdb, &path).unwrap();
        let loaded = load_snapshot(&path).unwrap();
        assert_eq!(keys_in(&loaded, 0), vec!["shared"]);
        assert_eq!(keys_in(&loaded, 1), vec!["only1"]);
        assert_eq!(keys_in(&loaded, 2), vec!["only2"]);

        let mut restored = RedisDb::new(3);
        restore_from_rdb(&mut restored, &path).unwrap();
        assert!(restored.databases[0].get(b"only1").is_none());
        assert!(restored.databases[1].get(b"only1").is_some());
        assert!(restored.databases[2].get(b"only2").is_some());

        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn test_rdb_ignores_databases_beyond_config() {
        let path = temp_path("too_many_db");
        let rdb = RedisDb::new(4);
        rdb.databases[3].set(b"deep", RedisObject::String(b"x".to_vec()), None);
        save_snapshot(&rdb, &path).unwrap();

        let mut small = RedisDb::new(2);
        restore_from_rdb(&mut small, &path).unwrap();
        assert!(small.databases[0].get(b"deep").is_none());
        assert!(small.databases[1].get(b"deep").is_none());

        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn test_rdb_rejects_other_versions() {
        let path = temp_path("bad_version");
        std::fs::write(&path, b"REDIS0001\xff").unwrap();
        let err = load_snapshot(&path).unwrap_err().to_string();
        assert!(err.contains("Unsupported RDB version"), "got: {}", err);
        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn test_rdb_empty_file() {
        let path = temp_path("empty");
        let rdb = RedisDb::new(1);
        save_snapshot(&rdb, &path).unwrap();
        let loaded = load_snapshot(&path).unwrap();
        // 每个库都会写一条选库标记，但没有任何键。
        assert!(loaded.values().all(|keys| keys.is_empty()));
        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn test_rdb_no_file() {
        let loaded = load_snapshot("/tmp/nonexistent_test_rdb.rdb").unwrap();
        assert!(loaded.is_empty());
    }

    #[test]
    fn test_restore_from_rdb() {
        let path = temp_path("restore");
        let rdb = RedisDb::new(1);
        rdb.databases[0].set(b"key1", RedisObject::String(b"val1".to_vec()), None);
        rdb.databases[0].set(b"key2", RedisObject::Integer(99), None);
        save_snapshot(&rdb, &path).unwrap();

        let mut new_rdb = RedisDb::new(1);
        restore_from_rdb(&mut new_rdb, &path).unwrap();

        match new_rdb.databases[0].get(b"key1") {
            Some(RedisObject::String(d)) => assert_eq!(d, b"val1"),
            other => panic!("Expected string, got {:?}", other),
        }
        match new_rdb.databases[0].get(b"key2") {
            Some(RedisObject::Integer(n)) => assert_eq!(n, 99),
            other => panic!("Expected integer, got {:?}", other),
        }

        let _ = std::fs::remove_file(path);
    }

    // -----------------------------------------------------------------------
    // 标准 Redis RDB（REDIS0003..REDIS0014）读取测试
    // -----------------------------------------------------------------------

    /// 构造标准 RDB 文件内容：文件头 + 文件体 + EOF + 8 字节 CRC64。
    fn std_rdb_file(version: &str, body: &[u8]) -> Vec<u8> {
        let mut data = b"REDIS".to_vec();
        data.extend_from_slice(version.as_bytes());
        data.extend_from_slice(body);
        data.push(0xFF); // RDB_OPCODE_EOF
        data.extend_from_slice(&[0u8; 8]); // 校验和（读取端不校验）
        data
    }

    /// 写入临时文件并返回路径。
    fn write_std_rdb(name: &str, data: &[u8]) -> String {
        let path = temp_path(name);
        std::fs::write(&path, data).unwrap();
        path
    }

    /// 标准 RDB 长度编码（rdb.c 的 `rdbSaveLen`）。
    fn std_len(n: u64) -> Vec<u8> {
        if n < (1 << 6) {
            vec![n as u8]
        } else if n < (1 << 14) {
            vec![((n >> 8) as u8) | 0x40, n as u8]
        } else if n <= u32::MAX as u64 {
            let mut v = vec![0x80];
            v.extend_from_slice(&(n as u32).to_be_bytes());
            v
        } else {
            let mut v = vec![0x81];
            v.extend_from_slice(&n.to_be_bytes());
            v
        }
    }

    /// 标准 RDB 字符串（裸串形式：长度 + 数据）。
    fn std_str(s: &[u8]) -> Vec<u8> {
        let mut v = std_len(s.len() as u64);
        v.extend_from_slice(s);
        v
    }

    /// listpack 回填长度编码（listpack.c 的 `lpEncodeBacklen`）。
    fn lp_backlen(l: usize) -> Vec<u8> {
        if l <= 127 {
            vec![l as u8]
        } else if l <= 16_383 {
            vec![(l >> 7) as u8, ((l & 127) | 128) as u8]
        } else {
            panic!("test listpack entry too long: {}", l);
        }
    }

    /// 构造只含字符串条目的 listpack。
    fn build_listpack(items: &[&[u8]]) -> Vec<u8> {
        let mut body = Vec::new();
        for item in items {
            let mut entry = Vec::new();
            if item.len() <= 0x3F {
                entry.push(0x80 | item.len() as u8); // 6 位长度字符串
            } else if item.len() <= 0xFFF {
                entry.push(0xE0 | ((item.len() >> 8) as u8 & 0x0F));
                entry.push((item.len() & 0xFF) as u8); // 12 位长度字符串
            } else {
                entry.push(0xF0);
                entry.extend_from_slice(&(item.len() as u32).to_le_bytes()); // 32 位长度
            }
            entry.extend_from_slice(item);
            let content_len = entry.len();
            entry.extend_from_slice(&lp_backlen(content_len));
            body.extend_from_slice(&entry);
        }
        let mut lp = Vec::new();
        lp.extend_from_slice(&((6 + body.len() + 1) as u32).to_le_bytes()); // 总字节数
        lp.extend_from_slice(&(items.len() as u16).to_le_bytes()); // 元素个数
        lp.extend_from_slice(&body);
        lp.push(0xFF);
        lp
    }

    /// 构造只含字符串条目的 ziplist。
    fn build_ziplist(items: &[&[u8]]) -> Vec<u8> {
        let mut entries = Vec::new();
        let mut prev_len = 0usize;
        for item in items {
            let mut entry = Vec::new();
            if prev_len < 254 {
                entry.push(prev_len as u8);
            } else {
                entry.push(254);
                entry.extend_from_slice(&(prev_len as u32).to_le_bytes());
            }
            if item.len() <= 0x3F {
                entry.push(item.len() as u8); // 06 位长度字符串
            } else if item.len() <= 0x3FFF {
                entry.push(0x40 | ((item.len() >> 8) as u8 & 0x3F));
                entry.push((item.len() & 0xFF) as u8); // 14 位长度
            } else {
                entry.push(0x80);
                entry.extend_from_slice(&(item.len() as u32).to_be_bytes()); // 32 位长度
            }
            entry.extend_from_slice(item);
            prev_len = entry.len();
            entries.extend_from_slice(&entry);
        }
        let mut zl = Vec::new();
        zl.extend_from_slice(&((10 + entries.len() + 1) as u32).to_le_bytes()); // 总字节数
        zl.extend_from_slice(&((10 + entries.len()) as u32).to_le_bytes()); // 尾条目偏移
        zl.extend_from_slice(&(items.len() as u16).to_le_bytes()); // 元素个数
        zl.extend_from_slice(&entries);
        zl.push(0xFF);
        zl
    }

    /// 构造 int32 编码的 intset。
    fn build_intset(values: &[i32]) -> Vec<u8> {
        let mut buf = 4u32.to_le_bytes().to_vec(); // INTSET_ENC_INT32 = sizeof(int32_t)
        buf.extend_from_slice(&(values.len() as u32).to_le_bytes());
        for v in values {
            buf.extend_from_slice(&v.to_le_bytes());
        }
        buf
    }

    #[test]
    fn test_standard_rdb_version_range() {
        for ver in ["0003", "0009", "0014"] {
            let body = [S_OPCODE_SELECTDB, 0x00];
            let path = write_std_rdb(&format!("std_ver_{}", ver), &std_rdb_file(ver, &body));
            let loaded = load_snapshot(&path).unwrap();
            assert!(loaded.is_empty(), "version {} should load", ver);
            let _ = std::fs::remove_file(path);
        }
        for ver in ["0001", "0015", "abcd"] {
            let path = write_std_rdb(
                &format!("std_bad_ver_{}", ver),
                &std_rdb_file(ver, &[S_OPCODE_SELECTDB, 0x00]),
            );
            let err = load_snapshot(&path).unwrap_err().to_string();
            assert!(
                err.contains("Unsupported RDB version"),
                "version {}: got {}",
                ver,
                err
            );
            let _ = std::fs::remove_file(path);
        }
    }

    #[test]
    fn test_standard_rdb_string_expire_and_lzf() {
        let mut body = Vec::new();
        // AUX 字段（两串）会被跳过
        body.push(S_OPCODE_AUX);
        body.extend_from_slice(&std_str(b"redis-ver"));
        body.extend_from_slice(&std_str(b"8.8.0"));
        // 选库 + 库大小提示
        body.push(S_OPCODE_SELECTDB);
        body.push(0x00);
        body.push(S_OPCODE_RESIZEDB);
        body.extend_from_slice(&std_len(5));
        body.extend_from_slice(&std_len(1));

        let now = current_time_ms();
        let exp_ms = now + 60_000;
        body.push(S_OPCODE_EXPIRETIME_MS);
        body.extend_from_slice(&(exp_ms as i64).to_le_bytes());
        body.push(S_TYPE_STRING);
        body.extend_from_slice(&std_str(b"exp"));
        body.extend_from_slice(&std_str(b"hello world"));

        // int16 整数编码：[(3<<6)|1][i16 小端]
        body.push(S_TYPE_STRING);
        body.extend_from_slice(&std_str(b"num"));
        body.push((3 << 6) | S_ENC_INT16);
        body.extend_from_slice(&42i16.to_le_bytes());

        // int32 整数编码
        body.push(S_TYPE_STRING);
        body.extend_from_slice(&std_str(b"bignum"));
        body.push((3 << 6) | S_ENC_INT32);
        body.extend_from_slice(&123_456_789i32.to_le_bytes());

        // LZF 压缩串：3 个字面量 + 7 次回溯引用 → 31 个 'a'
        let mut lzf = vec![0x02u8, b'a', b'a', b'a'];
        for _ in 0..7 {
            lzf.push(0x40); // len = 2，实际拷贝 4 字节
            lzf.push(0x00); // 距离 = 1
        }
        body.push(S_TYPE_STRING);
        body.extend_from_slice(&std_str(b"lzf"));
        body.push((3 << 6) | S_ENC_LZF);
        body.extend_from_slice(&std_len(lzf.len() as u64)); // 压缩后长度
        body.extend_from_slice(&std_len(31)); // 原始长度
        body.extend_from_slice(&lzf);

        // 14 位长度编码的长串
        body.push(S_TYPE_STRING);
        body.extend_from_slice(&std_str(b"long"));
        body.extend_from_slice(&std_str(&vec![b'y'; 100]));

        // 秒精度过期（i32 小端）
        let exp_secs = ((now / 1000) + 300) as i32;
        body.push(S_OPCODE_EXPIRETIME);
        body.extend_from_slice(&exp_secs.to_le_bytes());
        body.push(S_TYPE_STRING);
        body.extend_from_slice(&std_str(b"sexp"));
        body.extend_from_slice(&std_str(b"v"));

        let path = write_std_rdb("std_strings", &std_rdb_file("0014", &body));
        let loaded = load_snapshot(&path).unwrap();
        assert_eq!(
            keys_in(&loaded, 0),
            vec!["bignum", "exp", "long", "lzf", "num", "sexp"]
        );

        match value_of(&loaded, 0, b"exp") {
            RedisObject::String(d) => assert_eq!(d, b"hello world"),
            other => panic!("expected string, got {:?}", other),
        }
        match value_of(&loaded, 0, b"num") {
            RedisObject::Integer(n) => assert_eq!(*n, 42),
            other => panic!("expected integer, got {:?}", other),
        }
        match value_of(&loaded, 0, b"bignum") {
            RedisObject::Integer(n) => assert_eq!(*n, 123_456_789),
            other => panic!("expected integer, got {:?}", other),
        }
        match value_of(&loaded, 0, b"lzf") {
            RedisObject::String(d) => {
                assert_eq!(d.len(), 31);
                assert!(d.iter().all(|b| *b == b'a'));
            }
            other => panic!("expected string, got {:?}", other),
        }
        match value_of(&loaded, 0, b"long") {
            RedisObject::String(d) => assert_eq!(d, vec![b'y'; 100].as_slice()),
            other => panic!("expected string, got {:?}", other),
        }

        assert_eq!(expire_of(&loaded, 0, b"exp"), Some(exp_ms));
        assert_eq!(expire_of(&loaded, 0, b"sexp"), Some(exp_secs as u64 * 1000));
        assert_eq!(expire_of(&loaded, 0, b"num"), None);

        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn test_standard_rdb_raw_containers() {
        let mut body = Vec::new();
        body.push(S_OPCODE_SELECTDB);
        body.push(0x01); // 数据落在 1 号库

        // LIST：n + n 个串
        body.push(S_TYPE_LIST);
        body.extend_from_slice(&std_str(b"list1"));
        body.extend_from_slice(&std_len(3));
        for item in [b"a".as_slice(), b"b".as_slice(), b"c".as_slice()] {
            body.extend_from_slice(&std_str(item));
        }

        // SET：n + n 个串
        body.push(S_TYPE_SET);
        body.extend_from_slice(&std_str(b"set1"));
        body.extend_from_slice(&std_len(2));
        body.extend_from_slice(&std_str(b"m1"));
        body.extend_from_slice(&std_str(b"m2"));

        // HASH：n 对
        body.push(S_TYPE_HASH);
        body.extend_from_slice(&std_str(b"hash1"));
        body.extend_from_slice(&std_len(2));
        body.extend_from_slice(&std_str(b"f1"));
        body.extend_from_slice(&std_str(b"v1"));
        body.extend_from_slice(&std_str(b"f2"));
        body.extend_from_slice(&std_str(b"v2"));

        // ZSET：分数是长度前缀的 ASCII double
        body.push(S_TYPE_ZSET);
        body.extend_from_slice(&std_str(b"zset1"));
        body.extend_from_slice(&std_len(2));
        body.extend_from_slice(&std_str(b"alice"));
        body.push(3);
        body.extend_from_slice(b"1.5");
        body.extend_from_slice(&std_str(b"bob"));
        body.push(3);
        body.extend_from_slice(b"2.5");

        // ZSET_2：分数是 8 字节小端 double
        body.push(S_TYPE_ZSET_2);
        body.extend_from_slice(&std_str(b"zset2"));
        body.extend_from_slice(&std_len(1));
        body.extend_from_slice(&std_str(b"carol"));
        body.extend_from_slice(&3.25f64.to_le_bytes());

        let path = write_std_rdb("std_raw", &std_rdb_file("0011", &body));
        let loaded = load_snapshot(&path).unwrap();
        assert!(!loaded.contains_key(&0));
        assert_eq!(
            keys_in(&loaded, 1),
            vec!["hash1", "list1", "set1", "zset1", "zset2"]
        );

        match value_of(&loaded, 1, b"list1") {
            RedisObject::List(l) => {
                assert_eq!(l.len(), 3);
                assert_eq!(l[0], b"a");
                assert_eq!(l[2], b"c");
            }
            other => panic!("expected list, got {:?}", other),
        }
        match value_of(&loaded, 1, b"set1") {
            RedisObject::Set(s) => {
                assert_eq!(s.len(), 2);
                assert!(s.contains(b"m1".as_slice()));
            }
            other => panic!("expected set, got {:?}", other),
        }
        match value_of(&loaded, 1, b"hash1") {
            RedisObject::Hash(h) => {
                assert_eq!(h.len(), 2);
                assert_eq!(h[b"f1".as_slice()], b"v1");
            }
            other => panic!("expected hash, got {:?}", other),
        }
        match value_of(&loaded, 1, b"zset1") {
            RedisObject::ZSet(z) => {
                assert_eq!(z.len(), 2);
                assert_eq!(z.score(b"alice"), Some(1.5));
                assert_eq!(z.score(b"bob"), Some(2.5));
            }
            other => panic!("expected zset, got {:?}", other),
        }
        match value_of(&loaded, 1, b"zset2") {
            RedisObject::ZSet(z) => {
                assert_eq!(z.len(), 1);
                assert_eq!(z.score(b"carol"), Some(3.25));
            }
            other => panic!("expected zset, got {:?}", other),
        }

        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn test_standard_rdb_container_encodings() {
        let mut body = Vec::new();
        body.push(S_OPCODE_SELECTDB);
        body.push(0x00);

        // LIST_ZIPLIST
        body.push(S_TYPE_LIST_ZIPLIST);
        body.extend_from_slice(&std_str(b"zl_list"));
        body.extend_from_slice(&std_str(&build_ziplist(&[b"a", b"b", b"c"])));

        // LIST_QUICKLIST：两个 ziplist 节点
        body.push(S_TYPE_LIST_QUICKLIST);
        body.extend_from_slice(&std_str(b"ql"));
        body.extend_from_slice(&std_len(2));
        body.extend_from_slice(&std_str(&build_ziplist(&[b"x", b"y"])));
        body.extend_from_slice(&std_str(&build_ziplist(&[b"z"])));

        // LIST_QUICKLIST_2：packed 节点 + plain 节点
        body.push(S_TYPE_LIST_QUICKLIST_2);
        body.extend_from_slice(&std_str(b"ql2"));
        body.extend_from_slice(&std_len(2));
        body.extend_from_slice(&std_len(QL_CONTAINER_PACKED));
        body.extend_from_slice(&std_str(&build_listpack(&[b"p", b"q"])));
        body.extend_from_slice(&std_len(QL_CONTAINER_PLAIN));
        body.extend_from_slice(&std_str(b"plain-element"));

        // SET_INTSET
        body.push(S_TYPE_SET_INTSET);
        body.extend_from_slice(&std_str(b"ints"));
        body.extend_from_slice(&std_str(&build_intset(&[1, -2, 300_000])));

        // SET_LISTPACK
        body.push(S_TYPE_SET_LISTPACK);
        body.extend_from_slice(&std_str(b"lpset"));
        body.extend_from_slice(&std_str(&build_listpack(&[b"s1", b"s2"])));

        // HASH_ZIPLIST
        body.push(S_TYPE_HASH_ZIPLIST);
        body.extend_from_slice(&std_str(b"zlhash"));
        body.extend_from_slice(&std_str(&build_ziplist(&[b"f1", b"v1", b"f2", b"v2"])));

        // HASH_LISTPACK
        body.push(S_TYPE_HASH_LISTPACK);
        body.extend_from_slice(&std_str(b"lphash"));
        body.extend_from_slice(&std_str(&build_listpack(&[b"g1", b"w1"])));

        // ZSET_ZIPLIST：成员、分数交替
        body.push(S_TYPE_ZSET_ZIPLIST);
        body.extend_from_slice(&std_str(b"zlzset"));
        body.extend_from_slice(&std_str(&build_ziplist(&[b"m1", b"1.5", b"m2", b"2.5"])));

        // ZSET_LISTPACK
        body.push(S_TYPE_ZSET_LISTPACK);
        body.extend_from_slice(&std_str(b"lpzset"));
        body.extend_from_slice(&std_str(&build_listpack(&[b"m1", b"3", b"m2", b"4.5"])));

        let path = write_std_rdb("std_containers", &std_rdb_file("0009", &body));
        let loaded = load_snapshot(&path).unwrap();

        match value_of(&loaded, 0, b"zl_list") {
            RedisObject::List(l) => {
                assert_eq!(l.len(), 3);
                assert_eq!(l[0], b"a");
                assert_eq!(l[2], b"c");
            }
            other => panic!("expected list, got {:?}", other),
        }
        match value_of(&loaded, 0, b"ql") {
            RedisObject::List(l) => {
                assert_eq!(l.len(), 3);
                assert_eq!(l[0], b"x");
                assert_eq!(l[1], b"y");
                assert_eq!(l[2], b"z");
            }
            other => panic!("expected list, got {:?}", other),
        }
        match value_of(&loaded, 0, b"ql2") {
            RedisObject::List(l) => {
                assert_eq!(l.len(), 3);
                assert_eq!(l[0], b"p");
                assert_eq!(l[1], b"q");
                assert_eq!(l[2], b"plain-element");
            }
            other => panic!("expected list, got {:?}", other),
        }
        match value_of(&loaded, 0, b"ints") {
            RedisObject::Set(s) => {
                assert_eq!(s.len(), 3);
                assert!(s.contains(b"1".as_slice()));
                assert!(s.contains(b"-2".as_slice()));
                assert!(s.contains(b"300000".as_slice()));
            }
            other => panic!("expected set, got {:?}", other),
        }
        match value_of(&loaded, 0, b"lpset") {
            RedisObject::Set(s) => {
                assert_eq!(s.len(), 2);
                assert!(s.contains(b"s1".as_slice()));
            }
            other => panic!("expected set, got {:?}", other),
        }
        match value_of(&loaded, 0, b"zlhash") {
            RedisObject::Hash(h) => {
                assert_eq!(h.len(), 2);
                assert_eq!(h[b"f1".as_slice()], b"v1");
                assert_eq!(h[b"f2".as_slice()], b"v2");
            }
            other => panic!("expected hash, got {:?}", other),
        }
        match value_of(&loaded, 0, b"lphash") {
            RedisObject::Hash(h) => {
                assert_eq!(h.len(), 1);
                assert_eq!(h[b"g1".as_slice()], b"w1");
            }
            other => panic!("expected hash, got {:?}", other),
        }
        match value_of(&loaded, 0, b"zlzset") {
            RedisObject::ZSet(z) => {
                assert_eq!(z.len(), 2);
                assert_eq!(z.score(b"m1"), Some(1.5));
                assert_eq!(z.score(b"m2"), Some(2.5));
            }
            other => panic!("expected zset, got {:?}", other),
        }
        match value_of(&loaded, 0, b"lpzset") {
            RedisObject::ZSet(z) => {
                assert_eq!(z.len(), 2);
                assert_eq!(z.score(b"m1"), Some(3.0));
                assert_eq!(z.score(b"m2"), Some(4.5));
            }
            other => panic!("expected zset, got {:?}", other),
        }

        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn test_standard_rdb_unsupported_types() {
        // 未知类型的值没有长度前缀，无法跳过 → 解析到此为止；
        // 但**已解析的键必须保留**（partial load > nothing），不能让整个文件作废。
        for ty in [6u8, 7, 9, 15, 19, 21, 22, 24, 25] {
            let mut body = Vec::new();
            body.push(S_OPCODE_SELECTDB);
            body.push(0x00);
            // 先放一个能正常解析的字符串键
            body.push(S_TYPE_STRING);
            body.extend_from_slice(&std_str(b"keep"));
            body.extend_from_slice(&std_str(b"v"));
            // 紧跟一个不支持的类型 + 它的键
            body.push(ty);
            body.extend_from_slice(&std_str(b"unsupported_key"));

            let path = write_std_rdb(&format!("std_unsup_{}", ty), &std_rdb_file("0014", &body));
            let loaded = load_snapshot(&path).expect("遇到不支持的类型不应让整个加载失败");
            let keys = loaded.get(&0).expect("db0 里已解析的键要保留");
            assert_eq!(keys.len(), 1, "type {}", ty);
            assert_eq!(keys[0].key, b"keep".to_vec(), "type {}", ty);
            let _ = std::fs::remove_file(path);
        }

        // 未覆盖的操作码（FUNCTION2 = 245）：同样是部分加载
        let mut body = Vec::new();
        body.push(S_OPCODE_SELECTDB);
        body.push(0x00);
        body.push(S_TYPE_STRING);
        body.extend_from_slice(&std_str(b"keep"));
        body.extend_from_slice(&std_str(b"v"));
        body.push(0xF5);

        let path = write_std_rdb("std_unsup_op", &std_rdb_file("0014", &body));
        let loaded = load_snapshot(&path).expect("遇到不支持的操作码不应让整个加载失败");
        assert_eq!(loaded.get(&0).map(|k| k.len()), Some(1));
        let _ = std::fs::remove_file(path);
    }

    /// 用本机真实 redis-server 产出的标准 RDB 做端到端验证。
    ///
    /// 涵盖类型 0/2/4/5/11/16/17/18/20 与 LZF 压缩、过期时间；
    /// 找不到 redis-server 时打印日志并跳过。
    #[test]
    fn test_standard_rdb_from_real_redis() {
        let redis_server = "/opt/homebrew/bin/redis-server";
        let redis_cli = "/opt/homebrew/bin/redis-cli";
        if !Path::new(redis_server).exists() || !Path::new(redis_cli).exists() {
            eprintln!(
                "SKIP: {} not found, skipping real Redis RDB test",
                redis_server
            );
            return;
        }

        // 取一个空闲端口
        let port = std::net::TcpListener::bind("127.0.0.1:0")
            .unwrap()
            .local_addr()
            .unwrap()
            .port()
            .to_string();
        let dir = format!("/tmp/km_rdb_real_{}_{}", std::process::id(), port);
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let logfile = format!("{}/redis.log", dir);

        let mut child = std::process::Command::new(redis_server)
            .args([
                "--port",
                &port,
                "--bind",
                "127.0.0.1",
                "--dir",
                &dir,
                "--save",
                "",
                "--appendonly",
                "no",
                "--logfile",
                &logfile,
                // 每个 quicklist 节点只放 2 个元素，制造多节点列表
                "--list-max-listpack-size",
                "2",
            ])
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null())
            .spawn()
            .expect("spawn redis-server");

        let run = |args: &[&str]| -> String {
            let out = std::process::Command::new(redis_cli)
                .arg("-p")
                .arg(&port)
                .args(args)
                .output()
                .expect("spawn redis-cli");
            assert!(
                out.status.success(),
                "redis-cli {:?} failed: {}",
                args,
                String::from_utf8_lossy(&out.stderr)
            );
            String::from_utf8_lossy(&out.stdout).trim().to_string()
        };

        // 等服务就绪
        let mut ready = false;
        for _ in 0..100 {
            if let Ok(out) =
                std::process::Command::new(redis_cli).args(["-p", &port, "ping"]).output()
            {
                if String::from_utf8_lossy(&out.stdout).contains("PONG") {
                    ready = true;
                    break;
                }
            }
            std::thread::sleep(std::time::Duration::from_millis(100));
        }
        if !ready {
            let log = std::fs::read_to_string(&logfile).unwrap_or_default();
            let _ = child.kill();
            let _ = child.wait();
            panic!("redis-server did not start: {}", log);
        }

        // 写入各类数据
        run(&["set", "strkey", "hello world"]);
        run(&["set", "intkey", "42"]);
        let lzf_value = "a".repeat(2000);
        run(&["set", "lzfkey", &lzf_value]);
        let expire_secs = 300i64;
        let created_at = current_time_ms();
        run(&["set", "expkey", "temporary", "EX", &expire_secs.to_string()]);
        run(&["rpush", "mylist", "a", "b", "c", "d", "e"]);

        let elems: Vec<String> = (0..200).map(|i| format!("e{}", i)).collect();
        let mut largs: Vec<&str> = vec!["rpush", "elemlist"];
        largs.extend(elems.iter().map(|s| s.as_str()));
        run(&largs);

        run(&["hset", "myhash", "f1", "v1", "f2", "v2"]);
        let bigval = "x".repeat(100); // 超过 hash-max-listpack-value → 哈希表编码
        run(&["hset", "bighash", "field", &bigval]);

        run(&["sadd", "myset", "m1", "m2", "m3"]);
        run(&["sadd", "intsetkey", "101", "202", "303"]); // 整数集合 → intset
        let set_members: Vec<String> = (0..200).map(|i| format!("m{}", i)).collect();
        let mut sargs: Vec<&str> = vec!["sadd", "bigset"];
        sargs.extend(set_members.iter().map(|s| s.as_str()));
        run(&sargs);

        run(&["zadd", "myzset", "1.5", "alice", "2.5", "bob"]);
        let zs: Vec<String> = (0..200).map(|i| format!("{}.5", i)).collect();
        let zm: Vec<String> = (0..200).map(|i| format!("z{}", i)).collect();
        let mut zargs: Vec<&str> = vec!["zadd", "bigzset"];
        for i in 0..200 {
            zargs.push(&zs[i]);
            zargs.push(&zm[i]);
        }
        run(&zargs);

        assert_eq!(run(&["save"]), "OK");
        let _ = std::process::Command::new(redis_cli)
            .args(["-p", &port, "shutdown", "nosave"])
            .output();
        for _ in 0..100 {
            if child.try_wait().unwrap().is_some() {
                break;
            }
            std::thread::sleep(std::time::Duration::from_millis(100));
        }
        if child.try_wait().unwrap().is_none() {
            let _ = child.kill();
            let _ = child.wait();
        }

        let path = format!("{}/dump.rdb", dir);
        let raw = std::fs::read(&path).expect("redis-server should have written dump.rdb");
        assert_eq!(&raw[..5], b"REDIS");
        assert_ne!(&raw[5..9], b"0002", "real redis must produce a standard RDB");

        let loaded = load_snapshot(&path).unwrap();
        let keys = loaded.get(&0).expect("data should land in db 0");
        let find = |name: &[u8]| {
            keys.iter()
                .find(|k| k.key == name)
                .unwrap_or_else(|| panic!("missing key {:?}", String::from_utf8_lossy(name)))
        };

        match &find(b"strkey").value {
            RedisObject::String(d) => assert_eq!(d, b"hello world"),
            other => panic!("expected string, got {:?}", other),
        }
        match &find(b"intkey").value {
            RedisObject::Integer(n) => assert_eq!(*n, 42),
            other => panic!("expected integer, got {:?}", other),
        }
        match &find(b"lzfkey").value {
            RedisObject::String(d) => {
                assert_eq!(d.len(), lzf_value.len());
                assert!(d.iter().all(|b| *b == b'a'));
            }
            other => panic!("expected string, got {:?}", other),
        }
        assert!(find(b"strkey").expire_at_ms.is_none());
        let exp = find(b"expkey").expire_at_ms.expect("expkey should keep TTL");
        assert!(
            exp >= created_at + (expire_secs as u64 - 10) * 1000
                && exp <= created_at + (expire_secs as u64 + 10) * 1000,
            "unexpected expire: {} (created at {})",
            exp,
            created_at
        );

        match &find(b"mylist").value {
            RedisObject::List(l) => {
                assert_eq!(l.len(), 5);
                assert_eq!(l[0], b"a");
                assert_eq!(l[4], b"e");
            }
            other => panic!("expected list, got {:?}", other),
        }
        match &find(b"elemlist").value {
            RedisObject::List(l) => {
                assert_eq!(l.len(), 200);
                assert_eq!(l[0], b"e0");
                assert_eq!(l[199], b"e199");
            }
            other => panic!("expected list, got {:?}", other),
        }
        match &find(b"myhash").value {
            RedisObject::Hash(h) => {
                assert_eq!(h.len(), 2);
                assert_eq!(h[b"f1".as_slice()], b"v1");
                assert_eq!(h[b"f2".as_slice()], b"v2");
            }
            other => panic!("expected hash, got {:?}", other),
        }
        match &find(b"bighash").value {
            RedisObject::Hash(h) => {
                assert_eq!(h.len(), 1);
                assert_eq!(h[b"field".as_slice()], bigval.as_bytes());
            }
            other => panic!("expected hash, got {:?}", other),
        }
        match &find(b"myset").value {
            RedisObject::Set(s) => {
                assert_eq!(s.len(), 3);
                assert!(s.contains(b"m1".as_slice()));
            }
            other => panic!("expected set, got {:?}", other),
        }
        match &find(b"intsetkey").value {
            RedisObject::Set(s) => {
                assert_eq!(s.len(), 3);
                assert!(s.contains(b"101".as_slice()));
                assert!(s.contains(b"202".as_slice()));
                assert!(s.contains(b"303".as_slice()));
            }
            other => panic!("expected set, got {:?}", other),
        }
        match &find(b"bigset").value {
            RedisObject::Set(s) => assert_eq!(s.len(), 200),
            other => panic!("expected set, got {:?}", other),
        }
        match &find(b"myzset").value {
            RedisObject::ZSet(z) => {
                assert_eq!(z.len(), 2);
                assert_eq!(z.score(b"alice"), Some(1.5));
                assert_eq!(z.score(b"bob"), Some(2.5));
            }
            other => panic!("expected zset, got {:?}", other),
        }
        match &find(b"bigzset").value {
            RedisObject::ZSet(z) => {
                assert_eq!(z.len(), 200);
                assert_eq!(z.score(b"z10"), Some(10.5));
            }
            other => panic!("expected zset, got {:?}", other),
        }

        // restore_from_rdb 端到端
        let mut restored = RedisDb::new(1);
        restore_from_rdb(&mut restored, &path).unwrap();
        assert!(restored.databases[0].get(b"strkey").is_some());
        assert!(restored.databases[0].pttl(b"expkey") > 200_000);

        let _ = std::fs::remove_dir_all(&dir);
    }
}
