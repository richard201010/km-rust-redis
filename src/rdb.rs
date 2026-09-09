//! RDB 持久化模块。
//!
//! 实现简化版的 Redis RDB 快照持久化，支持将当前数据库状态序列化到文件，
//! 以及从文件中恢复数据。
//!
//! # 文件格式
//!
//! ```text
//! [magic: 5 bytes "REDIS"]
//! [version: 4 bytes "0001"]
//! [entries...]
//!   [type_byte: 1 byte]  // 0=String, 1=List, 2=Set, 3=ZSet, 4=Hash
//!   [key_len: u32]
//!   [key: key_len bytes]
//!   [value_data: variable, depends on type]
//! [end_marker: 1 byte 0xFF]
//! [crc64: 8 bytes]
//! ```
//!
//! # 各类型的值编码
//!
//! - **String**: `len(u32) + bytes`
//! - **List**: `count(u32) + [len(u32) + bytes]*`
//! - **Set**: `count(u32) + [len(u32) + bytes]*`
//! - **Hash**: `count(u32) + [key_len(u32) + key + val_len(u32) + val]*`
//! - **ZSet**: `count(u32) + [member_len(u32) + member + score(f64)]*`

use crate::db::{Database, RedisDb};
use crate::types::{OrderedFloat, RedisObject, ZSet};
use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet, VecDeque};
use std::fs::File;
use std::io::{self, BufReader, BufWriter, Read, Write};
use std::path::Path;

// RDB 文件魔数和版本
const RDB_MAGIC: &[u8; 5] = b"REDIS";
const RDB_VERSION: &[u8; 4] = b"0001";

// 类型字节常量
const TYPE_STRING: u8 = 0;
const TYPE_LIST: u8 = 1;
const TYPE_SET: u8 = 2;
const TYPE_ZSET: u8 = 3;
const TYPE_HASH: u8 = 4;

// 结束标记
const RDB_EOF: u8 = 0xFF;

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

    /// 写入单个键值对。
    fn write_entry(&mut self, key: &[u8], value: &RedisObject) -> io::Result<()> {
        match value {
            RedisObject::String(data) => {
                self.writer.write_all(&[TYPE_STRING])?;
                self.write_bytes_with_len(key)?;
                self.write_bytes_with_len(data)?;
            }
            RedisObject::Integer(n) => {
                // Integer 作为 String 类型存储
                self.writer.write_all(&[TYPE_STRING])?;
                self.write_bytes_with_len(key)?;
                let s = n.to_string().into_bytes();
                self.write_bytes_with_len(&s)?;
            }
            RedisObject::List(list) => {
                self.writer.write_all(&[TYPE_LIST])?;
                self.write_bytes_with_len(key)?;
                self.write_u32(list.len() as u32)?;
                for item in list {
                    self.write_bytes_with_len(item)?;
                }
            }
            RedisObject::Set(set) => {
                self.writer.write_all(&[TYPE_SET])?;
                self.write_bytes_with_len(key)?;
                self.write_u32(set.len() as u32)?;
                for item in set {
                    self.write_bytes_with_len(item)?;
                }
            }
            RedisObject::Hash(hash) => {
                self.writer.write_all(&[TYPE_HASH])?;
                self.write_bytes_with_len(key)?;
                self.write_u32(hash.len() as u32)?;
                for (k, v) in hash {
                    self.write_bytes_with_len(k)?;
                    self.write_bytes_with_len(v)?;
                }
            }
            RedisObject::ZSet(zset) => {
                self.writer.write_all(&[TYPE_ZSET])?;
                self.write_bytes_with_len(key)?;
                self.write_u32(zset.dict.len() as u32)?;
                for (member, score) in &zset.dict {
                    self.write_bytes_with_len(member)?;
                    self.write_f64(score.0)?;
                }
            }
            RedisObject::Stream(_) => {
                // Stream 类型暂不支持持久化，跳过
            }
        }
        Ok(())
    }

    /// 将单个数据库序列化到文件。
    fn write_database(&mut self, db: &Database) -> io::Result<()> {
        for entry in db.data.iter() {
            let key = entry.key();
            let value = entry.value();
            // 跳过已过期的键
            if let Some(exp) = db.expires.get(key) {
                if crate::types::current_time_ms() >= *exp {
                    continue;
                }
            }
            self.write_entry(key, value)?;
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

    /// 读取 u32（小端序）。
    fn read_u32(&mut self) -> io::Result<u32> {
        let mut buf = [0u8; 4];
        self.read_exact(&mut buf)?;
        Ok(u32::from_le_bytes(buf))
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

    /// 验证文件头。
    fn read_header(&mut self) -> io::Result<()> {
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
        if &version != RDB_VERSION {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "Unsupported RDB version: {}",
                    String::from_utf8_lossy(&version)
                ),
            ));
        }
        Ok(())
    }

    /// 读取单个条目。
    fn read_entry(&mut self) -> io::Result<Option<(Vec<u8>, RedisObject)>> {
        let mut type_buf = [0u8; 1];
        match self.reader.read_exact(&mut type_buf) {
            Ok(()) => {}
            Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => return Ok(None),
            Err(e) => return Err(e),
        }

        let type_byte = type_buf[0];
        if type_byte == RDB_EOF {
            return Ok(None);
        }

        let key = self.read_bytes_with_len()?;
        let value = match type_byte {
            TYPE_STRING => {
                let data = self.read_bytes_with_len()?;
                // 尝试解析为整数
                if let Ok(s) = std::str::from_utf8(&data) {
                    if let Ok(n) = s.parse::<i64>() {
                        RedisObject::Integer(n)
                    } else {
                        RedisObject::String(data)
                    }
                } else {
                    RedisObject::String(data)
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
            _ => {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!("Unknown RDB type byte: {}", type_byte),
                ));
            }
        };

        Ok(Some((key, value)))
    }
}

/// 从 RDB 文件加载数据。
///
/// 返回 db_index 0 的数据（简化版只支持单数据库恢复到 db 0）。
///
/// # 参数
/// - `path`: RDB 文件路径
///
/// # 返回
/// - `Ok(HashMap)`: 加载的键值对
/// - `Err(e)`: 文件不存在或格式错误
pub fn load_snapshot(path: &str) -> io::Result<HashMap<Vec<u8>, RedisObject>> {
    if !Path::new(path).exists() {
        log::info!(
            "No RDB file found at {}, starting with empty database",
            path
        );
        return Ok(HashMap::new());
    }

    let mut reader = RdbReader::new(path)?;
    reader.read_header()?;

    let mut data = HashMap::new();
    loop {
        match reader.read_entry()? {
            Some((key, value)) => {
                data.insert(key, value);
            }
            None => break,
        }
    }

    log::info!("Loaded {} keys from RDB file {}", data.len(), path);
    Ok(data)
}

/// 从 RDB 文件恢复数据到 RedisDb。
///
/// 将加载的数据插入到 db 0 中。
pub fn restore_from_rdb(rdb: &mut RedisDb, path: &str) -> io::Result<()> {
    let data = load_snapshot(path)?;
    let db = &rdb.databases[0];
    for (key, value) in data {
        db.set(key, value, None);
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::{HashMap, HashSet, VecDeque};

    #[test]
    fn test_rdb_string_roundtrip() {
        let path = "/tmp/test_rdb_string.rdb";
        let mut rdb = RedisDb::new(1);
        rdb.databases[0].set(
            b"hello".to_vec(),
            RedisObject::String(b"world".to_vec()),
            None,
        );
        rdb.databases[0].set(b"number".to_vec(), RedisObject::Integer(42), None);

        save_snapshot(&rdb, path).unwrap();
        let loaded = load_snapshot(path).unwrap();
        assert_eq!(loaded.len(), 2);

        match &loaded[b"hello".as_slice()] {
            RedisObject::String(d) => assert_eq!(d, b"world"),
            _ => panic!("Expected string"),
        }
        match &loaded[b"number".as_slice()] {
            RedisObject::Integer(n) => assert_eq!(*n, 42),
            _ => panic!("Expected integer"),
        }

        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn test_rdb_list_roundtrip() {
        let path = "/tmp/test_rdb_list.rdb";
        let mut rdb = RedisDb::new(1);
        let mut list = VecDeque::new();
        list.push_back(b"a".to_vec());
        list.push_back(b"b".to_vec());
        list.push_back(b"c".to_vec());
        rdb.databases[0].set(b"mylist".to_vec(), RedisObject::List(list), None);

        save_snapshot(&rdb, path).unwrap();
        let loaded = load_snapshot(path).unwrap();
        assert_eq!(loaded.len(), 1);

        match &loaded[b"mylist".as_slice()] {
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
        let path = "/tmp/test_rdb_hash.rdb";
        let mut rdb = RedisDb::new(1);
        let mut hash = HashMap::new();
        hash.insert(b"f1".to_vec(), b"v1".to_vec());
        hash.insert(b"f2".to_vec(), b"v2".to_vec());
        rdb.databases[0].set(b"myhash".to_vec(), RedisObject::Hash(hash), None);

        save_snapshot(&rdb, path).unwrap();
        let loaded = load_snapshot(path).unwrap();

        match &loaded[b"myhash".as_slice()] {
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
        let path = "/tmp/test_rdb_set.rdb";
        let mut rdb = RedisDb::new(1);
        let mut set = HashSet::new();
        set.insert(b"m1".to_vec());
        set.insert(b"m2".to_vec());
        rdb.databases[0].set(b"myset".to_vec(), RedisObject::Set(set), None);

        save_snapshot(&rdb, path).unwrap();
        let loaded = load_snapshot(path).unwrap();

        match &loaded[b"myset".as_slice()] {
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
        let path = "/tmp/test_rdb_zset.rdb";
        let mut rdb = RedisDb::new(1);
        let mut zset = ZSet::new();
        zset.add(b"alice".to_vec(), 1.0);
        zset.add(b"bob".to_vec(), 2.5);
        rdb.databases[0].set(b"myzset".to_vec(), RedisObject::ZSet(zset), None);

        save_snapshot(&rdb, path).unwrap();
        let loaded = load_snapshot(path).unwrap();

        match &loaded[b"myzset".as_slice()] {
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
    fn test_rdb_empty_file() {
        let path = "/tmp/test_rdb_empty.rdb";
        let rdb = RedisDb::new(1);
        save_snapshot(&rdb, path).unwrap();
        let loaded = load_snapshot(path).unwrap();
        assert!(loaded.is_empty());
        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn test_rdb_no_file() {
        let loaded = load_snapshot("/tmp/nonexistent_test_rdb.rdb").unwrap();
        assert!(loaded.is_empty());
    }

    #[test]
    fn test_restore_from_rdb() {
        let path = "/tmp/test_rdb_restore.rdb";
        let mut rdb = RedisDb::new(1);
        rdb.databases[0].set(
            b"key1".to_vec(),
            RedisObject::String(b"val1".to_vec()),
            None,
        );
        rdb.databases[0].set(b"key2".to_vec(), RedisObject::Integer(99), None);
        save_snapshot(&rdb, path).unwrap();

        // 创建新的 RedisDb 并恢复
        let mut new_rdb = RedisDb::new(1);
        restore_from_rdb(&mut new_rdb, path).unwrap();

        match new_rdb.databases[0].get(b"key1") {
            Some(RedisObject::String(d)) => assert_eq!(d, b"val1"),
            _ => panic!("Expected string"),
        }
        match new_rdb.databases[0].get(b"key2") {
            Some(RedisObject::Integer(n)) => assert_eq!(n, 99),
            _ => panic!("Expected integer"),
        }

        let _ = std::fs::remove_file(path);
    }
}
