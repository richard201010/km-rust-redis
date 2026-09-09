//! Stream 数据类型模块 — Redis 5.0+ 的消息队列。
//!
//! 对应 Redis 8 源码中的 `t_stream.c` / `stream.h`，实现完整的 Stream 数据结构。
//!
//! # 设计思路
//!
//! Redis 的 Stream 是一个仅追加（append-only）的日志数据结构，类似 Apache Kafka。
//! 每条消息（StreamEntry）由一个唯一 ID 标识，ID 由 `<时间戳>-<序列号>` 组成。
//!
//! 核心数据结构：
//! - `StreamId` — 消息 ID，由 `timestamp:sequence` 对组成，支持自动递增生成
//! - `StreamEntry` — 单条消息，包含 ID 和字段-值对
//! - `Stream` — 完整的流，包含消息列表、消费者组
//! - `ConsumerGroup` — 消费者组，支持消息的可靠投递（at-least-once）
//! - `Consumer` — 消费者，属于某个消费者组
//! - `PendingEntry` — 待确认消息（PEL - Pending Entries List）
//!
//! # 对应 Redis 命令
//!
//! | 命令          | 功能                              |
//! |--------------|----------------------------------|
//! | XADD         | 向流中添加消息                     |
//! | XLEN         | 返回流中的消息数量                  |
//! | XRANGE       | 按 ID 范围查询消息（正序）           |
//! | XREVRANGE    | 按 ID 范围查询消息（逆序）           |
//! | XREAD        | 读取一个或多个流的消息               |
//! | XDEL         | 删除指定 ID 的消息                  |
//! | XTRIM        | 裁剪流到指定长度                    |
//! | XGROUP       | 创建/管理消费者组                   |
//! | XREADGROUP   | 消费者组读取（带确认机制）            |
//! | XACK         | 确认消费者组中的消息                 |
//! | XINFO        | 查询流或消费者组信息                 |

use std::cmp::Ordering;
use std::collections::{HashMap, VecDeque};
use std::fmt;
use std::time::{SystemTime, UNIX_EPOCH};

// ---------------------------------------------------------------------------
// StreamId — 消息 ID（时间戳-序列号）
// ---------------------------------------------------------------------------

/// Stream 消息 ID，由 `<timestamp>-<sequence>` 组成。
///
/// 对应 Redis 中 streamID 结构体（`stream.h`）：
/// ```c
/// typedef struct streamID {
///     uint64_t ms;   // 毫秒时间戳
///     uint64_t seq;  // 序列号
/// } streamID;
/// ```
///
/// ID 排序规则：先比较时间戳，再比较序列号（升序）。
/// 特殊 ID：`(0, 0)` 表示最小 ID，`(u64::MAX, u64::MAX)` 表示最大 ID。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct StreamId {
    /// 毫秒级 Unix 时间戳
    pub timestamp: u64,
    /// 同一时间戳内的序列号
    pub sequence: u64,
}

impl StreamId {
    /// 创建新的 StreamId。
    pub fn new(timestamp: u64, sequence: u64) -> Self {
        Self {
            timestamp,
            sequence,
        }
    }

    /// 最小 ID（0-0），用于 XRANGE 的起始边界。
    pub fn min() -> Self {
        Self {
            timestamp: 0,
            sequence: 0,
        }
    }

    /// 最大 ID（MAX-MAX），用于 XRANGE 的结束边界。
    pub fn max() -> Self {
        Self {
            timestamp: u64::MAX,
            sequence: u64::MAX,
        }
    }

    /// 自动生成递增 ID。
    ///
    /// 对应 Redis 中 `streamAppendItem` 的 ID 生成逻辑：
    /// - 如果当前时间戳 > 上一个 ID 的时间戳，使用 `(当前时间戳, 0)`
    /// - 如果当前时间戳 == 上一个 ID 的时间戳，使用 `(当前时间戳, last_seq + 1)`
    /// - 如果当前时间戳 < 上一个 ID 的时间戳（时钟回退），使用 `(last_ts, last_seq + 1)`
    ///
    /// # 参数
    /// - `last_id`: 流中最后一个消息的 ID
    ///
    /// # 返回
    /// 自动生成的递增 ID
    pub fn generate(last_id: StreamId) -> Self {
        let now_ms = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64;

        if now_ms > last_id.timestamp {
            Self {
                timestamp: now_ms,
                sequence: 0,
            }
        } else {
            // 时间戳相同或时钟回退，递增序列号
            Self {
                timestamp: last_id.timestamp,
                sequence: last_id.sequence + 1,
            }
        }
    }

    /// 从字节解析 StreamId。
    ///
    /// 支持格式：
    /// - `"1234567890-0"` → `StreamId { timestamp: 1234567890, sequence: 0 }`
    /// - `"*"` → 表示自动生成 ID（返回 `None`，由调用方处理）
    /// - `"-"` → 最小 ID（返回 `StreamId::min()`）
    /// - `"+"` → 最大 ID（返回 `StreamId::max()`）
    pub fn parse(bytes: &[u8]) -> Option<Self> {
        let s = std::str::from_utf8(bytes).ok()?;
        match s {
            "-" => Some(Self::min()),
            "+" => Some(Self::max()),
            "*" => None, // 自动生成，由调用方处理
            _ => {
                let parts: Vec<&str> = s.splitn(2, '-').collect();
                let ts: u64 = parts.first()?.parse().ok()?;
                let seq = if parts.len() > 1 {
                    parts[1].parse().ok()?
                } else {
                    0
                };
                Some(Self {
                    timestamp: ts,
                    sequence: seq,
                })
            }
        }
    }

    /// 转换为字节表示，如 `b"1234567890-0"`。
    pub fn to_bytes(&self) -> Vec<u8> {
        format!("{}-{}", self.timestamp, self.sequence).into_bytes()
    }
}

impl Ord for StreamId {
    fn cmp(&self, other: &Self) -> Ordering {
        self.timestamp
            .cmp(&other.timestamp)
            .then(self.sequence.cmp(&other.sequence))
    }
}

impl PartialOrd for StreamId {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl fmt::Display for StreamId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}-{}", self.timestamp, self.sequence)
    }
}

// ---------------------------------------------------------------------------
// StreamEntry — 单条消息
// ---------------------------------------------------------------------------

/// Stream 中的单条消息（entry）。
///
/// 对应 Redis 中 `streamEntry` 结构体：
/// ```c
/// typedef struct streamEntry {
///     streamID id;       // 消息 ID
///     listpack *lp;      // 字段-值对（此处简化为 HashMap）
/// } streamEntry;
/// ```
///
/// 每条消息由一个唯一 ID 标识，包含若干字段-值对（类似 Hash）。
#[derive(Debug, Clone)]
pub struct StreamEntry {
    /// 消息 ID
    pub id: StreamId,
    /// 字段-值对（二进制安全）
    pub fields: HashMap<Vec<u8>, Vec<u8>>,
}

impl StreamEntry {
    pub fn new(id: StreamId, fields: HashMap<Vec<u8>, Vec<u8>>) -> Self {
        Self { id, fields }
    }
}

// ---------------------------------------------------------------------------
// PendingEntry — 待确认消息（PEL）
// ---------------------------------------------------------------------------

/// 消费者组的待确认消息条目（Pending Entries List entry）。
///
/// 对应 Redis 中 `streamNACK` 结构体：
/// ```c
/// typedef struct streamNACK {
///     mstime_t delivery_time;   // 首次投递时间
///     uint64_t delivery_count;  // 投递次数
///     streamConsumer *consumer; // 消费者（此处简化为名称字符串）
/// } streamNACK;
/// ```
///
/// 当消费者通过 XREADGROUP 读取消息后，消息进入 PEL 等待 XACK 确认。
/// 如果消息未被确认，可以通过 XCLAIM 转移给其他消费者重新处理。
#[derive(Debug, Clone)]
pub struct PendingEntry {
    /// 待确认的消息 ID
    pub entry_id: StreamId,
    /// 消费该消息的消费者名称
    pub consumer_name: String,
    /// 首次投递时间（毫秒时间戳）
    pub delivery_time: u64,
    /// 投递次数（每重新投递一次递增）
    pub delivery_count: u64,
}

// ---------------------------------------------------------------------------
// Consumer — 消费者
// ---------------------------------------------------------------------------

/// 消费者组中的消费者实例。
///
/// 对应 Redis 中 `streamConsumer` 结构体：
/// ```c
/// typedef struct streamConsumer {
///     sds name;                    // 消费者名称
///     mstime_t active_time;        // 最后活跃时间
///     streamConsumerGroup *group;  // 所属消费者组
///     rax *pel;                    // 该消费者的 PEL
/// } streamConsumer;
/// ```
///
/// 每个消费者属于一个消费者组，维护自己的待确认消息列表。
#[derive(Debug, Clone)]
pub struct Consumer {
    /// 消费者名称（组内唯一）
    pub name: String,
    /// 该消费者的待确认消息列表
    pub pending: Vec<PendingEntry>,
}

impl Consumer {
    pub fn new(name: String) -> Self {
        Self {
            name,
            pending: Vec::new(),
        }
    }
}

// ---------------------------------------------------------------------------
// ConsumerGroup — 消费者组
// ---------------------------------------------------------------------------

/// Stream 的消费者组。
///
/// 对应 Redis 中 `streamCG` 结构体：
/// ```c
/// typedef struct streamCG {
///     streamID last_id;           // 最后投递给该组的消息 ID
///     rax *consumers;             // 消费者字典
///     rax *pel;                   // 组级别的 PEL
///     uint64_t entries_read;      // 已读取的条目数
/// } streamCG;
/// ```
///
/// 消费者组实现消息的可靠投递模式（at-least-once）：
/// 1. 每条消息只投递给组内的一个消费者（负载均衡）
/// 2. 消费者需要通过 XACK 确认消息已处理
/// 3. 未确认的消息可以通过 XCLAIM 转移给其他消费者
/// 4. 每个消费者组维护自己的 last_delivered_id，新消费者从该 ID 之后开始读取
#[derive(Debug, Clone)]
pub struct ConsumerGroup {
    /// 消费者组名称
    pub name: String,
    /// 该组最后投递的消息 ID（XREADGROUP 从这个 ID 之后开始读取）
    pub last_delivered_id: StreamId,
    /// 消费者名称 → Consumer 的映射
    pub consumers: HashMap<String, Consumer>,
    /// 组级别的待确认消息列表（PEL）
    pub pending: Vec<PendingEntry>,
}

impl ConsumerGroup {
    /// 创建新的消费者组。
    ///
    /// # 参数
    /// - `name`: 消费者组名称
    /// - `start_id`: 该组的起始读取位置（通常为 `StreamId::min()` 或流的最后一个 ID）
    pub fn new(name: String, start_id: StreamId) -> Self {
        Self {
            name,
            last_delivered_id: start_id,
            consumers: HashMap::new(),
            pending: Vec::new(),
        }
    }

    /// 获取或创建消费者。
    ///
    /// 如果指定名称的消费者不存在，自动创建。
    fn get_or_create_consumer(&mut self, name: &str) -> &mut Consumer {
        self.consumers
            .entry(name.to_string())
            .or_insert_with(|| Consumer::new(name.to_string()))
    }
}

// ---------------------------------------------------------------------------
// Stream — 完整的消息流
// ---------------------------------------------------------------------------

/// Stream 数据结构 — 仅追加的消息日志。
///
/// 对应 Redis 中 `stream` 结构体：
/// ```c
/// typedef struct stream {
///     rax *rax;               // Radix Tree（存储消息，此处简化为 VecDeque）
///     uint64_t length;        // 消息总数
///     streamID last_id;       // 最后一条消息的 ID
///     streamID first_id;      // 第一条消息的 ID（可能因 XDEL 而更新）
///     streamID max_deleted_entry_id;  // 最大已删除 ID
///     uint64_t entries_added; // 已添加的条目总数（包括已删除的）
///     list *cgroups;          // 消费者组列表
/// } stream;
/// ```
///
/// # 内部实现
///
/// Redis 使用 Radix Tree（基数树）存储消息，每个节点使用 listpack 编码。
/// 此实现简化为 `VecDeque<StreamEntry>`，保持按 ID 排序，支持高效的双端操作。
/// 对于典型的消息队列场景（生产者在尾部追加，消费者从头部或指定位置读取），
/// VecDeque 的 O(1) 头尾操作和 O(log n) 二分查找足以满足需求。
#[derive(Debug, Clone)]
pub struct Stream {
    /// 消息列表（按 ID 升序排列）
    entries: VecDeque<StreamEntry>,
    /// 当前消息数量
    length: usize,
    /// 最后一条消息的 ID（用于自动生成递增 ID）
    last_id: StreamId,
    /// 消费者组名称 → ConsumerGroup 的映射
    consumer_groups: HashMap<String, ConsumerGroup>,
    /// 已添加的条目总数（包括已删除的，用于 XINFO）
    entries_added: u64,
    /// 最大已删除条目的 ID
    max_deleted_entry_id: StreamId,
}

impl Stream {
    /// 创建一个空的消息流。
    pub fn new() -> Self {
        Self {
            entries: VecDeque::new(),
            length: 0,
            last_id: StreamId::min(),
            consumer_groups: HashMap::new(),
            entries_added: 0,
            max_deleted_entry_id: StreamId::min(),
        }
    }

    // -----------------------------------------------------------------------
    // XADD — 添加消息
    // -----------------------------------------------------------------------

    /// 向流中添加一条消息，对应 XADD 命令。
    ///
    /// # 参数
    /// - `fields`: 字段-值对
    /// - `id_opt`: 可选的消息 ID；`None` 表示自动生成（`*`）；
    ///            `Some(id)` 表示用户指定的 ID（必须大于 last_id）
    /// - `max_len`: 可选的裁剪长度；添加后如果超过此长度，从头部裁剪
    /// - `approximate`: 是否使用近似裁剪（`~`）
    ///
    /// # 返回
    /// - `Ok(StreamId)`: 成功添加，返回实际使用的消息 ID
    /// - `Err(String)`: 添加失败（如 ID 不合法）
    pub fn add(
        &mut self,
        fields: HashMap<Vec<u8>, Vec<u8>>,
        id_opt: Option<StreamId>,
        max_len: Option<usize>,
        approximate: bool,
    ) -> Result<StreamId, String> {
        if fields.is_empty() {
            return Err("ERR wrong number of arguments for XADD".to_string());
        }

        // 确定消息 ID
        let id = match id_opt {
            Some(id) => {
                // 用户指定的 ID 必须大于最后一个 ID
                if id <= self.last_id {
                    return Err(format!(
                        "ERR The ID specified in XADD is equal or smaller than the target stream top item"
                    ));
                }
                // ID 不能为 0-0
                if id.timestamp == 0 && id.sequence == 0 {
                    return Err("ERR The ID specified in XADD must be greater than 0-0".to_string());
                }
                id
            }
            None => StreamId::generate(self.last_id),
        };

        let entry = StreamEntry::new(id, fields);
        self.entries.push_back(entry);
        self.length += 1;
        self.last_id = id;
        self.entries_added += 1;

        // 裁剪
        if let Some(max_len) = max_len {
            self.trim(max_len, approximate);
        }

        Ok(id)
    }

    // -----------------------------------------------------------------------
    // XLEN — 消息数量
    // -----------------------------------------------------------------------

    /// 返回流中的消息数量，对应 XLEN 命令。
    pub fn len(&self) -> usize {
        self.length
    }

    /// 判断流是否为空。
    pub fn is_empty(&self) -> bool {
        self.length == 0
    }

    // -----------------------------------------------------------------------
    // XRANGE / XREVRANGE — 范围查询
    // -----------------------------------------------------------------------

    /// 按 ID 范围查询消息（正序），对应 XRANGE 命令。
    ///
    /// # 参数
    /// - `start`: 起始 ID（包含）
    /// - `stop`: 结束 ID（包含）
    /// - `count`: 最多返回的条目数，`None` 表示不限制
    ///
    /// # 返回
    /// 按 ID 升序排列的消息列表
    pub fn range(
        &self,
        start: StreamId,
        stop: StreamId,
        count: Option<usize>,
    ) -> Vec<&StreamEntry> {
        let mut result = Vec::new();
        let max = count.unwrap_or(usize::MAX);

        for entry in &self.entries {
            if entry.id >= start && entry.id <= stop {
                result.push(entry);
                if result.len() >= max {
                    break;
                }
            }
            // 由于 entries 已排序，一旦超过 stop 就可以提前退出
            if entry.id > stop {
                break;
            }
        }

        result
    }

    /// 按 ID 范围查询消息（逆序），对应 XREVRANGE 命令。
    ///
    /// 与 `range()` 相同，但返回结果按 ID 降序排列。
    ///
    /// # 参数
    /// - `start`: 起始 ID（包含），注意此处 start 是较大的 ID
    /// - `stop`: 结束 ID（包含），注意此处 stop 是较小的 ID
    /// - `count`: 最多返回的条目数
    pub fn rev_range(
        &self,
        start: StreamId,
        stop: StreamId,
        count: Option<usize>,
    ) -> Vec<&StreamEntry> {
        let mut result = Vec::new();
        let max = count.unwrap_or(usize::MAX);

        for entry in self.entries.iter().rev() {
            if entry.id <= start && entry.id >= stop {
                result.push(entry);
                if result.len() >= max {
                    break;
                }
            }
            // 一旦小于 stop 就可以提前退出
            if entry.id < stop {
                break;
            }
        }

        result
    }

    // -----------------------------------------------------------------------
    // XREAD — 读取消息
    // -----------------------------------------------------------------------

    /// 从指定 ID 之后读取消息，对应 XREAD 命令。
    ///
    /// # 参数
    /// - `after_id`: 从该 ID 之后开始读取（不包含该 ID）
    /// - `count`: 最多返回的条目数
    ///
    /// # 返回
    /// after_id 之后的消息列表
    pub fn read(&self, after_id: StreamId, count: Option<usize>) -> Vec<&StreamEntry> {
        let max = count.unwrap_or(usize::MAX);
        let mut result = Vec::new();

        for entry in &self.entries {
            if entry.id > after_id {
                result.push(entry);
                if result.len() >= max {
                    break;
                }
            }
        }

        result
    }

    // -----------------------------------------------------------------------
    // XDEL — 删除消息
    // -----------------------------------------------------------------------

    /// 删除指定 ID 的消息，对应 XDEL 命令。
    ///
    /// # 参数
    /// - `ids`: 要删除的消息 ID 列表
    ///
    /// # 返回
    /// 实际删除的消息数量
    pub fn delete(&mut self, ids: &[StreamId]) -> usize {
        let mut deleted = 0;
        for id in ids {
            if let Some(pos) = self.entries.iter().position(|e| e.id == *id) {
                self.entries.remove(pos);
                self.length -= 1;
                deleted += 1;
                // 更新最大已删除 ID
                if *id > self.max_deleted_entry_id {
                    self.max_deleted_entry_id = *id;
                }
            }
        }
        deleted
    }

    // -----------------------------------------------------------------------
    // XTRIM — 裁剪流
    // -----------------------------------------------------------------------

    /// 裁剪流到指定长度，对应 XTRIM 命令。
    ///
    /// # 参数
    /// - `max_len`: 最大保留的消息数量
    /// - `approximate`: 是否允许近似裁剪（Redis 的 `~` 语法，允许保留略多于 max_len）
    ///
    /// # 返回
    /// 实际删除的消息数量
    pub fn trim(&mut self, max_len: usize, approximate: bool) -> usize {
        if self.length <= max_len {
            return 0;
        }

        let to_remove = if approximate {
            // 近似裁剪：允许保留到最近的 listpack 边界
            // 简化实现：允许额外保留 10% 的条目
            let threshold = max_len + max_len / 10;
            if self.length <= threshold {
                return 0;
            }
            self.length - max_len
        } else {
            self.length - max_len
        };

        let mut removed = 0;
        for _ in 0..to_remove {
            if let Some(oldest) = self.entries.pop_front() {
                // 更新最大已删除 ID
                if oldest.id > self.max_deleted_entry_id {
                    self.max_deleted_entry_id = oldest.id;
                }
                removed += 1;
            }
        }
        self.length -= removed;
        removed
    }

    // -----------------------------------------------------------------------
    // XGROUP — 消费者组管理
    // -----------------------------------------------------------------------

    /// 创建消费者组，对应 XGROUP CREATE 命令。
    ///
    /// # 参数
    /// - `group_name`: 消费者组名称
    /// - `start_id`: 该组的起始读取位置
    /// - `mkstream`: 如果流不存在是否自动创建（本实现始终创建流对象，此参数保留兼容性）
    ///
    /// # 返回
    /// - `Ok(())`: 创建成功
    /// - `Err(String)`: 消费者组已存在
    pub fn create_group(&mut self, group_name: &str, start_id: StreamId) -> Result<(), String> {
        if self.consumer_groups.contains_key(group_name) {
            return Err("BUSYGROUP Consumer Group name already exists".to_string());
        }
        let group = ConsumerGroup::new(group_name.to_string(), start_id);
        self.consumer_groups.insert(group_name.to_string(), group);
        Ok(())
    }

    /// 删除消费者组，对应 XGROUP DESTROY 命令。
    ///
    /// # 返回
    /// - `Ok(true)`: 删除成功
    /// - `Ok(false)`: 消费者组不存在
    pub fn destroy_group(&mut self, group_name: &str) -> bool {
        self.consumer_groups.remove(group_name).is_some()
    }

    /// 为消费者组设置新的 last_delivered_id，对应 XGROUP SETID 命令。
    ///
    /// # 返回
    /// - `Ok(())`: 设置成功
    /// - `Err(String)`: 消费者组不存在
    pub fn set_group_id(&mut self, group_name: &str, new_id: StreamId) -> Result<(), String> {
        match self.consumer_groups.get_mut(group_name) {
            Some(group) => {
                group.last_delivered_id = new_id;
                Ok(())
            }
            None => Err("NOGROUP No such consumer group".to_string()),
        }
    }

    /// 从消费者组中删除消费者，对应 XGROUP DELCONSUMER 命令。
    ///
    /// # 返回
    /// 该消费者名下待确认的消息数量
    pub fn del_consumer(&mut self, group_name: &str, consumer_name: &str) -> Result<usize, String> {
        match self.consumer_groups.get_mut(group_name) {
            Some(group) => {
                let count = group
                    .consumers
                    .get(consumer_name)
                    .map(|c| c.pending.len())
                    .unwrap_or(0);
                group.consumers.remove(consumer_name);
                // 同时从组级别的 PEL 中移除该消费者的消息
                group.pending.retain(|p| p.consumer_name != consumer_name);
                Ok(count)
            }
            None => Err("NOGROUP No such consumer group".to_string()),
        }
    }

    // -----------------------------------------------------------------------
    // XREADGROUP — 消费者组读取
    // -----------------------------------------------------------------------

    /// 消费者组读取消息，对应 XREADGROUP 命令。
    ///
    /// # 参数
    /// - `group_name`: 消费者组名称
    /// - `consumer_name`: 消费者名称（不存在则自动创建）
    /// - `count`: 最多读取的条目数
    /// - `noack`: 是否跳过 PEL（消息不会被追踪确认）
    /// - `after_id`: 从该 ID 之后开始读取；`None` 表示使用 `>`（从 last_delivered_id 之后）
    ///
    /// # 返回
    /// - `Ok(entries)`: 读取到的消息列表
    /// - `Err(String)`: 消费者组不存在
    pub fn read_group(
        &mut self,
        group_name: &str,
        consumer_name: &str,
        count: Option<usize>,
        noack: bool,
        after_id: Option<StreamId>,
    ) -> Result<Vec<StreamEntry>, String> {
        let now_ms = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64;

        // 确定读取起始 ID
        let start_from = match after_id {
            Some(id) => id,
            None => {
                // ">" 表示从未投递的新消息
                match self.consumer_groups.get(group_name) {
                    Some(group) => group.last_delivered_id,
                    None => return Err("NOGROUP No such consumer group".to_string()),
                }
            }
        };

        let max = count.unwrap_or(usize::MAX);
        let mut result = Vec::new();

        // 收集符合条件的消息 ID
        let mut eligible_ids = Vec::new();
        for entry in &self.entries {
            if entry.id > start_from {
                eligible_ids.push(entry.id);
                if eligible_ids.len() >= max {
                    break;
                }
            }
        }

        // 在获取消息之后更新消费者组状态
        let group = self
            .consumer_groups
            .get_mut(group_name)
            .ok_or_else(|| "NOGROUP No such consumer group".to_string())?;

        let consumer = group.get_or_create_consumer(consumer_name);

        // 收集待添加的 pending 条目，避免同时可变借用
        let mut pending_entries = Vec::new();

        for id in &eligible_ids {
            // 找到对应的条目
            if let Some(entry) = self.entries.iter().find(|e| e.id == *id) {
                result.push(entry.clone());

                // 如果不是 noack 模式，将消息加入 PEL
                if !noack {
                    let pending = PendingEntry {
                        entry_id: *id,
                        consumer_name: consumer_name.to_string(),
                        delivery_time: now_ms,
                        delivery_count: 1,
                    };
                    consumer.pending.push(pending.clone());
                    pending_entries.push(pending);
                }
            }
        }
        // consumer 借用结束后再操作 group.pending
        for pending in pending_entries {
            group.pending.push(pending);
        }

        // 更新组的 last_delivered_id
        if let Some(last) = eligible_ids.last() {
            if *last > group.last_delivered_id {
                group.last_delivered_id = *last;
            }
        }

        Ok(result)
    }

    // -----------------------------------------------------------------------
    // XACK — 确认消息
    // -----------------------------------------------------------------------

    /// 确认消费者组中的消息，对应 XACK 命令。
    ///
    /// # 参数
    /// - `group_name`: 消费者组名称
    /// - `ids`: 要确认的消息 ID 列表
    ///
    /// # 返回
    /// - `Ok(count)`: 成功确认的消息数量
    /// - `Err(String)`: 消费者组不存在
    pub fn ack(&mut self, group_name: &str, ids: &[StreamId]) -> Result<usize, String> {
        let group = self
            .consumer_groups
            .get_mut(group_name)
            .ok_or_else(|| "NOGROUP No such consumer group".to_string())?;

        let mut acked = 0;

        for id in ids {
            // 从组级别的 PEL 中移除
            let before = group.pending.len();
            group.pending.retain(|p| p.entry_id != *id);
            if group.pending.len() < before {
                acked += 1;
            }

            // 从消费者的 PEL 中移除
            for consumer in group.consumers.values_mut() {
                consumer.pending.retain(|p| p.entry_id != *id);
            }
        }

        Ok(acked)
    }

    // -----------------------------------------------------------------------
    // XINFO — 查询流信息
    // -----------------------------------------------------------------------

    /// 查询流的详细信息，对应 XINFO STREAM 命令。
    ///
    /// 返回的信息包括：长度、最后一个 ID、消费者组数量、已添加条目总数等。
    pub fn info(&self) -> StreamInfo {
        StreamInfo {
            length: self.length,
            last_id: self.last_id,
            consumer_groups_count: self.consumer_groups.len(),
            entries_added: self.entries_added,
            max_deleted_entry_id: self.max_deleted_entry_id,
            first_entry: self.entries.front().cloned(),
            last_entry: self.entries.back().cloned(),
        }
    }

    /// 查询消费者组列表信息，对应 XINFO GROUPS 命令。
    pub fn info_groups(&self) -> Vec<ConsumerGroupInfo> {
        self.consumer_groups
            .values()
            .map(|g| ConsumerGroupInfo {
                name: g.name.clone(),
                consumers_count: g.consumers.len(),
                pending_count: g.pending.len(),
                last_delivered_id: g.last_delivered_id,
            })
            .collect()
    }

    /// 查询指定消费者组的消费者列表信息，对应 XINFO CONSUMERS 命令。
    ///
    /// # 返回
    /// - `Ok(consumers)`: 消费者信息列表
    /// - `Err(String)`: 消费者组不存在
    pub fn info_consumers(&self, group_name: &str) -> Result<Vec<ConsumerInfo>, String> {
        let group = self
            .consumer_groups
            .get(group_name)
            .ok_or_else(|| "NOGROUP No such consumer group".to_string())?;

        let now_ms = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64;

        Ok(group
            .consumers
            .values()
            .map(|c| {
                let idle_ms = if c.pending.is_empty() {
                    now_ms // 简化处理
                } else {
                    now_ms.saturating_sub(
                        c.pending
                            .iter()
                            .map(|p| p.delivery_time)
                            .min()
                            .unwrap_or(now_ms),
                    )
                };
                ConsumerInfo {
                    name: c.name.clone(),
                    pending_count: c.pending.len(),
                    idle_time: idle_ms,
                }
            })
            .collect())
    }

    /// 获取指定消费者组的引用。
    pub fn get_group(&self, name: &str) -> Option<&ConsumerGroup> {
        self.consumer_groups.get(name)
    }

    /// 获取指定消费者的可变引用（通过组名）。
    pub fn get_consumer_mut(
        &mut self,
        group_name: &str,
        consumer_name: &str,
    ) -> Option<&mut Consumer> {
        self.consumer_groups
            .get_mut(group_name)?
            .consumers
            .get_mut(consumer_name)
    }

    /// 获取最后一条消息的 ID。
    pub fn last_id(&self) -> StreamId {
        self.last_id
    }
}

impl Default for Stream {
    fn default() -> Self {
        Self::new()
    }
}

// ---------------------------------------------------------------------------
// Info 结构体 — 用于 XINFO 命令的返回值
// ---------------------------------------------------------------------------

/// 流的详细信息（XINFO STREAM 返回）。
#[derive(Debug, Clone)]
pub struct StreamInfo {
    pub length: usize,
    pub last_id: StreamId,
    pub consumer_groups_count: usize,
    pub entries_added: u64,
    pub max_deleted_entry_id: StreamId,
    pub first_entry: Option<StreamEntry>,
    pub last_entry: Option<StreamEntry>,
}

/// 消费者组信息（XINFO GROUPS 返回）。
#[derive(Debug, Clone)]
pub struct ConsumerGroupInfo {
    pub name: String,
    pub consumers_count: usize,
    pub pending_count: usize,
    pub last_delivered_id: StreamId,
}

/// 消费者信息（XINFO CONSUMERS 返回）。
#[derive(Debug, Clone)]
pub struct ConsumerInfo {
    pub name: String,
    pub pending_count: usize,
    pub idle_time: u64,
}

// ---------------------------------------------------------------------------
// 单元测试
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    /// 测试 StreamId 排序和解析
    #[test]
    fn test_stream_id_ordering() {
        let id1 = StreamId::new(1000, 0);
        let id2 = StreamId::new(1000, 1);
        let id3 = StreamId::new(1001, 0);

        assert!(id1 < id2);
        assert!(id2 < id3);
        assert!(id1 < id3);

        // 测试解析
        let parsed = StreamId::parse(b"1000-5").unwrap();
        assert_eq!(parsed.timestamp, 1000);
        assert_eq!(parsed.sequence, 5);

        // 不带序列号的解析
        let parsed2 = StreamId::parse(b"2000").unwrap();
        assert_eq!(parsed2.timestamp, 2000);
        assert_eq!(parsed2.sequence, 0);

        // 特殊 ID
        assert_eq!(StreamId::parse(b"-"), Some(StreamId::min()));
        assert_eq!(StreamId::parse(b"+"), Some(StreamId::max()));
        assert_eq!(StreamId::parse(b"*"), None);

        // to_bytes
        assert_eq!(id1.to_bytes(), b"1000-0");
    }

    /// 测试 StreamId 自动生成
    #[test]
    fn test_stream_id_generate() {
        let id0 = StreamId::min();
        let id1 = StreamId::generate(id0);
        assert!(id1.timestamp > 0);
        assert_eq!(id1.sequence, 0);

        // 同一时间戳内应递增序列号
        let id2 = StreamId::generate(id1);
        assert_eq!(id2.timestamp, id1.timestamp);
        assert_eq!(id2.sequence, id1.sequence + 1);

        // 未来时间戳的 ID 应递增序列号
        let future = StreamId::new(u64::MAX - 1, 100);
        let id3 = StreamId::generate(future);
        assert_eq!(id3.timestamp, future.timestamp);
        assert_eq!(id3.sequence, 101);
    }

    /// 测试 Stream 基本操作：创建/添加/查询/长度
    #[test]
    fn test_stream_basic() {
        let mut stream = Stream::new();
        assert!(stream.is_empty());
        assert_eq!(stream.len(), 0);

        // 添加消息
        let mut fields = HashMap::new();
        fields.insert(b"name".to_vec(), b"alice".to_vec());
        fields.insert(b"age".to_vec(), b"30".to_vec());

        let id = stream.add(fields, None, None, false).unwrap();
        assert_eq!(stream.len(), 1);
        assert!(!stream.is_empty());
        assert_eq!(stream.last_id(), id);

        // 添加更多消息
        for i in 0..5 {
            let mut f = HashMap::new();
            f.insert(b"index".to_vec(), i.to_string().into_bytes());
            stream.add(f, None, None, false).unwrap();
        }
        assert_eq!(stream.len(), 6);
    }

    /// 测试指定 ID 添加消息
    #[test]
    fn test_stream_add_with_id() {
        let mut stream = Stream::new();

        let mut f1 = HashMap::new();
        f1.insert(b"key".to_vec(), b"val1".to_vec());
        stream
            .add(f1, Some(StreamId::new(100, 0)), None, false)
            .unwrap();

        let mut f2 = HashMap::new();
        f2.insert(b"key".to_vec(), b"val2".to_vec());
        stream
            .add(f2, Some(StreamId::new(100, 1)), None, false)
            .unwrap();

        // 不能添加小于等于 last_id 的 ID
        let mut f3 = HashMap::new();
        f3.insert(b"key".to_vec(), b"val3".to_vec());
        assert!(stream
            .add(f3.clone(), Some(StreamId::new(100, 0)), None, false)
            .is_err());
        assert!(stream
            .add(f3.clone(), Some(StreamId::new(99, 99)), None, false)
            .is_err());

        // 不能添加 0-0
        assert!(stream
            .add(f3.clone(), Some(StreamId::new(0, 0)), None, false)
            .is_err());

        assert_eq!(stream.len(), 2);
    }

    /// 测试 XRANGE / XREVRANGE 范围查询
    #[test]
    fn test_stream_range() {
        let mut stream = Stream::new();

        for i in 0..10 {
            let mut f = HashMap::new();
            f.insert(b"val".to_vec(), i.to_string().into_bytes());
            stream
                .add(f, Some(StreamId::new(1000, i)), None, false)
                .unwrap();
        }

        // 查询全部
        let all = stream.range(StreamId::min(), StreamId::max(), None);
        assert_eq!(all.len(), 10);
        assert_eq!(all[0].id, StreamId::new(1000, 0));
        assert_eq!(all[9].id, StreamId::new(1000, 9));

        // 查询部分
        let partial = stream.range(StreamId::new(1000, 3), StreamId::new(1000, 6), None);
        assert_eq!(partial.len(), 4);

        // 带 count 限制
        let limited = stream.range(StreamId::min(), StreamId::max(), Some(3));
        assert_eq!(limited.len(), 3);

        // XREVRANGE
        let rev = stream.rev_range(StreamId::max(), StreamId::min(), None);
        assert_eq!(rev.len(), 10);
        assert_eq!(rev[0].id, StreamId::new(1000, 9));
        assert_eq!(rev[9].id, StreamId::new(1000, 0));

        // XREVRANGE 带 count
        let rev_limited = stream.rev_range(StreamId::max(), StreamId::min(), Some(3));
        assert_eq!(rev_limited.len(), 3);
        assert_eq!(rev_limited[0].id, StreamId::new(1000, 9));
    }

    /// 测试 XREAD 读取
    #[test]
    fn test_stream_read() {
        let mut stream = Stream::new();

        for i in 0..10 {
            let mut f = HashMap::new();
            f.insert(b"val".to_vec(), i.to_string().into_bytes());
            stream
                .add(f, Some(StreamId::new(1000, i)), None, false)
                .unwrap();
        }

        // 从 1000-4 之后读取
        let entries = stream.read(StreamId::new(1000, 4), None);
        assert_eq!(entries.len(), 5);
        assert_eq!(entries[0].id, StreamId::new(1000, 5));

        // 带 count
        let entries2 = stream.read(StreamId::new(1000, 7), Some(2));
        assert_eq!(entries2.len(), 2);
        assert_eq!(entries2[0].id, StreamId::new(1000, 8));

        // 从最后一条之后读取
        let empty = stream.read(StreamId::new(1000, 9), None);
        assert!(empty.is_empty());
    }

    /// 测试 XDEL 删除消息
    #[test]
    fn test_stream_delete() {
        let mut stream = Stream::new();

        for i in 0..5 {
            let mut f = HashMap::new();
            f.insert(b"val".to_vec(), i.to_string().into_bytes());
            stream
                .add(f, Some(StreamId::new(1000, i)), None, false)
                .unwrap();
        }

        assert_eq!(stream.len(), 5);

        // 删除中间和尾部的条目
        let deleted = stream.delete(&[
            StreamId::new(1000, 1),
            StreamId::new(1000, 3),
            StreamId::new(1000, 99),
        ]);
        assert_eq!(deleted, 2); // 1000-99 不存在
        assert_eq!(stream.len(), 3);

        // 验证删除后的范围查询
        let all = stream.range(StreamId::min(), StreamId::max(), None);
        assert_eq!(all.len(), 3);
        assert_eq!(all[0].id, StreamId::new(1000, 0));
        assert_eq!(all[1].id, StreamId::new(1000, 2));
        assert_eq!(all[2].id, StreamId::new(1000, 4));
    }

    /// 测试 XTRIM 裁剪
    #[test]
    fn test_stream_trim() {
        let mut stream = Stream::new();

        for i in 0..20 {
            let mut f = HashMap::new();
            f.insert(b"val".to_vec(), i.to_string().into_bytes());
            stream
                .add(f, Some(StreamId::new(1000, i)), None, false)
                .unwrap();
        }

        assert_eq!(stream.len(), 20);

        // 精确裁剪到 5
        let trimmed = stream.trim(5, false);
        assert_eq!(trimmed, 15);
        assert_eq!(stream.len(), 5);

        // 验证保留的是最新的 5 条
        let all = stream.range(StreamId::min(), StreamId::max(), None);
        assert_eq!(all[0].id, StreamId::new(1000, 15));
        assert_eq!(all[4].id, StreamId::new(1000, 19));
    }

    /// 测试 XADD 带 max_len 自动裁剪
    #[test]
    fn test_stream_add_with_trim() {
        let mut stream = Stream::new();

        for i in 0..20 {
            let mut f = HashMap::new();
            f.insert(b"val".to_vec(), i.to_string().into_bytes());
            stream
                .add(f, Some(StreamId::new(1000, i)), Some(10), false)
                .unwrap();
        }

        assert_eq!(stream.len(), 10);
    }

    /// 测试消费者组创建和读取
    #[test]
    fn test_consumer_group() {
        let mut stream = Stream::new();

        // 添加消息
        for i in 0..10 {
            let mut f = HashMap::new();
            f.insert(b"msg".to_vec(), format!("message_{}", i).into_bytes());
            stream
                .add(f, Some(StreamId::new(1000, i)), None, false)
                .unwrap();
        }

        // 创建消费者组
        stream.create_group("mygroup", StreamId::min()).unwrap();

        // 不能重复创建
        assert!(stream.create_group("mygroup", StreamId::min()).is_err());

        // 消费者读取
        let entries = stream
            .read_group("mygroup", "consumer1", Some(3), false, None)
            .unwrap();
        assert_eq!(entries.len(), 3);
        assert_eq!(entries[0].id, StreamId::new(1000, 0));
        assert_eq!(entries[2].id, StreamId::new(1000, 2));

        // 验证 PEL
        let group = stream.get_group("mygroup").unwrap();
        assert_eq!(group.pending.len(), 3);
        let consumer = group.consumers.get("consumer1").unwrap();
        assert_eq!(consumer.pending.len(), 3);

        // 同一组的其他消费者读取后续消息
        let entries2 = stream
            .read_group("mygroup", "consumer2", Some(3), false, None)
            .unwrap();
        assert_eq!(entries2.len(), 3);
        assert_eq!(entries2[0].id, StreamId::new(1000, 3));
    }

    /// 测试 XACK 确认消息
    #[test]
    fn test_ack() {
        let mut stream = Stream::new();

        for i in 0..5 {
            let mut f = HashMap::new();
            f.insert(b"msg".to_vec(), format!("msg_{}", i).into_bytes());
            stream
                .add(f, Some(StreamId::new(1000, i)), None, false)
                .unwrap();
        }

        stream.create_group("mygroup", StreamId::min()).unwrap();
        let entries = stream
            .read_group("mygroup", "consumer1", Some(3), false, None)
            .unwrap();
        assert_eq!(entries.len(), 3);

        // 确认前两条
        let acked = stream
            .ack("mygroup", &[StreamId::new(1000, 0), StreamId::new(1000, 1)])
            .unwrap();
        assert_eq!(acked, 2);

        // PEL 应该减少
        let group = stream.get_group("mygroup").unwrap();
        assert_eq!(group.pending.len(), 1);
        assert_eq!(group.pending[0].entry_id, StreamId::new(1000, 2));
    }

    /// 测试 XINFO
    #[test]
    fn test_xinfo() {
        let mut stream = Stream::new();

        for i in 0..5 {
            let mut f = HashMap::new();
            f.insert(b"val".to_vec(), i.to_string().into_bytes());
            stream
                .add(f, Some(StreamId::new(1000, i)), None, false)
                .unwrap();
        }

        stream.create_group("group1", StreamId::min()).unwrap();
        stream
            .create_group("group2", StreamId::new(1000, 2))
            .unwrap();

        let info = stream.info();
        assert_eq!(info.length, 5);
        assert_eq!(info.last_id, StreamId::new(1000, 4));
        assert_eq!(info.consumer_groups_count, 2);
        assert!(info.first_entry.is_some());
        assert!(info.last_entry.is_some());

        let groups = stream.info_groups();
        assert_eq!(groups.len(), 2);

        // XINFO CONSUMERS
        stream
            .read_group("group1", "alice", Some(2), false, None)
            .unwrap();
        stream
            .read_group("group1", "bob", Some(1), false, None)
            .unwrap();

        let consumers = stream.info_consumers("group1").unwrap();
        assert_eq!(consumers.len(), 2);
    }

    /// 测试消费者组删除
    #[test]
    fn test_group_operations() {
        let mut stream = Stream::new();

        for i in 0..10 {
            let mut f = HashMap::new();
            f.insert(b"val".to_vec(), i.to_string().into_bytes());
            stream
                .add(f, Some(StreamId::new(1000, i)), None, false)
                .unwrap();
        }

        stream.create_group("mygroup", StreamId::min()).unwrap();
        stream
            .read_group("mygroup", "c1", Some(5), false, None)
            .unwrap();

        // 删除消费者
        let remaining = stream.del_consumer("mygroup", "c1").unwrap();
        assert_eq!(remaining, 5); // 5 条待确认消息

        let group = stream.get_group("mygroup").unwrap();
        assert!(group.consumers.is_empty());
        assert!(group.pending.is_empty());

        // 设置组 ID
        stream
            .set_group_id("mygroup", StreamId::new(1000, 8))
            .unwrap();
        let group = stream.get_group("mygroup").unwrap();
        assert_eq!(group.last_delivered_id, StreamId::new(1000, 8));

        // 删除消费者组
        assert!(stream.destroy_group("mygroup"));
        assert!(stream.get_group("mygroup").is_none());
    }

    /// 测试 XREADGROUP noack 模式
    #[test]
    fn test_read_group_noack() {
        let mut stream = Stream::new();

        for i in 0..5 {
            let mut f = HashMap::new();
            f.insert(b"val".to_vec(), i.to_string().into_bytes());
            stream
                .add(f, Some(StreamId::new(1000, i)), None, false)
                .unwrap();
        }

        stream.create_group("mygroup", StreamId::min()).unwrap();

        // noack 模式：消息不进入 PEL
        let entries = stream
            .read_group("mygroup", "consumer1", Some(3), true, None)
            .unwrap();
        assert_eq!(entries.len(), 3);

        let group = stream.get_group("mygroup").unwrap();
        assert!(group.pending.is_empty());
        assert!(group.consumers.get("consumer1").unwrap().pending.is_empty());
    }

    /// 测试空字段添加失败
    #[test]
    fn test_add_empty_fields() {
        let mut stream = Stream::new();
        let empty = HashMap::new();
        assert!(stream.add(empty, None, None, false).is_err());
    }
}
