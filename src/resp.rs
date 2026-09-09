//! RESP (REdis Serialization Protocol) 序列化协议解析器与编码器
//!
//! RESP 是 Redis 客户端与服务器之间的通信协议，采用基于文本行的格式：
//!   - 每条消息以 \r\n (CRLF) 结尾
//!   - 第一个字节标识数据类型
//!   - 支持 RESP2（经典）和 RESP3（Redis 6.0+）两种版本
//!
//! RESP2 支持的类型：
//!   +<字符串>\r\n          → SimpleString（简单字符串，不含 \r\n）
//!   -<错误信息>\r\n        → Error（错误）
//!   :<整数>\r\n           → Integer（64位有符号整数）
//!   $<长度>\r\n<数据>\r\n  → BulkString（二进制安全字符串）
//!   $-1\r\n               → Null（空BulkString，表示不存在）
//!   *<数量>\r\n<元素...>   → Array（数组）
//!   *-1\r\n               → NullArray（空数组）
//!
//! RESP3 额外支持的类型：
//!   #t\r\n / #f\r\n       → Boolean（布尔值）
//!   ,<浮点数>\r\n         → Double（双精度浮点）
//!   %<数量>\r\n<k><v>...  → Map（键值对映射）
//!   ~<数量>\r\n<元素...>  → Set（集合）
//!   _\r\n                 → NullResp3（RESP3空值）
//!
//! 此外还支持 inline 命令格式（如 redis-cli 直接输入的命令）：
//!   <命令> <参数1> <参数2>\r\n → 解析为 Array(BulkString, ...)

use bytes::{Buf, BytesMut};
use std::fmt;

/// RESP 值类型枚举，对应 Redis 协议中所有数据类型
///
/// 每个变体代表 RESP 协议中的一种数据类型。
/// 编码时根据 `resp3` 标志决定使用 RESP2 还是 RESP3 格式输出。
/// 对于 RESP3 特有类型（Boolean/Double/Map/Set），在 RESP2 模式下
/// 会自动降级为等价的 RESP2 表示。
#[derive(Debug, Clone, PartialEq)]
pub enum RespValue {
    /// 简单字符串：+OK\r\n
    ///
    /// 用于传输不含 \r\n 的短文本，如 "OK"、"PONG" 等状态回复。
    /// 格式：`+<字符串内容>\r\n`
    SimpleString(String),

    /// 错误信息：-ERR message\r\n
    ///
    /// 服务器返回的错误，如命令不存在、参数错误等。
    /// 格式：`-<错误类型> <错误描述>\r\n`
    Error(String),

    /// 64位有符号整数：:1000\r\n
    ///
    /// 用于返回计数、序号等整数值。
    /// 格式：`:<整数值>\r\n`
    Integer(i64),

    /// 二进制安全字符串：$6\r\nfoobar\r\n
    ///
    /// Redis 中最常用的数据类型，可包含任意二进制数据（包括 \0）。
    /// 格式：`$<字节长度>\r\n<原始字节数据>\r\n`
    /// 前缀的长度字段确保解析器知道需要读取多少字节。
    BulkString(Vec<u8>),

    /// 空值（Null Bulk String）：$-1\r\n
    ///
    /// 表示键不存在或结果为空。RESP2 中用 $-1\r\n 表示。
    Null,

    /// 数组：*2\r\n$3\r\nfoo\r\n$3\r\nbar\r\n
    ///
    /// 有序的值列表，元素可以是任意 RESP 类型（嵌套数组也支持）。
    /// 格式：`*<元素数量>\r\n<元素1><元素2>...`
    /// Redis 命令的请求和回复通常都是数组格式。
    Array(Vec<RespValue>),

    /// 空数组（Null Array）：*-1\r\n
    ///
    /// 表示空结果集，与 Null 语义略有不同（区分"空列表"和"不存在"）。
    NullArray,

    /// RESP3 布尔值：#t\r\n 或 #f\r\n
    ///
    /// RESP3 新增类型。`#t` 表示 true，`#f` 表示 false。
    /// 在 RESP2 模式下编码时会降级为 Integer(1) 或 Integer(0)。
    Boolean(bool),

    /// RESP3 双精度浮点数：,3.14\r\n
    ///
    /// RESP3 新增类型，支持 NaN、inf、-inf 等特殊值。
    /// 在 RESP2 模式下编码时会降级为 BulkString。
    Double(f64),

    /// RESP3 键值对映射： %2\r\n<key1><val1><key2><val2>
    ///
    /// RESP3 新增类型，元素按 key-value 交替排列。
    /// 在 RESP2 模式下编码时会展平为 Array（key1, val1, key2, val2, ...）。
    Map(Vec<(RespValue, RespValue)>),

    /// RESP3 集合：~2\r\n<元素1><元素2>
    ///
    /// RESP3 新增类型，与 Array 类似但语义上是无序集合。
    /// 在 RESP2 模式下编码时会降级为 Array。
    Set(Vec<RespValue>),

    /// RESP3 空值：_\r\n
    ///
    /// RESP3 统一的空值表示，替代 RESP2 中的 $-1 和 *-1。
    NullResp3,
}

impl RespValue {
    /// 创建 OK 简单字符串响应（最常见的成功回复）
    pub fn ok() -> Self {
        RespValue::SimpleString("OK".to_string())
    }

    /// 创建错误响应
    ///
    /// `msg` 支持任何可转为 String 的类型，如 &str、String 等。
    pub fn err(msg: impl Into<String>) -> Self {
        RespValue::Error(msg.into())
    }

    /// 创建整数响应
    pub fn integer(n: i64) -> Self {
        RespValue::Integer(n)
    }

    /// 创建二进制安全字符串（BulkString）
    ///
    /// `data` 支持 Vec<u8>、&[u8]、String 等类型。
    pub fn bulk(data: impl Into<Vec<u8>>) -> Self {
        RespValue::BulkString(data.into())
    }

    /// 创建简单字符串响应
    pub fn string(s: impl Into<String>) -> Self {
        RespValue::SimpleString(s.into())
    }

    /// 创建数组响应
    pub fn array(items: Vec<RespValue>) -> Self {
        RespValue::Array(items)
    }

    /// 创建空值响应（RESP2 的 Null BulkString）
    pub fn null() -> Self {
        RespValue::Null
    }

    /// 将 RespValue 编码为 RESP 协议字节序列
    ///
    /// `resp3` 参数决定输出格式：
    /// - `false`: 使用 RESP2 格式，RESP3 特有类型会自动降级
    /// - `true`: 使用 RESP3 格式，支持所有类型
    pub fn encode(&self, resp3: bool) -> Vec<u8> {
        let mut buf = Vec::new();
        self.encode_into(&mut buf, resp3);
        buf
    }

    /// 将 RespValue 编码并追加到指定缓冲区（避免重复分配）
    ///
    /// 编码逻辑按类型分派：
    /// 1. SimpleString/Error/Integer/BulkString → 直接按 RESP2 格式写入
    /// 2. Null → RESP2 用 $-1\r\n，RESP3 用 _\r\n
    /// 3. Array/NullArray → 递归编码每个元素
    /// 4. Boolean/Double/Map/Set → RESP3 直接写入，RESP2 降级处理
    pub fn encode_into(&self, buf: &mut Vec<u8>, resp3: bool) {
        match self {
            // 简单字符串：+<内容>\r\n
            RespValue::SimpleString(s) => {
                buf.extend_from_slice(b"+");
                buf.extend_from_slice(s.as_bytes());
                buf.extend_from_slice(b"\r\n");
            }
            // 错误：-<内容>\r\n
            RespValue::Error(s) => {
                buf.extend_from_slice(b"-");
                buf.extend_from_slice(s.as_bytes());
                buf.extend_from_slice(b"\r\n");
            }
            // 整数：:<数字>\r\n
            RespValue::Integer(n) => {
                buf.extend_from_slice(b":");
                buf.extend_from_slice(n.to_string().as_bytes());
                buf.extend_from_slice(b"\r\n");
            }
            // 二进制安全字符串：$<长度>\r\n<数据>\r\n
            RespValue::BulkString(data) => {
                buf.extend_from_slice(b"$");
                buf.extend_from_slice(data.len().to_string().as_bytes());
                buf.extend_from_slice(b"\r\n");
                buf.extend_from_slice(data);
                buf.extend_from_slice(b"\r\n");
            }
            // 空值：RESP2 用 $-1\r\n，RESP3 用 _\r\n
            RespValue::Null => {
                if resp3 {
                    buf.extend_from_slice(b"_\r\n");
                } else {
                    buf.extend_from_slice(b"$-1\r\n");
                }
            }
            // 数组：*<数量>\r\n，然后递归编码每个元素
            RespValue::Array(items) => {
                buf.extend_from_slice(b"*");
                buf.extend_from_slice(items.len().to_string().as_bytes());
                buf.extend_from_slice(b"\r\n");
                for item in items {
                    item.encode_into(buf, resp3);
                }
            }
            // 空数组：*-1\r\n（RESP2 和 RESP3 格式相同）
            RespValue::NullArray => {
                buf.extend_from_slice(b"*-1\r\n");
            }
            // RESP3 布尔值：#t\r\n 或 #f\r\n
            // RESP2 降级为整数 1 或 0
            RespValue::Boolean(b) => {
                if resp3 {
                    buf.extend_from_slice(if *b { b"#t\r\n" } else { b"#f\r\n" });
                } else {
                    RespValue::Integer(if *b { 1 } else { 0 }).encode_into(buf, resp3);
                }
            }
            // RESP3 浮点数：,<值>\r\n，特殊值用 nan/inf/-inf
            // RESP2 降级为 BulkString
            RespValue::Double(f) => {
                if resp3 {
                    buf.extend_from_slice(b",");
                    if f.is_nan() {
                        buf.extend_from_slice(b"nan");
                    } else if f.is_infinite() {
                        buf.extend_from_slice(if f.is_sign_positive() {
                            b"inf"
                        } else {
                            b"-inf"
                        });
                    } else {
                        buf.extend_from_slice(format!("{}", f).as_bytes());
                    }
                    buf.extend_from_slice(b"\r\n");
                } else {
                    RespValue::BulkString(format!("{}", f).into_bytes()).encode_into(buf, resp3);
                }
            }
            // RESP3 映射：%<数量>\r\n，key 和 value 交替编码
            // RESP2 降级为展平的 Array（key1, val1, key2, val2, ...）
            RespValue::Map(pairs) => {
                if resp3 {
                    buf.extend_from_slice(b"%");
                    buf.extend_from_slice(pairs.len().to_string().as_bytes());
                    buf.extend_from_slice(b"\r\n");
                    for (k, v) in pairs {
                        k.encode_into(buf, resp3);
                        v.encode_into(buf, resp3);
                    }
                } else {
                    let mut items = Vec::with_capacity(pairs.len() * 2);
                    for (k, v) in pairs {
                        items.push(k.clone());
                        items.push(v.clone());
                    }
                    RespValue::Array(items).encode_into(buf, resp3);
                }
            }
            // RESP3 集合：~<数量>\r\n，与 Array 编码方式相同
            // RESP2 降级为 Array
            RespValue::Set(items) => {
                if resp3 {
                    buf.extend_from_slice(b"~");
                    buf.extend_from_slice(items.len().to_string().as_bytes());
                    buf.extend_from_slice(b"\r\n");
                    for item in items {
                        item.encode_into(buf, resp3);
                    }
                } else {
                    RespValue::Array(items.clone()).encode_into(buf, resp3);
                }
            }
            // RESP3 空值：_\r\n（总是直接写入，不区分版本）
            RespValue::NullResp3 => {
                buf.extend_from_slice(b"_\r\n");
            }
        }
    }

    /// 获取值的原始字节引用（仅 BulkString 和 SimpleString 支持）
    ///
    /// 用于命令参数提取等场景，其他类型返回 None。
    pub fn as_bytes(&self) -> Option<&[u8]> {
        match self {
            RespValue::BulkString(d) => Some(d),
            RespValue::SimpleString(s) => Some(s.as_bytes()),
            _ => None,
        }
    }

    /// 将值转为可读字符串（有损转换，二进制数据用 UTF-8 替换字符）
    ///
    /// 用于日志输出和调试显示，非字符串类型会做适当格式化。
    pub fn to_string_lossy(&self) -> String {
        match self {
            RespValue::BulkString(d) => String::from_utf8_lossy(d).to_string(),
            RespValue::SimpleString(s) => s.clone(),
            RespValue::Error(s) => s.clone(),
            RespValue::Integer(n) => n.to_string(),
            RespValue::Boolean(b) => b.to_string(),
            RespValue::Double(f) => f.to_string(),
            _ => String::new(),
        }
    }

    /// 尝试将值解析为 i64 整数
    ///
    /// 支持 Integer（直接返回）、BulkString/SimpleString（尝试解析字符串）。
    pub fn as_i64(&self) -> Option<i64> {
        match self {
            RespValue::Integer(n) => Some(*n),
            RespValue::BulkString(d) => {
                let s = std::str::from_utf8(d).ok()?;
                s.parse().ok()
            }
            RespValue::SimpleString(s) => s.parse().ok(),
            _ => None,
        }
    }

    /// 尝试将值解析为 f64 浮点数
    ///
    /// 支持 Double（直接返回）、Integer（转为 f64）、BulkString（解析字符串）。
    pub fn as_f64(&self) -> Option<f64> {
        match self {
            RespValue::Double(f) => Some(*f),
            RespValue::Integer(n) => Some(*n as f64),
            RespValue::BulkString(d) => {
                let s = std::str::from_utf8(d).ok()?;
                s.parse().ok()
            }
            _ => None,
        }
    }
}

/// Display trait 实现，用于 RESP 值的人类可读输出
///
/// 格式参考 redis-cli 的输出风格：
/// - 简单字符串/整数/浮点/布尔 → 直接显示值
/// - 错误 → 前缀 "ERR "
/// - 空值 → "(nil)"
/// - 数组 → 元素间用空格分隔
/// - Map/Set → 显示类型标记
impl fmt::Display for RespValue {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            RespValue::SimpleString(s) => write!(f, "{}", s),
            RespValue::Error(s) => write!(f, "ERR {}", s),
            RespValue::Integer(n) => write!(f, "{}", n),
            RespValue::BulkString(d) => write!(f, "{}", String::from_utf8_lossy(d)),
            RespValue::Null => write!(f, "(nil)"),
            RespValue::Array(items) => {
                for (i, item) in items.iter().enumerate() {
                    if i > 0 {
                        write!(f, " ")?;
                    }
                    write!(f, "{}", item)?;
                }
                Ok(())
            }
            RespValue::NullArray => write!(f, "(nil)"),
            RespValue::Boolean(b) => write!(f, "{}", b),
            RespValue::Double(d) => write!(f, "{}", d),
            RespValue::Map(_) => write!(f, "(map)"),
            RespValue::Set(_) => write!(f, "(set)"),
            RespValue::NullResp3 => write!(f, "(nil)"),
        }
    }
}

/// RESP 协议解析器
///
/// 使用 BytesMut 作为内部缓冲区，支持增量解析（TCP 流式场景）。
/// 解析流程：
///   1. 调用 `extend()` 将从网络读取的字节追加到缓冲区
///   2. 调用 `parse()` 尝试从缓冲区中解析一个完整的 RESP 值
///   3. 返回 Ok(Some(value)) 表示成功，Ok(None) 表示数据不足需要继续读取
///
/// 支持两种输入格式：
///   - RESP 格式：首字节为类型标识符（+、-、:、$、*、_、#、,）
///   - Inline 格式：以普通文本开头，按空格分割为命令+参数数组
#[derive(Debug)]
pub struct RespParser {
    /// 内部字节缓冲区，累积从网络读取的原始数据
    buf: BytesMut,
    /// 是否启用 RESP3 协议模式（影响 #、,、%、~ 等类型的解析）
    resp3: bool,
}

impl RespParser {
    /// 创建新的解析器实例（默认 RESP2 模式，缓冲区初始容量 8KB）
    pub fn new() -> Self {
        Self {
            buf: BytesMut::with_capacity(8192),
            resp3: false,
        }
    }

    /// 设置是否启用 RESP3 协议模式
    pub fn set_resp3(&mut self, v: bool) {
        self.resp3 = v;
    }

    /// 查询当前是否为 RESP3 模式
    pub fn is_resp3(&self) -> bool {
        self.resp3
    }

    /// 将从网络读取的原始字节追加到内部缓冲区
    ///
    /// 每次从 TCP socket 读取数据后调用此方法。
    pub fn extend(&mut self, data: &[u8]) {
        self.buf.extend_from_slice(data);
    }

    /// 获取当前缓冲区中待解析的字节数
    pub fn buf_len(&self) -> usize {
        self.buf.len()
    }

    /// 从缓冲区中尝试解析一个完整的 RESP 值
    ///
    /// # 返回值
    /// - `Ok(Some(value))` — 成功解析出一个完整值
    /// - `Ok(None)` — 数据不完整，需要调用 `extend()` 追加更多数据后重试
    /// - `Err(msg)` — 协议格式错误
    ///
    /// # 解析策略
    /// 根据缓冲区首字节判断类型：
    /// - `+` → SimpleString
    /// - `-` → Error
    /// - `:` → Integer
    /// - `$` → BulkString（含 Null）
    /// - `*` → Array（含 NullArray）
    /// - `_` → RESP3 Null（仅 resp3 模式）
    /// - `#` → RESP3 Boolean（仅 resp3 模式）
    /// - `,` → RESP3 Double（仅 resp3 模式）
    /// - 其他 → 尝试 Inline 命令解析
    pub fn parse(&mut self) -> Result<Option<RespValue>, String> {
        if self.buf.is_empty() {
            return Ok(None);
        }

        let first = self.buf[0];
        match first {
            // RESP2 标准类型（+、-、:、$、*）
            b'+' | b'-' | b':' | b'$' | b'*' => self.parse_resp(),
            // RESP3 空值：_\r\n
            b'_' if self.resp3 => {
                if let Some(pos) = find_crlf(&self.buf, 1) {
                    self.buf.advance(pos + 2);
                    return Ok(Some(RespValue::NullResp3));
                }
                Ok(None)
            }
            // RESP3 布尔值：#t\r\n 或 #f\r\n
            // 需要至少 4 字节：前缀 + 值 + \r\n
            b'#' if self.resp3 => {
                if self.buf.len() >= 4 && self.buf[2] == b'\r' && self.buf[3] == b'\n' {
                    let val = match self.buf[1] {
                        b't' => true,
                        b'f' => false,
                        _ => return Err("Invalid boolean".to_string()),
                    };
                    self.buf.advance(4);
                    return Ok(Some(RespValue::Boolean(val)));
                }
                Ok(None)
            }
            // RESP3 浮点数：,<值>\r\n
            // 特殊值：nan、inf、-inf
            b',' if self.resp3 => {
                if let Some(pos) = find_crlf(&self.buf, 1) {
                    let s = std::str::from_utf8(&self.buf[1..pos]).map_err(|_| "Invalid double")?;
                    let val: f64 = match s {
                        "nan" => f64::NAN,
                        "inf" => f64::INFINITY,
                        "-inf" => f64::NEG_INFINITY,
                        _ => s.parse().map_err(|_| "Invalid double")?,
                    };
                    self.buf.advance(pos + 2);
                    return Ok(Some(RespValue::Double(val)));
                }
                Ok(None)
            }
            // 非标准前缀字节 → 尝试 Inline 命令解析
            _ => self.parse_inline(),
        }
    }

    /// 解析 RESP2 标准类型（由首字节分派到具体解析方法）
    fn parse_resp(&mut self) -> Result<Option<RespValue>, String> {
        if self.buf.is_empty() {
            return Ok(None);
        }

        match self.buf[0] {
            b'+' => self.parse_simple_string(),
            b'-' => self.parse_error(),
            b':' => self.parse_integer(),
            b'$' => self.parse_bulk_string(),
            b'*' => self.parse_array(),
            _ => Err(format!("Unexpected type byte: {}", self.buf[0])),
        }
    }

    /// 解析简单字符串：+<内容>\r\n
    ///
    /// 从 `+` 后的第一个字节开始扫描 \r\n，提取中间的文本。
    fn parse_simple_string(&mut self) -> Result<Option<RespValue>, String> {
        if let Some(pos) = find_crlf(&self.buf, 1) {
            let s = String::from_utf8_lossy(&self.buf[1..pos]).to_string();
            self.buf.advance(pos + 2);
            Ok(Some(RespValue::SimpleString(s)))
        } else {
            Ok(None)
        }
    }

    /// 解析错误：-<内容>\r\n
    ///
    /// 与 SimpleString 格式相同，只是前缀不同。
    fn parse_error(&mut self) -> Result<Option<RespValue>, String> {
        if let Some(pos) = find_crlf(&self.buf, 1) {
            let s = String::from_utf8_lossy(&self.buf[1..pos]).to_string();
            self.buf.advance(pos + 2);
            Ok(Some(RespValue::Error(s)))
        } else {
            Ok(None)
        }
    }

    /// 解析整数：:<数字>\r\n
    ///
    /// 提取 `:` 和 `\r\n` 之间的字符串并解析为 i64。
    fn parse_integer(&mut self) -> Result<Option<RespValue>, String> {
        if let Some(pos) = find_crlf(&self.buf, 1) {
            let s =
                std::str::from_utf8(&self.buf[1..pos]).map_err(|_| "Invalid integer encoding")?;
            let n: i64 = s.parse().map_err(|_| format!("Invalid integer: {}", s))?;
            self.buf.advance(pos + 2);
            Ok(Some(RespValue::Integer(n)))
        } else {
            Ok(None)
        }
    }

    /// 解析二进制安全字符串：$<长度>\r\n<数据>\r\n
    ///
    /// 解析流程：
    /// 1. 读取 `$` 和第一个 `\r\n` 之间的长度字段
    /// 2. 若长度为 -1，表示 Null（$-1\r\n）
    /// 3. 检查缓冲区是否包含足够数据（长度 + 2字节结尾 CRLF）
    /// 4. 验证数据末尾确实是 \r\n
    /// 5. 提取数据并消费缓冲区
    fn parse_bulk_string(&mut self) -> Result<Option<RespValue>, String> {
        if let Some(pos) = find_crlf(&self.buf, 1) {
            let len_str =
                std::str::from_utf8(&self.buf[1..pos]).map_err(|_| "Invalid bulk string length")?;
            let len: i64 = len_str
                .parse()
                .map_err(|_| format!("Invalid bulk string length: {}", len_str))?;

            // 负长度表示 Null
            if len < 0 {
                self.buf.advance(pos + 2);
                return Ok(Some(RespValue::Null));
            }

            let len = len as usize;
            let data_start = pos + 2; // 数据起始位置（跳过长度行的 \r\n）
            let data_end = data_start + len; // 数据结束位置

            // 数据不足，等待更多字节
            if self.buf.len() < data_end + 2 {
                return Ok(None); // Need more data
            }

            // 验证数据末尾的 \r\n
            if self.buf[data_end] != b'\r' || self.buf[data_end + 1] != b'\n' {
                return Err("Invalid bulk string terminator".to_string());
            }

            // 提取数据并消费已解析的字节
            let data = self.buf[data_start..data_end].to_vec();
            self.buf.advance(data_end + 2);
            Ok(Some(RespValue::BulkString(data)))
        } else {
            Ok(None)
        }
    }

    /// 解析数组：*<数量>\r\n<元素1><元素2>...
    ///
    /// 解析流程：
    /// 1. 读取 `*` 和第一个 `\r\n` 之间的元素数量
    /// 2. 若数量为 -1，表示 NullArray（*-1\r\n）
    /// 3. 跳过数量行的 \r\n
    /// 4. 循环调用 `parse_resp()` 解析每个子元素（递归解析）
    ///
    /// 注意：如果子元素数据不完整，当前实现会返回错误，
    /// 因为无法安全地将已消费的字节放回缓冲区。
    fn parse_array(&mut self) -> Result<Option<RespValue>, String> {
        if let Some(pos) = find_crlf(&self.buf, 1) {
            let len_str =
                std::str::from_utf8(&self.buf[1..pos]).map_err(|_| "Invalid array length")?;
            let len: i64 = len_str
                .parse()
                .map_err(|_| format!("Invalid array length: {}", len_str))?;

            // 负数量表示 NullArray
            if len < 0 {
                self.buf.advance(pos + 2);
                return Ok(Some(RespValue::NullArray));
            }

            // 跳过数量行
            self.buf.advance(pos + 2);

            let len = len as usize;
            let mut items = Vec::with_capacity(len);
            for _ in 0..len {
                // 递归解析子元素
                match self.parse_resp()? {
                    Some(v) => items.push(v),
                    None => {
                        // 数据不完整 — 当前实现不支持部分数组的增量解析
                        return Err("Incomplete array".to_string());
                    }
                }
            }
            Ok(Some(RespValue::Array(items)))
        } else {
            Ok(None)
        }
    }

    /// 解析 Inline 命令（非 RESP 格式的文本命令）
    ///
    /// Inline 格式：`<命令> <参数1> <参数2>\r\n`
    /// redis-cli 在某些场景下会使用这种格式直接发送命令。
    ///
    /// 解析流程：
    /// 1. 扫描 \r\n（优先）或单独的 \n（兼容部分客户端）
    /// 2. 按空格分割为多个单词
    /// 3. 将每个单词包装为 BulkString
    /// 4. 返回 Array 形式（与 RESP 格式的命令结构一致）
    fn parse_inline(&mut self) -> Result<Option<RespValue>, String> {
        // 优先查找 \r\n
        if let Some(pos) = find_crlf(&self.buf, 0) {
            let line = String::from_utf8_lossy(&self.buf[..pos]).to_string();
            self.buf.advance(pos + 2);

            let parts: Vec<RespValue> = line
                .split_whitespace()
                .map(|s| RespValue::BulkString(s.as_bytes().to_vec()))
                .collect();

            if parts.is_empty() {
                return Ok(None);
            }
            Ok(Some(RespValue::Array(parts)))
        } else {
            // 兼容只用 \n 换行的客户端
            if let Some(pos) = find_lf(&self.buf, 0) {
                let line = String::from_utf8_lossy(&self.buf[..pos]).to_string();
                self.buf.advance(pos + 1);

                let parts: Vec<RespValue> = line
                    .split_whitespace()
                    .map(|s| RespValue::BulkString(s.as_bytes().to_vec()))
                    .collect();

                if parts.is_empty() {
                    return Ok(None);
                }
                return Ok(Some(RespValue::Array(parts)));
            }
            Ok(None)
        }
    }

    /// 判断缓冲区是否为空（无待解析数据）
    pub fn is_empty(&self) -> bool {
        self.buf.is_empty()
    }
}

/// 在字节缓冲区中查找 \r\n (CRLF) 的位置
///
/// 从 `start` 位置开始扫描，返回 `\r` 的索引。
/// 用于 RESP 协议的行终止符定位。
///
/// 注意：会检查 `i+1 < buf.len()`，因此不会越界访问。
fn find_crlf(buf: &[u8], start: usize) -> Option<usize> {
    for i in start..buf.len() - 1 {
        if buf[i] == b'\r' && buf[i + 1] == b'\n' {
            return Some(i);
        }
    }
    None
}

/// 在字节缓冲区中查找 \n (LF) 的位置
///
/// 仅用于 Inline 命令解析的后备方案，兼容只使用 LF 换行的客户端。
fn find_lf(buf: &[u8], start: usize) -> Option<usize> {
    for i in start..buf.len() {
        if buf[i] == b'\n' {
            return Some(i);
        }
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;

    /// 测试简单字符串解析：+OK\r\n
    #[test]
    fn test_parse_simple_string() {
        let mut parser = RespParser::new();
        parser.extend(b"+OK\r\n");
        let val = parser.parse().unwrap().unwrap();
        assert_eq!(val, RespValue::SimpleString("OK".to_string()));
    }

    /// 测试错误信息解析：-ERR unknown command\r\n
    #[test]
    fn test_parse_error() {
        let mut parser = RespParser::new();
        parser.extend(b"-ERR unknown command\r\n");
        let val = parser.parse().unwrap().unwrap();
        assert_eq!(val, RespValue::Error("ERR unknown command".to_string()));
    }

    /// 测试整数解析：:1000\r\n
    #[test]
    fn test_parse_integer() {
        let mut parser = RespParser::new();
        parser.extend(b":1000\r\n");
        let val = parser.parse().unwrap().unwrap();
        assert_eq!(val, RespValue::Integer(1000));
    }

    /// 测试二进制安全字符串解析：$6\r\nfoobar\r\n
    #[test]
    fn test_parse_bulk_string() {
        let mut parser = RespParser::new();
        parser.extend(b"$6\r\nfoobar\r\n");
        let val = parser.parse().unwrap().unwrap();
        assert_eq!(val, RespValue::BulkString(b"foobar".to_vec()));
    }

    /// 测试空值解析：$-1\r\n
    #[test]
    fn test_parse_null() {
        let mut parser = RespParser::new();
        parser.extend(b"$-1\r\n");
        let val = parser.parse().unwrap().unwrap();
        assert_eq!(val, RespValue::Null);
    }

    /// 测试数组解析：*2\r\n$3\r\nfoo\r\n$3\r\nbar\r\n
    #[test]
    fn test_parse_array() {
        let mut parser = RespParser::new();
        parser.extend(b"*2\r\n$3\r\nfoo\r\n$3\r\nbar\r\n");
        let val = parser.parse().unwrap().unwrap();
        assert_eq!(
            val,
            RespValue::Array(vec![
                RespValue::BulkString(b"foo".to_vec()),
                RespValue::BulkString(b"bar".to_vec()),
            ])
        );
    }

    /// 测试 Inline 命令解析：PING\r\n
    #[test]
    fn test_parse_inline() {
        let mut parser = RespParser::new();
        parser.extend(b"PING\r\n");
        let val = parser.parse().unwrap().unwrap();
        match val {
            RespValue::Array(items) => {
                assert_eq!(items.len(), 1);
                assert_eq!(items[0].to_string_lossy(), "PING");
            }
            _ => panic!("Expected array"),
        }
    }

    /// 测试增量解析（数据分两次到达）
    #[test]
    fn test_partial_parse() {
        let mut parser = RespParser::new();
        parser.extend(b"$6\r\nfoo");
        let val = parser.parse().unwrap();
        assert!(val.is_none()); // 数据不完整，返回 None
        parser.extend(b"bar\r\n");
        let val = parser.parse().unwrap().unwrap();
        assert_eq!(val, RespValue::BulkString(b"foobar".to_vec()));
    }

    /// 测试 RESP3 布尔值解析：#t\r\n
    #[test]
    fn test_resp3_boolean() {
        let mut parser = RespParser::new();
        parser.set_resp3(true);
        parser.extend(b"#t\r\n");
        let val = parser.parse().unwrap().unwrap();
        assert_eq!(val, RespValue::Boolean(true));
    }

    /// 测试 RESP3 浮点数解析：,3.14\r\n
    #[test]
    fn test_resp3_double() {
        let mut parser = RespParser::new();
        parser.set_resp3(true);
        parser.extend(b",3.14\r\n");
        let val = parser.parse().unwrap().unwrap();
        assert_eq!(val, RespValue::Double(3.14));
    }

    /// 测试编码-解码往返一致性（encode → parse 结果应等于原始值）
    #[test]
    fn test_encode_roundtrip() {
        let val = RespValue::Array(vec![
            RespValue::BulkString(b"SET".to_vec()),
            RespValue::BulkString(b"key".to_vec()),
            RespValue::BulkString(b"value".to_vec()),
        ]);
        let encoded = val.encode(false);
        let mut parser = RespParser::new();
        parser.extend(&encoded);
        let decoded = parser.parse().unwrap().unwrap();
        assert_eq!(val, decoded);
    }
}
