//! AOF (Append-Only File) 持久化模块
//!
//! 实现 Redis 风格的 AOF 持久化，将每个写命令以 RESP 格式追加到文件中。
//! 启动时可从 AOF 文件同步读取、解析并重放命令以恢复数据。
//!
//! # 设计要点
//!
//! - [`AofWriter`] 持有 `tokio::io::BufWriter<File>` 异步文件句柄
//! - `open(path)` 打开/创建 AOF 文件（追加模式）
//! - `write_command(argv)` 将命令以 RESP 数组格式追加到文件
//! - `load_aof(path)` 同步读取并解析 AOF 文件中的所有命令
//! - 每次 SET/DEL/EXPIRE 等写命令执行后调用 `write_command`
//!
//! # RESP 格式
//!
//! 命令以 RESP 数组格式存储：
//! ```text
//! *<argc>\r\n
//! $<len1>\r\n<arg1>\r\n
//! $<len2>\r\n<arg2>\r\n
//! ...
//! ```

use std::path::Path;
use tokio::fs::{File, OpenOptions};
use tokio::io::{AsyncWriteExt, BufWriter};

/// AOF 写入器
///
/// 持有异步 BufWriter 文件句柄，将写命令以 RESP 格式追加到 AOF 文件。
/// 通过 `Arc<Mutex<AofWriter>>` 在多个 tokio 任务间共享。
pub struct AofWriter {
    writer: BufWriter<File>,
}

impl AofWriter {
    /// 打开或创建 AOF 文件（追加模式）
    ///
    /// 如果文件存在则在末尾追加写入，不存在则创建新文件。
    /// 对应 Redis 的 `openAppendOnlyFile()` 函数。
    pub async fn open(path: &Path) -> std::io::Result<Self> {
        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(path)
            .await?;
        Ok(Self {
            writer: BufWriter::new(file),
        })
    }

    /// 将命令以 RESP 数组格式追加到 AOF 文件
    ///
    /// 编码格式：`*<argc>\r\n$<len>\r\n<arg>\r\n...`
    /// 写入后立即 flush，确保数据落盘。
    ///
    /// # 参数
    /// - `argv`: 命令参数列表，`argv[0]` 为命令名，后续为参数
    pub async fn write_command(&mut self, argv: &[Vec<u8>]) -> std::io::Result<()> {
        let mut buf = Vec::new();
        // RESP 数组头：*<argc>\r\n
        buf.extend_from_slice(b"*");
        buf.extend_from_slice(argv.len().to_string().as_bytes());
        buf.extend_from_slice(b"\r\n");
        // 每个参数编码为 BulkString：$<len>\r\n<data>\r\n
        for arg in argv {
            buf.extend_from_slice(b"$");
            buf.extend_from_slice(arg.len().to_string().as_bytes());
            buf.extend_from_slice(b"\r\n");
            buf.extend_from_slice(arg);
            buf.extend_from_slice(b"\r\n");
        }
        self.writer.write_all(&buf).await?;
        self.writer.flush().await?;
        Ok(())
    }

    /// 同步加载 AOF 文件并返回解析出的命令列表
    ///
    /// 简化版实现：同步读取整个文件，逐条解析 RESP 数组格式命令。
    /// 用于服务器启动时回放 AOF 以恢复数据库状态。
    ///
    /// # 返回
    /// - `Ok(Vec<Vec<Vec<u8>>>)` — 解析出的命令列表，每个元素是一条命令的 argv
    /// - `Err(...)` — 文件不存在返回空列表，其他 IO/解析错误返回 Err
    pub fn load_aof(path: &Path) -> std::io::Result<Vec<Vec<Vec<u8>>>> {
        use std::fs;
        use std::io::Read;

        let mut file = match fs::File::open(path) {
            Ok(f) => f,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(vec![]),
            Err(e) => return Err(e),
        };

        let mut data = Vec::new();
        file.read_to_end(&mut data)?;

        let mut commands = Vec::new();
        let mut pos = 0;

        while pos < data.len() {
            // 期望 '*'
            if data[pos] != b'*' {
                break;
            }
            pos += 1;

            // 读取数组长度（元素数量）
            let argc_end = find_crlf(&data, pos)?;
            let argc: usize = std::str::from_utf8(&data[pos..argc_end])
                .map_err(|_| {
                    std::io::Error::new(std::io::ErrorKind::InvalidData, "invalid argc encoding")
                })?
                .parse()
                .map_err(|_| {
                    std::io::Error::new(std::io::ErrorKind::InvalidData, "invalid argc value")
                })?;
            pos = argc_end + 2;

            let mut argv = Vec::with_capacity(argc);
            for _ in 0..argc {
                // 期望 '$'
                if pos >= data.len() || data[pos] != b'$' {
                    return Err(std::io::Error::new(
                        std::io::ErrorKind::InvalidData,
                        "expected '$' for bulk string",
                    ));
                }
                pos += 1;

                // 读取 BulkString 长度
                let len_end = find_crlf(&data, pos)?;
                let len: usize = std::str::from_utf8(&data[pos..len_end])
                    .map_err(|_| {
                        std::io::Error::new(
                            std::io::ErrorKind::InvalidData,
                            "invalid bulk string length encoding",
                        )
                    })?
                    .parse()
                    .map_err(|_| {
                        std::io::Error::new(
                            std::io::ErrorKind::InvalidData,
                            "invalid bulk string length value",
                        )
                    })?;
                pos = len_end + 2;

                // 读取 BulkString 数据 + 验证结尾 \r\n
                if pos + len + 2 > data.len() {
                    return Err(std::io::Error::new(
                        std::io::ErrorKind::InvalidData,
                        "truncated bulk string data",
                    ));
                }
                argv.push(data[pos..pos + len].to_vec());
                pos += len + 2; // 跳过数据 + \r\n
            }

            commands.push(argv);
        }

        Ok(commands)
    }
}

/// 在字节切片中查找 \r\n (CRLF)，返回 \r 的索引位置
///
/// 用于 RESP 协议的行终止符定位，与 resp.rs 中的 `find_crlf` 功能一致。
fn find_crlf(data: &[u8], start: usize) -> std::io::Result<usize> {
    for i in start..data.len().saturating_sub(1) {
        if data[i] == b'\r' && data[i + 1] == b'\n' {
            return Ok(i);
        }
    }
    Err(std::io::Error::new(
        std::io::ErrorKind::InvalidData,
        "CRLF not found in AOF data",
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_load_aof_parse_single_command() {
        // 构造 RESP 格式的 SET key value 命令
        let resp = b"*3\r\n$3\r\nSET\r\n$3\r\nkey\r\n$5\r\nvalue\r\n";
        let commands = parse_aof_bytes(resp).unwrap();
        assert_eq!(commands.len(), 1);
        assert_eq!(commands[0][0], b"SET");
        assert_eq!(commands[0][1], b"key");
        assert_eq!(commands[0][2], b"value");
    }

    #[test]
    fn test_load_aof_parse_multiple_commands() {
        let resp = b"*2\r\n$3\r\nDEL\r\n$3\r\nfoo\r\n*3\r\n$3\r\nSET\r\n$3\r\nbar\r\n$3\r\nbaz\r\n";
        let commands = parse_aof_bytes(resp).unwrap();
        assert_eq!(commands.len(), 2);
        assert_eq!(commands[0][0], b"DEL");
        assert_eq!(commands[0][1], b"foo");
        assert_eq!(commands[1][0], b"SET");
        assert_eq!(commands[1][1], b"bar");
        assert_eq!(commands[1][2], b"baz");
    }

    #[test]
    fn test_load_aof_empty_file() {
        let commands = parse_aof_bytes(b"").unwrap();
        assert!(commands.is_empty());
    }

    /// 辅助函数：从字节切片解析 AOF 命令（避免需要实际文件）
    fn parse_aof_bytes(data: &[u8]) -> std::io::Result<Vec<Vec<Vec<u8>>>> {
        let mut commands = Vec::new();
        let mut pos = 0;

        while pos < data.len() {
            if data[pos] != b'*' {
                break;
            }
            pos += 1;

            let argc_end = find_crlf(&data, pos)?;
            let argc: usize = std::str::from_utf8(&data[pos..argc_end])
                .unwrap()
                .parse()
                .unwrap();
            pos = argc_end + 2;

            let mut argv = Vec::with_capacity(argc);
            for _ in 0..argc {
                assert!(pos < data.len() && data[pos] == b'$');
                pos += 1;
                let len_end = find_crlf(&data, pos)?;
                let len: usize = std::str::from_utf8(&data[pos..len_end])
                    .unwrap()
                    .parse()
                    .unwrap();
                pos = len_end + 2;
                argv.push(data[pos..pos + len].to_vec());
                pos += len + 2;
            }
            commands.push(argv);
        }

        Ok(commands)
    }
}
