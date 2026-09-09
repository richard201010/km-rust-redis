//! 主从复制模块（Replication）
//!
//! 实现 Redis 风格的主从复制状态管理，包括角色切换、从节点跟踪和复制偏移量记录。
//!
//! # 设计要点
//!
//! - [`ReplicationState`] 维护当前节点的复制角色（Master/Slave）及从节点列表
//! - [`SlaveInfo`] 记录每个已连接从节点的状态（地址、偏移量、延迟等）
//! - 支持 `REPLICAOF host port` 设置为某主节点的从节点
//! - 支持 `REPLICAOF NO ONE` 提升为主节点
//! - `info_replication()` 生成与 Redis `INFO replication` 一致的输出格式
//!
//! # 简化说明
//!
//! 本模块仅实现角色管理和状态跟踪，不实现真正的 PSYNC/全量同步/增量同步。
//! REPLICAOF 命令更新内部状态并返回 OK。

use std::sync::atomic::{AtomicU64, Ordering};

// ---------------------------------------------------------------------------
// 角色与状态枚举
// ---------------------------------------------------------------------------

/// 节点角色 — 对应 Redis 的 `server.repl_state`
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Role {
    Master,
    Slave,
}

impl std::fmt::Display for Role {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Role::Master => write!(f, "master"),
            Role::Slave => write!(f, "slave"),
        }
    }
}

/// 主节点连接状态 — 对应 Redis 的 `master_link_status`
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MasterLinkStatus {
    Up,
    Down,
}

impl std::fmt::Display for MasterLinkStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            MasterLinkStatus::Up => write!(f, "up"),
            MasterLinkStatus::Down => write!(f, "down"),
        }
    }
}

/// 从节点连接状态 — 对应 Redis 的 slave 状态字段
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SlaveState {
    Online,
    WaitBgsave,
    SentHandshake,
}

impl std::fmt::Display for SlaveState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            SlaveState::Online => write!(f, "online"),
            SlaveState::WaitBgsave => write!(f, "wait_bgsave"),
            SlaveState::SentHandshake => write!(f, "sent_handshake"),
        }
    }
}

// ---------------------------------------------------------------------------
// SlaveInfo — 从节点信息
// ---------------------------------------------------------------------------

/// 已连接从节点信息。
///
/// 对应 Redis `INFO replication` 输出中的 `slave<N>` 字段。
#[derive(Debug, Clone)]
pub struct SlaveInfo {
    pub id: u64,
    pub addr: String,
    pub state: SlaveState,
    pub offset: u64,
    pub lag: u64,
}

// ---------------------------------------------------------------------------
// ReplicationState — 复制状态管理器
// ---------------------------------------------------------------------------

/// 复制状态管理器。
///
/// 维护当前节点的复制角色、主节点信息、从节点列表和复制偏移量。
/// 对应 Redis 源码中分散在 `server` 结构体中的多个复制相关字段。
pub struct ReplicationState {
    /// 当前角色
    pub role: Role,
    /// 主节点地址（仅 Slave 角色时有值）
    pub master_host: Option<String>,
    /// 主节点端口（仅 Slave 角色时有值）
    pub master_port: Option<u16>,
    /// 主节点连接状态（仅 Slave 角色时有意义）
    pub master_link_status: MasterLinkStatus,
    /// 已连接从节点列表（仅 Master 角色时有意义）
    pub connected_slaves: Vec<SlaveInfo>,
    /// 复制偏移量
    pub repl_offset: AtomicU64,
    /// 从节点 ID 自增计数器
    slave_id_counter: AtomicU64,
}

impl ReplicationState {
    /// 创建新的复制状态（默认为 Master 角色）。
    pub fn new() -> Self {
        Self {
            role: Role::Master,
            master_host: None,
            master_port: None,
            master_link_status: MasterLinkStatus::Down,
            connected_slaves: Vec::new(),
            repl_offset: AtomicU64::new(0),
            slave_id_counter: AtomicU64::new(0),
        }
    }

    /// 执行 REPLICAOF 命令。
    ///
    /// - `REPLICAOF host port` → 将当前节点设为指定主节点的从节点
    /// - `REPLICAOF NO ONE` → 提升当前节点为主节点，断开与原主节点的连接
    ///
    /// # 返回
    /// 始终返回 `Ok(())`，表示状态已更新。
    pub fn replicaof(&mut self, host: &str, port: u16) {
        self.role = Role::Slave;
        self.master_host = Some(host.to_string());
        self.master_port = Some(port);
        self.master_link_status = MasterLinkStatus::Up;
        // 清空从节点列表（从节点不再管理其他从节点）
        self.connected_slaves.clear();
        log::info!("REPLICAOF: now slave of {}:{}", host, port);
    }

    /// 执行 `REPLICAOF NO ONE`，提升当前节点为主节点。
    pub fn replicaof_no_one(&mut self) {
        self.role = Role::Master;
        self.master_host = None;
        self.master_port = None;
        self.master_link_status = MasterLinkStatus::Down;
        log::info!("REPLICAOF NO ONE: promoted to master");
    }

    /// 添加一个从节点到已连接列表（主节点调用）。
    ///
    /// # 返回
    /// 新从节点的 ID。
    pub fn add_slave(&mut self, addr: String) -> u64 {
        let id = self.slave_id_counter.fetch_add(1, Ordering::Relaxed);
        let slave = SlaveInfo {
            id,
            addr,
            state: SlaveState::SentHandshake,
            offset: 0,
            lag: 0,
        };
        self.connected_slaves.push(slave);
        id
    }

    /// 移除指定 ID 的从节点。
    ///
    /// # 返回
    /// `true` 表示成功移除，`false` 表示未找到。
    pub fn remove_slave(&mut self, id: u64) -> bool {
        let len_before = self.connected_slaves.len();
        self.connected_slaves.retain(|s| s.id != id);
        self.connected_slaves.len() < len_before
    }

    /// 推进复制偏移量（模拟写入传播）。
    pub fn advance_offset(&self, delta: u64) {
        self.repl_offset.fetch_add(delta, Ordering::Relaxed);
    }

    /// 获取当前复制偏移量。
    pub fn offset(&self) -> u64 {
        self.repl_offset.load(Ordering::Relaxed)
    }

    /// 生成 `INFO replication` 的输出内容。
    ///
    /// 格式与 Redis 的 `INFO replication` 输出一致，包含：
    /// - role / connected_slaves / repl_offset（主节点）
    /// - master_host / master_port / master_link_status（从节点）
    /// - slave<N> 条目（主节点的从节点列表）
    pub fn info_replication(&self) -> String {
        let mut info = String::new();

        info.push_str("# Replication\r\n");
        info.push_str(&format!("role:{}\r\n", self.role));
        info.push_str(&format!(
            "connected_slaves:{}\r\n",
            self.connected_slaves.len()
        ));
        info.push_str(&format!("repl_offset:{}\r\n", self.offset()));

        // 从节点特有字段
        if self.role == Role::Slave {
            info.push_str(&format!(
                "master_host:{}\r\n",
                self.master_host.as_deref().unwrap_or("")
            ));
            info.push_str(&format!(
                "master_port:{}\r\n",
                self.master_port.unwrap_or(0)
            ));
            info.push_str(&format!(
                "master_link_status:{}\r\n",
                self.master_link_status
            ));
        }

        // 主节点：列出所有已连接从节点
        for (i, slave) in self.connected_slaves.iter().enumerate() {
            info.push_str(&format!(
                "slave{}:ip={},port={},state={},offset={},lag={}\r\n",
                i, slave.addr, slave.id, slave.state, slave.offset, slave.lag
            ));
        }

        info.push_str("\r\n");
        info
    }
}

impl Default for ReplicationState {
    fn default() -> Self {
        Self::new()
    }
}

// ---------------------------------------------------------------------------
// REPLICAOF 命令处理函数（供 commands.rs 调用）
// ---------------------------------------------------------------------------

/// 解析并执行 REPLICAOF 命令参数。
///
/// - `REPLICAOF NO ONE` → 返回 `Ok(true)` 表示应调用 `replicaof_no_one()`
/// - `REPLICAOF host port` → 返回 `Ok((host, port))` 表示应调用 `replicaof(host, port)`
/// - 参数错误 → 返回 `Err(错误消息)`
///
/// 此函数仅做参数解析，实际状态更新由调用方（commands.rs 或服务端循环）完成，
/// 因为当前 `CmdCtx` 不持有 `ReplicationState` 引用。
pub fn parse_replicaof_args(args: &[Vec<u8>]) -> Result<ReplicaOfCmd, String> {
    if args.len() < 3 {
        return Err("ERR wrong number of arguments for 'replicaof' command".to_string());
    }

    let host = String::from_utf8_lossy(&args[1]);
    let port_str = String::from_utf8_lossy(&args[2]);

    if host.eq_ignore_ascii_case("no") && port_str.eq_ignore_ascii_case("one") {
        return Ok(ReplicaOfCmd::NoOne);
    }

    let port: u16 = port_str
        .parse()
        .map_err(|_| "ERR invalid port number".to_string())?;

    if port == 0 {
        return Err("ERR invalid port number".to_string());
    }

    Ok(ReplicaOfCmd::SetMaster {
        host: host.to_string(),
        port,
    })
}

/// REPLICAOF 命令解析结果。
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ReplicaOfCmd {
    /// `REPLICAOF NO ONE` — 提升为主节点
    NoOne,
    /// `REPLICAOF host port` — 设置主节点
    SetMaster { host: String, port: u16 },
}

// ---------------------------------------------------------------------------
// 单元测试
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_is_master() {
        let repl = ReplicationState::new();
        assert_eq!(repl.role, Role::Master);
        assert!(repl.master_host.is_none());
        assert!(repl.master_port.is_none());
        assert_eq!(repl.connected_slaves.len(), 0);
        assert_eq!(repl.offset(), 0);
    }

    #[test]
    fn test_replicaof_sets_slave_role() {
        let mut repl = ReplicationState::new();
        repl.replicaof("127.0.0.1", 6379);
        assert_eq!(repl.role, Role::Slave);
        assert_eq!(repl.master_host.as_deref(), Some("127.0.0.1"));
        assert_eq!(repl.master_port, Some(6379));
        assert_eq!(repl.master_link_status, MasterLinkStatus::Up);
    }

    #[test]
    fn test_replicaof_no_one_promotes_to_master() {
        let mut repl = ReplicationState::new();
        repl.replicaof("127.0.0.1", 6379);
        assert_eq!(repl.role, Role::Slave);

        repl.replicaof_no_one();
        assert_eq!(repl.role, Role::Master);
        assert!(repl.master_host.is_none());
        assert!(repl.master_port.is_none());
        assert_eq!(repl.master_link_status, MasterLinkStatus::Down);
    }

    #[test]
    fn test_replicaof_clears_slaves() {
        let mut repl = ReplicationState::new();
        repl.add_slave("192.168.1.10:6380".to_string());
        repl.add_slave("192.168.1.11:6380".to_string());
        assert_eq!(repl.connected_slaves.len(), 2);

        // 当设为从节点时，从节点列表应被清空
        repl.replicaof("127.0.0.1", 6379);
        assert_eq!(repl.connected_slaves.len(), 0);
    }

    #[test]
    fn test_add_and_remove_slave() {
        let mut repl = ReplicationState::new();
        let id1 = repl.add_slave("127.0.0.1:6381".to_string());
        let id2 = repl.add_slave("127.0.0.1:6382".to_string());
        assert_eq!(repl.connected_slaves.len(), 2);
        assert_ne!(id1, id2);

        assert!(repl.remove_slave(id1));
        assert_eq!(repl.connected_slaves.len(), 1);
        assert_eq!(repl.connected_slaves[0].id, id2);

        // 不存在的 ID
        assert!(!repl.remove_slave(999));
    }

    #[test]
    fn test_advance_offset() {
        let repl = ReplicationState::new();
        assert_eq!(repl.offset(), 0);

        repl.advance_offset(100);
        assert_eq!(repl.offset(), 100);

        repl.advance_offset(50);
        assert_eq!(repl.offset(), 150);
    }

    #[test]
    fn test_info_replication_master() {
        let mut repl = ReplicationState::new();
        repl.add_slave("192.168.1.10:6380".to_string());
        repl.advance_offset(42);

        let info = repl.info_replication();
        assert!(info.contains("role:master\r\n"));
        assert!(info.contains("connected_slaves:1\r\n"));
        assert!(info.contains("repl_offset:42\r\n"));
        assert!(info.contains("# Replication\r\n"));
        // 主节点不应包含 master_host 字段
        assert!(!info.contains("master_host:"));
    }

    #[test]
    fn test_info_replication_slave() {
        let mut repl = ReplicationState::new();
        repl.replicaof("10.0.0.1", 6379);

        let info = repl.info_replication();
        assert!(info.contains("role:slave\r\n"));
        assert!(info.contains("master_host:10.0.0.1\r\n"));
        assert!(info.contains("master_port:6379\r\n"));
        assert!(info.contains("master_link_status:up\r\n"));
        // 从节点不应列出 slave 条目
        assert!(!info.contains("slave0:"));
    }

    #[test]
    fn test_info_replication_format() {
        let repl = ReplicationState::new();
        let info = repl.info_replication();
        // 应以 # Replication 开头
        assert!(info.starts_with("# Replication\r\n"));
        // 应以空行结尾
        assert!(info.ends_with("\r\n\r\n"));
    }

    #[test]
    fn test_parse_replicaof_no_one() {
        let args: Vec<Vec<u8>> = vec![b"REPLICAOF".to_vec(), b"NO".to_vec(), b"ONE".to_vec()];
        let result = parse_replicaof_args(&args).unwrap();
        assert_eq!(result, ReplicaOfCmd::NoOne);
    }

    #[test]
    fn test_parse_replicaof_no_one_case_insensitive() {
        let args: Vec<Vec<u8>> = vec![b"replicaof".to_vec(), b"no".to_vec(), b"one".to_vec()];
        let result = parse_replicaof_args(&args).unwrap();
        assert_eq!(result, ReplicaOfCmd::NoOne);
    }

    #[test]
    fn test_parse_replicaof_set_master() {
        let args: Vec<Vec<u8>> = vec![
            b"REPLICAOF".to_vec(),
            b"127.0.0.1".to_vec(),
            b"6379".to_vec(),
        ];
        let result = parse_replicaof_args(&args).unwrap();
        assert_eq!(
            result,
            ReplicaOfCmd::SetMaster {
                host: "127.0.0.1".to_string(),
                port: 6379,
            }
        );
    }

    #[test]
    fn test_parse_replicaof_invalid_port() {
        let args: Vec<Vec<u8>> = vec![
            b"REPLICAOF".to_vec(),
            b"127.0.0.1".to_vec(),
            b"notaport".to_vec(),
        ];
        assert!(parse_replicaof_args(&args).is_err());
    }

    #[test]
    fn test_parse_replicaof_zero_port() {
        let args: Vec<Vec<u8>> = vec![b"REPLICAOF".to_vec(), b"127.0.0.1".to_vec(), b"0".to_vec()];
        assert!(parse_replicaof_args(&args).is_err());
    }

    #[test]
    fn test_parse_replicaof_too_few_args() {
        let args: Vec<Vec<u8>> = vec![b"REPLICAOF".to_vec()];
        assert!(parse_replicaof_args(&args).is_err());
    }

    #[test]
    fn test_role_display() {
        assert_eq!(format!("{}", Role::Master), "master");
        assert_eq!(format!("{}", Role::Slave), "slave");
    }

    #[test]
    fn test_master_link_status_display() {
        assert_eq!(format!("{}", MasterLinkStatus::Up), "up");
        assert_eq!(format!("{}", MasterLinkStatus::Down), "down");
    }

    #[test]
    fn test_slave_state_display() {
        assert_eq!(format!("{}", SlaveState::Online), "online");
        assert_eq!(format!("{}", SlaveState::WaitBgsave), "wait_bgsave");
        assert_eq!(format!("{}", SlaveState::SentHandshake), "sent_handshake");
    }

    #[test]
    fn test_full_lifecycle() {
        let mut repl = ReplicationState::new();
        assert_eq!(repl.role, Role::Master);

        // 添加从节点
        let s1 = repl.add_slave("10.0.0.2:6380".to_string());
        let s2 = repl.add_slave("10.0.0.3:6380".to_string());
        repl.advance_offset(1000);
        assert_eq!(repl.connected_slaves.len(), 2);
        assert_eq!(repl.offset(), 1000);

        // 降级为从节点
        repl.replicaof("10.0.0.1", 6379);
        assert_eq!(repl.role, Role::Slave);
        assert_eq!(repl.connected_slaves.len(), 0); // 从节点列表被清空

        // 提升回主节点
        repl.replicaof_no_one();
        assert_eq!(repl.role, Role::Master);
        assert!(repl.connected_slaves.is_empty());
        // offset 在 replicaof 时没有重置，这与 Redis 行为一致
        assert_eq!(repl.offset(), 1000);
    }
}
