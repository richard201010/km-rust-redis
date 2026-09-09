//! Sentinel 哨兵监控模块
//!
//! 实现 Redis Sentinel 风格的高可用监控功能。管理被监控主节点的状态，
//! 支持主观下线（SDOWN）和客观下线（ODOWN）判定，提供 SENTINEL 命令集。
//!
//! # 设计要点
//!
//! - [`Sentinel`] 维护 `HashMap<String, MasterMonitor>` 被监控主节点映射
//! - [`MasterMonitor`] 跟踪单个主节点的健康状态和从节点列表
//! - [`SlaveMonitor`] 跟踪从节点的基本状态
//! - 提供 `MasterStatus` 结构体用于序列化输出（INFO sentinel / SENTINEL masters）
//!
//! # 故障检测
//!
//! - **主观下线 (SDOWN)**: 单个 Sentinel 实例判定节点不可达（超过 `down_after_ms` 无 PONG 响应）
//! - **客观下线 (ODOWN)**: 达到法定人数（quorum）的 Sentinel 实例均判定 SDOWN
//!
//! # 简化说明
//!
//! 本模块仅实现状态管理和查询接口，不包含：
//! - 真正的故障检测心跳机制
//! - 自动故障转移（failover）流程
//! - Sentinel 实例间的 gossip 协议通信
//! - 配置纪元（config epoch）和领导者选举

use std::collections::HashMap;

use crate::resp::RespValue;
use crate::types::current_time_ms;

/// Sentinel 哨兵实例
///
/// 管理所有被监控的 Redis 主节点，维护全局配置参数。
/// 对应 Redis Sentinel 的核心状态机。
pub struct Sentinel {
    /// 主节点名称 → 监控状态
    monitor_masters: HashMap<String, MasterMonitor>,
    /// 法定人数：判定客观下线所需的最大 Sentinel 同意数
    quorum: u32,
    /// 主观下线阈值（毫秒）：超过此时间未收到 PONG 响应则判定 SDOWN
    down_after_ms: u64,
    /// 故障转移超时时间（毫秒）
    failover_timeout_ms: u64,
}

/// 主节点监控状态
///
/// 跟踪单个 Redis 主节点的健康信息、下线状态和从节点列表。
pub struct MasterMonitor {
    /// 主节点名称（配置时指定的逻辑名称）
    name: String,
    /// 主节点地址（host:port 格式）
    addr: String,
    /// 是否主观下线（Subjectively Down）
    is_sdown: bool,
    /// 是否客观下线（Objectively Down）
    is_odown: bool,
    /// 其他 Sentinel 实例数量（用于 ODOWN 判定）
    num_other_sentinels: u32,
    /// 从节点监控列表
    slaves: Vec<SlaveMonitor>,
    /// 最近一次收到 PONG 响应的时间戳（毫秒）
    last_pong: u64,
    /// 最近一次确认正常的时间戳（毫秒）
    last_ok: u64,
}

/// 从节点监控状态
///
/// 跟踪单个 Redis 从节点的基本健康信息。
pub struct SlaveMonitor {
    /// 从节点名称
    name: String,
    /// 从节点地址（host:port 格式）
    addr: String,
    /// 是否主观下线
    is_sdown: bool,
    /// 最近一次收到 PONG 响应的时间戳（毫秒）
    last_pong: u64,
}

/// 主节点状态快照
///
/// 用于 SENTINEL masters / INFO sentinel 命令输出的可序列化数据结构。
/// 包含某个时间点的完整主节点状态信息。
#[derive(Debug, Clone, PartialEq)]
pub struct MasterStatus {
    /// 主节点名称
    pub name: String,
    /// 主节点地址
    pub addr: String,
    /// 是否主观下线
    pub is_sdown: bool,
    /// 是否客观下线
    pub is_odown: bool,
    /// 从节点数量
    pub num_slaves: usize,
    /// 其他 Sentinel 实例数量
    pub num_other_sentinels: u32,
    /// 距上次正常响应的毫秒数
    pub time_since_last_pong: u64,
    /// 距上次确认正常的毫秒数
    pub time_since_last_ok: u64,
}

impl Sentinel {
    /// 创建新的 Sentinel 实例
    ///
    /// # 参数
    /// - `quorum`: 法定人数，判定客观下线所需的最小 Sentinel 同意数
    /// - `down_after_ms`: 主观下线阈值（毫秒），超过此时间无响应则判定 SDOWN
    ///
    /// # 默认值
    /// - `failover_timeout_ms`: 60000ms（60秒）
    pub fn new(quorum: u32, down_after_ms: u64) -> Self {
        Self {
            monitor_masters: HashMap::new(),
            quorum,
            down_after_ms,
            failover_timeout_ms: 60_000,
        }
    }

    /// 添加监控的主节点
    ///
    /// 注册一个新的主节点到监控列表。如果同名主节点已存在，更新其地址。
    ///
    /// # 参数
    /// - `name`: 主节点逻辑名称（如 "mymaster"）
    /// - `addr`: 主节点地址（如 "127.0.0.1:6379"）
    pub fn monitor_master(&mut self, name: &str, addr: &str) {
        let now = current_time_ms();
        let monitor = MasterMonitor {
            name: name.to_string(),
            addr: addr.to_string(),
            is_sdown: false,
            is_odown: false,
            num_other_sentinels: 0,
            slaves: Vec::new(),
            last_pong: now,
            last_ok: now,
        };
        self.monitor_masters.insert(name.to_string(), monitor);
    }

    /// 检查主节点状态
    ///
    /// 根据当前时间与最近 PONG 响应的时间差，更新主观下线状态。
    /// 返回主节点的当前状态快照。
    ///
    /// # 参数
    /// - `name`: 主节点名称
    ///
    /// # 返回
    /// `Some(MasterStatus)` 如果主节点存在，`None` 如果不存在
    pub fn check_master(&mut self, name: &str) -> Option<MasterStatus> {
        let now = current_time_ms();
        if let Some(master) = self.monitor_masters.get_mut(name) {
            // 更新主观下线状态：超过阈值未收到 PONG 则判定 SDOWN
            let elapsed = now.saturating_sub(master.last_pong);
            master.is_sdown = elapsed >= self.down_after_ms;

            // 更新客观下线状态：SDOWN 且其他 Sentinel 数量达到法定人数
            if master.is_sdown {
                master.is_odown = master.num_other_sentinels + 1 >= self.quorum;
            } else {
                master.is_odown = false;
            }

            Some(MasterStatus {
                name: master.name.clone(),
                addr: master.addr.clone(),
                is_sdown: master.is_sdown,
                is_odown: master.is_odown,
                num_slaves: master.slaves.len(),
                num_other_sentinels: master.num_other_sentinels,
                time_since_last_pong: now.saturating_sub(master.last_pong),
                time_since_last_ok: now.saturating_sub(master.last_ok),
            })
        } else {
            None
        }
    }

    /// 获取所有被监控主节点的状态
    ///
    /// 返回所有已注册主节点的当前状态快照列表。
    /// 对应 `SENTINEL masters` 命令和 `INFO sentinel` 输出。
    pub fn master_status(&mut self) -> Vec<MasterStatus> {
        let names: Vec<String> = self.monitor_masters.keys().cloned().collect();
        names
            .iter()
            .filter_map(|name| self.check_master(name))
            .collect()
    }

    /// 获取主节点地址
    ///
    /// 返回指定主节点的当前地址（host:port）。
    /// 对应 `SENTINEL get-master-addr-by-name <name>` 命令。
    pub fn get_master_addr_by_name(&self, name: &str) -> Option<(&str, &str)> {
        self.monitor_masters
            .get(name)
            .map(|m| (m.name.as_str(), m.addr.as_str()))
    }

    /// 查询主节点是否下线
    ///
    /// 检查指定主节点当前的客观下线状态。
    /// 对应 `SENTINEL is-master-down-by-addr` 命令。
    pub fn is_master_down_by_addr(&mut self, name: &str) -> Option<bool> {
        self.check_master(name).map(|status| status.is_odown)
    }

    /// 添加从节点到指定主节点
    ///
    /// 为已注册的主节点添加一个从节点监控。
    ///
    /// # 参数
    /// - `master_name`: 主节点名称
    /// - `slave_name`: 从节点名称
    /// - `slave_addr`: 从节点地址（host:port）
    pub fn add_slave(&mut self, master_name: &str, slave_name: &str, slave_addr: &str) {
        if let Some(master) = self.monitor_masters.get_mut(master_name) {
            let now = current_time_ms();
            master.slaves.push(SlaveMonitor {
                name: slave_name.to_string(),
                addr: slave_addr.to_string(),
                is_sdown: false,
                last_pong: now,
            });
        }
    }

    /// 更新主节点的 PONG 时间戳
    ///
    /// 当收到主节点的 PONG 响应时调用，重置下线计时器。
    pub fn master_pong(&mut self, name: &str) {
        if let Some(master) = self.monitor_masters.get_mut(name) {
            let now = current_time_ms();
            master.last_pong = now;
            master.last_ok = now;
            master.is_sdown = false;
            master.is_odown = false;
        }
    }

    /// 设置其他 Sentinel 实例数量
    ///
    /// 用于模拟或配置其他 Sentinel 实例的数量，影响 ODOWN 判定。
    pub fn set_num_other_sentinels(&mut self, name: &str, count: u32) {
        if let Some(master) = self.monitor_masters.get_mut(name) {
            master.num_other_sentinels = count;
        }
    }

    /// 获取法定人数配置
    pub fn quorum(&self) -> u32 {
        self.quorum
    }

    /// 获取主观下线阈值配置
    pub fn down_after_ms(&self) -> u64 {
        self.down_after_ms
    }

    /// 生成 INFO sentinel 格式输出
    ///
    /// 返回 Redis INFO sentinel 风格的文本信息，包含：
    /// - Sentinel 全局配置
    /// - 每个被监控主节点的详细状态
    pub fn info_sentinel(&mut self) -> String {
        let mut info = String::new();

        // Sentinel 全局信息
        info.push_str("# Sentinel\r\n");
        info.push_str(&format!(
            "sentinel_masters:{}\r\n",
            self.monitor_masters.len()
        ));
        info.push_str(&format!("sentinel_tilt:0\r\n"));
        info.push_str(&format!("sentinel_running_scripts:0\r\n"));
        info.push_str(&format!("sentinel_scripts_queue_length:0\r\n"));
        info.push_str(&format!("sentinel_simulate_failure_flags:0\r\n"));
        info.push_str(&format!("quorum:{}\r\n", self.quorum));
        info.push_str(&format!("down_after_ms:{}\r\n", self.down_after_ms));
        info.push_str(&format!(
            "failover_timeout:{}\r\n",
            self.failover_timeout_ms
        ));

        // 每个主节点的状态信息
        let masters = self.master_status();
        for m in &masters {
            info.push_str(&format!(
                "master{name}:name={name},addr={addr},is_sdown={is_sdown},is_odown={is_odown},slaves={slaves},sentinels={sentinels},last_pong_ms={last_pong_ms},last_ok_ms={last_ok_ms}\r\n",
                name = m.name,
                addr = m.addr,
                is_sdown = m.is_sdown as i32,
                is_odown = m.is_odown as i32,
                slaves = m.num_slaves,
                sentinels = m.num_other_sentinels,
                last_pong_ms = m.time_since_last_pong,
                last_ok_ms = m.time_since_last_ok,
            ));
        }

        info
    }

    /// 处理 SENTINEL 子命令
    ///
    /// 统一入口，根据子命令名称分发到对应的处理逻辑。
    ///
    /// # 支持的子命令
    ///
    /// - `SENTINEL monitor <name> <addr> <quorum>` → 添加监控主节点
    /// - `SENTINEL masters` → 返回所有主节点状态
    /// - `SENTINEL get-master-addr-by-name <name>` → 返回主节点地址
    /// - `SENTINEL is-master-down-by-addr <addr>` → 查询主节点是否下线
    /// - `SENTINEL slaves <name>` → 返回从节点列表
    /// - `SENTINEL info` → 返回 INFO sentinel 格式信息
    ///
    /// # 参数
    /// - `argv`: 完整的命令参数列表（argv[0] = "SENTINEL", argv[1] = 子命令, ...）
    ///
    /// # 返回
    /// RESP 协议格式的命令执行结果
    pub fn handle_sentinel_command(&mut self, argv: &[Vec<u8>]) -> RespValue {
        if argv.len() < 2 {
            return RespValue::err("ERR wrong number of arguments for 'sentinel' command");
        }

        let subcmd = match std::str::from_utf8(&argv[1]) {
            Ok(s) => s.to_ascii_lowercase(),
            Err(_) => return RespValue::err("ERR invalid subcommand"),
        };

        match subcmd.as_str() {
            // SENTINEL monitor <name> <addr> <quorum>
            "monitor" => {
                if argv.len() < 5 {
                    return RespValue::err(
                        "ERR wrong number of arguments for 'sentinel|monitor' command",
                    );
                }
                let name = match std::str::from_utf8(&argv[2]) {
                    Ok(s) => s,
                    Err(_) => return RespValue::err("ERR invalid master name"),
                };
                let addr = match std::str::from_utf8(&argv[3]) {
                    Ok(s) => s,
                    Err(_) => return RespValue::err("ERR invalid address"),
                };
                self.monitor_master(name, addr);
                RespValue::ok()
            }

            // SENTINEL masters
            "masters" => {
                let masters = self.master_status();
                let mut result = Vec::new();
                for m in &masters {
                    let mut master_info = Vec::new();
                    master_info.push(RespValue::bulk("name"));
                    master_info.push(RespValue::bulk(m.name.clone()));
                    master_info.push(RespValue::bulk("ip"));
                    // 拆分 addr 为 ip:port
                    let parts: Vec<&str> = m.addr.splitn(2, ':').collect();
                    let ip = parts.first().unwrap_or(&"");
                    let port = parts.get(1).unwrap_or(&"0");
                    master_info.push(RespValue::bulk(ip.to_string()));
                    master_info.push(RespValue::bulk("port"));
                    master_info.push(RespValue::bulk(port.to_string()));
                    master_info.push(RespValue::bulk("runid"));
                    master_info.push(RespValue::bulk(""));
                    master_info.push(RespValue::bulk("flags"));
                    let flags = if m.is_odown {
                        "s_down,o_down,sentinel"
                    } else if m.is_sdown {
                        "s_down,sentinel"
                    } else {
                        "sentinel"
                    };
                    master_info.push(RespValue::bulk(flags));
                    master_info.push(RespValue::bulk("link-pending-commands"));
                    master_info.push(RespValue::bulk("0"));
                    master_info.push(RespValue::bulk("link-refcount"));
                    master_info.push(RespValue::bulk("1"));
                    master_info.push(RespValue::bulk("last-ping-sent"));
                    master_info.push(RespValue::bulk(m.time_since_last_pong.to_string()));
                    master_info.push(RespValue::bulk("last-ok-ping-reply"));
                    master_info.push(RespValue::bulk(m.time_since_last_ok.to_string()));
                    master_info.push(RespValue::bulk("last-ping-reply"));
                    master_info.push(RespValue::bulk(m.time_since_last_pong.to_string()));
                    master_info.push(RespValue::bulk("s-down-time"));
                    let sdown_time = if m.is_sdown {
                        m.time_since_last_pong.to_string()
                    } else {
                        "0".to_string()
                    };
                    master_info.push(RespValue::bulk(sdown_time));
                    master_info.push(RespValue::bulk("num-slaves"));
                    master_info.push(RespValue::bulk(m.num_slaves.to_string()));
                    master_info.push(RespValue::bulk("num-other-sentinels"));
                    master_info.push(RespValue::bulk(m.num_other_sentinels.to_string()));
                    master_info.push(RespValue::bulk("quorum"));
                    master_info.push(RespValue::bulk(self.quorum.to_string()));
                    master_info.push(RespValue::bulk("failover-timeout"));
                    master_info.push(RespValue::bulk(self.failover_timeout_ms.to_string()));
                    master_info.push(RespValue::bulk("down-after-milliseconds"));
                    master_info.push(RespValue::bulk(self.down_after_ms.to_string()));
                    result.push(RespValue::Array(master_info));
                }
                RespValue::Array(result)
            }

            // SENTINEL get-master-addr-by-name <name>
            "get-master-addr-by-name" => {
                if argv.len() < 3 {
                    return RespValue::err(
                        "ERR wrong number of arguments for 'sentinel|get-master-addr-by-name' command",
                    );
                }
                let name = match std::str::from_utf8(&argv[2]) {
                    Ok(s) => s,
                    Err(_) => return RespValue::err("ERR invalid master name"),
                };
                match self.get_master_addr_by_name(name) {
                    Some((_name, addr)) => {
                        let parts: Vec<&str> = addr.splitn(2, ':').collect();
                        let ip = parts.first().unwrap_or(&"").to_string();
                        let port = parts.get(1).unwrap_or(&"0").to_string();
                        RespValue::Array(vec![RespValue::bulk(ip), RespValue::bulk(port)])
                    }
                    None => RespValue::null(),
                }
            }

            // SENTINEL is-master-down-by-addr <addr>
            "is-master-down-by-addr" => {
                // 简化实现：通过遍历查找匹配地址的主节点
                let target_addr = if argv.len() >= 3 {
                    match std::str::from_utf8(&argv[2]) {
                        Ok(s) => s,
                        Err(_) => return RespValue::err("ERR invalid address"),
                    }
                } else {
                    return RespValue::err(
                        "ERR wrong number of arguments for 'sentinel|is-master-down-by-addr' command",
                    );
                };

                // 查找匹配地址的主节点
                let master_name = self
                    .monitor_masters
                    .iter()
                    .find(|(_, m)| m.addr == target_addr)
                    .map(|(name, _)| name.clone());

                match master_name {
                    Some(name) => {
                        let is_down = self.is_master_down_by_addr(&name).unwrap_or(false);
                        RespValue::Array(vec![
                            RespValue::integer(if is_down { 1 } else { 0 }),
                            RespValue::bulk(""), // leader 选举信息（简化实现为空）
                        ])
                    }
                    None => RespValue::Array(vec![RespValue::integer(0), RespValue::bulk("")]),
                }
            }

            // SENTINEL slaves <name>
            "slaves" => {
                if argv.len() < 3 {
                    return RespValue::err(
                        "ERR wrong number of arguments for 'sentinel|slaves' command",
                    );
                }
                let name = match std::str::from_utf8(&argv[2]) {
                    Ok(s) => s,
                    Err(_) => return RespValue::err("ERR invalid master name"),
                };
                if let Some(master) = self.monitor_masters.get(name) {
                    let mut result = Vec::new();
                    for slave in &master.slaves {
                        let mut slave_info = Vec::new();
                        slave_info.push(RespValue::bulk("name"));
                        slave_info.push(RespValue::bulk(slave.name.clone()));
                        let parts: Vec<&str> = slave.addr.splitn(2, ':').collect();
                        let ip = parts.first().unwrap_or(&"");
                        let port = parts.get(1).unwrap_or(&"0");
                        slave_info.push(RespValue::bulk("ip"));
                        slave_info.push(RespValue::bulk(ip.to_string()));
                        slave_info.push(RespValue::bulk("port"));
                        slave_info.push(RespValue::bulk(port.to_string()));
                        slave_info.push(RespValue::bulk("flags"));
                        let flags = if slave.is_sdown {
                            "s_down,slave"
                        } else {
                            "slave"
                        };
                        slave_info.push(RespValue::bulk(flags));
                        result.push(RespValue::Array(slave_info));
                    }
                    RespValue::Array(result)
                } else {
                    RespValue::Array(vec![])
                }
            }

            // SENTINEL info — 返回 INFO sentinel 格式
            "info" => RespValue::bulk(self.info_sentinel()),

            _ => RespValue::err(&format!("ERR unknown sentinel subcommand '{}'", subcmd)),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_sentinel_new() {
        let sentinel = Sentinel::new(2, 30_000);
        assert_eq!(sentinel.quorum(), 2);
        assert_eq!(sentinel.down_after_ms(), 30_000);
    }

    #[test]
    fn test_monitor_master() {
        let mut sentinel = Sentinel::new(2, 30_000);
        sentinel.monitor_master("mymaster", "127.0.0.1:6379");

        let masters = sentinel.master_status();
        assert_eq!(masters.len(), 1);
        assert_eq!(masters[0].name, "mymaster");
        assert_eq!(masters[0].addr, "127.0.0.1:6379");
        assert!(!masters[0].is_sdown);
        assert!(!masters[0].is_odown);
    }

    #[test]
    fn test_monitor_multiple_masters() {
        let mut sentinel = Sentinel::new(2, 30_000);
        sentinel.monitor_master("master1", "127.0.0.1:6379");
        sentinel.monitor_master("master2", "127.0.0.1:6380");

        let masters = sentinel.master_status();
        assert_eq!(masters.len(), 2);
    }

    #[test]
    fn test_get_master_addr() {
        let mut sentinel = Sentinel::new(2, 30_000);
        sentinel.monitor_master("mymaster", "192.168.1.100:6379");

        let result = sentinel.get_master_addr_by_name("mymaster");
        assert!(result.is_some());
        let (name, addr) = result.unwrap();
        assert_eq!(name, "mymaster");
        assert_eq!(addr, "192.168.1.100:6379");

        assert!(sentinel.get_master_addr_by_name("nonexistent").is_none());
    }

    #[test]
    fn test_check_master_not_sdown() {
        let mut sentinel = Sentinel::new(2, 30_000);
        sentinel.monitor_master("mymaster", "127.0.0.1:6379");

        // 刚注册的主节点不应该是 SDOWN
        let status = sentinel.check_master("mymaster");
        assert!(status.is_some());
        let status = status.unwrap();
        assert!(!status.is_sdown);
        assert!(!status.is_odown);
    }

    #[test]
    fn test_check_master_nonexistent() {
        let mut sentinel = Sentinel::new(2, 30_000);
        assert!(sentinel.check_master("nonexistent").is_none());
    }

    #[test]
    fn test_master_pong_resets_sdown() {
        let mut sentinel = Sentinel::new(2, 30_000);
        sentinel.monitor_master("mymaster", "127.0.0.1:6379");

        // 手动设置 SDOWN 状态
        if let Some(master) = sentinel.monitor_masters.get_mut("mymaster") {
            master.is_sdown = true;
            master.is_odown = true;
        }

        // 发送 PONG 应重置状态
        sentinel.master_pong("mymaster");

        let status = sentinel.check_master("mymaster").unwrap();
        assert!(!status.is_sdown);
        assert!(!status.is_odown);
    }

    #[test]
    fn test_odown_requires_quorum() {
        let mut sentinel = Sentinel::new(3, 30_000);
        sentinel.monitor_master("mymaster", "127.0.0.1:6379");

        // 设置 SDOWN 但其他 Sentinel 数量不足
        if let Some(master) = sentinel.monitor_masters.get_mut("mymaster") {
            master.is_sdown = true;
            master.last_pong = 0; // 触发 SDOWN 判定
            master.num_other_sentinels = 1; // quorum=3, 需要 3 个，目前只有 1+1=2
        }

        let status = sentinel.check_master("mymaster").unwrap();
        assert!(status.is_sdown);
        // quorum=3, num_other_sentinels+1 = 2 < 3，不应判定 ODOWN
        assert!(!status.is_odown);
    }

    #[test]
    fn test_odown_with_enough_sentinels() {
        let mut sentinel = Sentinel::new(2, 30_000);
        sentinel.monitor_master("mymaster", "127.0.0.1:6379");

        // 设置 SDOWN 且其他 Sentinel 数量达到法定人数
        if let Some(master) = sentinel.monitor_masters.get_mut("mymaster") {
            master.is_sdown = true;
            master.last_pong = 0;
            master.num_other_sentinels = 1; // quorum=2, 1+1=2 >= 2
        }

        let status = sentinel.check_master("mymaster").unwrap();
        assert!(status.is_sdown);
        assert!(status.is_odown);
    }

    #[test]
    fn test_add_slave() {
        let mut sentinel = Sentinel::new(2, 30_000);
        sentinel.monitor_master("mymaster", "127.0.0.1:6379");
        sentinel.add_slave("mymaster", "slave1", "127.0.0.1:6380");
        sentinel.add_slave("mymaster", "slave2", "127.0.0.1:6381");

        let status = sentinel.check_master("mymaster").unwrap();
        assert_eq!(status.num_slaves, 2);
    }

    #[test]
    fn test_add_slave_to_nonexistent_master() {
        let mut sentinel = Sentinel::new(2, 30_000);
        // 不应 panic，静默忽略
        sentinel.add_slave("nonexistent", "slave1", "127.0.0.1:6380");
    }

    #[test]
    fn test_set_num_other_sentinels() {
        let mut sentinel = Sentinel::new(2, 30_000);
        sentinel.monitor_master("mymaster", "127.0.0.1:6379");
        sentinel.set_num_other_sentinels("mymaster", 5);

        let status = sentinel.check_master("mymaster").unwrap();
        assert_eq!(status.num_other_sentinels, 5);
    }

    #[test]
    fn test_handle_sentinel_monitor() {
        let mut sentinel = Sentinel::new(2, 30_000);
        let argv: Vec<Vec<u8>> = vec![
            b"SENTINEL".to_vec(),
            b"monitor".to_vec(),
            b"mymaster".to_vec(),
            b"127.0.0.1:6379".to_vec(),
            b"2".to_vec(),
        ];
        let result = sentinel.handle_sentinel_command(&argv);
        assert_eq!(result, RespValue::ok());
    }

    #[test]
    fn test_handle_sentinel_monitor_wrong_arity() {
        let mut sentinel = Sentinel::new(2, 30_000);
        let argv: Vec<Vec<u8>> = vec![b"SENTINEL".to_vec(), b"monitor".to_vec()];
        let result = sentinel.handle_sentinel_command(&argv);
        // 应返回错误
        match result {
            RespValue::Error(msg) => assert!(msg.contains("wrong number")),
            _ => panic!("Expected error response"),
        }
    }

    #[test]
    fn test_handle_sentinel_masters_empty() {
        let mut sentinel = Sentinel::new(2, 30_000);
        let argv: Vec<Vec<u8>> = vec![b"SENTINEL".to_vec(), b"masters".to_vec()];
        let result = sentinel.handle_sentinel_command(&argv);
        match result {
            RespValue::Array(arr) => assert!(arr.is_empty()),
            _ => panic!("Expected array response"),
        }
    }

    #[test]
    fn test_handle_sentinel_masters_with_data() {
        let mut sentinel = Sentinel::new(2, 30_000);
        sentinel.monitor_master("mymaster", "127.0.0.1:6379");

        let argv: Vec<Vec<u8>> = vec![b"SENTINEL".to_vec(), b"masters".to_vec()];
        let result = sentinel.handle_sentinel_command(&argv);
        match result {
            RespValue::Array(arr) => assert_eq!(arr.len(), 1),
            _ => panic!("Expected array response"),
        }
    }

    #[test]
    fn test_handle_sentinel_get_master_addr() {
        let mut sentinel = Sentinel::new(2, 30_000);
        sentinel.monitor_master("mymaster", "192.168.1.100:6379");

        let argv: Vec<Vec<u8>> = vec![
            b"SENTINEL".to_vec(),
            b"get-master-addr-by-name".to_vec(),
            b"mymaster".to_vec(),
        ];
        let result = sentinel.handle_sentinel_command(&argv);
        match result {
            RespValue::Array(arr) => {
                assert_eq!(arr.len(), 2);
                assert_eq!(arr[0], RespValue::bulk("192.168.1.100"));
                assert_eq!(arr[1], RespValue::bulk("6379"));
            }
            _ => panic!("Expected array response"),
        }
    }

    #[test]
    fn test_handle_sentinel_get_master_addr_not_found() {
        let mut sentinel = Sentinel::new(2, 30_000);
        let argv: Vec<Vec<u8>> = vec![
            b"SENTINEL".to_vec(),
            b"get-master-addr-by-name".to_vec(),
            b"nonexistent".to_vec(),
        ];
        let result = sentinel.handle_sentinel_command(&argv);
        assert_eq!(result, RespValue::null());
    }

    #[test]
    fn test_handle_sentinel_is_master_down() {
        let mut sentinel = Sentinel::new(2, 30_000);
        sentinel.monitor_master("mymaster", "127.0.0.1:6379");

        let argv: Vec<Vec<u8>> = vec![
            b"SENTINEL".to_vec(),
            b"is-master-down-by-addr".to_vec(),
            b"127.0.0.1:6379".to_vec(),
        ];
        let result = sentinel.handle_sentinel_command(&argv);
        match result {
            RespValue::Array(arr) => {
                assert_eq!(arr.len(), 2);
                assert_eq!(arr[0], RespValue::integer(0)); // 不是下线状态
            }
            _ => panic!("Expected array response"),
        }
    }

    #[test]
    fn test_handle_sentinel_slaves() {
        let mut sentinel = Sentinel::new(2, 30_000);
        sentinel.monitor_master("mymaster", "127.0.0.1:6379");
        sentinel.add_slave("mymaster", "slave1", "127.0.0.1:6380");

        let argv: Vec<Vec<u8>> = vec![
            b"SENTINEL".to_vec(),
            b"slaves".to_vec(),
            b"mymaster".to_vec(),
        ];
        let result = sentinel.handle_sentinel_command(&argv);
        match result {
            RespValue::Array(arr) => assert_eq!(arr.len(), 1),
            _ => panic!("Expected array response"),
        }
    }

    #[test]
    fn test_handle_sentinel_info() {
        let mut sentinel = Sentinel::new(2, 30_000);
        sentinel.monitor_master("mymaster", "127.0.0.1:6379");

        let argv: Vec<Vec<u8>> = vec![b"SENTINEL".to_vec(), b"info".to_vec()];
        let result = sentinel.handle_sentinel_command(&argv);
        match result {
            RespValue::BulkString(data) => {
                let info = String::from_utf8(data).unwrap();
                assert!(info.contains("# Sentinel"));
                assert!(info.contains("sentinel_masters:1"));
                assert!(info.contains("mymaster"));
            }
            _ => panic!("Expected bulk string response"),
        }
    }

    #[test]
    fn test_handle_sentinel_unknown_subcommand() {
        let mut sentinel = Sentinel::new(2, 30_000);
        let argv: Vec<Vec<u8>> = vec![b"SENTINEL".to_vec(), b"foobar".to_vec()];
        let result = sentinel.handle_sentinel_command(&argv);
        match result {
            RespValue::Error(msg) => assert!(msg.contains("unknown sentinel subcommand")),
            _ => panic!("Expected error response"),
        }
    }

    #[test]
    fn test_handle_sentinel_no_args() {
        let mut sentinel = Sentinel::new(2, 30_000);
        let argv: Vec<Vec<u8>> = vec![b"SENTINEL".to_vec()];
        let result = sentinel.handle_sentinel_command(&argv);
        match result {
            RespValue::Error(msg) => assert!(msg.contains("wrong number")),
            _ => panic!("Expected error response"),
        }
    }

    #[test]
    fn test_info_sentinel_format() {
        let mut sentinel = Sentinel::new(2, 50_000);
        sentinel.monitor_master("mymaster", "127.0.0.1:6379");
        sentinel.monitor_master("master2", "192.168.1.1:6380");

        let info = sentinel.info_sentinel();
        assert!(info.contains("# Sentinel\r\n"));
        assert!(info.contains("sentinel_masters:2\r\n"));
        assert!(info.contains("quorum:2\r\n"));
        assert!(info.contains("down_after_ms:50000\r\n"));
        assert!(info.contains("mymaster"));
        assert!(info.contains("master2"));
    }

    #[test]
    fn test_master_status_fields() {
        let mut sentinel = Sentinel::new(2, 30_000);
        sentinel.monitor_master("mymaster", "127.0.0.1:6379");
        sentinel.add_slave("mymaster", "s1", "127.0.0.1:6380");
        sentinel.add_slave("mymaster", "s2", "127.0.0.1:6381");
        sentinel.set_num_other_sentinels("mymaster", 3);

        let status = sentinel.check_master("mymaster").unwrap();
        assert_eq!(status.name, "mymaster");
        assert_eq!(status.addr, "127.0.0.1:6379");
        assert!(!status.is_sdown);
        assert!(!status.is_odown);
        assert_eq!(status.num_slaves, 2);
        assert_eq!(status.num_other_sentinels, 3);
        assert_eq!(status.time_since_last_pong, 0); // 刚初始化，差值为 0
        assert_eq!(status.time_since_last_ok, 0);
    }

    #[test]
    fn test_monitor_master_overwrite() {
        let mut sentinel = Sentinel::new(2, 30_000);
        sentinel.monitor_master("mymaster", "127.0.0.1:6379");
        sentinel.monitor_master("mymaster", "192.168.1.1:6380"); // 覆盖

        let (name, addr) = sentinel.get_master_addr_by_name("mymaster").unwrap();
        assert_eq!(name, "mymaster");
        assert_eq!(addr, "192.168.1.1:6380");
    }
}
