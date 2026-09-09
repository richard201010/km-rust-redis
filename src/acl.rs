//! ACL（Access Control List）访问控制列表模块
//!
//! 对应 Redis 6.0+ 引入的 ACL 系统，提供基于用户的细粒度权限控制：
//! - 用户管理：创建/删除/启用/禁用用户
//! - 密码认证：支持每用户多密码，明文密码哈希存储
//! - 命令权限：允许/禁止特定命令或命令类别
//! - Key 模式限制：限制用户可访问的 key 模式（glob 风格）
//! - 频道模式限制：限制用户可订阅的 Pub/Sub 频道
//! - ACL 命令集：ACL LIST / SETUSER / GETUSER / DELUSER / WHOAMI / LOG / SAVE / LOAD
//! - 审计日志：记录认证失败和权限拒绝事件

use std::collections::{HashMap, HashSet, VecDeque};

// ===========================================================================
// 常量
// ===========================================================================

/// ACL 审计日志默认最大条目数
const DEFAULT_ACL_LOG_MAX_LEN: usize = 128;

// ===========================================================================
// AclPermissions — 命令权限定义
// ===========================================================================

/// 命令权限控制。
///
/// - `all_commands = true` 且 `denied_commands` 为空：允许所有命令
/// - `all_commands = false`：仅允许 `allowed_commands` 中的命令
/// - `denied_commands`：即使 `all_commands = true`，也禁止列表中的命令
#[derive(Debug, Clone)]
pub struct AclPermissions {
    /// 是否允许所有命令（默认 true）
    pub all_commands: bool,
    /// 显式允许的命令集合（小写命令名），当 `all_commands = false` 时使用
    pub allowed_commands: HashSet<String>,
    /// 显式禁止的命令集合（小写命令名），优先级高于 allowed
    pub denied_commands: HashSet<String>,
}

impl AclPermissions {
    /// 创建默认权限：允许所有命令
    fn new_all_allowed() -> Self {
        Self {
            all_commands: true,
            allowed_commands: HashSet::new(),
            denied_commands: HashSet::new(),
        }
    }

    /// 创建空权限：禁止所有命令
    fn new_none() -> Self {
        Self {
            all_commands: false,
            allowed_commands: HashSet::new(),
            denied_commands: HashSet::new(),
        }
    }

    /// 检查指定命令是否被允许执行
    pub fn is_command_allowed(&self, command: &str) -> bool {
        let cmd = command.to_ascii_lowercase();
        // 被显式禁止的命令始终拒绝
        if self.denied_commands.contains(&cmd) {
            return false;
        }
        // 如果允许所有命令，放行
        if self.all_commands {
            return true;
        }
        // 否则只允许显式列出的命令
        self.allowed_commands.contains(&cmd)
    }
}

// ===========================================================================
// AclLogEntry — 审计日志条目
// ===========================================================================

/// ACL 审计日志条目，记录认证失败和权限拒绝事件
#[derive(Debug, Clone)]
pub struct AclLogEntry {
    /// 事件发生的时间戳（Unix 秒）
    pub timestamp: u64,
    /// 事件类型：`auth` 或 `command`
    pub event_type: String,
    /// 相关用户名
    pub username: String,
    /// 客户端地址（可选）
    pub client_addr: String,
    /// 详细描述
    pub reason: String,
}

impl AclLogEntry {
    /// 转换为 RESP 友好的字符串表示
    pub fn to_info_string(&self) -> String {
        format!(
            "{}: type={} user={} addr={} reason={}",
            self.timestamp, self.event_type, self.username, self.client_addr, self.reason
        )
    }
}

// ===========================================================================
// AclUser — 用户定义
// ===========================================================================

/// ACL 用户定义
#[derive(Debug, Clone)]
pub struct AclUser {
    /// 用户名
    pub name: String,
    /// 密码列表（SHA-256 哈希，hex 编码），支持多密码
    pub passwords: Vec<String>,
    /// 是否启用
    pub enabled: bool,
    /// 命令权限
    pub commands: AclPermissions,
    /// 允许访问的 key 模式列表（glob 风格，如 `cache:*`、`*`）
    pub keys: Vec<String>,
    /// 允许订阅的频道模式列表（glob 风格）
    pub channels: Vec<String>,
}

impl AclUser {
    /// 创建新的禁用用户（空白权限，需后续配置）
    fn new(name: &str) -> Self {
        Self {
            name: name.to_string(),
            passwords: Vec::new(),
            enabled: false,
            commands: AclPermissions::new_none(),
            keys: Vec::new(),
            channels: Vec::new(),
        }
    }

    /// 创建默认用户（启用，允许所有命令，允许所有 key 和频道）
    fn new_default() -> Self {
        Self {
            name: "default".to_string(),
            passwords: Vec::new(),
            enabled: true,
            commands: AclPermissions::new_all_allowed(),
            keys: vec!["*".to_string()],
            channels: vec!["*".to_string()],
        }
    }

    /// 添加一个密码（存储 SHA-256 哈希）
    pub fn add_password(&mut self, password: &str) {
        let hash = sha256_hex(password);
        if !self.passwords.contains(&hash) {
            self.passwords.push(hash);
        }
    }

    /// 移除一个密码
    pub fn remove_password(&mut self, password: &str) -> bool {
        let hash = sha256_hex(password);
        let before = self.passwords.len();
        self.passwords.retain(|p| p != &hash);
        self.passwords.len() < before
    }

    /// 清除所有密码
    pub fn clear_passwords(&mut self) {
        self.passwords.clear();
    }

    /// 验证密码是否匹配
    pub fn verify_password(&self, password: &str) -> bool {
        if self.passwords.is_empty() {
            // 无密码用户：空密码即可认证
            return password.is_empty();
        }
        let hash = sha256_hex(password);
        self.passwords.contains(&hash)
    }

    /// 检查是否有密码
    pub fn has_password(&self) -> bool {
        !self.passwords.is_empty()
    }

    /// 检查 key 是否匹配允许的模式列表
    pub fn is_key_allowed(&self, key: &[u8]) -> bool {
        if self.keys.is_empty() {
            return false;
        }
        let key_str = String::from_utf8_lossy(key);
        self.keys.iter().any(|pat| glob_match(pat, &key_str))
    }

    /// 检查频道是否匹配允许的模式列表
    pub fn is_channel_allowed(&self, channel: &str) -> bool {
        if self.channels.is_empty() {
            return false;
        }
        self.channels.iter().any(|pat| glob_match(pat, channel))
    }

    /// 转换为 ACL LIST 格式的字符串
    pub fn to_acl_list_string(&self) -> String {
        let mut parts = Vec::new();
        parts.push(format!("user {}", self.name));

        // on/off
        if self.enabled {
            parts.push("on".to_string());
        } else {
            parts.push("off".to_string());
        }

        // passwords
        for pwd in &self.passwords {
            parts.push(format!("#{}", pwd));
        }
        if self.passwords.is_empty() {
            parts.push("nopass".to_string());
        }

        // commands
        if self.commands.all_commands {
            if self.commands.denied_commands.is_empty() {
                parts.push("allcommands".to_string());
            } else {
                parts.push("allcommands".to_string());
                let mut denied: Vec<_> = self.commands.denied_commands.iter().collect();
                denied.sort();
                for cmd in denied {
                    parts.push(format!("~{}", cmd));
                }
            }
        } else {
            if self.commands.allowed_commands.is_empty() {
                parts.push("nocommands".to_string());
            } else {
                let mut allowed: Vec<_> = self.commands.allowed_commands.iter().collect();
                allowed.sort();
                for cmd in allowed {
                    parts.push(format!("+{}", cmd));
                }
                let mut denied: Vec<_> = self.commands.denied_commands.iter().collect();
                denied.sort();
                for cmd in denied {
                    parts.push(format!("-{}", cmd));
                }
            }
        }

        // keys
        if self.keys.is_empty() {
            parts.push("nokeys".to_string());
        } else if self.keys.len() == 1 && self.keys[0] == "*" {
            parts.push("allkeys".to_string());
        } else {
            let mut sorted_keys = self.keys.clone();
            sorted_keys.sort();
            for k in &sorted_keys {
                parts.push(format!("~{}", k));
            }
        }

        // channels
        if self.channels.is_empty() {
            parts.push("nochannels".to_string());
        } else if self.channels.len() == 1 && self.channels[0] == "*" {
            parts.push("allchannels".to_string());
        } else {
            let mut sorted_ch = self.channels.clone();
            sorted_ch.sort();
            for ch in &sorted_ch {
                parts.push(format!("&{}", ch));
            }
        }

        parts.join(" ")
    }
}

// ===========================================================================
// AclState — 全局 ACL 状态
// ===========================================================================

/// 全局 ACL 状态，管理所有用户和审计日志。
///
/// 线程安全：通过 `Mutex` 保护内部状态。
pub struct AclState {
    /// 用户表（用户名 → 用户定义）
    users: HashMap<String, AclUser>,
    /// 默认用户名（未认证连接以该用户身份运行）
    default_user: String,
    /// 审计日志
    log: VecDeque<AclLogEntry>,
    /// 审计日志最大条目数
    log_max_len: usize,
}

impl AclState {
    /// 创建 ACL 状态，包含一个默认用户（启用，允许所有）
    pub fn new() -> Self {
        let mut users = HashMap::new();
        users.insert("default".to_string(), AclUser::new_default());
        Self {
            users,
            default_user: "default".to_string(),
            log: VecDeque::new(),
            log_max_len: DEFAULT_ACL_LOG_MAX_LEN,
        }
    }

    // -----------------------------------------------------------------------
    // 用户管理
    // -----------------------------------------------------------------------

    /// 创建新用户（初始为禁用状态，无密码，无权限）
    pub fn create_user(&mut self, name: &str) -> Result<(), String> {
        if name.is_empty() {
            return Err("ERR Username must not be empty".to_string());
        }
        if self.users.contains_key(name) {
            // 用户已存在，不报错（与 Redis 一致：SETUSER 可对已有用户操作）
            return Ok(());
        }
        self.users.insert(name.to_string(), AclUser::new(name));
        Ok(())
    }

    /// 删除用户
    pub fn delete_user(&mut self, name: &str) -> Result<bool, String> {
        if name == "default" {
            return Err("ERR The 'default' user cannot be removed".to_string());
        }
        Ok(self.users.remove(name).is_some())
    }

    /// 获取用户可变引用
    pub fn get_user_mut(&mut self, name: &str) -> Option<&mut AclUser> {
        self.users.get_mut(name)
    }

    /// 获取用户不可变引用
    pub fn get_user(&self, name: &str) -> Option<&AclUser> {
        self.users.get(name)
    }

    /// 列出所有用户名
    pub fn list_users(&self) -> Vec<String> {
        self.users.keys().cloned().collect()
    }

    /// 启用用户
    pub fn enable_user(&mut self, name: &str) -> Result<(), String> {
        let user = self
            .users
            .get_mut(name)
            .ok_or_else(|| format!("ERR no such user '{}'", name))?;
        user.enabled = true;
        Ok(())
    }

    /// 禁用用户
    pub fn disable_user(&mut self, name: &str) -> Result<(), String> {
        if name == "default" {
            return Err("ERR The 'default' user cannot be disabled".to_string());
        }
        let user = self
            .users
            .get_mut(name)
            .ok_or_else(|| format!("ERR no such user '{}'", name))?;
        user.enabled = false;
        Ok(())
    }

    // -----------------------------------------------------------------------
    // 密码管理
    // -----------------------------------------------------------------------

    /// 为用户设置密码（追加）
    pub fn set_password(&mut self, name: &str, password: &str) -> Result<(), String> {
        let user = self
            .users
            .get_mut(name)
            .ok_or_else(|| format!("ERR no such user '{}'", name))?;
        user.add_password(password);
        Ok(())
    }

    /// 移除用户的一个密码
    pub fn remove_password(&mut self, name: &str, password: &str) -> Result<bool, String> {
        let user = self
            .users
            .get_mut(name)
            .ok_or_else(|| format!("ERR no such user '{}'", name))?;
        Ok(user.remove_password(password))
    }

    /// 清除用户所有密码（设置为无密码）
    pub fn clear_passwords(&mut self, name: &str) -> Result<(), String> {
        let user = self
            .users
            .get_mut(name)
            .ok_or_else(|| format!("ERR no such user '{}'", name))?;
        user.clear_passwords();
        Ok(())
    }

    // -----------------------------------------------------------------------
    // 命令权限管理
    // -----------------------------------------------------------------------

    /// 允许用户执行指定命令
    pub fn allow_command(&mut self, name: &str, command: &str) -> Result<(), String> {
        let user = self
            .users
            .get_mut(name)
            .ok_or_else(|| format!("ERR no such user '{}'", name))?;
        user.commands
            .allowed_commands
            .insert(command.to_ascii_lowercase());
        user.commands
            .denied_commands
            .remove(&command.to_ascii_lowercase());
        Ok(())
    }

    /// 禁止用户执行指定命令
    pub fn deny_command(&mut self, name: &str, command: &str) -> Result<(), String> {
        let user = self
            .users
            .get_mut(name)
            .ok_or_else(|| format!("ERR no such user '{}'", name))?;
        user.commands
            .denied_commands
            .insert(command.to_ascii_lowercase());
        user.commands
            .allowed_commands
            .remove(&command.to_ascii_lowercase());
        Ok(())
    }

    /// 允许用户执行所有命令
    pub fn allow_all_commands(&mut self, name: &str) -> Result<(), String> {
        let user = self
            .users
            .get_mut(name)
            .ok_or_else(|| format!("ERR no such user '{}'", name))?;
        user.commands.all_commands = true;
        user.commands.allowed_commands.clear();
        user.commands.denied_commands.clear();
        Ok(())
    }

    /// 禁止用户执行所有命令
    pub fn deny_all_commands(&mut self, name: &str) -> Result<(), String> {
        let user = self
            .users
            .get_mut(name)
            .ok_or_else(|| format!("ERR no such user '{}'", name))?;
        user.commands.all_commands = false;
        user.commands.allowed_commands.clear();
        user.commands.denied_commands.clear();
        Ok(())
    }

    /// 设置用户允许的 key 模式
    pub fn set_key_patterns(&mut self, name: &str, patterns: Vec<String>) -> Result<(), String> {
        let user = self
            .users
            .get_mut(name)
            .ok_or_else(|| format!("ERR no such user '{}'", name))?;
        user.keys = patterns;
        Ok(())
    }

    /// 设置用户允许的频道模式
    pub fn set_channel_patterns(
        &mut self,
        name: &str,
        patterns: Vec<String>,
    ) -> Result<(), String> {
        let user = self
            .users
            .get_mut(name)
            .ok_or_else(|| format!("ERR no such user '{}'", name))?;
        user.channels = patterns;
        Ok(())
    }

    // -----------------------------------------------------------------------
    // 认证
    // -----------------------------------------------------------------------

    /// 认证用户。返回认证成功的用户名，或错误信息。
    ///
    /// 逻辑：
    /// 1. 查找用户，不存在则记录日志并返回错误
    /// 2. 用户禁用则记录日志并返回错误
    /// 3. 密码不匹配则记录日志并返回错误
    pub fn authenticate(
        &mut self,
        username: &str,
        password: &str,
        client_addr: &str,
    ) -> Result<String, String> {
        let user = match self.users.get(username) {
            Some(u) => u,
            None => {
                self.push_log(AclLogEntry {
                    timestamp: current_unix_secs(),
                    event_type: "auth".to_string(),
                    username: username.to_string(),
                    client_addr: client_addr.to_string(),
                    reason: format!("Unknown user '{}'", username),
                });
                return Err("ERR invalid username-password pair or user is disabled".to_string());
            }
        };

        if !user.enabled {
            self.push_log(AclLogEntry {
                timestamp: current_unix_secs(),
                event_type: "auth".to_string(),
                username: username.to_string(),
                client_addr: client_addr.to_string(),
                reason: "User is disabled".to_string(),
            });
            return Err("ERR invalid username-password pair or user is disabled".to_string());
        }

        if !user.verify_password(password) {
            self.push_log(AclLogEntry {
                timestamp: current_unix_secs(),
                event_type: "auth".to_string(),
                username: username.to_string(),
                client_addr: client_addr.to_string(),
                reason: "Wrong password".to_string(),
            });
            return Err("ERR invalid username-password pair or user is disabled".to_string());
        }

        Ok(username.to_string())
    }

    // -----------------------------------------------------------------------
    // 权限检查
    // -----------------------------------------------------------------------

    /// 检查用户是否有权执行指定命令
    ///
    /// 返回 `Ok(())` 或拒绝原因 `Err(String)`
    pub fn check_command(
        &mut self,
        username: &str,
        command: &str,
        client_addr: &str,
    ) -> Result<(), String> {
        let user = match self.users.get(username) {
            Some(u) => u,
            None => {
                return Err(format!("ERR no such user '{}'", username));
            }
        };

        if !user.enabled {
            return Err("ERR user is disabled".to_string());
        }

        if !user.commands.is_command_allowed(command) {
            self.push_log(AclLogEntry {
                timestamp: current_unix_secs(),
                event_type: "command".to_string(),
                username: username.to_string(),
                client_addr: client_addr.to_string(),
                reason: format!("Command '{}' not allowed for user '{}'", command, username),
            });
            return Err(format!(
                "NOPERM User {} has no permissions to run the '{}' command",
                username, command
            ));
        }

        Ok(())
    }

    /// 检查用户是否有权访问指定 key
    pub fn check_key(&self, username: &str, key: &[u8]) -> Result<(), String> {
        let user = match self.users.get(username) {
            Some(u) => u,
            None => return Err(format!("ERR no such user '{}'", username)),
        };
        if !user.is_key_allowed(key) {
            return Err(format!(
                "NOPERM User {} has no permissions to access the requested key",
                username
            ));
        }
        Ok(())
    }

    /// 检查用户是否有权订阅指定频道
    pub fn check_channel(&self, username: &str, channel: &str) -> Result<(), String> {
        let user = match self.users.get(username) {
            Some(u) => u,
            None => return Err(format!("ERR no such user '{}'", username)),
        };
        if !user.is_channel_allowed(channel) {
            return Err(format!(
                "NOPERM User {} has no permissions to access the requested channel",
                username
            ));
        }
        Ok(())
    }

    // -----------------------------------------------------------------------
    // 审计日志
    // -----------------------------------------------------------------------

    /// 追加日志条目，超过上限时淘汰最旧条目
    fn push_log(&mut self, entry: AclLogEntry) {
        if self.log.len() >= self.log_max_len {
            self.log.pop_front();
        }
        self.log.push_back(entry);
    }

    /// 获取审计日志（最近 N 条）
    pub fn get_log(&self, count: Option<usize>) -> Vec<&AclLogEntry> {
        let n = count.unwrap_or(self.log.len());
        let skip = if self.log.len() > n {
            self.log.len() - n
        } else {
            0
        };
        self.log.iter().skip(skip).collect()
    }

    /// 清空审计日志
    pub fn clear_log(&mut self) {
        self.log.clear();
    }

    // -----------------------------------------------------------------------
    // INFO ACL 输出
    // -----------------------------------------------------------------------

    /// 返回 INFO ACL 格式的统计信息
    pub fn info_acl(&self) -> String {
        let enabled_count = self.users.values().filter(|u| u.enabled).count();
        let disabled_count = self.users.len() - enabled_count;
        format!(
            "# ACL\r\nacl_enabled:{}\r\nacl_users_total:{}\r\nacl_users_enabled:{}\r\nacl_users_disabled:{}\r\nacl_log_entries:{}\r\n",
            1,
            self.users.len(),
            enabled_count,
            disabled_count,
            self.log.len()
        )
    }

    // -----------------------------------------------------------------------
    // ACL SETUSER — 批量设置用户属性
    // -----------------------------------------------------------------------

    /// 按 Redis ACL SETUSER 风格的规则列表设置用户属性。
    ///
    /// 规则格式：
    /// - `on` / `off` — 启用/禁用
    /// - `>password` — 添加密码
    /// - `<password` — 移除密码
    /// - `nopass` — 清除密码
    /// - `resetpass` — 同 nopass
    /// - `+command` — 允许命令
    /// - `-command` — 禁止命令
    /// - `+@all` — 允许所有命令
    /// - `-@all` — 禁止所有命令
    /// - `~pattern` — 添加 key 模式
    /// - `~*` — 所有 key
    /// - `resetkeys` — 清除 key 模式
    /// - `&pattern` — 添加频道模式
    /// - `&*` — 所有频道
    /// - `resetchannels` — 清除频道模式
    /// - `nocommands` — 禁止所有命令
    /// - `allcommands` — 允许所有命令
    /// - `allkeys` — 允许所有 key
    /// - `allchannels` — 允许所有频道
    pub fn set_user_rules(
        &mut self,
        name: &str,
        rules: &[String],
    ) -> Result<(), String> {
        // 确保用户存在
        self.create_user(name)?;

        for rule in rules {
            let rule = rule.trim();
            if rule.is_empty() {
                continue;
            }
            let lower = rule.to_ascii_lowercase();

            match lower.as_str() {
                "on" => {
                    self.enable_user(name)?;
                }
                "off" => {
                    self.disable_user(name)?;
                }
                "nopass" | "resetpass" => {
                    self.clear_passwords(name)?;
                }
                "allcommands" | "+@all" => {
                    self.allow_all_commands(name)?;
                }
                "nocommands" | "-@all" => {
                    self.deny_all_commands(name)?;
                }
                "allkeys" => {
                    self.set_key_patterns(name, vec!["*".to_string()])?;
                }
                "nokeys" | "resetkeys" => {
                    self.set_key_patterns(name, vec![])?;
                }
                "allchannels" => {
                    self.set_channel_patterns(name, vec!["*".to_string()])?;
                }
                "nochannels" | "resetchannels" => {
                    self.set_channel_patterns(name, vec![])?;
                }
                _ if rule.starts_with('>') => {
                    let pwd = &rule[1..];
                    if pwd.is_empty() {
                        return Err("ERR syntax error".to_string());
                    }
                    self.set_password(name, pwd)?;
                }
                _ if rule.starts_with('#') => {
                    // #hash: add raw password hash directly
                    let hash = &rule[1..];
                    if hash.is_empty() {
                        return Err("ERR syntax error".to_string());
                    }
                    let user = self.users.get_mut(name).unwrap();
                    if !user.passwords.contains(&hash.to_string()) {
                        user.passwords.push(hash.to_string());
                    }
                }
                _ if rule.starts_with('<') => {
                    let pwd = &rule[1..];
                    if pwd.is_empty() {
                        return Err("ERR syntax error".to_string());
                    }
                    self.remove_password(name, pwd)?;
                }
                _ if rule.starts_with('+') => {
                    let cmd = &rule[1..];
                    if cmd.is_empty() {
                        return Err("ERR syntax error".to_string());
                    }
                    // +@category 暂不实现分类展开，当作单命令处理
                    if cmd.starts_with('@') {
                        // 忽略未知分类
                        continue;
                    }
                    self.allow_command(name, cmd)?;
                }
                _ if rule.starts_with('-') => {
                    let cmd = &rule[1..];
                    if cmd.is_empty() {
                        return Err("ERR syntax error".to_string());
                    }
                    if cmd.starts_with('@') {
                        continue;
                    }
                    self.deny_command(name, cmd)?;
                }
                _ if rule.starts_with('~') => {
                    let pattern = &rule[1..];
                    let user = self.users.get_mut(name).unwrap();
                    if !user.keys.contains(&pattern.to_string()) {
                        user.keys.push(pattern.to_string());
                    }
                }
                _ if rule.starts_with('&') => {
                    let pattern = &rule[1..];
                    let user = self.users.get_mut(name).unwrap();
                    if !user.channels.contains(&pattern.to_string()) {
                        user.channels.push(pattern.to_string());
                    }
                }
                _ => {
                    return Err(format!("ERR unknown ACL rule '{}'", rule));
                }
            }
        }

        Ok(())
    }

    // -----------------------------------------------------------------------
    // ACL SAVE / LOAD
    // -----------------------------------------------------------------------

    /// 将 ACL 配置序列化为可保存的行格式（每行一个用户的 ACL LIST 输出）
    pub fn save_to_string(&self) -> String {
        let mut lines: Vec<String> = self
            .users
            .values()
            .map(|u| u.to_acl_list_string())
            .collect();
        lines.sort();
        lines.join("\n")
    }

    /// 从字符串加载 ACL 配置（格式同 ACL LIST 输出，每行一个用户）
    pub fn load_from_string(&mut self, data: &str) -> Result<(), String> {
        for line in data.lines() {
            let line = line.trim();
            if line.is_empty() || line.starts_with('#') {
                continue;
            }
            // 解析 "user <name> <rules...>"
            let parts: Vec<&str> = line.split_whitespace().collect();
            if parts.len() < 2 || parts[0] != "user" {
                return Err(format!("ERR invalid ACL line: {}", line));
            }
            let name = parts[1];
            let rules: Vec<String> = parts[2..].iter().map(|s| s.to_string()).collect();
            self.set_user_rules(name, &rules)?;
        }
        Ok(())
    }

    // -----------------------------------------------------------------------
    // ACL 文件持久化 (SAVE/LOAD)
    // -----------------------------------------------------------------------

    /// 将所有 ACL 用户保存到文件（Redis ACL 文件格式）
    ///
    /// 文件格式：
    /// ```text
    /// # Comments
    /// user default on nopass ~* +@all
    /// user alice on >hash123 ~data:* +get +set
    /// ```
    pub fn save_to_file(&self, path: &str) -> Result<(), String> {
        let mut lines = Vec::new();
        lines.push("# KM-Rust-Redis ACL file".to_string());
        lines.push("# Generated by ACL SAVE".to_string());
        lines.push(String::new());

        let mut usernames: Vec<&String> = self.users.keys().collect();
        usernames.sort();

        for name in usernames {
            let user = &self.users[name];
            lines.push(user.to_acl_list_string());
        }

        let content = lines.join("\n");
        std::fs::write(path, content)
            .map_err(|e| format!("ERR Failed to write ACL file: {}", e))?;
        Ok(())
    }

    /// 从文件加载 ACL 用户（Redis ACL 文件格式）
    ///
    /// 返回成功加载的用户数。
    pub fn load_from_file(&mut self, path: &str) -> Result<usize, String> {
        let content = std::fs::read_to_string(path)
            .map_err(|e| format!("ERR Failed to read ACL file: {}", e))?;

        let mut loaded = 0;
        for line in content.lines() {
            let line = line.trim();
            if line.is_empty() || line.starts_with('#') {
                continue;
            }

            // Parse: "user <name> <rules...>"
            let parts: Vec<&str> = line.split_whitespace().collect();
            if parts.len() < 2 || parts[0] != "user" {
                continue;
            }

            let username = parts[1];
            let rules: Vec<String> = parts[2..].iter().map(|s| s.to_string()).collect();

            // Create user if not exists
            self.create_user(username)?;
            self.set_user_rules(username, &rules)?;
            loaded += 1;
        }

        Ok(loaded)
    }
}

// ===========================================================================
// 辅助函数
// ===========================================================================

/// 简易 SHA-256 哈希（hex 编码）。
///
/// 使用标准库可访问的最小依赖实现：基于固定比特操作的 SHA-256。
/// 为避免引入额外依赖，此处实现一个纯 Rust 的 SHA-256。
fn sha256_hex(input: &str) -> String {
    let digest = sha256(input.as_bytes());
    digest
        .iter()
        .map(|b| format!("{:02x}", b))
        .collect::<String>()
}

/// 纯 Rust SHA-256 实现（RFC 6234）
fn sha256(data: &[u8]) -> [u8; 32] {
    const K: [u32; 64] = [
        0x428a2f98, 0x71374491, 0xb5c0fbcf, 0xe9b5dba5, 0x3956c25b, 0x59f111f1, 0x923f82a4,
        0xab1c5ed5, 0xd807aa98, 0x12835b01, 0x243185be, 0x550c7dc3, 0x72be5d74, 0x80deb1fe,
        0x9bdc06a7, 0xc19bf174, 0xe49b69c1, 0xefbe4786, 0x0fc19dc6, 0x240ca1cc, 0x2de92c6f,
        0x4a7484aa, 0x5cb0a9dc, 0x76f988da, 0x983e5152, 0xa831c66d, 0xb00327c8, 0xbf597fc7,
        0xc6e00bf3, 0xd5a79147, 0x06ca6351, 0x14292967, 0x27b70a85, 0x2e1b2138, 0x4d2c6dfc,
        0x53380d13, 0x650a7354, 0x766a0abb, 0x81c2c92e, 0x92722c85, 0xa2bfe8a1, 0xa81a664b,
        0xc24b8b70, 0xc76c51a3, 0xd192e819, 0xd6990624, 0xf40e3585, 0x106aa070, 0x19a4c116,
        0x1e376c08, 0x2748774c, 0x34b0bcb5, 0x391c0cb3, 0x4ed8aa4a, 0x5b9cca4f, 0x682e6ff3,
        0x748f82ee, 0x78a5636f, 0x84c87814, 0x8cc70208, 0x90befffa, 0xa4506ceb, 0xbef9a3f7,
        0xc67178f2,
    ];

    let mut h: [u32; 8] = [
        0x6a09e667, 0xbb67ae85, 0x3c6ef372, 0xa54ff53a, 0x510e527f, 0x9b05688c, 0x1f83d9ab,
        0x5be0cd19,
    ];

    // 预处理：填充
    let original_len = data.len();
    let bit_len = (original_len as u64) * 8;
    let mut padded = data.to_vec();
    padded.push(0x80);
    while (padded.len() % 64) != 56 {
        padded.push(0);
    }
    padded.extend_from_slice(&bit_len.to_be_bytes());

    // 处理每个 512-bit 块
    for chunk in padded.chunks(64) {
        let mut w = [0u32; 64];
        for i in 0..16 {
            w[i] = u32::from_be_bytes([
                chunk[i * 4],
                chunk[i * 4 + 1],
                chunk[i * 4 + 2],
                chunk[i * 4 + 3],
            ]);
        }
        for i in 16..64 {
            let s0 = w[i - 15].rotate_right(7) ^ w[i - 15].rotate_right(18) ^ (w[i - 15] >> 3);
            let s1 = w[i - 2].rotate_right(17) ^ w[i - 2].rotate_right(19) ^ (w[i - 2] >> 10);
            w[i] = w[i - 16]
                .wrapping_add(s0)
                .wrapping_add(w[i - 7])
                .wrapping_add(s1);
        }

        let [mut a, mut b, mut c, mut d, mut e, mut f, mut g, mut hh] = h;

        for i in 0..64 {
            let s1 = e.rotate_right(6) ^ e.rotate_right(11) ^ e.rotate_right(25);
            let ch = (e & f) ^ ((!e) & g);
            let temp1 = hh
                .wrapping_add(s1)
                .wrapping_add(ch)
                .wrapping_add(K[i])
                .wrapping_add(w[i]);
            let s0 = a.rotate_right(2) ^ a.rotate_right(13) ^ a.rotate_right(22);
            let maj = (a & b) ^ (a & c) ^ (b & c);
            let temp2 = s0.wrapping_add(maj);

            hh = g;
            g = f;
            f = e;
            e = d.wrapping_add(temp1);
            d = c;
            c = b;
            b = a;
            a = temp1.wrapping_add(temp2);
        }

        h[0] = h[0].wrapping_add(a);
        h[1] = h[1].wrapping_add(b);
        h[2] = h[2].wrapping_add(c);
        h[3] = h[3].wrapping_add(d);
        h[4] = h[4].wrapping_add(e);
        h[5] = h[5].wrapping_add(f);
        h[6] = h[6].wrapping_add(g);
        h[7] = h[7].wrapping_add(hh);
    }

    let mut result = [0u8; 32];
    for i in 0..8 {
        result[i * 4..i * 4 + 4].copy_from_slice(&h[i].to_be_bytes());
    }
    result
}

/// 获取当前 Unix 时间戳（秒）
fn current_unix_secs() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs()
}

/// 简易 glob 模式匹配（支持 `*` 匹配任意字符序列，`?` 匹配单个字符）
fn glob_match(pattern: &str, text: &str) -> bool {
    glob_match_inner(pattern.as_bytes(), text.as_bytes())
}

fn glob_match_inner(pattern: &[u8], text: &[u8]) -> bool {
    let mut pi = 0;
    let mut ti = 0;
    let mut star_pi = usize::MAX;
    let mut star_ti = 0;

    while ti < text.len() {
        if pi < pattern.len() && (pattern[pi] == b'?' || pattern[pi] == text[ti]) {
            pi += 1;
            ti += 1;
        } else if pi < pattern.len() && pattern[pi] == b'*' {
            star_pi = pi;
            star_ti = ti;
            pi += 1;
        } else if star_pi != usize::MAX {
            pi = star_pi + 1;
            star_ti += 1;
            ti = star_ti;
        } else {
            return false;
        }
    }

    while pi < pattern.len() && pattern[pi] == b'*' {
        pi += 1;
    }

    pi == pattern.len()
}

// ===========================================================================
// 单元测试
// ===========================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_acl_state_new() {
        let state = AclState::new();
        assert!(state.users.contains_key("default"));
        assert_eq!(state.default_user, "default");
        let default_user = state.users.get("default").unwrap();
        assert!(default_user.enabled);
        assert!(default_user.commands.all_commands);
        assert_eq!(default_user.keys, vec!["*"]);
    }

    #[test]
    fn test_create_and_list_users() {
        let mut state = AclState::new();
        state.create_user("alice").unwrap();
        state.create_user("bob").unwrap();

        let mut users = state.list_users();
        users.sort();
        assert_eq!(users, vec!["alice", "bob", "default"]);
    }

    #[test]
    fn test_enable_disable_user() {
        let mut state = AclState::new();
        state.create_user("alice").unwrap();

        // 新用户默认禁用
        assert!(!state.get_user("alice").unwrap().enabled);

        state.enable_user("alice").unwrap();
        assert!(state.get_user("alice").unwrap().enabled);

        state.disable_user("alice").unwrap();
        assert!(!state.get_user("alice").unwrap().enabled);
    }

    #[test]
    fn test_cannot_disable_default() {
        let mut state = AclState::new();
        let result = state.disable_user("default");
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("cannot be disabled"));
    }

    #[test]
    fn test_cannot_delete_default() {
        let mut state = AclState::new();
        let result = state.delete_user("default");
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("cannot be removed"));
    }

    #[test]
    fn test_delete_user() {
        let mut state = AclState::new();
        state.create_user("alice").unwrap();
        assert!(state.delete_user("alice").unwrap());
        assert!(!state.delete_user("alice").unwrap()); // 再删一次返回 false
        assert!(state.get_user("alice").is_none());
    }

    #[test]
    fn test_password_authentication() {
        let mut state = AclState::new();
        state.create_user("alice").unwrap();
        state.enable_user("alice").unwrap();
        state.set_password("alice", "secret123").unwrap();

        // 正确密码
        let result = state.authenticate("alice", "secret123", "127.0.0.1:12345");
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), "alice");

        // 错误密码
        let result = state.authenticate("alice", "wrong", "127.0.0.1:12345");
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("invalid username-password"));
    }

    #[test]
    fn test_multi_password() {
        let mut state = AclState::new();
        state.create_user("alice").unwrap();
        state.enable_user("alice").unwrap();
        state.set_password("alice", "pass1").unwrap();
        state.set_password("alice", "pass2").unwrap();

        assert!(state.authenticate("alice", "pass1", "").is_ok());
        assert!(state.authenticate("alice", "pass2", "").is_ok());
        assert!(state.authenticate("alice", "pass3", "").is_err());
    }

    #[test]
    fn test_remove_password() {
        let mut state = AclState::new();
        state.create_user("alice").unwrap();
        state.enable_user("alice").unwrap();
        state.set_password("alice", "pass1").unwrap();
        state.set_password("alice", "pass2").unwrap();

        state.remove_password("alice", "pass1").unwrap();
        assert!(state.authenticate("alice", "pass1", "").is_err());
        assert!(state.authenticate("alice", "pass2", "").is_ok());
    }

    #[test]
    fn test_clear_passwords() {
        let mut state = AclState::new();
        state.create_user("alice").unwrap();
        state.enable_user("alice").unwrap();
        state.set_password("alice", "pass1").unwrap();

        state.clear_passwords("alice").unwrap();
        // 无密码用户用空密码认证
        assert!(state.authenticate("alice", "", "").is_ok());
    }

    #[test]
    fn test_disabled_user_auth_fails() {
        let mut state = AclState::new();
        state.create_user("alice").unwrap();
        // alice 未启用
        let result = state.authenticate("alice", "", "");
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("disabled"));
    }

    #[test]
    fn test_unknown_user_auth_fails() {
        let mut state = AclState::new();
        let result = state.authenticate("nobody", "x", "");
        assert!(result.is_err());
    }

    #[test]
    fn test_command_permission() {
        let mut state = AclState::new();
        state.create_user("alice").unwrap();
        state.enable_user("alice").unwrap();

        // 默认无权限
        assert!(state.check_command("alice", "get", "").is_err());

        // 允许 get
        state.allow_command("alice", "get").unwrap();
        assert!(state.check_command("alice", "get", "").is_ok());
        assert!(state.check_command("alice", "set", "").is_err());

        // 禁止 get
        state.deny_command("alice", "get").unwrap();
        assert!(state.check_command("alice", "get", "").is_err());
    }

    #[test]
    fn test_allow_all_commands() {
        let mut state = AclState::new();
        state.create_user("alice").unwrap();
        state.enable_user("alice").unwrap();

        state.allow_all_commands("alice").unwrap();
        assert!(state.check_command("alice", "get", "").is_ok());
        assert!(state.check_command("alice", "set", "").is_ok());
        assert!(state.check_command("alice", "del", "").is_ok());
    }

    #[test]
    fn test_deny_all_commands() {
        let mut state = AclState::new();
        state.create_user("alice").unwrap();
        state.enable_user("alice").unwrap();

        state.deny_all_commands("alice").unwrap();
        assert!(state.check_command("alice", "get", "").is_err());
        assert!(state.check_command("alice", "ping", "").is_err());
    }

    #[test]
    fn test_key_pattern_restriction() {
        let mut state = AclState::new();
        state.create_user("alice").unwrap();

        state
            .set_key_patterns("alice", vec!["cache:*".to_string()])
            .unwrap();
        let user = state.get_user("alice").unwrap();
        assert!(user.is_key_allowed(b"cache:foo"));
        assert!(user.is_key_allowed(b"cache:bar"));
        assert!(!user.is_key_allowed(b"user:123"));
        assert!(!user.is_key_allowed(b"other"));
    }

    #[test]
    fn test_key_pattern_wildcard() {
        let mut state = AclState::new();
        state.create_user("alice").unwrap();

        state
            .set_key_patterns("alice", vec!["*".to_string()])
            .unwrap();
        let user = state.get_user("alice").unwrap();
        assert!(user.is_key_allowed(b"anything"));
        assert!(user.is_key_allowed(b""));
    }

    #[test]
    fn test_channel_pattern_restriction() {
        let mut state = AclState::new();
        state.create_user("alice").unwrap();

        state
            .set_channel_patterns("alice", vec!["news:*".to_string()])
            .unwrap();
        let user = state.get_user("alice").unwrap();
        assert!(user.is_channel_allowed("news:sports"));
        assert!(!user.is_channel_allowed("chat:general"));
    }

    #[test]
    fn test_glob_match() {
        assert!(glob_match("*", "anything"));
        assert!(glob_match("cache:*", "cache:foo"));
        assert!(glob_match("cache:*", "cache:"));
        assert!(!glob_match("cache:*", "other:foo"));
        assert!(glob_match("h?llo", "hello"));
        assert!(glob_match("h?llo", "hallo"));
        assert!(!glob_match("h?llo", "hllo"));
        assert!(!glob_match("h?llo", "heello"));
        assert!(glob_match("test*", "test"));
        assert!(glob_match("test*", "testing"));
    }

    #[test]
    fn test_sha256_hex() {
        // 已知的 SHA-256 测试向量
        let hash = sha256_hex("");
        assert_eq!(
            hash,
            "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
        );

        let hash = sha256_hex("abc");
        assert_eq!(
            hash,
            "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"
        );
    }

    #[test]
    fn test_acl_log() {
        let mut state = AclState::new();
        state.create_user("alice").unwrap();
        // 触发认证失败日志
        let _ = state.authenticate("alice", "wrong", "127.0.0.1:1");
        let _ = state.authenticate("nobody", "x", "127.0.0.1:2");

        let log = state.get_log(None);
        assert_eq!(log.len(), 2);
        assert_eq!(log[0].event_type, "auth");
        assert_eq!(log[0].username, "alice");
        assert_eq!(log[1].username, "nobody");
    }

    #[test]
    fn test_acl_log_max_len() {
        let mut state = AclState::new();
        state.log_max_len = 3;

        state.create_user("alice").unwrap();
        for _ in 0..5 {
            let _ = state.authenticate("alice", "wrong", "");
        }

        let log = state.get_log(None);
        assert_eq!(log.len(), 3); // 最多保留 3 条
    }

    #[test]
    fn test_clear_log() {
        let mut state = AclState::new();
        state.create_user("alice").unwrap();
        let _ = state.authenticate("alice", "wrong", "");
        assert!(!state.get_log(None).is_empty());

        state.clear_log();
        assert!(state.get_log(None).is_empty());
    }

    #[test]
    fn test_set_user_rules() {
        let mut state = AclState::new();
        let rules = vec![
            "on".to_string(),
            ">mypassword".to_string(),
            "+get".to_string(),
            "+set".to_string(),
            "-del".to_string(),
            "~cache:*".to_string(),
        ];
        state.set_user_rules("alice", &rules).unwrap();

        let user = state.get_user("alice").unwrap();
        assert!(user.enabled);
        assert!(user.has_password());
        assert!(user.commands.is_command_allowed("get"));
        assert!(user.commands.is_command_allowed("set"));
        assert!(!user.commands.is_command_allowed("del"));
        assert!(user.is_key_allowed(b"cache:foo"));
        assert!(!user.is_key_allowed(b"user:123"));

        // 认证验证
        assert!(state.authenticate("alice", "mypassword", "").is_ok());
    }

    #[test]
    fn test_set_user_rules_resetkeys() {
        let mut state = AclState::new();
        let rules = vec!["on".to_string(), "~*".to_string()];
        state.set_user_rules("alice", &rules).unwrap();
        assert!(state.get_user("alice").unwrap().is_key_allowed(b"any"));

        let rules2 = vec!["resetkeys".to_string()];
        state.set_user_rules("alice", &rules2).unwrap();
        assert!(!state.get_user("alice").unwrap().is_key_allowed(b"any"));
    }

    #[test]
    fn test_acl_list_output() {
        let mut state = AclState::new();
        state.create_user("alice").unwrap();
        state.enable_user("alice").unwrap();
        state.set_password("alice", "secret").unwrap();
        state.allow_command("alice", "get").unwrap();
        state
            .set_key_patterns("alice", vec!["cache:*".to_string()])
            .unwrap();

        let info = state.get_user("alice").unwrap().to_acl_list_string();
        assert!(info.starts_with("user alice"));
        assert!(info.contains("on"));
        assert!(info.contains("+get"));
        assert!(info.contains("cache:*"));
    }

    #[test]
    fn test_info_acl() {
        let mut state = AclState::new();
        state.create_user("alice").unwrap();
        state.enable_user("alice").unwrap();
        state.create_user("bob").unwrap(); // 禁用

        let info = state.info_acl();
        assert!(info.contains("acl_users_total:3"));
        assert!(info.contains("acl_users_enabled:2"));
        assert!(info.contains("acl_users_disabled:1"));
    }

    #[test]
    fn test_save_and_load() {
        let mut state = AclState::new();
        state.create_user("alice").unwrap();
        state.enable_user("alice").unwrap();
        state.set_password("alice", "pass1").unwrap();
        state.allow_command("alice", "get").unwrap();

        let saved = state.save_to_string();
        assert!(!saved.is_empty());

        // 加载到新状态
        let mut state2 = AclState::new();
        state2.load_from_string(&saved).unwrap();

        let alice = state2.get_user("alice").unwrap();
        assert!(alice.enabled);
        assert!(alice.commands.is_command_allowed("get"));
    }

    #[test]
    fn test_unknown_command_rule() {
        let mut state = AclState::new();
        state.create_user("alice").unwrap();
        let rules = vec!["bogus_rule".to_string()];
        let result = state.set_user_rules("alice", &rules);
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("unknown ACL rule"));
    }

    #[test]
    fn test_default_user_no_password_auth() {
        let mut state = AclState::new();
        // 默认用户无密码，空密码即可认证
        let result = state.authenticate("default", "", "");
        assert!(result.is_ok());
    }
}
