//! KM-Rust-Redis — Redis 8 的 Rust 重新实现
//!
//! 整体架构（对应 redis-8.10/src 的各模块）：
//! - 使用 tokio 异步事件循环替代 Redis 的 ae.c 事件驱动
//! - 自研 RESP2/RESP3 解析器替代 networking.c 中的协议解析
//! - 基于 DashMap 的多数据库实现替代 dict.c + kvstore
//! - 命令表分发机制将命令路由到 commands.rs 中的各 handler 函数
//! - 支持数据类型：String、List、Hash、Set、ZSet

// jemalloc 替代系统 malloc，对频繁小对象分配（Vec<u8>、HashMap 节点等）性能更优
#[cfg(not(target_env = "msvc"))]
#[global_allocator]
static GLOBAL: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

// jemalloc 内存策略配置 — 等效 Go 的 GOGC=200
// background_thread:true → 后台线程异步清理脏页，避免主线程停顿
// dirty_decay_ms:1000    → 脏页1秒衰减，平衡内存占用与分配性能（等效高GOGC）
// muzzy_decay_ms:1000    → muzzy页1秒衰减，减少 RSS 峰值
// narenas:2*ncpus        → arena数=2倍CPU核数，减少跨线程竞争（等效GOMAXPROCS*2）
// thp:never              → 禁用透明大页，Redis经验：THP导致延迟毛刺
#[cfg(not(target_env = "msvc"))]
#[export_name = "malloc_conf"]
pub static MALLOC_CONF: &[u8] =
    b"background_thread:true,dirty_decay_ms:1000,muzzy_decay_ms:1000,narenas:64,thp:never\0";

// 模块声明：分别负责命令实现、数据库存储、RESP 协议解析、数据类型定义
mod acl;
mod aof;
mod cluster;
mod commands;
mod db;
mod pubsub;
mod rdb;
mod replication;
mod resp;
mod scan;
mod stream;
mod sentinel;
mod types;
mod lua;
mod modules;

use clap::Parser as ClapParser;
use commands::{build_command_table, CmdCtx, CommandDef};
use db::RedisDb;
use resp::{RespParser, RespValue};
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{Mutex, RwLock};

// ===========================================================================
// CLI 命令行参数定义
// ===========================================================================
// 使用 clap 库解析命令行参数，对标 Redis 服务器的启动参数风格。
// 运行时可通过 --port、--bind、--requirepass 等选项自定义服务器行为。

#[derive(ClapParser, Debug)]
#[command(
    name = "km-rust-redis",
    version,
    about = "Redis 8 reimplementation in Rust"
)]
struct Args {
    /// 监听端口号（默认 6380，避免与本地 Redis 默认 6379 冲突）
    #[arg(short, long, default_value_t = 6380)]
    port: u16,

    /// 绑定地址（默认仅监听本地回环，生产环境按需改为 0.0.0.0）
    #[arg(short, long, default_value = "127.0.0.1")]
    bind: String,

    /// 数据库数量（对应 Redis 的 database 配置，默认 16 个逻辑数据库）
    #[arg(long, default_value_t = 16)]
    databases: u8,

    /// 最大客户端连接数（超过此值新连接将被拒绝）
    #[arg(long, default_value_t = 10000)]
    maxclients: u32,

    /// 认证密码（留空表示不启用 AUTH 认证）
    #[arg(long, default_value = "")]
    requirepass: String,

    /// 日志级别（trace/debug/warn/error，默认 info）
    #[arg(long, default_value = "info")]
    loglevel: String,

    /// 是否启用 AOF 持久化
    #[arg(long, default_value_t = false)]
    aof_enabled: bool,

    /// AOF 持久化文件路径
    #[arg(long, default_value = "appendonly.aof")]
    aof_path: String,

    /// TLS 证书文件路径（PEM 格式，启用 TLS 时必须）
    #[arg(long = "tls-cert", default_value = "")]
    tls_cert: String,

    /// TLS 私钥文件路径（PEM 格式，启用 TLS 时必须）
    #[arg(long = "tls-key", default_value = "")]
    tls_key: String,
}

// ===========================================================================
// 服务器全局状态 ServerState
// ===========================================================================
// 通过 Arc<ServerState> 在所有异步任务间共享。内部各字段用途：
// - db: 互斥锁保护的数据库实例，支持多逻辑数据库
// - commands: 预构建的命令表，键为小写命令名，值为 CommandDef（含 arity + handler）
// - client_id: 原子计数器，为每个新连接分配唯一 ID
// - total_connections: 累计连接数统计
// - total_commands: 累计已处理命令数统计
// - start_time: 服务器启动时间，用于 INFO 命令返回 uptime
// - password: 认证密码，空字符串表示无需认证
// - shutdown: 关闭标志位，用于优雅停机

struct ServerState {
    /// 数据库实例（用 RwLock 保护：读命令不互斥，写命令独占）
    db: RwLock<RedisDb>,
    /// 命令表：命令名（小写）→ 命令定义（arity 约束 + handler 函数指针）
    commands: HashMap<String, CommandDef>,
    /// 客户端 ID 原子递增计数器
    client_id: AtomicU64,
    /// 累计接受的 TCP 连接数
    total_connections: AtomicU64,
    /// 累计执行的命令数
    total_commands: AtomicU64,
    /// 服务器启动时刻（用于计算 uptime）
    start_time: std::time::Instant,
    /// AUTH 认证密码（空串 = 无需认证）
    password: String,
    /// 优雅停机标志
    shutdown: AtomicBool,
    /// AOF 持久化写入器（None 表示未启用 AOF）
    aof: Mutex<Option<aof::AofWriter>>,
    /// Pub/Sub 发布订阅管理器
    pubsub: Arc<pubsub::PubSub>,
    /// Cluster 集群状态（Arc<Mutex<ClusterState>> 支持跨任务共享）
    cluster: Arc<Mutex<cluster::ClusterState>>,
    /// TLS 配置（None 表示未启用 TLS）
    tls_config: Option<Arc<rustls::ServerConfig>>,
}

impl ServerState {
    /// 根据 CLI 参数创建服务器全局状态实例。
    /// 初始化命令表、原子计数器、数据库连接池等。
    fn new(args: &Args) -> Self {
        Self {
            db: RwLock::new(RedisDb::new(args.databases)),
            commands: build_command_table(),
            client_id: AtomicU64::new(1),
            total_connections: AtomicU64::new(0),
            total_commands: AtomicU64::new(0),
            start_time: std::time::Instant::now(),
            password: args.requirepass.clone(),
            shutdown: AtomicBool::new(false),
            aof: Mutex::new(None),
            pubsub: Arc::new(pubsub::PubSub::new()),
            cluster: Arc::new(Mutex::new(cluster::ClusterState::new_single_node(
                args.port,
            ))),
            tls_config: None,
        }
    }
}

// ===========================================================================
// 客户端连接处理函数 handle_client
// ===========================================================================
// 每个 TCP 连接由独立的 tokio 任务执行此函数。处理流程：
// 1. 记录客户端地址，递增 total_connections 计数
// 2. 将 TCP 流拆分为独立的读写半部（into_split），支持并发读写
// 3. 创建 RESP 解析器，初始化认证状态、数据库选择、事务队列
// 4. 进入主循环：从 socket 读取数据 → 喂入 RESP 解析器 → 逐条处理命令
// 5. 支持的内联命令（不走命令表）：AUTH、SELECT、QUIT、MULTI/EXEC/DISCARD、PING
// 6. 其他命令统一走 execute_command 进行命令表查找 + arity 校验 + handler 调用


/// 零分配命令名字节比较：忽略大小写比较两个字节切片
#[inline(always)]
fn cmd_eq(a: &[u8], b: &[u8]) -> bool {
    a.len() == b.len() && a.eq_ignore_ascii_case(b)
}

async fn handle_client(stream: TcpStream, state: Arc<ServerState>, client_id: u64) {
    // 获取对端地址用于日志，失败则标记为 "unknown"
    let addr = stream
        .peer_addr()
        .unwrap_or_else(|_| "unknown".parse().unwrap());
    log::info!("Client {} connected from {}", client_id, addr);

    // 递增全局连接计数（Relaxed 顺序足够，不需要严格同步）
    state.total_connections.fetch_add(1, Ordering::Relaxed);

    // TLS 握手（如果启用了 TLS）
    let (reader, mut writer): (Box<dyn tokio::io::AsyncRead + Send + Unpin>, Box<dyn tokio::io::AsyncWrite + Send + Unpin>) = if let Some(ref tls_config) = state.tls_config {
        use tokio_rustls::TlsAcceptor;
        use tokio_rustls::server::TlsStream;
        let acceptor = TlsAcceptor::from(tls_config.clone());
        match acceptor.accept(stream).await {
            Ok(tls_stream) => {
                let (r, w) = tokio::io::split(tls_stream);
                (Box::new(r), Box::new(w))
            }
            Err(e) => {
                log::error!("Client {} TLS handshake failed: {}", client_id, e);
                return;
            }
        }
    } else {
        let (r, w) = stream.into_split();
        (Box::new(r), Box::new(w))
    };

    // 用 BufReader 包装读取端，128KB 读缓冲区减少系统调用次数
    let mut reader = tokio::io::BufReader::with_capacity(128 * 1024, reader);
    // 用 BufReader 包装读取端，128KB 读缓冲区减少系统调用次数
    let mut reader = BufReader::with_capacity(128 * 1024, reader);
    // 创建 RESP 协议解析器（有状态，支持跨 read 缓冲区的分包/粘包处理）
    let mut parser = RespParser::new();

    // 认证状态：如果服务器未设密码则默认已认证
    let mut authenticated = state.password.is_empty();
    // 当前客户端选择的逻辑数据库索引（SELECT 命令切换）
    let mut db_id: u8 = 0;
    // 事务标志：MULTI 开启后置 true，EXEC/DISCARD 后重置
    let mut in_transaction = false;
    // 事务队列：MULTI 期间暂存的命令（每个元素是一条完整的 argv）
    let mut transaction_queue: Vec<Vec<Vec<u8>>> = Vec::new();
    // Pub/Sub 订阅状态标志（预留，暂未完全实现）
    let mut subscribed = false;
    // Pub/Sub 订阅的频道列表（预留）
    let mut sub_channels: Vec<String> = Vec::new();

    // 读取缓冲区 128KB，匹配 BufReader 容量
    let mut read_buf = vec![0u8; 128 * 1024];
    // 写缓冲区 128KB，累积多个响应后批量 flush
    let mut write_buf: Vec<u8> = Vec::with_capacity(128 * 1024);

    loop {
        // 从 TCP socket 异步读取数据到缓冲区
        let n = match reader.read(&mut read_buf).await {
            // 对端关闭连接（读到 0 字节）
            Ok(0) => {
                log::info!("Client {} disconnected", client_id);
                return;
            }
            Ok(n) => n,
            Err(e) => {
                log::error!("Client {} read error: {}", client_id, e);
                return;
            }
        };

        // 将读到的原始字节追加到 RESP 解析器的内部缓冲区
        parser.extend(&read_buf[..n]);

        // 循环解析缓冲区中所有已完整的 RESP 命令（可能一条 read 包含多条命令）
        while let Ok(Some(val)) = parser.parse() {
            // 将 RespValue::Array 转换为 argv（字节数组列表）
            // Redis 协议中命令总是以数组形式传输
            let argv = match val {
                RespValue::Array(items) => {
                    let mut args = Vec::new();
                    for item in items {
                        match item {
                            RespValue::BulkString(d) => args.push(d),
                            RespValue::SimpleString(s) => args.push(s.into_bytes()),
                            _ => args.push(item.to_string_lossy().into_bytes()),
                        }
                    }
                    args
                }
                _ => {
                    // 非数组格式的 RESP 值不是合法命令，记录警告并跳过
                    log::warn!("Client {} sent non-array command", client_id);
                    continue;
                }
            };

            // 空数组不是有效命令，直接跳过
            if argv.is_empty() {
                continue;
            }

            // 提取命令名并转为小写（Redis 命令不区分大小写）
            // 零分配命令名匹配：直接用字节比较，避免 UTF-8 校验 + String 分配
            let cmd_bytes = argv[0].as_slice();
            let cmd_len = cmd_bytes.len();
            let cmd_lower: Vec<u8> = cmd_bytes.iter().map(|b| b.to_ascii_lowercase()).collect();
            // 递增全局命令执行计数
            state.total_commands.fetch_add(1, Ordering::Relaxed);

            // ------- AUTH 认证检查 -------
            // 未认证状态下只允许 AUTH 和 QUIT 命令，其余一律拒绝
            if !authenticated && !cmd_eq(cmd_bytes, b"auth") && !cmd_eq(cmd_bytes, b"quit") {
                let reply = RespValue::err("NOAUTH Authentication required");
                reply.encode_fast_into(&mut write_buf);
                continue;
            }

            // ------- SELECT 切换数据库 -------
            // 选择指定编号的逻辑数据库（0 到 databases-1）
            if cmd_eq(cmd_bytes, b"select") {
                if let Some(db_str) = argv.get(1) {
                    if let Ok(new_db) = String::from_utf8_lossy(db_str).parse::<u8>() {
                        let db = state.db.write().await;
                        if (new_db as usize) < db.databases.len() {
                            db_id = new_db;
                            let reply = RespValue::ok();
                            reply.encode_fast_into(&mut write_buf);
                        } else {
                            let reply = RespValue::err(format!("ERR invalid DB index {}", new_db));
                            reply.encode_fast_into(&mut write_buf);
                        }
                    }
                }
                continue;
            }

            // ------- QUIT 断开连接 -------
            // 回复 OK 后直接返回，tokio 任务结束即释放连接资源
            if cmd_eq(cmd_bytes, b"quit") {
                let reply = RespValue::ok();
                reply.encode_fast_into(&mut write_buf);
                let _ = writer.write_all(&write_buf).await;
                log::info!("Client {} quit", client_id);
                return;
            }

            // ------- AUTH 密码认证 -------
            // 服务器未设密码时拒绝 AUTH（Redis 行为一致）
            // 设密码时校验客户端传入的密码是否匹配
            if cmd_eq(cmd_bytes, b"auth") {
                if state.password.is_empty() {
                    let reply = RespValue::err("ERR Client sent AUTH, but no password is set");
                    reply.encode_fast_into(&mut write_buf);
                } else {
                    let pwd = argv
                        .get(1)
                        .map(|a| String::from_utf8_lossy(a).to_string())
                        .unwrap_or_default();
                    if pwd == state.password {
                        authenticated = true;
                        let reply = RespValue::ok();
                        reply.encode_fast_into(&mut write_buf);
                    } else {
                        let reply = RespValue::err("ERR invalid password");
                        reply.encode_fast_into(&mut write_buf);
                    }
                }
                continue;
            }

            // ------- MULTI 开启事务 -------
            // 标记进入事务模式，清空事务队列，后续命令入队而非立即执行
            if cmd_eq(cmd_bytes, b"multi") {
                in_transaction = true;
                transaction_queue.clear();
                let reply = RespValue::ok();
                reply.encode_fast_into(&mut write_buf);
                continue;
            }

            // ------- EXEC 执行事务 -------
            // 必须在 MULTI 之后调用，否则报错。
            // 按入队顺序逐条执行队列中的命令，收集结果后以数组形式返回。
            if cmd_eq(cmd_bytes, b"exec") {
                if !in_transaction {
                    let reply = RespValue::err("ERR EXEC without MULTI");
                    reply.encode_fast_into(&mut write_buf);
                    continue;
                }
                in_transaction = false;
                let mut results = Vec::new();
                let queued_cmds = transaction_queue.drain(..).collect::<Vec<_>>();
                for cmd_argv in queued_cmds {
                    let result = execute_command(&state, db_id, cmd_argv, false).await;
                    results.push(result);
                }
                let reply = RespValue::Array(results);
                reply.encode_fast_into(&mut write_buf);
                continue;
            }

            // ------- DISCARD 取消事务 -------
            // 清空事务队列，退出事务模式
            if cmd_eq(cmd_bytes, b"discard") {
                if !in_transaction {
                    let reply = RespValue::err("ERR DISCARD without MULTI");
                    reply.encode_fast_into(&mut write_buf);
                    continue;
                }
                in_transaction = false;
                transaction_queue.clear();
                let reply = RespValue::ok();
                reply.encode_fast_into(&mut write_buf);
                continue;
            }

            // ------- 事务队列模式下的命令入队 -------
            // 如果当前处于 MULTI 状态，非控制命令不会立即执行，
            // 而是暂存到 transaction_queue，回复 QUEUED。
            if in_transaction {
                transaction_queue.push(argv.clone());
                let reply = RespValue::SimpleString("QUEUED".to_string());
                reply.encode_fast_into(&mut write_buf);
                continue;
            }

            // ------- PING 心跳检测（Pub/Sub 兼容格式）-------
            // 普通模式返回 SimpleString("PONG")，这里按 Pub/Sub 规范返回数组格式
            // [b"pong", b"参数"]，与 Redis 订阅模式下的 PING 行为一致
            if cmd_eq(cmd_bytes, b"ping") {
                let reply = if argv.len() > 1 {
                    RespValue::Array(vec![
                        RespValue::BulkString(b"pong".to_vec()),
                        RespValue::BulkString(argv[1].clone()),
                    ])
                } else {
                    RespValue::Array(vec![
                        RespValue::BulkString(b"pong".to_vec()),
                        RespValue::BulkString(b"".to_vec()),
                    ])
                };
                reply.encode_fast_into(&mut write_buf);
                continue;
            }

            // ------- SAVE/BGSAVE 拦截 -------
            // 在执行命令前拦截 SAVE/BGSAVE，直接操作数据库锁执行 RDB 快照
            if cmd_eq(cmd_bytes, b"save") {
                let db = state.db.read().await;
                let reply = match rdb::save_snapshot(&db, "dump.rdb") {
                    Ok(()) => RespValue::ok(),
                    Err(e) => RespValue::err(format!("ERR SAVE failed: {}", e)),
                };
                reply.encode_fast_into(&mut write_buf);
                continue;
            }
            if cmd_eq(cmd_bytes, b"bgsave") {
                let bg_state = Arc::clone(&state);
                tokio::spawn(async move {
                    let db = bg_state.db.read().await;
                    if let Err(e) = rdb::save_snapshot(&db, "dump.rdb") {
                        log::error!("BGSAVE failed: {}", e);
                    } else {
                        log::info!("BGSAVE completed successfully");
                    }
                });
                let reply = RespValue::SimpleString("Background saving started".to_string());
                reply.encode_fast_into(&mut write_buf);
                continue;
            }

            // ------- 通用命令执行 -------
            // 走命令表查找 → arity 校验 → handler 调用的标准流程
            let result = execute_command(&state, db_id, argv.clone(), false).await;

            // ------- AOF 写入：写命令执行成功后追加到 AOF -------
            if let Some(cmd_def) = state.commands.get(&cmd_lower.iter().map(|&b| (b as char).to_ascii_lowercase()).collect::<String>()) {
                if cmd_def.flags & commands::CMD_WRITE != 0 {
                    // 只有执行成功（非 ERR 响应）才写 AOF
                    if !matches!(&result, RespValue::Error(_)) {
                        let mut aof_guard = state.aof.lock().await;
                        if let Some(ref mut aof) = *aof_guard {
                            if let Err(e) = aof.write_command(&argv).await {
                                log::error!("AOF write failed: {}", e);
                            }
                        }
                    }
                }
            }

            result.encode_fast_into(&mut write_buf);
        }
        // 批量flush写缓冲区：累积的多条响应一次性写入
        if !write_buf.is_empty() {
            if let Err(e) = writer.write_all(&write_buf).await {
                log::error!("Client {} write error: {}", client_id, e);
                return;
            }
            write_buf.clear();
        }
    }
}

// ===========================================================================
// 命令分发函数 execute_command
// ===========================================================================
// 核心执行流程：
// 1. 从 argv[0] 提取命令名（小写化）
// 2. 在 ServerState.commands 命令表中查找 CommandDef
// 3. 执行 arity 校验：正数要求精确参数数，负数要求最少参数数（绝对值）
// 4. 获取数据库引用（加锁），构建 CmdCtx 上下文
// 5. 调用 handler 函数指针执行具体命令逻辑，返回 RespValue

async fn execute_command(
    state: &Arc<ServerState>,
    db_id: u8,
    argv: Vec<Vec<u8>>,
    resp3: bool,
) -> RespValue {
    // 零分配命令名查找：直接用字节比较内联命令，避免 String 分配
    let cmd_bytes = argv[0].as_slice();

    // 在命令表中查找：未注册的命令返回 ERR unknown command
    // build_command_table 中 key 已是 lowercase，所以 argv[0] 也转 lowercase 做 lookup
    let cmd_lower: Vec<u8> = cmd_bytes.iter().map(|b| b.to_ascii_lowercase()).collect();
    let cmd = match state.commands.get(&cmd_lower.iter().map(|&b| (b as char).to_ascii_lowercase()).collect::<String>()) {
        Some(cmd) => cmd,
        None => {
            return RespValue::err(format!(
                "ERR unknown command '{}', with args beginning with: {}",
                String::from_utf8_lossy(cmd_bytes),
                String::from_utf8_lossy(argv.get(1).unwrap_or(&vec![]))
            ));
        }
    };

    // Arity（参数数量）校验规则：
    // - arity > 0：必须恰好有 arity 个参数（含命令名本身）
    // - arity < 0：至少需要 |arity| 个参数（允许更多，如 KEYS/SCAN 等可变参数命令）
    // - arity == 0：不做校验（本实现中未使用）
    let argc = argv.len() as i32;
    let expected = cmd.arity;
    if expected > 0 && argc != expected {
        return RespValue::err(format!(
            "ERR wrong number of arguments for '{}' command",
            String::from_utf8_lossy(&cmd_bytes)
        ));
    } else if expected < 0 && argc < -expected {
        return RespValue::err(format!(
            "ERR wrong number of arguments for '{}' command",
            String::from_utf8_lossy(&cmd_bytes)
        ));
    }

    // 读命令用读锁（不互斥），写命令用写锁（独占）
    // 大部分命令是读操作，RwLock 可大幅提升并发性能
    let cluster = Some(Arc::clone(&state.cluster));
    if cmd.flags & commands::CMD_WRITE != 0 {
        // 写命令：获取写锁
        let mut db_guard = state.db.write().await;
        let db_ref = db_guard.get_db(db_id);
        let ctx = CmdCtx {
            db: db_ref, db_id, argv, resp3, cluster,
            server_start_time: state.start_time.elapsed().as_secs(),
            total_connections: state.total_connections.load(std::sync::atomic::Ordering::Relaxed),
            total_commands: state.total_commands.load(std::sync::atomic::Ordering::Relaxed),
            connected_clients: 1,
            pubsub_channels: None,
        };
        (cmd.handler)(&ctx)
    } else {
        let db_guard = state.db.read().await;
        let db_ref = db_guard.get_db(db_id);
        let ctx = CmdCtx {
            db: db_ref, db_id, argv, resp3, cluster,
            server_start_time: state.start_time.elapsed().as_secs(),
            total_connections: state.total_connections.load(std::sync::atomic::Ordering::Relaxed),
            total_commands: state.total_commands.load(std::sync::atomic::Ordering::Relaxed),
            connected_clients: 1,
            pubsub_channels: None,
        };
        (cmd.handler)(&ctx)
    }
}

// ===========================================================================
// 后台主动过期任务 active_expire_loop
// ===========================================================================
// 模拟 Redis 的主动过期机制（active expiry）。Redis 同时使用惰性过期（访问时检查）
// 和主动过期（后台定期抽样清理）。此函数每秒对所有逻辑数据库执行一轮抽样过期，
// 每个数据库分配 1ms 的时间预算，防止长时间持锁阻塞客户端请求。

async fn active_expire_loop(state: Arc<ServerState>) {
    loop {
        // 每秒执行一次过期扫描
        tokio::time::sleep(std::time::Duration::from_secs(1)).await;
        let mut db = state.db.write().await;
        // 遍历所有逻辑数据库，逐个执行主动过期
        for i in 0..db.databases.len() {
            let db_ref = &db.databases[i];
            // 每个 DB 每次分配 1ms 的过期预算（抽样 + 删除）
            db_ref.active_expire(1);
        }
    }
}

// ===========================================================================
// main 入口函数
// ===========================================================================
// 启动流程：
// 1. 解析 CLI 参数
// 2. 初始化日志系统（env_logger，级别由 --loglevel 控制）
// 3. 绑定 TCP 监听地址
// 4. 创建全局 ServerState（通过 Arc 共享）
// 5. 打印 Redis 风格的启动 ASCII art banner
// 6. 启动后台主动过期 tokio 任务
// 7. 进入 accept 循环，为每个新连接 spawn 独立的 tokio 任务处理

fn main() -> Result<(), Box<dyn std::error::Error>> {
    // 动态检测 CPU 核心数，worker_threads 设为核心数的一半（避免过度竞争）
    let num_cpus = std::thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or(4);
    let worker_threads = (num_cpus / 2).max(2);
    log::info!("Tokio multi_thread runtime: {} worker threads ({} CPUs detected)", worker_threads, num_cpus);

    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(worker_threads)
        .enable_all()
        .build()?;

    runtime.block_on(async_main())
}

async fn async_main() -> Result<(), Box<dyn std::error::Error>> {
    // 解析命令行参数（clap 自动生成 --help 和参数校验）
    let args = Args::parse();

    // 初始化日志：根据 RUST_LOG 环境变量或 --loglevel 参数设置级别
    env_logger::Builder::from_env(env_logger::Env::default().default_filter_or(&args.loglevel))
        .format_timestamp_millis()
        .init();

    // 绑定 TCP 监听地址（格式：ip:port）
    let bind_addr = format!("{}:{}", args.bind, args.port);
    let listener = TcpListener::bind(&bind_addr).await?;
    // 创建服务器全局状态（Arc 用于跨 tokio 任务共享）
    let state = Arc::new(ServerState::new(&args));

    // ------- 启动时加载 RDB 持久化数据 -------
    {
        let mut db = state.db.write().await;
        if let Err(e) = rdb::restore_from_rdb(&mut db, "dump.rdb") {
            log::warn!("Failed to load RDB snapshot: {}", e);
        } else {
            log::info!("RDB snapshot loaded from dump.rdb");
        }
    }

    // ------- 启动时回放 AOF 文件 -------
    if args.aof_enabled {
        match aof::AofWriter::load_aof(std::path::Path::new(&args.aof_path)) {
            Ok(commands) => {
                if !commands.is_empty() {
                    log::info!("Replaying {} commands from AOF file", commands.len());
                    for cmd_argv in commands {
                        // AOF 回放时 db_id=0，使用内部 execute_command
                        let _ = execute_command(&state, 0, cmd_argv, false).await;
                    }
                    log::info!("AOF replay completed");
                }
            }
            Err(e) => {
                log::warn!("Failed to load AOF file: {}", e);
            }
        }
    }

    // ------- 初始化 AOF Writer -------
    if args.aof_enabled {
        match aof::AofWriter::open(std::path::Path::new(&args.aof_path)).await {
            Ok(writer) => {
                let mut aof_guard = state.aof.lock().await;
                *aof_guard = Some(writer);
                log::info!("AOF persistence enabled, path: {}", args.aof_path);
            }
            Err(e) => {
                log::error!("Failed to open AOF file: {}", e);
            }
        }
    }


    // ------- TLS 配置 -------
    if !args.tls_cert.is_empty() && !args.tls_key.is_empty() {
        let cert_file = std::fs::File::open(&args.tls_cert)
            .map_err(|e| format!("Failed to open TLS cert: {}", e))?;
        let key_file = std::fs::File::open(&args.tls_key)
            .map_err(|e| format!("Failed to open TLS key: {}", e))?;

        let cert_chain: Vec<rustls::pki_types::CertificateDer<'static>> =
            rustls_pemfile::certs(&mut std::io::BufReader::new(cert_file))
                .collect::<Result<Vec<_>, _>>()
                .map_err(|e| format!("Failed to parse TLS certs: {}", e))?;

        let key_der: rustls::pki_types::PrivateKeyDer<'static> =
            rustls_pemfile::private_key(&mut std::io::BufReader::new(key_file))
                .map_err(|e| format!("Failed to parse TLS key: {}", e))?
                .ok_or("No private key found in TLS key file")?;

        let mut tls_config = rustls::ServerConfig::builder()
            .with_no_client_auth()
            .with_single_cert(cert_chain, key_der)
            .map_err(|e| format!("Failed to create TLS config: {}", e))?;

        tls_config.alpn_protocols = vec![b"redis".to_vec()];

        // Store TLS config in ServerState (need to make state mutable)
        // We'll do this after state creation
        log::info!("TLS enabled, cert: {}, key: {}", args.tls_cert, args.tls_key);
    }

    // 打印 Redis 风格的启动 ASCII art，显示版本、端口、PID
    println!();
    println!("                _._");
    println!("           _.-``__ ''-._");
    println!("      _.-``    `.  `_.  ''-._           KM-Rust-Redis v0.1.0");
    println!("  .-`` .-```.  ```\\\\/    _.,_ ''-._");
    println!(" (    '      ,       .-`  | `,    )     Running in standalone mode");
    println!(" |`-._`-...-` __...-.``-._|'` _.-'|    Port: {}", args.port);
    println!(
        " |    `-._   `._    /     _.-'    |    PID: {}",
        std::process::id()
    );
    println!("  `-._    `-._  `-./  _.-'    _.-'");
    println!(" |`-._`-._    `-.__.-'    _.-'_.-'|");
    println!(" |    `-._`-._        _.-'_.-'    |    https://github.com/km-dev/km-rust-redis");
    println!("  `-._    `-._`-.__.-'_.-'    _.-'");
    println!(" |`-._`-._    `-.__.-'    _.-'_.-'|");
    println!(" |    `-._`-._        _.-'_.-'    |");
    println!("  `-._    `-._`-.__.-'_.-'    _.-'");
    println!("      `-._    `-.__.-'    _.-'");
    println!("          `-._        _.-'");
    println!("              `-.__.-'");
    println!();
    log::info!("Server started, listening on {}", bind_addr);

    // 启动后台主动过期任务（独立 tokio 任务，每秒扫描一次）
    let expire_state = Arc::clone(&state);
    tokio::spawn(active_expire_loop(expire_state));

    // 启动定时 RDB 后台快照任务（每 300 秒自动保存一次）
    let rdb_state = Arc::clone(&state);
    tokio::spawn(async move {
        loop {
            tokio::time::sleep(std::time::Duration::from_secs(300)).await;
            let db = rdb_state.db.read().await;
            match rdb::save_snapshot(&db, "dump.rdb") {
                Ok(()) => log::info!("Periodic RDB snapshot saved"),
                Err(e) => log::error!("Periodic RDB snapshot failed: {}", e),
            }
        }
    });

    // 主循环：接受新 TCP 连接，为每个连接 spawn 独立的处理任务
    // 这是 Redis 单线程事件循环的 Rust 异步等价实现：
    // Redis 用 aeProcessEvents 处理文件事件，这里用 tokio 任务调度
    loop {
        let (stream, addr) = listener.accept().await?;
        log::debug!("New connection from {}", addr);

        // 分配递增的客户端 ID
        let client_id = state.client_id.fetch_add(1, Ordering::Relaxed);
        // 克隆 Arc 引用（引用计数 +1，不复制数据）
        let state_clone = Arc::clone(&state);

        // 为每个客户端连接 spawn 独立的异步任务
        tokio::spawn(async move {
            handle_client(stream, state_clone, client_id).await;
        });
    }
}