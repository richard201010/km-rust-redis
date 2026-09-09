//! Lua 脚本引擎模块（简化版）。
//!
//! 实现 Redis EVAL/EVALSHA/SCRIPT 命令支持。
//! 不依赖外部 Lua 解释器，通过正则解析 `redis.call(...)` 调用并转发到 Redis 命令处理器。
//!
//! # 支持的特性
//!
//! - `redis.call('COMMAND', arg1, arg2, ...)` — 解析并执行 Redis 命令
//! - SHA1 脚本缓存（SCRIPT LOAD / EVALSHA）
//! - KEYS[] 和 ARGV[] 参数替换
//!
//! # 限制
//!
//! - 不支持完整 Lua 语法（变量、循环、条件、函数定义等）
//! - 仅支持单个 `redis.call(...)` 调用
//! - 多个 redis.call 仅返回最后一个的结果

use std::collections::HashMap;
use std::sync::Mutex;

use sha1::{Digest, Sha1};

use crate::commands::CmdCtx;
use crate::resp::RespValue;

/// Lua 脚本引擎。
///
/// 管理脚本缓存（SHA1 → 脚本内容），提供 EVAL/EVALSHA/SCRIPT 系列操作。
pub struct LuaEngine {
    /// SHA1(hex string) → Lua 脚本源码
    scripts: Mutex<HashMap<String, String>>,
}

impl LuaEngine {
    /// 创建一个新的 Lua 脚本引擎。
    pub fn new() -> Self {
        Self {
            scripts: Mutex::new(HashMap::new()),
        }
    }

    /// 计算脚本的 SHA1 摘要（40 位十六进制小写字符串）。
    pub fn script_sha1(script: &str) -> String {
        let mut hasher = Sha1::new();
        hasher.update(script.as_bytes());
        let result = hasher.finalize();
        hex_encode(&result)
    }

    /// SCRIPT LOAD — 加载脚本到缓存并返回 SHA1。
    pub fn script_load(&self, script: &str) -> String {
        let sha = Self::script_sha1(script);
        let mut scripts = self.scripts.lock().unwrap();
        scripts.insert(sha.clone(), script.to_string());
        sha
    }

    /// SCRIPT EXISTS — 检查一个或多个 SHA1 对应的脚本是否已缓存。
    /// 返回与输入等长的 0/1 数组。
    pub fn script_exists(&self, shas: &[&str]) -> Vec<bool> {
        let scripts = self.scripts.lock().unwrap();
        shas.iter().map(|s| scripts.contains_key(*s)).collect()
    }

    /// SCRIPT FLUSH — 清空所有已缓存的脚本。
    pub fn script_flush(&self) {
        let mut scripts = self.scripts.lock().unwrap();
        scripts.clear();
    }

    /// 返回当前缓存的脚本数量。
    pub fn script_count(&self) -> usize {
        let scripts = self.scripts.lock().unwrap();
        scripts.len()
    }

    /// EVALSHA — 通过 SHA1 执行已缓存的脚本。
    pub fn evalsha(
        &self,
        sha: &str,
        numkeys: usize,
        keys: &[Vec<u8>],
        args: &[Vec<u8>],
        ctx: &CmdCtx,
    ) -> RespValue {
        let script = {
            let scripts = self.scripts.lock().unwrap();
            match scripts.get(sha) {
                Some(s) => s.clone(),
                None => {
                    return RespValue::err("NOSCRIPT No matching script. Use EVAL.");
                }
            }
        };
        self.eval_script(&script, numkeys, keys, args, ctx)
    }

    /// EVAL — 直接执行 Lua 脚本。
    ///
    /// 脚本会被自动缓存（与 Redis 行为一致）。
    pub fn eval(
        &self,
        script: &str,
        numkeys: usize,
        keys: &[Vec<u8>],
        args: &[Vec<u8>],
        ctx: &CmdCtx,
    ) -> RespValue {
        // 缓存脚本
        self.script_load(script);
        self.eval_script(script, numkeys, keys, args, ctx)
    }

    /// 内部：执行脚本核心逻辑。
    fn eval_script(
        &self,
        script: &str,
        numkeys: usize,
        keys: &[Vec<u8>],
        args: &[Vec<u8>],
        ctx: &CmdCtx,
    ) -> RespValue {
        // 1. 替换 KEYS[n] 和 ARGV[n]
        let expanded = expand_keys_argv(script, numkeys, keys, args);

        // 2. 解析所有 redis.call(...) / redis.pcall(...) 调用
        let calls = parse_redis_calls(&expanded);
        if calls.is_empty() {
            return RespValue::err("ERR No redis.call() found in script");
        }

        // 3. 依次执行，返回最后一个结果
        let mut result = RespValue::Null;
        for call in &calls {
            match execute_redis_call(call, ctx) {
                Ok(r) => result = r,
                Err(e) => {
                    // redis.pcall 不应 panic，redis.call 会传播错误
                    // 简化版统一返回错误
                    return RespValue::err(e);
                }
            }
        }
        result
    }
}

impl Default for LuaEngine {
    fn default() -> Self {
        Self::new()
    }
}

// ---------------------------------------------------------------------------
// 内部辅助函数
// ---------------------------------------------------------------------------

/// 将 SHA-256/SHA-1 的字节数组编码为十六进制小写字符串。
fn hex_encode(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{:02x}", b)).collect()
}

/// 替换脚本中的 `KEYS[n]` 和 `ARGV[n]` 为实际参数值。
///
/// - `KEYS[1]` → keys[0] 的字符串表示
/// - `ARGV[1]` → args[0] 的字符串表示
/// 索引从 1 开始（与 Redis 行为一致）。
fn expand_keys_argv(script: &str, numkeys: usize, keys: &[Vec<u8>], args: &[Vec<u8>]) -> String {
    let mut result = script.to_string();

    // 替换 KEYS[n]
    for i in 0..numkeys {
        let placeholder = format!("KEYS[{}]", i + 1);
        if let Some(key) = keys.get(i) {
            let val = String::from_utf8_lossy(key).to_string();
            result = result.replace(&placeholder, &format!("'{}'", val));
        }
    }

    // 替换 ARGV[n]
    for (i, arg) in args.iter().enumerate() {
        let placeholder = format!("ARGV[{}]", i + 1);
        let val = String::from_utf8_lossy(arg).to_string();
        result = result.replace(&placeholder, &format!("'{}'", val));
    }

    result
}

/// 解析出的单个 redis.call 调用。
struct RedisCall {
    /// 命令名（如 "GET", "SET"）
    command: String,
    /// 命令参数列表
    args: Vec<String>,
}

/// 从脚本文本中解析所有 `redis.call(...)` 和 `redis.pcall(...)` 调用。
///
/// 简化解析器：使用状态机匹配括号深度，支持字符串内的逗号/括号。
fn parse_redis_calls(script: &str) -> Vec<RedisCall> {
    let mut calls = Vec::new();
    let bytes = script.as_bytes();
    let len = bytes.len();
    let mut i = 0;

    while i < len {
        // 查找 "redis.call(" 或 "redis.pcall("
        let remaining = &script[i..];
        let start_offset =
            if remaining.starts_with("redis.call(") || remaining.starts_with("redis.pcall(") {
                if remaining.starts_with("redis.call(") {
                    11 // "redis.call(".len()
                } else {
                    12 // "redis.pcall(".len()
                }
            } else {
                i += 1;
                continue;
            };

        let call_start = i + start_offset;
        // 解析括号内的参数
        let mut depth = 1;
        let mut j = call_start;
        let mut in_string = false;
        let mut string_char = 0u8;

        while j < len && depth > 0 {
            let c = bytes[j];
            if in_string {
                if c == b'\\' {
                    j += 2; // 跳过转义字符
                    continue;
                }
                if c == string_char {
                    in_string = false;
                }
            } else {
                match c {
                    b'\'' | b'"' => {
                        in_string = true;
                        string_char = c;
                    }
                    b'(' => depth += 1,
                    b')' => depth -= 1,
                    _ => {}
                }
            }
            j += 1;
        }

        if depth == 0 {
            // j 现在指向 ) 之后，括号内内容为 call_start..j-1
            let inner = &script[call_start..j - 1];
            if let Some(parsed) = parse_call_args(inner) {
                calls.push(parsed);
            }
            i = j;
        } else {
            i += 1;
        }
    }

    calls
}

/// 解析 `redis.call(...)` 括号内的参数字符串。
///
/// 输入示例: `'SET', 'mykey', 'myvalue'` 或 `'GET', KEYS[1]`
/// （KEYS[n]/ARGV[n] 已在上层被替换为字符串字面量）
fn parse_call_args(inner: &str) -> Option<RedisCall> {
    let parts = split_call_args(inner);
    if parts.is_empty() {
        return None;
    }

    let command = parts[0].trim_matches(|c| c == '\'' || c == '"').to_string();
    let args: Vec<String> = parts[1..]
        .iter()
        .map(|s| s.trim().trim_matches(|c| c == '\'' || c == '"').to_string())
        .collect();

    Some(RedisCall { command, args })
}

/// 按逗号拆分参数，正确处理引号内的逗号。
fn split_call_args(inner: &str) -> Vec<String> {
    let mut parts = Vec::new();
    let mut current = String::new();
    let bytes = inner.as_bytes();
    let len = bytes.len();
    let mut i = 0;
    let mut in_string = false;
    let mut string_char = 0u8;

    while i < len {
        let c = bytes[i];
        if in_string {
            current.push(c as char);
            if c == b'\\' && i + 1 < len {
                i += 1;
                current.push(bytes[i] as char);
            } else if c == string_char {
                in_string = false;
            }
        } else {
            match c {
                b'\'' | b'"' => {
                    in_string = true;
                    string_char = c;
                    current.push(c as char);
                }
                b',' => {
                    if !current.trim().is_empty() {
                        parts.push(current.trim().to_string());
                    }
                    current.clear();
                }
                _ => {
                    current.push(c as char);
                }
            }
        }
        i += 1;
    }
    if !current.trim().is_empty() {
        parts.push(current.trim().to_string());
    }
    parts
}

/// 执行一个解析出的 redis.call，转发到对应的命令处理器。
fn execute_redis_call(call: &RedisCall, ctx: &CmdCtx) -> Result<RespValue, String> {
    // 构建 argv: [COMMAND, arg1, arg2, ...]
    let mut argv: Vec<Vec<u8>> = Vec::new();
    argv.push(call.command.to_ascii_uppercase().into_bytes());
    for arg in &call.args {
        argv.push(arg.as_bytes().to_vec());
    }

    // 复用 CmdCtx 的结构，创建一个临时上下文来执行
    let sub_ctx = CmdCtx {
        db: ctx.db,
        db_id: ctx.db_id,
        argv,
        resp3: ctx.resp3, cluster: None,
    };

    // 通过命令表查找处理器
    let table = crate::commands::build_command_table();
    let cmd_name = call.command.to_ascii_lowercase();

    match table.get(&cmd_name) {
        Some(cmd_def) => {
            // 参数数量校验
            if cmd_def.arity > 0 && (sub_ctx.argc() as i32) != cmd_def.arity {
                return Err(format!(
                    "ERR wrong number of arguments for '{}' command",
                    call.command
                ));
            }
            if cmd_def.arity < 0 && (sub_ctx.argc() as i32) < -cmd_def.arity {
                return Err(format!(
                    "ERR wrong number of arguments for '{}' command",
                    call.command
                ));
            }
            Ok((cmd_def.handler)(&sub_ctx))
        }
        None => Err(format!("ERR unknown command '{}'", call.command)),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_script_sha1() {
        let sha = LuaEngine::script_sha1("return 1");
        assert_eq!(sha.len(), 40);
        // 验证相同输入产生相同 SHA1
        assert_eq!(sha, LuaEngine::script_sha1("return 1"));
        // 验证不同输入产生不同 SHA1
        assert_ne!(sha, LuaEngine::script_sha1("return 2"));
    }

    #[test]
    fn test_script_load_and_exists() {
        let engine = LuaEngine::new();
        let sha = engine.script_load("return 1");
        assert_eq!(sha.len(), 40);

        let results = engine.script_exists(&[&sha, "nonexistent_sha"]);
        assert_eq!(results, vec![true, false]);
    }

    #[test]
    fn test_script_flush() {
        let engine = LuaEngine::new();
        engine.script_load("return 1");
        engine.script_load("return 2");
        assert_eq!(engine.script_count(), 2);

        engine.script_flush();
        assert_eq!(engine.script_count(), 0);
    }

    #[test]
    fn test_script_count() {
        let engine = LuaEngine::new();
        assert_eq!(engine.script_count(), 0);

        engine.script_load("return 1");
        assert_eq!(engine.script_count(), 1);

        // 加载相同脚本不会增加计数（SHA1 相同，覆盖）
        engine.script_load("return 1");
        assert_eq!(engine.script_count(), 1);
    }

    #[test]
    fn test_hex_encode() {
        assert_eq!(hex_encode(&[0x0a, 0xff, 0x00]), "0aff00");
        assert_eq!(hex_encode(&[]), "");
    }

    #[test]
    fn test_expand_keys_argv() {
        let script = "redis.call('SET', KEYS[1], ARGV[1])";
        let keys: Vec<Vec<u8>> = vec![b"mykey".to_vec()];
        let args: Vec<Vec<u8>> = vec![b"myval".to_vec()];
        let expanded = expand_keys_argv(script, 1, &keys, &args);
        assert_eq!(expanded, "redis.call('SET', 'mykey', 'myval')");
    }

    #[test]
    fn test_expand_multiple_keys_argv() {
        let script = "redis.call('MSET', KEYS[1], ARGV[1], KEYS[2], ARGV[2])";
        let keys: Vec<Vec<u8>> = vec![b"k1".to_vec(), b"k2".to_vec()];
        let args: Vec<Vec<u8>> = vec![b"v1".to_vec(), b"v2".to_vec()];
        let expanded = expand_keys_argv(script, 2, &keys, &args);
        assert_eq!(expanded, "redis.call('MSET', 'k1', 'v1', 'k2', 'v2')");
    }

    #[test]
    fn test_parse_redis_calls_single() {
        let script = "redis.call('SET', 'mykey', 'myvalue')";
        let calls = parse_redis_calls(script);
        assert_eq!(calls.len(), 1);
        assert_eq!(calls[0].command, "SET");
        assert_eq!(calls[0].args, vec!["mykey", "myvalue"]);
    }

    #[test]
    fn test_parse_redis_calls_multiple() {
        let script = "redis.call('SET', 'k1', 'v1') redis.call('GET', 'k1')";
        let calls = parse_redis_calls(script);
        assert_eq!(calls.len(), 2);
        assert_eq!(calls[0].command, "SET");
        assert_eq!(calls[1].command, "GET");
        assert_eq!(calls[1].args, vec!["k1"]);
    }

    #[test]
    fn test_parse_redis_pcall() {
        let script = "redis.pcall('INCR', 'counter')";
        let calls = parse_redis_calls(script);
        assert_eq!(calls.len(), 1);
        assert_eq!(calls[0].command, "INCR");
        assert_eq!(calls[0].args, vec!["counter"]);
    }

    #[test]
    fn test_parse_call_args_set() {
        let parsed = parse_call_args("'SET', 'key1', 'value1'").unwrap();
        assert_eq!(parsed.command, "SET");
        assert_eq!(parsed.args, vec!["key1", "value1"]);
    }

    #[test]
    fn test_parse_call_args_get() {
        let parsed = parse_call_args("'GET', 'mykey'").unwrap();
        assert_eq!(parsed.command, "GET");
        assert_eq!(parsed.args, vec!["mykey"]);
    }

    #[test]
    fn test_parse_call_args_no_args() {
        let parsed = parse_call_args("'PING'").unwrap();
        assert_eq!(parsed.command, "PING");
        assert!(parsed.args.is_empty());
    }

    #[test]
    fn test_split_call_args() {
        let parts = split_call_args("'SET', 'mykey', 'my,value'");
        assert_eq!(parts.len(), 3);
        assert_eq!(parts[0], "'SET'");
        assert_eq!(parts[1], "'mykey'");
        assert_eq!(parts[2], "'my,value'");
    }
}
