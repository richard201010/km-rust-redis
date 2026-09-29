//! Lua 脚本引擎模块 — 基于 mlua 的 Lua 5.4 完整实现。
//!
//! 通过 mlua crate 提供真正的 Lua 5.4 解释器，支持：
//! - 完整 Lua 语法（变量、循环、条件、函数、表操作）
//! - `redis.call()` / `redis.pcall()` 调用 Redis 命令
//! - `redis.error_reply()` / `redis.status_reply()` 辅助函数
//! - KEYS[] 和 ARGV[] 参数
//! - SHA1 脚本缓存（SCRIPT LOAD / EVALSHA）

use std::collections::HashMap;
use std::sync::Mutex;

use sha1::{Digest, Sha1};

use crate::db::Database;
use crate::resp::RespValue;
use crate::types::RedisObject;

/// Lua 脚本引擎
pub struct LuaEngine {
    scripts: Mutex<HashMap<String, String>>,
}

impl LuaEngine {
    pub fn new() -> Self {
        Self {
            scripts: Mutex::new(HashMap::new()),
        }
    }

    pub fn script_sha1(script: &str) -> String {
        let mut hasher = Sha1::new();
        hasher.update(script.as_bytes());
        hex::encode(hasher.finalize())
    }

    pub fn cache_script(&self, script: &str) -> String {
        let sha = Self::script_sha1(script);
        self.scripts.lock().unwrap().insert(sha.clone(), script.to_string());
        sha
    }

    fn get_cached_script(&self, sha: &str) -> Option<String> {
        self.scripts.lock().unwrap().get(sha).cloned()
    }

    /// 脚本缓存中是否已存在该 SHA1（对应 `SCRIPT EXISTS`）。
    pub fn has_script(&self, sha: &str) -> bool {
        self.scripts.lock().unwrap().contains_key(sha)
    }

    /// 清空脚本缓存（对应 `SCRIPT FLUSH`）。
    pub fn flush_scripts(&self) {
        self.scripts.lock().unwrap().clear();
    }

    /// 缓存里已缓存的脚本数量（对应 `SCRIPT HELP`/调试用途）。
    pub fn cached_script_count(&self) -> usize {
        self.scripts.lock().unwrap().len()
    }

    pub fn eval_script(&self, script: &str, keys: &[Vec<u8>], argv: &[Vec<u8>], db: &Database) -> RespValue {
        self.execute_lua(script, keys, argv, db)
    }

    pub fn evalsha_script(&self, sha: &str, keys: &[Vec<u8>], argv: &[Vec<u8>], db: &Database) -> Result<RespValue, RespValue> {
        match self.get_cached_script(sha) {
            Some(script) => Ok(self.execute_lua(&script, keys, argv, db)),
            None => Err(RespValue::err("NOSCRIPT No matching script. Use EVAL.")),
        }
    }

    fn execute_lua(&self, script: &str, keys: &[Vec<u8>], argv: &[Vec<u8>], db: &Database) -> RespValue {
        let lua = mlua::Lua::new();

        // 设置 KEYS
        if let Ok(table) = lua.create_table() {
            for (i, key) in keys.iter().enumerate() {
                let _ = table.set(i + 1, lua.create_string(key).unwrap_or_else(|_| lua.create_string(b"").unwrap()));
            }
            let _ = lua.globals().set("KEYS", table);
        }

        // 设置 ARGV
        if let Ok(table) = lua.create_table() {
            for (i, arg) in argv.iter().enumerate() {
                let _ = table.set(i + 1, lua.create_string(arg).unwrap_or_else(|_| lua.create_string(b"").unwrap()));
            }
            let _ = lua.globals().set("ARGV", table);
        }

        // 注册 redis 模块
        if let Err(e) = self.register_redis(&lua, db) {
            return RespValue::err(format!("ERR Failed to register redis: {}", e));
        }

        // 执行脚本
        match lua.load(script).eval::<mlua::Value>() {
            Ok(val) => lua_to_resp(&val),
            Err(e) => RespValue::err(format!("ERR Error running script: {}", e)),
        }
    }

    fn register_redis(&self, lua: &mlua::Lua, db: &Database) -> Result<(), mlua::Error> {
        let redis = lua.create_table()?;
        let db_usize = db as *const Database as usize;

        // redis.call(command, arg1, arg2, ...)
        {
            let ptr = db_usize;
            let lua_ref = lua.clone();
            let func = lua.create_function(move |_, args: mlua::MultiValue| {
                let db = unsafe { &*(ptr as *const Database) };
                match redis_call(db, &args) {
                    Ok(val) => resp_to_lua(&lua_ref, &val).map_err(|e| mlua::Error::RuntimeError(e.to_string())),
                    Err(e) => {
                        let msg = match e { RespValue::Error(s) => s, _ => "redis.call error".to_string() };
                        Err(mlua::Error::RuntimeError(msg))
                    }
                }
            })?;
            redis.set("call", func)?;
        }

        // redis.pcall — 同 redis.call，错误返回 {err=...} 而非抛异常
        {
            let ptr = db_usize;
            let lua_ref = lua.clone();
            let func = lua.create_function(move |_, args: mlua::MultiValue| {
                let db = unsafe { &*(ptr as *const Database) };
                match redis_call(db, &args).map_err(|e| mlua::Error::RuntimeError(match e { RespValue::Error(s) => s, _ => "redis.call error".to_string() })) {
                    Ok(val) => {
                        let t = lua_ref.create_table()?;
                        t.set("ok", resp_to_lua(&lua_ref, &val)?)?;
                        Ok(mlua::Value::Table(t))
                    }
                    Err(e) => {
                        let t = lua_ref.create_table()?;
                        let msg = e.to_string();
                        t.set("err", msg)?;
                        Ok(mlua::Value::Table(t))
                    }
                }
            })?;
            redis.set("pcall", func)?;
        }

        // redis.error_reply(msg)
        {
            let func = lua.create_function(move |_, msg: mlua::String| {
                Ok(mlua::Value::String(msg))
            })?;
            redis.set("error_reply", func)?;
        }

        // redis.status_reply(msg)
        {
            let func = lua.create_function(move |_, msg: mlua::String| {
                Ok(mlua::Value::String(msg))
            })?;
            redis.set("status_reply", func)?;
        }

        lua.globals().set("redis", redis)?;
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// Lua ↔ RespValue 互转
// ---------------------------------------------------------------------------

fn lua_to_resp(val: &mlua::Value) -> RespValue {
    match val {
        mlua::Value::Nil => RespValue::Null,
        mlua::Value::Boolean(b) => RespValue::Integer(if *b { 1 } else { 0 }),
        mlua::Value::Integer(n) => RespValue::Integer(*n),
        mlua::Value::Number(f) => {
            if *f == *f as i64 as f64 {
                RespValue::Integer(*f as i64)
            } else {
                RespValue::BulkString(format!("{}", f).into_bytes())
            }
        }
        mlua::Value::String(s) => {
            let bytes = s.as_bytes().to_vec();
            RespValue::BulkString(bytes)
        }
        mlua::Value::Table(table) => {
            // 检查 redis 错误/状态标记
            if let Ok(mlua::Value::String(s)) = table.get::<mlua::Value>("err") {
                return RespValue::err(s.to_str().map(|v| v.to_string()).unwrap_or_else(|_| "ERR".to_string()));
            }
            if let Ok(mlua::Value::Nil) = table.get::<mlua::Value>("ok") { } else if let Ok(_) = table.get::<mlua::Value>("ok") {
                if let Ok(inner) = table.get::<mlua::Value>("ok") {
                    return lua_to_resp(&inner);
                }
                return RespValue::ok();
            }
            // 数组表 → RESP Array
            let mut items = Vec::new();
            let mut idx = 1;
            loop {
                match table.get::<mlua::Value>(idx) {
                    Ok(mlua::Value::Nil) => break,
                    Ok(v) => {
                        items.push(lua_to_resp(&v));
                        idx += 1;
                    }
                    Err(_) => break,
                }
            }
            if items.is_empty() { RespValue::Null } else { RespValue::Array(items) }
        }
        _ => RespValue::Null,
    }
}

fn resp_to_lua(lua: &mlua::Lua, val: &RespValue) -> Result<mlua::Value, mlua::Error> {
    match val {
        RespValue::Null | RespValue::NullArray => Ok(mlua::Value::Nil),
        RespValue::Integer(n) => Ok(mlua::Value::Integer(*n)),
        RespValue::Boolean(b) => Ok(mlua::Value::Boolean(*b)),
        RespValue::SimpleString(s) => Ok(mlua::Value::String(lua.create_string(s.as_bytes())?)),
        RespValue::Error(s) => Ok(mlua::Value::String(lua.create_string(format!("ERR {}", s).as_bytes())?)),
        RespValue::BulkString(d) => Ok(mlua::Value::String(lua.create_string(d)?)),
        RespValue::Array(items) => {
            let table = lua.create_table()?;
            for (i, item) in items.iter().enumerate() {
                table.set(i + 1, resp_to_lua(lua, item)?)?;
            }
            Ok(mlua::Value::Table(table))
        }
        _ => Ok(mlua::Value::Nil),
    }
}

// ---------------------------------------------------------------------------
// redis.call 实现 — 直接在 Database 上执行常见命令
// ---------------------------------------------------------------------------

fn redis_call(db: &Database, args: &mlua::MultiValue) -> Result<RespValue, RespValue> {
    if args.is_empty() {
        return Err(RespValue::err("ERR wrong number of arguments for 'redis.call' command"));
    }

    let cmd_name = match &args[0] {
        mlua::Value::String(s) => s.to_str()
            .map(|v| v.to_uppercase())
            .map_err(|_| RespValue::err("ERR invalid command name"))?,
        mlua::Value::Integer(n) => n.to_string().to_uppercase(),
        _ => return Err(RespValue::err("ERR invalid command name type")),
    };

    let mut cmd_args: Vec<Vec<u8>> = Vec::new();
    for i in 1..args.len() {
        match &args[i] {
            mlua::Value::String(s) => cmd_args.push(s.as_bytes().to_vec()),
            mlua::Value::Integer(n) => cmd_args.push(n.to_string().into_bytes()),
            mlua::Value::Number(f) => cmd_args.push(format!("{}", f).into_bytes()),
            mlua::Value::Nil => cmd_args.push(Vec::new()),
            mlua::Value::Boolean(b) => cmd_args.push(if *b { b"1".to_vec() } else { b"0".to_vec() }),
            _ => cmd_args.push(Vec::new()),
        }
    }

    exec_cmd(db, &cmd_name, &cmd_args)
}

fn exec_cmd(db: &Database, cmd: &str, args: &[Vec<u8>]) -> Result<RespValue, RespValue> {
    match cmd {
        "GET" => {
            if args.is_empty() { return Err(RespValue::err("ERR wrong number of arguments")); }
            match db.get(&args[0]) {
                Some(RedisObject::String(d)) => Ok(RespValue::BulkString(d)),
                Some(RedisObject::Integer(n)) => Ok(RespValue::BulkString(n.to_string().into_bytes())),
                Some(_) => Err(RespValue::err("WRONGTYPE Operation against a key holding the wrong kind of value")),
                None => Ok(RespValue::Null),
            }
        }
        "SET" => {
            if args.len() < 2 { return Err(RespValue::err("ERR wrong number of arguments")); }
            db.set(&args[0], RedisObject::String(args[1].clone()), None);
            Ok(RespValue::ok())
        }
        "DEL" => {
            let mut count = 0;
            for arg in args { if db.delete(arg) { count += 1; } }
            Ok(RespValue::Integer(count))
        }
        "EXISTS" => {
            let mut count = 0;
            for arg in args { if db.exists(arg) { count += 1; } }
            Ok(RespValue::Integer(count))
        }
        "INCR" => {
            if args.is_empty() { return Err(RespValue::err("ERR wrong number of arguments")); }
            match db.get(&args[0]) {
                Some(RedisObject::Integer(n)) => {
                    let v = n + 1; db.set(&args[0], RedisObject::Integer(v), None); Ok(RespValue::Integer(v))
                }
                Some(RedisObject::String(d)) => {
                    let n: i64 = String::from_utf8_lossy(&d).parse().map_err(|_| RespValue::err("ERR value is not an integer"))?;
                    let v = n + 1; db.set(&args[0], RedisObject::Integer(v), None); Ok(RespValue::Integer(v))
                }
                Some(_) => Err(RespValue::err("WRONGTYPE Operation against a key holding the wrong kind of value")),
                None => { db.set(&args[0], RedisObject::Integer(1), None); Ok(RespValue::Integer(1)) }
            }
        }
        "INCRBY" => {
            if args.len() < 2 { return Err(RespValue::err("ERR wrong number of arguments")); }
            let inc: i64 = String::from_utf8_lossy(&args[1]).parse().map_err(|_| RespValue::err("ERR value is not an integer"))?;
            match db.get(&args[0]) {
                Some(RedisObject::Integer(n)) => { let v = n + inc; db.set(&args[0], RedisObject::Integer(v), None); Ok(RespValue::Integer(v)) }
                Some(RedisObject::String(d)) => {
                    let n: i64 = String::from_utf8_lossy(&d).parse().map_err(|_| RespValue::err("ERR value is not an integer"))?;
                    let v = n + inc; db.set(&args[0], RedisObject::Integer(v), None); Ok(RespValue::Integer(v))
                }
                Some(_) => Err(RespValue::err("WRONGTYPE Operation against a key holding the wrong kind of value")),
                None => { db.set(&args[0], RedisObject::Integer(inc), None); Ok(RespValue::Integer(inc)) }
            }
        }
        "DECR" => { exec_cmd(db, "INCRBY", &[args[0].clone(), b"-1".to_vec()]) }
        "APPEND" => {
            if args.len() < 2 { return Err(RespValue::err("ERR wrong number of arguments")); }
            match db.get(&args[0]) {
                Some(RedisObject::String(mut d)) => { d.extend_from_slice(&args[1]); let l = d.len() as i64; db.set(&args[0], RedisObject::String(d), None); Ok(RespValue::Integer(l)) }
                Some(RedisObject::Integer(n)) => { let mut d = n.to_string().into_bytes(); d.extend_from_slice(&args[1]); let l = d.len() as i64; db.set(&args[0], RedisObject::String(d), None); Ok(RespValue::Integer(l)) }
                Some(_) => Err(RespValue::err("WRONGTYPE Operation against a key holding the wrong kind of value")),
                None => { let l = args[1].len() as i64; db.set(&args[0], RedisObject::String(args[1].clone()), None); Ok(RespValue::Integer(l)) }
            }
        }
        "STRLEN" => {
            if args.is_empty() { return Err(RespValue::err("ERR wrong number of arguments")); }
            match db.get(&args[0]) {
                Some(RedisObject::String(d)) => Ok(RespValue::Integer(d.len() as i64)),
                Some(RedisObject::Integer(n)) => Ok(RespValue::Integer(n.to_string().len() as i64)),
                Some(_) => Err(RespValue::err("WRONGTYPE Operation against a key holding the wrong kind of value")),
                None => Ok(RespValue::Integer(0)),
            }
        }
        "MGET" => {
            let r: Vec<RespValue> = args.iter().map(|k| match db.get(k) {
                Some(RedisObject::String(d)) => RespValue::BulkString(d),
                Some(RedisObject::Integer(n)) => RespValue::BulkString(n.to_string().into_bytes()),
                _ => RespValue::Null,
            }).collect();
            Ok(RespValue::Array(r))
        }
        "LPUSH" | "RPUSH" => {
            if args.len() < 2 { return Err(RespValue::err("ERR wrong number of arguments")); }
            use std::sync::Arc;
            use std::collections::VecDeque;
            match db.get_object_mut(&args[0]) {
                Some(mut obj) => {
                    if let RedisObject::List(ref mut list) = *obj {
                        let m = list;
                        for a in args.iter().skip(1) {
                            if cmd == "LPUSH" { m.push_front(a.clone()); } else { m.push_back(a.clone()); }
                        }
                        Ok(RespValue::Integer(m.len() as i64))
                    } else {
                        Err(RespValue::err("WRONGTYPE Operation against a key holding the wrong kind of value"))
                    }
                }
                None => {
                    let mut list = VecDeque::new();
                    for a in args.iter().skip(1) {
                        if cmd == "LPUSH" { list.push_front(a.clone()); } else { list.push_back(a.clone()); }
                    }
                    let l = list.len() as i64;
                    db.set(&args[0], RedisObject::List(list), None);
                    Ok(RespValue::Integer(l))
                }
            }
        }
        "LLEN" => {
            if args.is_empty() { return Err(RespValue::err("ERR wrong number of arguments")); }
            match db.get(&args[0]) {
                Some(RedisObject::List(l)) => Ok(RespValue::Integer(l.len() as i64)),
                Some(_) => Err(RespValue::err("WRONGTYPE Operation against a key holding the wrong kind of value")),
                None => Ok(RespValue::Integer(0)),
            }
        }
        "HSET" => {
            if args.len() < 3 || args.len() % 2 == 0 { return Err(RespValue::err("ERR wrong number of arguments")); }
            let mut count = 0;
            match db.get_object_mut(&args[0]) {
                Some(mut obj) => {
                    if let RedisObject::Hash(ref mut map) = *obj {
                        for i in (1..args.len()).step_by(2) {
                            if !map.contains_key(&args[i]) { count += 1; }
                            map.insert(args[i].clone(), args[i + 1].clone());
                        }
                        Ok(RespValue::Integer(count))
                    } else { Err(RespValue::err("WRONGTYPE Operation against a key holding the wrong kind of value")) }
                }
                None => {
                    let mut map = std::collections::HashMap::new();
                    for i in (1..args.len()).step_by(2) { map.insert(args[i].clone(), args[i + 1].clone()); count += 1; }
                    db.set(&args[0], RedisObject::Hash(map), None);
                    Ok(RespValue::Integer(count))
                }
            }
        }
        "HGET" => {
            if args.len() < 2 { return Err(RespValue::err("ERR wrong number of arguments")); }
            match db.get(&args[0]) {
                Some(RedisObject::Hash(m)) => Ok(m.get(&args[1]).map(|v| RespValue::BulkString(v.clone())).unwrap_or(RespValue::Null)),
                Some(_) => Err(RespValue::err("WRONGTYPE Operation against a key holding the wrong kind of value")),
                None => Ok(RespValue::Null),
            }
        }
        "HDEL" => {
            if args.len() < 2 { return Err(RespValue::err("ERR wrong number of arguments")); }
            match db.get_object_mut(&args[0]) {
                Some(mut obj) => {
                    if let RedisObject::Hash(ref mut map) = *obj {
                        let mut c = 0;
                        for f in &args[1..] { if map.remove(f).is_some() { c += 1; } }
                        Ok(RespValue::Integer(c))
                    } else { Err(RespValue::err("WRONGTYPE Operation against a key holding the wrong kind of value")) }
                }
                None => Ok(RespValue::Integer(0)),
            }
        }
        "SADD" => {
            if args.len() < 2 { return Err(RespValue::err("ERR wrong number of arguments")); }
            match db.get_object_mut(&args[0]) {
                Some(mut obj) => {
                    if let RedisObject::Set(ref mut set) = *obj {
                        let mut c = 0;
                        for m in &args[1..] { if set.insert(m.clone()) { c += 1; } }
                        Ok(RespValue::Integer(c))
                    } else { Err(RespValue::err("WRONGTYPE Operation against a key holding the wrong kind of value")) }
                }
                None => {
                    let mut set = std::collections::HashSet::new();
                    let mut c = 0;
                    for m in &args[1..] { if set.insert(m.clone()) { c += 1; } }
                    db.set(&args[0], RedisObject::Set(set), None);
                    Ok(RespValue::Integer(c))
                }
            }
        }
        "SCARD" => {
            if args.is_empty() { return Err(RespValue::err("ERR wrong number of arguments")); }
            match db.get(&args[0]) {
                Some(RedisObject::Set(s)) => Ok(RespValue::Integer(s.len() as i64)),
                Some(_) => Err(RespValue::err("WRONGTYPE Operation against a key holding the wrong kind of value")),
                None => Ok(RespValue::Integer(0)),
            }
        }
        "KEYS" => {
            let p = if args.is_empty() { b"*" as &[u8] } else { &args[0] };
            Ok(RespValue::Array(db.keys(p).into_iter().map(RespValue::BulkString).collect()))
        }
        "TYPE" => {
            if args.is_empty() { return Err(RespValue::err("ERR wrong number of arguments")); }
            Ok(RespValue::SimpleString(db.key_type(&args[0]).unwrap_or("none").to_string()))
        }
        "TTL" => { if args.is_empty() { Err(RespValue::err("ERR wrong number of arguments")) } else { Ok(RespValue::Integer(db.ttl(&args[0]))) } }
        "PTTL" => { if args.is_empty() { Err(RespValue::err("ERR wrong number of arguments")) } else { Ok(RespValue::Integer(db.pttl(&args[0]))) } }
        "EXPIRE" => {
            if args.len() < 2 { return Err(RespValue::err("ERR wrong number of arguments")); }
            let s: i64 = String::from_utf8_lossy(&args[1]).parse().map_err(|_| RespValue::err("ERR value is not an integer"))?;
            Ok(RespValue::Integer(if db.set_expire(&args[0], (s * 1000) as u64) { 1 } else { 0 }))
        }
        "PERSIST" => {
            if args.is_empty() { return Err(RespValue::err("ERR wrong number of arguments")); }
            Ok(RespValue::Integer(if db.persist(&args[0]) { 1 } else { 0 }))
        }
        "DBSIZE" => Ok(RespValue::Integer(db.dbsize() as i64)),
        "RENAME" => {
            if args.len() < 2 { return Err(RespValue::err("ERR wrong number of arguments")); }
            if db.rename(&args[0], &args[1]) { Ok(RespValue::ok()) } else { Err(RespValue::err("ERR no such key")) }
        }
        _ => Err(RespValue::err(format!("ERR Unknown command '{}' in redis.call", cmd))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_script_sha1() {
        let sha = LuaEngine::script_sha1("return 1");
        assert_eq!(sha.len(), 40);
    }

    #[test]
    fn test_cache_and_eval() {
        let engine = LuaEngine::new();
        let sha = engine.cache_script("return 1 + 2");
        let db = crate::db::Database::new(0);
        let result = engine.evalsha_script(&sha, &[], &[], &db);
        assert!(result.is_ok());
    }

    #[test]
    fn test_evalsha_not_found() {
        let engine = LuaEngine::new();
        let db = crate::db::Database::new(0);
        let result = engine.evalsha_script("nonexistent", &[], &[], &db);
        assert!(result.is_err());
    }
}
