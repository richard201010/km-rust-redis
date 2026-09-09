//! Redis Modules API 实现。
//!
//! 支持动态加载共享库 (.so/.dylib) 扩展 Redis 命令。
//! 模块必须导出 `RedisModule_OnLoad` 函数作为入口点。

use std::collections::HashMap;
use std::sync::Arc;

use crate::commands::{CommandDef, CmdCtx};
use crate::resp::RespValue;

/// 模块信息
pub struct ModuleInfo {
    /// 模块名称
    pub name: String,
    /// 模块版本
    pub version: String,
    /// 模块描述
    pub description: String,
    /// 动态库句柄（保持加载状态）
    _library: libloading::Library,
}

/// 模块管理器
pub struct ModuleManager {
    /// 已加载的模块（名称 → 模块信息）
    modules: HashMap<String, ModuleInfo>,
}

impl ModuleManager {
    /// 创建空的模块管理器
    pub fn new() -> Self {
        Self {
            modules: HashMap::new(),
        }
    }

    /// 加载模块
    ///
    /// 从指定路径加载共享库，调用其 `RedisModule_OnLoad` 入口函数。
    /// 成功后模块信息被注册到管理器中。
    pub fn load_module(&mut self, path: &str) -> Result<String, String> {
        // 安全加载共享库
        let library = unsafe {
            libloading::Library::new(path)
                .map_err(|e| format!("ERR Failed to load module '{}': {}", path, e))?
        };

        // 查找入口函数 RedisModule_OnLoad
        // 模块入口函数签名: fn(module_name: &str) -> Result<(String, String, String), String>
        // 返回 (name, version, description)
        type OnLoadFn = fn(&str) -> Result<(String, String, String), String>;

        let on_load: libloading::Symbol<OnLoadFn> = unsafe {
            library.get(b"RedisModule_OnLoad")
                .map_err(|e| format!("ERR Module '{}' has no RedisModule_OnLoad entry point: {}", path, e))?
        };

        // 调用模块入口
        let (name, version, description) = on_load(path)
            .map_err(|e| format!("ERR Module '{}' initialization failed: {}", path, e))?;

        log::info!("Module loaded: {} v{} - {}", name, version, description);

        self.modules.insert(name.clone(), ModuleInfo {
            name: name.clone(),
            version,
            description,
            _library: library,
        });

        Ok(name)
    }

    /// 卸载模块
    pub fn unload_module(&mut self, name: &str) -> Result<(), String> {
        if let Some(module) = self.modules.remove(name) {
            // Library 在 drop 时自动卸载
            drop(module);
            log::info!("Module unloaded: {}", name);
            Ok(())
        } else {
            Err(format!("ERR Module '{}' not loaded", name))
        }
    }

    /// 获取已加载模块列表
    pub fn list_modules(&self) -> Vec<(&str, &str, &str)> {
        self.modules.values()
            .map(|m| (m.name.as_str(), m.version.as_str(), m.description.as_str()))
            .collect()
    }

    /// 检查模块是否已加载
    pub fn is_loaded(&self, name: &str) -> bool {
        self.modules.contains_key(name)
    }

    /// 获取已加载模块数量
    pub fn count(&self) -> usize {
        self.modules.len()
    }
}

/// 示例模块入口函数（供外部模块参考）
///
/// 一个简单的 Redis 模块应导出此函数：
/// ```no_run
/// #[no_mangle]
/// pub extern "C" fn RedisModule_OnLoad(_path: &str) -> Result<(String, String, String), String> {
///     Ok(("example".to_string(), "1.0".to_string(), "Example module".to_string()))
/// }
/// ```
pub fn example_module_entry(_path: &str) -> Result<(String, String, String), String> {
    Ok((
        "example".to_string(),
        "1.0".to_string(),
        "Example Redis module for KM-Rust-Redis".to_string(),
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_module_manager_new() {
        let mgr = ModuleManager::new();
        assert_eq!(mgr.count(), 0);
        assert!(mgr.list_modules().is_empty());
    }

    #[test]
    fn test_unload_nonexistent() {
        let mut mgr = ModuleManager::new();
        assert!(mgr.unload_module("nonexistent").is_err());
    }

    #[test]
    fn test_is_loaded() {
        let mgr = ModuleManager::new();
        assert!(!mgr.is_loaded("test"));
    }
}
