//! SCAN 游标实现模块。
//!
//! 实现 Redis 的 SCAN 命令游标机制，支持增量遍历数据库中的键。
//! 游标是一个简单的索引值，指向所有键的有序快照中的起始位置。
//!
//! # 设计思路
//!
//! Redis 的 SCAN 使用哈希表的 rehash 游标实现渐进式遍历。
//! 本实现采用更简单的方式：将所有键收集到 Vec 中，游标为起始索引。
//! 虽然不是 O(1) 空间，但实现简单且对小规模数据集性能足够。

use crate::db::Database;
use crate::types::current_time_ms;

/// 扫描结果：(next_cursor, keys)
///
/// - `next_cursor`: 下次扫描的起始游标，0 表示扫描完成
/// - `keys`: 本次返回的键列表
pub type ScanResult = (u64, Vec<Vec<u8>>);

/// 从数据库中增量扫描键。
///
/// # 参数
/// - `db`: 目标数据库引用
/// - `cursor`: 当前游标位置（0 表示从头开始）
/// - `pattern`: 可选的 glob 模式过滤（None 表示返回所有键）
/// - `count`: 每次返回的最大键数（提示值，实际可能更多或更少）
///
/// # 返回
/// `(next_cursor, keys)` — next_cursor=0 表示扫描完成
///
/// # 游标机制
///
/// 1. 收集所有未过期的键到有序 Vec（按键排序以保证确定性）
/// 2. cursor 作为起始索引
/// 3. 返回从 cursor 开始的 count 个键
/// 4. 如果已遍历完所有键，next_cursor = 0
pub fn scan_keys(db: &Database, cursor: u64, pattern: Option<&[u8]>, count: usize) -> ScanResult {
    let now = current_time_ms();

    // 收集所有未过期的键
    let mut all_keys: Vec<Vec<u8>> = db
        .data
        .iter()
        .filter(|entry| {
            let key = entry.key();
            // 跳过已过期的键
            if let Some(exp) = db.expires.get(key) {
                if now >= *exp {
                    return false;
                }
            }
            // 如果有模式过滤，检查是否匹配
            if let Some(pat) = pattern {
                if pat != b"*" {
                    return glob_match(pat, key);
                }
            }
            true
        })
        .map(|entry| entry.key().clone())
        .collect();

    // 排序以保证确定性遍历顺序
    all_keys.sort();

    let total = all_keys.len();
    if total == 0 {
        return (0, vec![]);
    }

    let start = cursor as usize;
    if start >= total {
        // cursor 超出范围，返回空结果
        return (0, vec![]);
    }

    let end = (start + count).min(total);
    let keys = all_keys[start..end].to_vec();

    let next_cursor = if end >= total { 0 } else { end as u64 };
    (next_cursor, keys)
}

/// 简易通配符匹配（与 db.rs 中的实现保持一致）。
///
/// 支持 `*`（匹配任意字符序列）和 `?`（匹配单个字符）。
fn glob_match(pattern: &[u8], text: &[u8]) -> bool {
    let (mut pi, mut ti) = (0, 0);
    let (mut star_pi, mut star_ti) = (usize::MAX, 0);

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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::RedisObject;

    #[test]
    fn test_scan_empty_db() {
        let db = Database::new(0);
        let (next, keys) = scan_keys(&db, 0, None, 10);
        assert_eq!(next, 0);
        assert!(keys.is_empty());
    }

    #[test]
    fn test_scan_all_keys() {
        let db = Database::new(0);
        db.set(b"alpha".to_vec(), RedisObject::String(b"1".to_vec()), None);
        db.set(b"beta".to_vec(), RedisObject::String(b"2".to_vec()), None);
        db.set(b"gamma".to_vec(), RedisObject::String(b"3".to_vec()), None);

        let (next, keys) = scan_keys(&db, 0, None, 10);
        assert_eq!(next, 0);
        assert_eq!(keys.len(), 3);
    }

    #[test]
    fn test_scan_with_count() {
        let db = Database::new(0);
        for i in 0..10 {
            let key = format!("key{:02}", i);
            db.set(key.into_bytes(), RedisObject::String(b"v".to_vec()), None);
        }

        // 第一次扫描，返回 3 个键
        let (next, keys) = scan_keys(&db, 0, None, 3);
        assert_eq!(keys.len(), 3);
        assert_ne!(next, 0); // 还有更多

        // 继续扫描
        let (next2, keys2) = scan_keys(&db, next, None, 3);
        assert_eq!(keys2.len(), 3);

        // 继续扫描直到完成
        let mut cursor = next2;
        let mut total_scanned = 6;
        while cursor != 0 {
            let (c, k) = scan_keys(&db, cursor, None, 3);
            total_scanned += k.len();
            cursor = c;
        }
        assert_eq!(total_scanned, 10);
    }

    #[test]
    fn test_scan_with_pattern() {
        let db = Database::new(0);
        db.set(b"user:1".to_vec(), RedisObject::String(b"a".to_vec()), None);
        db.set(b"user:2".to_vec(), RedisObject::String(b"b".to_vec()), None);
        db.set(b"item:1".to_vec(), RedisObject::String(b"c".to_vec()), None);

        let (next, keys) = scan_keys(&db, 0, Some(b"user:*"), 100);
        assert_eq!(next, 0);
        assert_eq!(keys.len(), 2);
        for k in &keys {
            assert!(k.starts_with(b"user:"));
        }
    }

    #[test]
    fn test_scan_cursor_zero_after_complete() {
        let db = Database::new(0);
        db.set(b"a".to_vec(), RedisObject::String(b"1".to_vec()), None);
        db.set(b"b".to_vec(), RedisObject::String(b"2".to_vec()), None);

        let (next, _) = scan_keys(&db, 0, None, 100);
        assert_eq!(next, 0); // 全部返回，游标归零
    }
}
