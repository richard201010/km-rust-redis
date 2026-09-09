//! Pub/Sub 发布订阅模块
//!
//! 实现 Redis 风格的发布/订阅（Publish/Subscribe）功能。
//! 管理频道与订阅者之间的映射关系，支持 SUBSCRIBE/UNSUBSCRIBE/PUBLISH 操作。
//!
//! # 设计要点
//!
//! - [`PubSub`] 维护 `HashMap<String, Vec<Arc<Mutex<StreamWriter>>>>` 频道→订阅者映射
//! - `subscribe(channel, writer)` 注册订阅，返回频道当前订阅者总数
//! - `unsubscribe(channel, writer)` 取消订阅，返回频道剩余订阅者数
//! - `publish(channel, message)` 向频道所有订阅者发送消息，返回接收者数量
//! - 消息格式：RESP 数组 `[message, channel, payload]`
//!
//! # 客户端状态
//!
//! 每个客户端连接维护：
//! - `sub_channels: HashSet<String>` — 已订阅的频道集合
//! - 进入订阅模式后，只能执行 SUBSCRIBE/UNSUBSCRIBE/PING/QUIT 命令
//!
//! # 简化说明
//!
//! 由于当前 `CmdCtx` 不持有 PubSub 引用，命令层返回正确格式但不实际广播。
//! 完整集成需要将 PubSub 状态注入命令上下文或在服务端循环中处理。

use std::collections::HashMap;
use std::sync::Arc;
use tokio::io::AsyncWriteExt;
use tokio::net::tcp::OwnedWriteHalf;
use tokio::sync::Mutex;

/// Pub/Sub 管理器
///
/// 维护频道名到订阅者写端列表的映射。
/// 每个订阅者通过 `Arc<Mutex<OwnedWriteHalf>>` 接收消息。
pub struct PubSub {
    /// 频道名 → 订阅者写端列表
    channels: Mutex<HashMap<String, Vec<Arc<Mutex<OwnedWriteHalf>>>>>,
}

impl PubSub {
    /// 创建新的 Pub/Sub 管理器
    pub fn new() -> Self {
        Self {
            channels: Mutex::new(HashMap::new()),
        }
    }

    /// 订阅指定频道
    ///
    /// 将客户端的 TCP 写端注册到频道的订阅者列表中。
    /// 如果该客户端已订阅同一频道，不会重复添加。
    ///
    /// # 返回
    /// 该频道当前的订阅者总数（包含本次新注册的）
    pub async fn subscribe(&self, channel: &str, writer: Arc<Mutex<OwnedWriteHalf>>) -> usize {
        let mut channels = self.channels.lock().await;
        let subscribers = channels.entry(channel.to_string()).or_insert_with(Vec::new);
        // 避免重复添加同一个 writer
        if !subscribers.iter().any(|w| Arc::ptr_eq(w, &writer)) {
            subscribers.push(writer);
        }
        subscribers.len()
    }

    /// 取消订阅指定频道
    ///
    /// 从频道的订阅者列表中移除指定客户端。
    /// 如果频道已无订阅者，自动清理该频道条目。
    ///
    /// # 返回
    /// 该频道剩余的订阅者数量
    pub async fn unsubscribe(&self, channel: &str, writer: &Arc<Mutex<OwnedWriteHalf>>) -> usize {
        let mut channels = self.channels.lock().await;
        if let Some(subscribers) = channels.get_mut(channel) {
            subscribers.retain(|w| !Arc::ptr_eq(w, writer));
            let remaining = subscribers.len();
            if remaining == 0 {
                channels.remove(channel);
            }
            remaining
        } else {
            0
        }
    }

    /// 向指定频道发布消息
    ///
    /// 向频道的所有订阅者发送消息，消息格式为 RESP 数组：
    /// `*3\r\n$7\r\nmessage\r\n$<ch_len>\r\n<channel>\r\n$<msg_len>\r\n<payload>\r\n`
    ///
    /// # 返回
    /// 成功接收到消息的订阅者数量（对应 Redis PUBLISH 的返回值）
    pub async fn publish(&self, channel: &str, message: &[u8]) -> usize {
        let channels = self.channels.lock().await;
        let subscribers = match channels.get(channel) {
            Some(subs) => subs,
            None => return 0,
        };

        if subscribers.is_empty() {
            return 0;
        }

        // 构建 RESP 消息：*3\r\n$7\r\nmessage\r\n$<ch_len>\r\n<channel>\r\n$<msg_len>\r\n<payload>\r\n
        let mut resp_msg = Vec::new();
        resp_msg.extend_from_slice(b"*3\r\n");
        // "message" bulk string
        resp_msg.extend_from_slice(b"$7\r\nmessage\r\n");
        // channel name bulk string
        resp_msg.extend_from_slice(b"$");
        resp_msg.extend_from_slice(channel.len().to_string().as_bytes());
        resp_msg.extend_from_slice(b"\r\n");
        resp_msg.extend_from_slice(channel.as_bytes());
        resp_msg.extend_from_slice(b"\r\n");
        // payload bulk string
        resp_msg.extend_from_slice(b"$");
        resp_msg.extend_from_slice(message.len().to_string().as_bytes());
        resp_msg.extend_from_slice(b"\r\n");
        resp_msg.extend_from_slice(message);
        resp_msg.extend_from_slice(b"\r\n");

        let mut count = 0;
        for writer in subscribers {
            let mut w = writer.lock().await;
            if w.write_all(&resp_msg).await.is_ok() {
                let _ = w.flush().await;
                count += 1;
            }
        }
        count
    }

    /// 获取指定频道的订阅者数量
    pub async fn subscriber_count(&self, channel: &str) -> usize {
        let channels = self.channels.lock().await;
        channels.get(channel).map(|s| s.len()).unwrap_or(0)
    }

    /// 获取所有活跃频道的列表
    pub async fn channel_list(&self) -> Vec<String> {
        let channels = self.channels.lock().await;
        channels.keys().cloned().collect()
    }

    /// 获取所有频道的订阅者总数
    pub async fn total_subscribers(&self) -> usize {
        let channels = self.channels.lock().await;
        channels.values().map(|s| s.len()).sum()
    }
}

impl Default for PubSub {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_pubsub_subscribe_count() {
        let pubsub = PubSub::new();
        assert_eq!(pubsub.subscriber_count("test").await, 0);
        assert_eq!(pubsub.total_subscribers().await, 0);
    }

    #[tokio::test]
    async fn test_pubsub_channel_list_empty() {
        let pubsub = PubSub::new();
        assert!(pubsub.channel_list().await.is_empty());
    }

    #[tokio::test]
    async fn test_publish_no_subscribers() {
        let pubsub = PubSub::new();
        let count = pubsub.publish("channel", b"hello").await;
        assert_eq!(count, 0);
    }
}
