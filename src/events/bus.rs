//! 事件总线 trait 和广播实现
//!
//! 提供发布/订阅事件机制，支持多个订阅者同时接收事件。

use std::sync::Arc;
use tokio::sync::broadcast;
use tokio::sync::RwLock;

use super::types::DownloadEvent;

/// 事件总线 trait
///
/// 支持多生产者多消费者的事件分发。
/// 所有实现必须线程安全（Send + Sync）。
pub trait EventBus: Send + Sync {
    /// 发布事件到所有订阅者
    fn publish(&self, event: DownloadEvent);

    /// 订阅事件，返回新的接收器
    fn subscribe(&self) -> broadcast::Receiver<DownloadEvent>;
}

/// 广播事件总线
///
/// 基于 tokio::sync::broadcast 实现，支持多订阅者。
/// 通道容量为 256，超出时最旧的事件被丢弃。
pub struct BroadcastEventBus {
    sender: Arc<RwLock<broadcast::Sender<DownloadEvent>>>,
}

impl BroadcastEventBus {
    /// 创建新的广播事件总线
    pub fn new() -> Self {
        let (sender, _) = broadcast::channel(256);
        Self {
            sender: Arc::new(RwLock::new(sender)),
        }
    }

    /// 获取当前订阅者数量
    pub fn receiver_count(&self) -> usize {
        let sender = self.sender.blocking_read();
        sender.receiver_count()
    }
}

impl Default for BroadcastEventBus {
    fn default() -> Self {
        Self::new()
    }
}

impl Clone for BroadcastEventBus {
    fn clone(&self) -> Self {
        Self {
            sender: Arc::clone(&self.sender),
        }
    }
}

impl EventBus for BroadcastEventBus {
    fn publish(&self, event: DownloadEvent) {
        let sender = self.sender.blocking_read();
        let _ = sender.send(event);
    }

    fn subscribe(&self) -> broadcast::Receiver<DownloadEvent> {
        let sender = self.sender.blocking_read();
        sender.subscribe()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_broadcast_event_bus_publish_subscribe() {
        let bus = BroadcastEventBus::new();
        let mut rx1 = bus.subscribe();
        let mut rx2 = bus.subscribe();

        bus.publish(DownloadEvent::DownloadPaused);

        // 两个订阅者都应该收到事件
        let event1 = rx1.recv().await.unwrap();
        let event2 = rx2.recv().await.unwrap();

        assert!(matches!(event1, DownloadEvent::DownloadPaused));
        assert!(matches!(event2, DownloadEvent::DownloadPaused));
    }

    #[tokio::test]
    async fn test_broadcast_event_bus_clone() {
        let bus1 = BroadcastEventBus::new();
        let bus2 = bus1.clone();
        let mut rx = bus1.subscribe();

        bus2.publish(DownloadEvent::DownloadResumed);
        let event = rx.recv().await.unwrap();
        assert!(matches!(event, DownloadEvent::DownloadResumed));
    }

    #[tokio::test]
    async fn test_broadcast_event_bus_receiver_count() {
        let bus = BroadcastEventBus::new();
        assert_eq!(bus.receiver_count(), 0);

        let _rx1 = bus.subscribe();
        assert_eq!(bus.receiver_count(), 1);

        let _rx2 = bus.subscribe();
        assert_eq!(bus.receiver_count(), 2);
    }
}
