//! 重试策略模块
//!
//! 提供可配置的重试策略，支持指数退避和无重试两种模式。

use std::time::Duration;

/// 重试策略 trait
///
/// 定义下载失败时的重试行为。
/// 所有实现必须线程安全（Send + Sync）。
pub trait RetryPolicy: Send + Sync {
    /// 判断是否应该重试，返回重试前的等待时间
    ///
    /// # 参数
    /// * `attempt` - 当前尝试次数（从 1 开始）
    /// * `error_msg` - 错误信息，可用于判断错误是否可重试
    ///
    /// # 返回
    /// * `Some(Duration)` - 等待该时长后重试
    /// * `None` - 不重试
    fn should_retry(&self, attempt: u32, error_msg: &str) -> Option<Duration>;

    /// 最大重试次数
    fn max_retries(&self) -> u32;
}

// ---- 无重试策略 ----

/// 永不重试的策略
pub struct NoRetry;

impl RetryPolicy for NoRetry {
    fn should_retry(&self, _attempt: u32, _error_msg: &str) -> Option<Duration> {
        None
    }

    fn max_retries(&self) -> u32 {
        0
    }
}

// ---- 指数退避重试策略 ----

/// 指数退避重试策略
///
/// 每次重试的等待时间 = base_delay * 2^(attempt-1)
pub struct ExponentialBackoffRetry {
    /// 基础延迟
    base_delay: Duration,
    /// 最大重试次数
    max_retries: u32,
}

impl ExponentialBackoffRetry {
    /// 使用默认配置创建（base_delay=1s, max_retries=3）
    pub fn new() -> Self {
        Self {
            base_delay: Duration::from_millis(1000),
            max_retries: 3,
        }
    }

    /// 自定义最大重试次数
    pub fn with_max_retries(mut self, max_retries: u32) -> Self {
        self.max_retries = max_retries;
        self
    }

    /// 自定义基础延迟
    pub fn with_base_delay(mut self, base_delay: Duration) -> Self {
        self.base_delay = base_delay;
        self
    }
}

impl Default for ExponentialBackoffRetry {
    fn default() -> Self {
        Self::new()
    }
}

impl RetryPolicy for ExponentialBackoffRetry {
    fn should_retry(&self, attempt: u32, _error_msg: &str) -> Option<Duration> {
        if attempt > self.max_retries {
            return None;
        }
        // delay = base_delay * 2^(attempt-1)
        let multiplier = 2u64.pow(attempt.saturating_sub(1));
        Some(self.base_delay * multiplier as u32)
    }

    fn max_retries(&self) -> u32 {
        self.max_retries
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_no_retry_never_retries() {
        let policy = NoRetry;
        assert_eq!(policy.max_retries(), 0);
        assert!(policy.should_retry(1, "error").is_none());
        assert!(policy.should_retry(100, "error").is_none());
    }

    #[test]
    fn test_exponential_backoff_retry_count() {
        let policy = ExponentialBackoffRetry::new();
        assert_eq!(policy.max_retries(), 3);

        // 前 3 次应该重试
        assert!(policy.should_retry(1, "error").is_some());
        assert!(policy.should_retry(2, "error").is_some());
        assert!(policy.should_retry(3, "error").is_some());

        // 第 4 次不重试
        assert!(policy.should_retry(4, "error").is_none());
    }

    #[test]
    fn test_exponential_backoff_delay_grows() {
        let policy = ExponentialBackoffRetry::new();

        let d1 = policy.should_retry(1, "error").unwrap();
        let d2 = policy.should_retry(2, "error").unwrap();
        let d3 = policy.should_retry(3, "error").unwrap();

        // 延迟应该成倍增长: 1s, 2s, 4s
        assert_eq!(d1, Duration::from_millis(1000));
        assert_eq!(d2, Duration::from_millis(2000));
        assert_eq!(d3, Duration::from_millis(4000));
    }

    #[test]
    fn test_custom_max_retries() {
        let policy = ExponentialBackoffRetry::new().with_max_retries(5);
        assert_eq!(policy.max_retries(), 5);
        assert!(policy.should_retry(5, "error").is_some());
        assert!(policy.should_retry(6, "error").is_none());
    }

    #[test]
    fn test_custom_base_delay() {
        let policy = ExponentialBackoffRetry::new()
            .with_base_delay(Duration::from_millis(500))
            .with_max_retries(2);

        let d1 = policy.should_retry(1, "error").unwrap();
        let d2 = policy.should_retry(2, "error").unwrap();

        assert_eq!(d1, Duration::from_millis(500));
        assert_eq!(d2, Duration::from_millis(1000));
    }
}
