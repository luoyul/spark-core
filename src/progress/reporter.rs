//! 进度报告 trait 和简单实现
//!
//! 定义下载进度事件的报告接口，所有进度报告器必须实现此 trait。

/// 进度事件
///
/// 包含当前下载进度的完整快照
#[derive(Debug, Clone)]
pub struct ProgressEvent {
    /// 已下载字节数
    pub downloaded_bytes: u64,
    /// 总字节数
    pub total_bytes: u64,
    /// 当前下载速度（字节/秒）
    pub speed_bytes_per_sec: u64,
    /// 当前活跃下载块数
    pub active_chunks: usize,
    /// 已完成下载块数
    pub completed_chunks: usize,
    /// 总下载块数
    pub total_chunks: usize,
}

impl ProgressEvent {
    /// 计算下载进度百分比（0.0 - 100.0）
    pub fn percentage(&self) -> f64 {
        if self.total_bytes == 0 {
            return 0.0;
        }
        (self.downloaded_bytes as f64 / self.total_bytes as f64) * 100.0
    }

    /// 是否下载完成
    pub fn is_complete(&self) -> bool {
        self.downloaded_bytes >= self.total_bytes && self.completed_chunks >= self.total_chunks
    }
}

/// 进度报告器 trait
///
/// 接收下载进度事件并进行展示或处理。
/// 所有实现必须线程安全（Send + Sync）。
pub trait ProgressReporter: Send + Sync {
    /// 报告进度更新
    ///
    /// # 返回
    /// * `true` - 继续下载
    /// * `false` - 取消下载
    fn on_progress(&self, event: &ProgressEvent) -> bool;

    /// 下载开始时调用
    fn on_start(&self, total_bytes: u64, total_chunks: usize);

    /// 下载完成时调用
    fn on_complete(&self);

    /// 下载出错时调用
    fn on_error(&self, error: &str);
}

// ---- 空操作报告器 ----

/// 空操作进度报告器
///
/// 所有方法为空实现，用于不需要进度显示的场景。
pub struct NoopReporter;

impl ProgressReporter for NoopReporter {
    fn on_progress(&self, _event: &ProgressEvent) -> bool {
        true
    }

    fn on_start(&self, _total_bytes: u64, _total_chunks: usize) {}

    fn on_complete(&self) {}

    fn on_error(&self, _error: &str) {}
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_progress_event_percentage() {
        let event = ProgressEvent {
            downloaded_bytes: 500,
            total_bytes: 1000,
            speed_bytes_per_sec: 100,
            active_chunks: 2,
            completed_chunks: 5,
            total_chunks: 10,
        };
        assert!((event.percentage() - 50.0).abs() < 0.01);
        assert!(!event.is_complete());
    }

    #[test]
    fn test_progress_event_zero_total() {
        let event = ProgressEvent {
            downloaded_bytes: 0,
            total_bytes: 0,
            speed_bytes_per_sec: 0,
            active_chunks: 0,
            completed_chunks: 0,
            total_chunks: 0,
        };
        assert!((event.percentage() - 0.0).abs() < 0.01);
    }

    #[test]
    fn test_progress_event_complete() {
        let event = ProgressEvent {
            downloaded_bytes: 1000,
            total_bytes: 1000,
            speed_bytes_per_sec: 0,
            active_chunks: 0,
            completed_chunks: 10,
            total_chunks: 10,
        };
        assert!(event.is_complete());
    }

    #[test]
    fn test_noop_reporter_always_returns_true() {
        let reporter = NoopReporter;
        let event = ProgressEvent {
            downloaded_bytes: 0,
            total_bytes: 100,
            speed_bytes_per_sec: 0,
            active_chunks: 0,
            completed_chunks: 0,
            total_chunks: 1,
        };
        assert!(reporter.on_progress(&event));
        // 空方法不应 panic
        reporter.on_start(100, 1);
        reporter.on_complete();
        reporter.on_error("test error");
    }
}
