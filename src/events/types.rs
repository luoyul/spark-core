//! 下载事件类型
//!
//! 定义下载过程中产生的各类事件。

/// 下载事件
///
/// 覆盖下载生命周期的各个阶段，用于事件驱动的下载管理。
#[derive(Debug, Clone)]
pub enum DownloadEvent {
    /// 下载块开始
    ChunkStarted {
        /// 块 ID
        chunk_id: u64,
        /// 起始字节
        range_start: u64,
        /// 结束字节（不含）
        range_end: u64,
    },

    /// 下载块完成
    ChunkCompleted {
        /// 块 ID
        chunk_id: u64,
        /// 下载字节数
        bytes: u64,
    },

    /// 下载块失败
    ChunkFailed {
        /// 块 ID
        chunk_id: u64,
        /// 错误信息
        error: String,
    },

    /// 所有下载块完成
    AllChunksCompleted,

    /// 下载已暂停
    DownloadPaused,

    /// 下载已恢复
    DownloadResumed,

    /// I/O 队列积压告警
    IoQueuePressure {
        /// 积压任务数
        backlog: usize,
    },

    /// 致命错误
    FatalError {
        /// 错误信息
        message: String,
    },
}
