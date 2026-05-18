//! 类型化错误层次
//!
//! 替代 anyhow 的透明错误包装，提供结构化的错误信息

use thiserror::Error;

use crate::types::range::RangeError;

/// 顶层 Spark 错误
#[derive(Debug, Error)]
pub enum SparkError {
    #[error("[下载错误] {0}")]
    Download(#[from] DownloadError),

    #[error("[网络错误] {0}")]
    Network(#[from] NetworkError),

    #[error("[存储错误] {0}")]
    Storage(#[from] StorageError),

    #[error("[配置错误] {0}")]
    Config(#[from] ConfigError),

    #[error("[范围错误] {0}")]
    Range(#[from] RangeError),

    #[error("操作已取消")]
    Cancelled,
}

/// 下载错误
#[derive(Debug, Error)]
pub enum DownloadError {
    #[error("数据块下载失败")]
    ChunkFailed,

    #[error("重试次数已耗尽")]
    RetriesExhausted,

    #[error("IO 通道已关闭")]
    IoChannelClosed,

    #[error("下载已停止")]
    Stopped,
}

/// 网络错误
#[derive(Debug, Error)]
pub enum NetworkError {
    #[error("HTTP 请求失败: {0}")]
    Http(#[from] reqwest::Error),

    #[error("服务器错误")]
    Server,

    #[error("无效的服务器响应: {0}")]
    InvalidResponse(String),

    #[error("超过最大重试次数: {0}")]
    MaxRetriesExceeded(String),
}

/// 存储错误
#[derive(Debug, Error)]
pub enum StorageError {
    #[error("IO 操作失败: {0}")]
    Io(#[from] std::io::Error),

    #[error("文件定位失败")]
    SeekError,

    #[error("文件写入失败")]
    WriteError,
}

/// 配置错误
#[derive(Debug, Error)]
pub enum ConfigError {
    #[error("无效的下载线程数: {0}，必须在 1-256 之间")]
    InvalidThreadCount(usize),

    #[error("无效的 IO 线程数: {0}，必须在 1-8 之间")]
    InvalidIoThreadCount(usize),

    #[error("无效的输出路径: {0}")]
    InvalidOutputPath(String),
}
