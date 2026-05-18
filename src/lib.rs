//! Spark Core - 高性能多线程下载器
//!
//! 使用Tokio异步运行时，支持1-256线程并发下载

pub mod config;
pub mod downloader;
pub mod events;
pub mod io;
pub mod network;
pub mod progress;
pub mod storage;
pub mod types;
pub mod utils;

pub use config::download::DownloadConfig;
pub use downloader::chunk_downloader::{ChunkDownloader, ControlFlags};
pub use downloader::engine::{ChunkDescriptor, DownloadEngine, DownloadState, DownloadStats};
pub use downloader::io_worker::IoWorkerPool;
pub use downloader::retry::{ExponentialBackoffRetry, NoRetry, RetryPolicy};
pub use downloader::scheduler::{DownloadTask as SchedulerTask, Scheduler};
pub use downloader::task::{DownloadTask, TaskManager, TaskResult};
pub use network::client::{HttpClient, ProxyConfig};
pub use storage::backend::StorageBackend;
pub use storage::memory::MemoryBackend;
pub use storage::sparse_file::SparseFileBackend;
pub use types::range::HttpRange;
pub use utils::progress::ProgressReporter;

use anyhow::Result;
use std::path::PathBuf;

/// 下载器版本
pub const VERSION: &str = env!("CARGO_PKG_VERSION");

/// 默认线程数
pub const DEFAULT_THREADS: usize = 8;

/// 默认缓冲区大小 (64MB)
pub const DEFAULT_BUFFER_SIZE: usize = 64 * 1024 * 1024;

/// 最大线程数
pub const MAX_THREADS: usize = 256;

/// 最小线程数
pub const MIN_THREADS: usize = 1;

/// 创建新的下载引擎
pub fn create_engine(config: DownloadConfig) -> Result<DownloadEngine> {
    DownloadEngine::new(config)
}

/// 快速下载函数
pub async fn download(url: &str, output: PathBuf, threads: usize, proxy: ProxyConfig) -> Result<()> {
    let config = DownloadConfig {
        thread_count: threads.clamp(MIN_THREADS, MAX_THREADS),
        io_thread_count: 2,
        buffer_size: 64 * 1024,
        io_channel_capacity: 128,
        retry_limit: 3,
        output_dir: output.parent().unwrap_or(&PathBuf::from(".")).to_path_buf(),
        min_chunk_size: 1024 * 1024,
        proxy,
    };

    let engine = DownloadEngine::new(config)?;
    engine.download(url, &output).await
}
