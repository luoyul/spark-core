//! 下载引擎
//!
//! 核心下载引擎，协调查看分片计算、并发下载、IO 写入和进度报告。
//! 架构：下载线程与 IO 线程分离，通过有界通道通信，支持背压控制。
//!
//! 组件：
//! - ChunkDownloader: 单分片 HTTP 下载 + 重试
//! - IoWorkerPool: IO 线程池，写入 StorageBackend
//! - RetryPolicy: 可插拔重试策略
//! - ProgressReporter: 可插拔进度报告
//! - EventBus: 内部事件分发

use std::path::Path;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;

use anyhow::{anyhow, Context, Result};
use tokio::sync::{mpsc, Mutex, RwLock, Semaphore};
use tokio::task::JoinSet;
use tracing::{debug, error, info, warn};

use crate::config::download::DownloadConfig;
use crate::network::client::HttpClient;
use crate::storage::backend::StorageBackend;
use crate::storage::sparse_file::SparseFileBackend;
use crate::types::range::HttpRange;

use super::chunk_downloader::{ChunkDownloader, ControlFlags, IoTask};
use super::io_worker::IoWorkerPool;
use super::retry::ExponentialBackoffRetry;

/// 下载状态
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DownloadState {
    Idle,
    Downloading,
    Paused,
    Completed,
    Error,
}

/// 下载统计信息
#[derive(Debug, Default, Clone)]
pub struct DownloadStats {
    pub downloaded_bytes: u64,
    pub total_bytes: u64,
    pub speed: u64,
    pub completed_chunks: usize,
    pub total_chunks: usize,
    pub active_download_threads: usize,
    pub io_queue_backlog: usize,
}

/// 下载引擎
///
/// 组装的顶层组件，提供 download/pause/resume/stop 高级 API。
pub struct DownloadEngine {
    client: HttpClient,
    config: DownloadConfig,
    control: Arc<ControlFlags>,
    state: Arc<RwLock<DownloadState>>,
    stats: Arc<RwLock<DownloadStats>>,
    /// 活跃下载计数（用于统计）
    active_downloads: Arc<AtomicUsize>,
    /// 已完成字节数（原子，供进度报告快速读取）
    downloaded_bytes: Arc<AtomicU64>,
}

impl DownloadEngine {
    /// 创建新的下载引擎
    pub fn new(config: DownloadConfig) -> Result<Self> {
        let client = HttpClient::with_proxy(config.proxy.clone())
            .map_err(|e| anyhow!("创建 HTTP 客户端失败: {}", e))?;

        Ok(Self {
            client,
            config,
            control: Arc::new(ControlFlags::default()),
            state: Arc::new(RwLock::new(DownloadState::Idle)),
            stats: Arc::new(RwLock::new(DownloadStats::default())),
            active_downloads: Arc::new(AtomicUsize::new(0)),
            downloaded_bytes: Arc::new(AtomicU64::new(0)),
        })
    }

    /// 获取当前状态
    pub async fn state(&self) -> DownloadState {
        *self.state.read().await
    }

    /// 获取统计信息
    pub async fn stats(&self) -> DownloadStats {
        self.stats.read().await.clone()
    }

    /// 暂停下载
    pub fn pause(&self) {
        self.control.pause();
        info!("下载已暂停");
    }

    /// 恢复下载
    pub fn resume(&self) {
        self.control.resume();
        info!("下载已恢复");
    }

    /// 停止下载
    pub fn stop(&self) {
        self.control.stop();
        info!("下载已停止");
    }

    /// 下载文件
    ///
    /// 完整的下载流程：获取文件大小 → 创建存储 → 计算分片 → 并发下载 → IO 写入
    pub async fn download(&self, url: &str, output: &Path) -> Result<()> {
        // 设置状态为下载中
        {
            let mut state = self.state.write().await;
            *state = DownloadState::Downloading;
        }

        // 重置控制标志
        self.control.resume(); // 清除暂停状态
        // stopped 状态在每个下载流程开始时重置
        self.control.stopped.store(false, Ordering::SeqCst);

        info!("开始下载: {}", url);
        debug!("输出路径: {:?}", output);

        // 1. 获取文件大小
        let file_size = self
            .client
            .get_file_size(url)
            .await
            .map_err(|e| anyhow!("获取文件大小失败: {}", e))?;

        info!("文件大小: {} 字节", file_size);

        {
            let mut stats = self.stats.write().await;
            stats.total_bytes = file_size;
        }

        // 2. 创建存储后端（稀疏文件）
        let file = std::fs::File::create(output)
            .with_context(|| format!("创建文件失败: {:?}", output))?;
        let mut backend = SparseFileBackend::from_file(file, output.to_path_buf());
        backend
            .create(file_size)
            .await
            .context("预分配文件失败")?;

        let storage: Arc<Mutex<dyn StorageBackend>> = Arc::new(Mutex::new(backend));

        // 3. 启动 IO 线程池
        let (io_pool, io_tx) = IoWorkerPool::spawn(
            self.config.io_thread_count,
            Arc::clone(&storage),
            self.config.io_channel_capacity,
            Arc::clone(&self.control),
        );

        // 4. 计算分片
        let chunks = self.calculate_chunks(file_size);
        let chunk_count = chunks.len();

        info!("总分片: {}, IO 线程: {}", chunk_count, self.config.io_thread_count);

        {
            let mut stats = self.stats.write().await;
            stats.total_chunks = chunk_count;
        }

        // 5. 创建分片下载器
        let retry_policy = Arc::new(ExponentialBackoffRetry::new()
            .with_max_retries(self.config.retry_limit as u32));
        let _chunk_downloader = ChunkDownloader::new(
            self.client.clone(),
            retry_policy,
            self.config.buffer_size,
        );

        // 6. 并发下载：Semaphore + JoinSet
        let semaphore = Arc::new(Semaphore::new(self.config.thread_count));
        let mut join_set = JoinSet::new();

        for chunk in chunks {
            if self.control.is_stopped() {
                info!("用户停止下载，中断任务提交");
                break;
            }

            // 等待暂停恢复
            if self.control.wait_if_paused().await {
                break;
            }

            // 获取信号量许可
            let permit = semaphore
                .clone()
                .acquire_owned()
                .await
                .map_err(|_| anyhow!("信号量关闭"))?;

            let url_owned = url.to_string();
            let io_tx = io_tx.clone();
            let control = Arc::clone(&self.control);
            let downloaded = Arc::clone(&self.downloaded_bytes);
            let active = Arc::clone(&self.active_downloads);
            let client = self.client.clone();

            join_set.spawn(async move {
                let _permit = permit;
                active.fetch_add(1, Ordering::SeqCst);

                let range_header = format!("bytes={}-{}", chunk.range.start, chunk.range.end.saturating_sub(1));

                let result = Self::download_single_chunk(
                    &client,
                    &url_owned,
                    &range_header,
                    chunk,
                    &io_tx,
                    &control,
                    downloaded.clone(),
                )
                .await;

                active.fetch_sub(1, Ordering::SeqCst);
                (chunk.index, result)
            });
        }

        // 7. 等待所有分片完成
        let mut failed_chunks: Vec<(usize, String)> = Vec::new();

        while let Some(result) = join_set.join_next().await {
            match result {
                Ok((index, Ok(bytes))) => {
                    debug!("分片 {} 完成: {} 字节", index, bytes);
                    let mut stats = self.stats.write().await;
                    stats.completed_chunks += 1;
                }
                Ok((index, Err(e))) => {
                    error!("分片 {} 失败: {}", index, e);
                    failed_chunks.push((index, e.to_string()));
                }
                Err(e) => {
                    error!("任务 panic: {}", e);
                }
            }
        }

        // 8. 关闭 IO 通道并等待 IO 线程完成
        drop(io_tx);
        info!("等待 IO 线程完成...");
        io_pool.join_all().await?;

        // 9. 刷新存储
        {
            let mut backend = storage.lock().await;
            backend.flush().await.context("刷新文件失败")?;
        }

        // 10. 处理结果
        if !failed_chunks.is_empty() {
            let mut state = self.state.write().await;
            *state = DownloadState::Error;
            return Err(anyhow!(
                "下载不完整: {} 个分片失败",
                failed_chunks.len()
            ));
        }

        if self.control.is_stopped() {
            let mut state = self.state.write().await;
            *state = DownloadState::Paused;
            return Err(anyhow!("下载被用户停止"));
        }

        // 完成
        {
            let mut stats = self.stats.write().await;
            stats.downloaded_bytes = self.downloaded_bytes.load(Ordering::SeqCst);
        }

        {
            let mut state = self.state.write().await;
            *state = DownloadState::Completed;
        }

        info!("下载完成");
        Ok(())
    }

    /// 计算分片（内部方法）
    fn calculate_chunks(&self, file_size: u64) -> Vec<ChunkDescriptor> {
        Self::compute_chunks(file_size, self.config.thread_count, self.config.min_chunk_size)
    }

    /// 分片计算逻辑（纯函数，便于测试）
    pub fn compute_chunks(file_size: u64, thread_count: usize, min_chunk_size: u64) -> Vec<ChunkDescriptor> {
        let chunk_size = std::cmp::max(
            file_size / thread_count as u64,
            min_chunk_size,
        );

        let mut chunks = Vec::new();
        let mut start = 0u64;
        let mut index = 0usize;

        while start < file_size {
            let end = std::cmp::min(start + chunk_size, file_size);
            chunks.push(ChunkDescriptor {
                index,
                range: HttpRange::new_unchecked(start, end),
            });
            start = end;
            index += 1;
        }

        chunks
    }

    /// 下载单个分片的内部实现
    ///
    /// 发送 Range 请求，流式读取数据并通过 io_tx 发送到 IO 线程。
    async fn download_single_chunk(
        client: &HttpClient,
        url: &str,
        range_header: &str,
        chunk: ChunkDescriptor,
        io_tx: &mpsc::Sender<IoTask>,
        control: &ControlFlags,
        downloaded: Arc<AtomicU64>,
    ) -> Result<u64> {
        let mut attempt = 0u32;

        loop {
            if control.is_stopped() {
                return Err(anyhow!("已停止"));
            }

            if control.wait_if_paused().await {
                return Err(anyhow!("已停止"));
            }

            match Self::try_download_chunk(
                client, url, range_header, chunk, io_tx, &downloaded,
            ).await {
                Ok(bytes) => return Ok(bytes),
                Err(e) => {
                    attempt += 1;
                    if attempt > 3 {
                        return Err(anyhow!("重试 {} 次后失败: {}", 3, e));
                    }
                    let delay = tokio::time::Duration::from_millis(
                        1000u64 * 2u64.pow(attempt),
                    );
                    warn!("分片 {} 下载失败 (第 {}/3 次): {}。等待 {:?}",
                        chunk.index, attempt, e, delay);
                    tokio::time::sleep(delay).await;
                }
            }
        }
    }

    /// 下载单个分片的内部实现（单次尝试）
    async fn try_download_chunk(
        client: &HttpClient,
        url: &str,
        range_header: &str,
        chunk: ChunkDescriptor,
        io_tx: &mpsc::Sender<IoTask>,
        downloaded: &AtomicU64,
    ) -> Result<u64> {
        use futures::StreamExt;

        let response = client
            .inner()
            .get(url)
            .header(reqwest::header::RANGE, range_header)
            .send()
            .await
            .context("HTTP 请求失败")?;

        let status = response.status();
        if !status.is_success() && status != reqwest::StatusCode::PARTIAL_CONTENT {
            return Err(anyhow!("服务器错误: {}", status));
        }

        let mut stream = response.bytes_stream();
        let mut offset = chunk.range.start;
        let buffer_size = 64 * 1024; // 64KB
        let mut buffer = Vec::with_capacity(buffer_size);
        let mut total_bytes: u64 = 0;

        while let Some(chunk_result) = stream.next().await {
            let data = chunk_result.context("读取流失败")?;
            buffer.extend_from_slice(&data);

            if buffer.len() >= buffer_size {
                let data_len = buffer.len() as u64;
                let task = IoTask {
                    offset,
                    data: std::mem::take(&mut buffer),
                    chunk_index: chunk.index,
                };

                // 发送到 IO 通道（通道满时等待）
                let mut task = task;
                loop {
                    match io_tx.try_send(task) {
                        Ok(()) => break,
                        Err(mpsc::error::TrySendError::Full(t)) => {
                            tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;
                            task = t;
                        }
                        Err(mpsc::error::TrySendError::Closed(_)) => {
                            return Err(anyhow!("IO 通道关闭"));
                        }
                    }
                }

                offset += data_len;
                total_bytes += data_len;
                downloaded.fetch_add(data_len, Ordering::SeqCst);
                buffer = Vec::with_capacity(buffer_size);
            }
        }

        // 发送剩余数据
        if !buffer.is_empty() {
            let data_len = buffer.len() as u64;
            io_tx
                .send(IoTask { offset, data: buffer, chunk_index: chunk.index })
                .await
                .map_err(|_| anyhow!("发送尾部数据到 IO 通道失败"))?;
            total_bytes += data_len;
            downloaded.fetch_add(data_len, Ordering::SeqCst);
        }

        Ok(total_bytes)
    }
}

/// 分片描述符
#[derive(Debug, Clone, Copy)]
pub struct ChunkDescriptor {
    pub index: usize,
    pub range: HttpRange,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_calculate_chunks() {
        // 10MB 文件，4 线程 → 4 个分片，每个 ~2.5MB
        let chunks = DownloadEngine::compute_chunks(10 * 1024 * 1024, 4, 1024 * 1024);
        assert_eq!(chunks.len(), 4);

        // 第一个分片从 0 开始
        assert_eq!(chunks[0].range.start, 0);
        // 最后一个分片结束于文件大小
        assert_eq!(chunks.last().unwrap().range.end, 10 * 1024 * 1024);
        // 分片连续性
        for i in 1..chunks.len() {
            assert_eq!(chunks[i].range.start, chunks[i - 1].range.end);
        }
    }

    #[test]
    fn test_calculate_chunks_small_file() {
        // 512KB 文件 → 单个分片
        let chunks = DownloadEngine::compute_chunks(512 * 1024, 8, 1024 * 1024);
        assert_eq!(chunks.len(), 1);
        assert_eq!(chunks[0].range.start, 0);
        assert_eq!(chunks[0].range.end, 512 * 1024);
    }

    #[test]
    fn test_calculate_chunks_min_size() {
        // 1MB 文件，8 线程，最小 1MB → 1 个分片
        let chunks = DownloadEngine::compute_chunks(1024 * 1024, 8, 1024 * 1024);
        assert_eq!(chunks.len(), 1);
    }

    #[test]
    fn test_control_flags() {
        let flags = ControlFlags::default();
        assert!(!flags.is_paused());
        assert!(!flags.is_stopped());

        flags.pause();
        assert!(flags.is_paused());
        assert!(!flags.is_stopped());

        flags.resume();
        assert!(!flags.is_paused());

        flags.stop();
        assert!(flags.is_stopped());
    }

    #[test]
    fn test_download_state() {
        // 验证状态枚举存在
        let _state = DownloadState::Idle;
    }
}
