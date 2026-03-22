//! 下载引擎
//!
//! 核心下载引擎，负责任务调度、分片下载、错误处理和暂停恢复
//! 架构：下载线程与 IO 线程分离，通过有界通道通信，支持背压控制

use anyhow::{anyhow, Context, Result};
use futures::StreamExt;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;
use tokio::sync::{mpsc, Mutex, RwLock, Semaphore};
use tokio::task::JoinSet;
use tracing::{debug, error, info, warn};

use crate::downloader::scheduler::ByteRange;
use crate::io::sparse_file::SparseFile;
use crate::network::client::HttpClient;

/// 下载配置
#[derive(Debug, Clone)]
pub struct DownloadConfig {
    /// 下载线程数 (1-256)
    pub thread_count: usize,
    /// IO 线程数 (1-8)
    pub io_thread_count: usize,
    /// 内存缓冲区大小（每个下载线程）
    pub buffer_size: usize,
    /// IO 通道容量（控制背压）
    pub io_channel_capacity: usize,
    /// 重试次数限制
    pub retry_limit: u8,
    /// 输出目录
    pub output_dir: PathBuf,
}

impl Default for DownloadConfig {
    fn default() -> Self {
        Self {
            thread_count: 8,
            io_thread_count: 2,
            buffer_size: 64 * 1024,      // 64KB 每个下载线程
            io_channel_capacity: 128,     // IO 通道容量
            retry_limit: 3,
            output_dir: PathBuf::from("./downloads"),
        }
    }
}

impl DownloadConfig {
    /// 创建新的下载配置
    pub fn new() -> Self {
        Self::default()
    }

    /// 设置下载线程数
    pub fn thread_count(mut self, count: usize) -> Self {
        self.thread_count = count.clamp(1, 256);
        self
    }

    /// 设置 IO 线程数
    pub fn io_thread_count(mut self, count: usize) -> Self {
        self.io_thread_count = count.clamp(1, 8);
        self
    }

    /// 设置缓冲区大小
    pub fn buffer_size(mut self, size: usize) -> Self {
        self.buffer_size = size.max(4096);
        self
    }

    /// 设置 IO 通道容量
    pub fn io_channel_capacity(mut self, capacity: usize) -> Self {
        self.io_channel_capacity = capacity.max(16);
        self
    }

    /// 设置重试限制
    pub fn retry_limit(mut self, limit: u8) -> Self {
        self.retry_limit = limit;
        self
    }

    /// 设置输出目录
    pub fn output_dir<P: AsRef<Path>>(mut self, dir: P) -> Self {
        self.output_dir = dir.as_ref().to_path_buf();
        self
    }
}

/// 下载状态
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DownloadState {
    /// 空闲
    Idle,
    /// 正在下载
    Downloading,
    /// 已暂停
    Paused,
    /// 已完成
    Completed,
    /// 出错
    Error,
}

/// 下载统计信息
#[derive(Debug, Default)]
pub struct DownloadStats {
    /// 已下载字节数
    pub downloaded_bytes: u64,
    /// 总字节数
    pub total_bytes: u64,
    /// 当前速度 (bytes/s)
    pub speed: u64,
    /// 已完成的任务数
    pub completed_chunks: usize,
    /// 总任务数
    pub total_chunks: usize,
    /// 当前活跃下载线程
    pub active_download_threads: usize,
    /// IO 队列积压数量
    pub io_queue_backlog: usize,
}

/// IO 任务（下载线程 -> IO 线程）
#[derive(Debug)]
struct IoTask {
    /// 文件偏移量
    offset: u64,
    /// 数据
    data: Vec<u8>,
    /// 所属分片索引
    chunk_index: usize,
}

/// 下载引擎
pub struct DownloadEngine {
    /// HTTP客户端
    client: HttpClient,
    /// 下载配置
    config: DownloadConfig,
    /// 暂停标志 (原子变量)
    paused: Arc<AtomicBool>,
    /// 停止标志 (原子变量)
    stopped: Arc<AtomicBool>,
    /// 当前状态
    state: Arc<RwLock<DownloadState>>,
    /// 统计信息
    stats: Arc<RwLock<DownloadStats>>,
    /// 活跃下载线程计数（用于背压控制）
    active_downloads: Arc<AtomicUsize>,
}

impl DownloadEngine {
    /// 创建新的下载引擎
    pub fn new(config: DownloadConfig) -> Result<Self> {
        let client = HttpClient::new()
            .map_err(|e| anyhow!("Failed to create HTTP client: {}", e))?;

        Ok(Self {
            client,
            config,
            paused: Arc::new(AtomicBool::new(false)),
            stopped: Arc::new(AtomicBool::new(false)),
            state: Arc::new(RwLock::new(DownloadState::Idle)),
            stats: Arc::new(RwLock::new(DownloadStats::default())),
            active_downloads: Arc::new(AtomicUsize::new(0)),
        })
    }

    /// 获取当前状态
    pub async fn state(&self) -> DownloadState {
        *self.state.read().await
    }

    /// 获取统计信息
    pub async fn stats(&self) -> DownloadStats {
        let stats = self.stats.read().await;
        DownloadStats {
            downloaded_bytes: stats.downloaded_bytes,
            total_bytes: stats.total_bytes,
            speed: stats.speed,
            completed_chunks: stats.completed_chunks,
            total_chunks: stats.total_chunks,
            active_download_threads: stats.active_download_threads,
            io_queue_backlog: stats.io_queue_backlog,
        }
    }

    /// 暂停下载
    pub fn pause(&self) {
        self.paused.store(true, Ordering::SeqCst);
        info!("Download paused");
    }

    /// 恢复下载
    pub fn resume(&self) {
        self.paused.store(false, Ordering::SeqCst);
        info!("Download resumed");
    }

    /// 停止下载
    pub fn stop(&self) {
        self.stopped.store(true, Ordering::SeqCst);
        info!("Download stopped");
    }

    /// 检查是否已暂停
    fn is_paused(&self) -> bool {
        self.paused.load(Ordering::SeqCst)
    }

    /// 检查是否已停止
    fn is_stopped(&self) -> bool {
        self.stopped.load(Ordering::SeqCst)
    }

    /// 等待直到恢复或停止
    async fn wait_if_paused(&self) {
        while self.is_paused() && !self.is_stopped() {
            tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
        }
    }

    /// 下载文件
    pub async fn download(&self, url: &str, output: &Path) -> Result<()> {
        // 设置状态为下载中
        {
            let mut state = self.state.write().await;
            *state = DownloadState::Downloading;
        }

        // 重置停止标志
        self.stopped.store(false, Ordering::SeqCst);

        info!("Starting download from: {}", url);
        debug!("Output path: {:?}", output);

        // 1. 获取文件大小
        let file_size = self
            .client
            .get_file_size(url)
            .await
            .map_err(|e| anyhow!("Failed to fetch file size: {}", e))?;

        info!("File size: {} bytes", file_size);

        // 更新统计信息
        {
            let mut stats = self.stats.write().await;
            stats.total_bytes = file_size;
        }

        // 2. 创建稀疏文件
        let sparse_file =
            SparseFile::create(output, file_size).context("Failed to create sparse file")?;
        let sparse_file = Arc::new(Mutex::new(sparse_file));

        // 3. 创建 IO 通道（有界，用于背压控制）
        let (io_tx, io_rx) = mpsc::channel::<IoTask>(self.config.io_channel_capacity);
        let io_rx = Arc::new(Mutex::new(io_rx));
        
        // 创建 IO 背压信号量（控制未完成 IO 任务数量）
        let io_semaphore = Arc::new(Semaphore::new(self.config.io_channel_capacity));
        let io_semaphore_clone = Arc::clone(&io_semaphore);

        // 4. 启动 IO 线程
        let mut io_handles = Vec::new();
        for io_id in 0..self.config.io_thread_count {
            let io_rx = Arc::clone(&io_rx);
            let sparse_file = Arc::clone(&sparse_file);
            let stopped = Arc::clone(&self.stopped);
            let stats = Arc::clone(&self.stats);

            let handle = tokio::spawn(async move {
                Self::io_worker(io_id, io_rx, sparse_file, stopped, stats).await;
            });
            io_handles.push(handle);
        }

        // 5. 计算分片任务
        let chunks = self.calculate_chunks(file_size);
        let chunk_count = chunks.len();

        info!("Total chunks: {}, IO threads: {}", chunk_count, self.config.io_thread_count);

        // 更新统计信息
        {
            let mut stats = self.stats.write().await;
            stats.total_chunks = chunk_count;
        }

        // 6. 创建下载信号量限制并发数
        let semaphore = Arc::new(Semaphore::new(self.config.thread_count));
        let mut download_handles = JoinSet::new();

        // 7. 提交下载任务
        for (index, range) in chunks.into_iter().enumerate() {
            if self.is_stopped() {
                info!("Download stopped by user");
                break;
            }

            self.wait_if_paused().await;

            // 背压控制：获取 IO 信号量许可
            // 当 IO 队列满时会阻塞，自然实现背压
            let io_permit = match io_semaphore_clone.clone().try_acquire_owned() {
                Ok(permit) => permit,
                Err(_) => {
                    // 队列满，等待一段时间再试
                    warn!("IO queue full, throttling download...");
                    let io_permit = io_semaphore_clone
                        .clone()
                        .acquire_owned()
                        .await
                        .map_err(|_| anyhow!("IO semaphore closed"))?;
                    io_permit
                }
            };

            let url = url.to_string();
            let permit = semaphore.clone().acquire_owned().await?;
            let client = self.client.clone();
            let paused = self.paused.clone();
            let stopped = self.stopped.clone();
            let stats = self.stats.clone();
            let active_downloads = self.active_downloads.clone();
            let buffer_size = self.config.buffer_size;
            let io_tx = io_tx.clone();

            download_handles.spawn(async move {
                let _permit = permit;
                let _io_permit = io_permit; // 保持 IO 许可直到任务完成
                active_downloads.fetch_add(1, Ordering::SeqCst);
                
                let result = Self::download_chunk_with_retry(
                    &client,
                    &url,
                    range,
                    index,
                    buffer_size,
                    &paused,
                    &stopped,
                    &stats,
                    &io_tx,
                )
                .await;

                active_downloads.fetch_sub(1, Ordering::SeqCst);
                (index, range, result)
            });
        }

        // 8. 等待所有下载任务完成
        let mut failed_chunks = Vec::new();

        while let Some(result) = download_handles.join_next().await {
            match result {
                Ok((index, range, Ok(()))) => {
                    debug!("Chunk {} completed successfully", index);
                    {
                        let mut stats = self.stats.write().await;
                        stats.completed_chunks += 1;
                        stats.downloaded_bytes += range.size();
                    }
                }
                Ok((index, _range, Err(e))) => {
                    error!("Chunk {} failed: {}", index, e);
                    failed_chunks.push((index, e));
                }
                Err(e) => {
                    error!("Task panicked: {}", e);
                }
            }
        }

        // 9. 关闭 IO 通道，等待 IO 线程完成
        drop(io_tx);
        info!("Waiting for IO workers to complete...");
        for handle in io_handles {
            let _ = handle.await;
        }

        // 10. 刷新文件
        {
            let mut file = sparse_file.lock().await;
            file.sync().context("Failed to sync file")?;
        }

        // 11. 处理失败的块
        if !failed_chunks.is_empty() {
            error!("{} chunks failed to download", failed_chunks.len());
            let mut state = self.state.write().await;
            *state = DownloadState::Error;
            return Err(anyhow!(
                "Download incomplete: {} chunks failed",
                failed_chunks.len()
            ));
        }

        if self.is_stopped() {
            let mut state = self.state.write().await;
            *state = DownloadState::Paused;
            return Err(anyhow!("Download was stopped by user"));
        }

        // 更新状态为完成
        {
            let mut state = self.state.write().await;
            *state = DownloadState::Completed;
        }

        info!("Download completed successfully");
        Ok(())
    }

    /// IO 工作线程
    async fn io_worker(
        _io_id: usize,
        io_rx: Arc<Mutex<mpsc::Receiver<IoTask>>>,
        sparse_file: Arc<Mutex<SparseFile>>,
        stopped: Arc<AtomicBool>,
        _stats: Arc<RwLock<DownloadStats>>,
    ) {
        loop {
            if stopped.load(Ordering::SeqCst) {
                break;
            }

            let task = {
                let mut rx = io_rx.lock().await;
                rx.recv().await
            };

            match task {
                Some(IoTask { offset, data, chunk_index }) => {
                    debug!("IO worker writing chunk {} at offset {}", chunk_index, offset);
                    
                    let mut file = sparse_file.lock().await;
                    if let Err(e) = file.write_at(offset, &data) {
                        error!("Failed to write chunk {} at offset {}: {}", chunk_index, offset, e);
                    }
                }
                None => {
                    // 通道关闭，退出
                    break;
                }
            }
        }
    }

    /// 计算分片
    fn calculate_chunks(&self, file_size: u64) -> Vec<ByteRange> {
        let chunk_size = std::cmp::max(
            file_size / self.config.thread_count as u64,
            1024 * 1024, // 最小1MB
        );

        let mut chunks = Vec::new();
        let mut start = 0u64;

        while start < file_size {
            let end = std::cmp::min(start + chunk_size, file_size);
            chunks.push(ByteRange::new(start, end));
            start = end;
        }

        chunks
    }

    /// 带重试的块下载
    async fn download_chunk_with_retry(
        client: &HttpClient,
        url: &str,
        range: ByteRange,
        chunk_index: usize,
        buffer_size: usize,
        paused: &AtomicBool,
        stopped: &AtomicBool,
        _stats: &RwLock<DownloadStats>,
        io_tx: &mpsc::Sender<IoTask>,
    ) -> Result<()> {
        let mut attempts = 0u8;

        loop {
            if stopped.load(Ordering::SeqCst) {
                return Err(anyhow!("Download stopped"));
            }

            while paused.load(Ordering::SeqCst) && !stopped.load(Ordering::SeqCst) {
                tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
            }

            match Self::download_chunk(client, url, range, chunk_index, buffer_size, io_tx).await {
                Ok(()) => return Ok(()),
                Err(e) => {
                    attempts += 1;
                    if attempts > 3 {
                        return Err(anyhow!("Failed after {} attempts: {}", 3, e));
                    }
                    warn!(
                        "Chunk {} download failed (attempt {}/{}): {}",
                        chunk_index, attempts, 3, e
                    );
                    let delay = tokio::time::Duration::from_millis(1000u64 * 2u64.pow(attempts as u32));
                    tokio::time::sleep(delay).await;
                }
            }
        }
    }

    /// 下载单个数据块（发送到 IO 通道）
    async fn download_chunk(
        client: &HttpClient,
        url: &str,
        range: ByteRange,
        chunk_index: usize,
        buffer_size: usize,
        io_tx: &mpsc::Sender<IoTask>,
    ) -> Result<()> {
        debug!("Downloading chunk {}: {}-{} bytes", chunk_index, range.start, range.end);

        let range_header = format!("bytes={}-{}", range.start, range.end.saturating_sub(1));
        let response = client
            .inner()
            .get(url)
            .header(reqwest::header::RANGE, &range_header)
            .send()
            .await
            .context("HTTP request failed")?;

        let status = response.status();
        if !status.is_success() && status != reqwest::StatusCode::PARTIAL_CONTENT {
            return Err(anyhow!("Server returned error: {}", status));
        }

        let mut stream = response.bytes_stream();
        let mut offset = range.start;
        let mut buffer = Vec::with_capacity(buffer_size);

        while let Some(chunk_result) = stream.next().await {
            let chunk = chunk_result.context("Failed to read response stream")?;
            buffer.extend_from_slice(&chunk);

            // 缓冲区满时发送到 IO 通道
            if buffer.len() >= buffer_size {
                let mut task = IoTask {
                    offset,
                    data: std::mem::take(&mut buffer),
                    chunk_index,
                };
                
                // 发送失败时重试
                loop {
                    match io_tx.try_send(task) {
                        Ok(()) => break,
                        Err(mpsc::error::TrySendError::Full(t)) => {
                            // 通道满，等待后重试
                            tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;
                            task = t;
                        }
                        Err(mpsc::error::TrySendError::Closed(_)) => {
                            return Err(anyhow!("IO channel closed"));
                        }
                    }
                }

                offset += buffer.capacity() as u64;
                buffer = Vec::with_capacity(buffer_size);
            }
        }

        // 发送剩余数据
        if !buffer.is_empty() {
            let task = IoTask {
                offset,
                data: buffer,
                chunk_index,
            };
            
            io_tx.send(task).await
                .map_err(|_| anyhow!("Failed to send final buffer to IO channel"))?;
        }

        debug!("Chunk {} downloaded and queued for IO", chunk_index);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_download_config_default() {
        let config = DownloadConfig::default();
        assert_eq!(config.thread_count, 8);
        assert_eq!(config.io_thread_count, 2);
        assert_eq!(config.buffer_size, 64 * 1024);
        assert_eq!(config.io_channel_capacity, 128);
    }

    #[test]
    fn test_download_config_builder() {
        let config = DownloadConfig::new()
            .thread_count(16)
            .io_thread_count(4)
            .buffer_size(128 * 1024)
            .io_channel_capacity(256);

        assert_eq!(config.thread_count, 16);
        assert_eq!(config.io_thread_count, 4);
        assert_eq!(config.buffer_size, 128 * 1024);
        assert_eq!(config.io_channel_capacity, 256);
    }

    #[test]
    fn test_thread_count_limits() {
        let config = DownloadConfig::new().thread_count(0);
        assert_eq!(config.thread_count, 1);

        let config = DownloadConfig::new().thread_count(500);
        assert_eq!(config.thread_count, 256);
    }

    #[test]
    fn test_io_thread_limits() {
        let config = DownloadConfig::new().io_thread_count(0);
        assert_eq!(config.io_thread_count, 1);

        let config = DownloadConfig::new().io_thread_count(20);
        assert_eq!(config.io_thread_count, 8);
    }
}
