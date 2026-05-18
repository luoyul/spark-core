//! 分片下载器
//!
//! 负责单个分片的 HTTP 下载，支持暂停/停止控制和重试策略。
//! 独立可测试，不依赖其他下载引擎组件。

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use anyhow::{anyhow, Context, Result};
use futures::StreamExt;
use tokio::sync::mpsc;
use tracing::{debug, warn};

use crate::network::client::HttpClient;
use crate::types::range::HttpRange;

use super::retry::RetryPolicy;

/// IO 任务
#[derive(Debug)]
pub struct IoTask {
    pub offset: u64,
    pub data: Vec<u8>,
    pub chunk_index: usize,
}

/// 分片下载结果
#[derive(Debug)]
pub struct ChunkResult {
    pub index: usize,
    pub bytes_downloaded: u64,
}

/// 暂停/停止控制标志
#[derive(Default)]
pub struct ControlFlags {
    pub paused: AtomicBool,
    pub stopped: AtomicBool,
}

impl ControlFlags {
    /// 暂停
    pub fn pause(&self) { self.paused.store(true, Ordering::SeqCst); }
    /// 恢复
    pub fn resume(&self) { self.paused.store(false, Ordering::SeqCst); }
    /// 停止
    pub fn stop(&self) { self.stopped.store(true, Ordering::SeqCst); }
    /// 是否暂停
    pub fn is_paused(&self) -> bool { self.paused.load(Ordering::SeqCst) }
    /// 是否停止
    pub fn is_stopped(&self) -> bool { self.stopped.load(Ordering::SeqCst) }

    /// 等待直到恢复或停止，返回是否应该停止
    pub async fn wait_if_paused(&self) -> bool {
        while self.is_paused() && !self.is_stopped() {
            tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
        }
        self.is_stopped()
    }
}

/// 分片下载器
///
/// 封装单个 HTTP Range 请求的完整生命周期：
/// HTTP 请求 → 流式读取 → 缓冲分块 → 发送到 IO 通道
pub struct ChunkDownloader {
    client: HttpClient,
    retry_policy: Arc<dyn RetryPolicy>,
    buffer_size: usize,
}

impl ChunkDownloader {
    /// 创建新的分片下载器
    pub fn new(client: HttpClient, retry_policy: Arc<dyn RetryPolicy>, buffer_size: usize) -> Self {
        Self { client, retry_policy, buffer_size }
    }

    /// 下载单个分片（带重试）
    pub async fn download_with_retry(
        &self,
        url: &str,
        range: HttpRange,
        chunk_index: usize,
        io_tx: &mpsc::Sender<IoTask>,
        control: &ControlFlags,
    ) -> Result<ChunkResult> {
        let mut attempt = 0u32;

        loop {
            if control.is_stopped() {
                return Err(anyhow!("下载已停止"));
            }

            control.wait_if_paused().await;

            match self.download_chunk(url, range, chunk_index, io_tx).await {
                Ok(result) => return Ok(result),
                Err(e) => {
                    attempt += 1;
                    let error_msg = e.to_string();

                    match self.retry_policy.should_retry(attempt, &error_msg) {
                        Some(delay) => {
                            warn!("分片 {} 下载失败 (第 {}/{} 次): {}。等待 {:?} 后重试...",
                                chunk_index, attempt, self.retry_policy.max_retries() + 1,
                                error_msg, delay);
                            tokio::time::sleep(delay).await;
                        }
                        None => {
                            return Err(anyhow!("分片 {} 在 {} 次尝试后失败: {}",
                                chunk_index, attempt, error_msg));
                        }
                    }
                }
            }
        }
    }

    /// 下载单个分片（单次，不重试）
    ///
    /// 从 HTTP 流读取数据，达到 buffer_size 时分块发送到 IO 通道。
    /// 修复了 offset 计算 bug：使用实际数据长度而非 buffer capacity。
    async fn download_chunk(
        &self,
        url: &str,
        range: HttpRange,
        chunk_index: usize,
        io_tx: &mpsc::Sender<IoTask>,
    ) -> Result<ChunkResult> {
        debug!("下载分片 {}: {}-{}", chunk_index, range.start, range.end);

        let range_header = range.to_http_range();

        let response = self
            .client
            .inner()
            .get(url)
            .header(reqwest::header::RANGE, &range_header)
            .send()
            .await
            .context("HTTP 请求失败")?;

        let status = response.status();
        if !status.is_success() && status != reqwest::StatusCode::PARTIAL_CONTENT {
            return Err(anyhow!("服务器返回错误: {}", status));
        }

        let mut stream = response.bytes_stream();
        let mut offset = range.start;
        let mut buffer = Vec::with_capacity(self.buffer_size);
        let mut total_bytes: u64 = 0;

        while let Some(chunk_result) = stream.next().await {
            let chunk = chunk_result.context("读取响应流失败")?;
            buffer.extend_from_slice(&chunk);

            if buffer.len() >= self.buffer_size {
                let data_len = buffer.len() as u64;
                let task = IoTask {
                    offset,
                    data: std::mem::take(&mut buffer),
                    chunk_index,
                };

                // 发送到 IO 通道，通道满时等待后重试
                let mut task = task;
                loop {
                    match io_tx.try_send(task) {
                        Ok(()) => break,
                        Err(mpsc::error::TrySendError::Full(t)) => {
                            tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;
                            task = t;
                        }
                        Err(mpsc::error::TrySendError::Closed(_)) => {
                            return Err(anyhow!("IO 通道已关闭"));
                        }
                    }
                }

                offset += data_len;
                total_bytes += data_len;
                buffer = Vec::with_capacity(self.buffer_size);
            }
        }

        // 发送剩余数据
        if !buffer.is_empty() {
            let data_len = buffer.len() as u64;
            io_tx
                .send(IoTask { offset, data: buffer, chunk_index })
                .await
                .map_err(|_| anyhow!("发送尾部数据到 IO 通道失败"))?;
            total_bytes += data_len;
        }

        debug!("分片 {} 下载完成，共 {} 字节", chunk_index, total_bytes);
        Ok(ChunkResult { index: chunk_index, bytes_downloaded: total_bytes })
    }
}
