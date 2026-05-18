//! 统一下载配置
//!
//! 合并了 engine.rs 的 DownloadConfig 和 utils/config.rs 的 Config

use std::path::{Path, PathBuf};

use crate::network::client::ProxyConfig;

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
    /// 每个分片的最小大小 (字节)
    pub min_chunk_size: u64,
    /// 代理配置
    pub proxy: ProxyConfig,
}

impl Default for DownloadConfig {
    fn default() -> Self {
        Self {
            thread_count: 8,
            io_thread_count: 2,
            buffer_size: 64 * 1024,
            io_channel_capacity: 128,
            retry_limit: 3,
            output_dir: PathBuf::from("./downloads"),
            min_chunk_size: 1024 * 1024,
            proxy: ProxyConfig::from_env(),
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

    /// 设置最小分片大小
    pub fn min_chunk_size(mut self, size: u64) -> Self {
        self.min_chunk_size = size.max(1024);
        self
    }

    /// 设置代理
    pub fn proxy(mut self, proxy: ProxyConfig) -> Self {
        self.proxy = proxy;
        self
    }

    /// 设置 HTTP/HTTPS 代理（同时用于 HTTP 和 HTTPS）
    pub fn proxy_url(mut self, url: &str) -> Self {
        self.proxy = ProxyConfig {
            http_proxy: Some(url.to_string()),
            https_proxy: Some(url.to_string()),
            no_proxy: None,
        };
        self
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
        assert_eq!(config.min_chunk_size, 1024 * 1024);
    }

    #[test]
    fn test_download_config_builder() {
        let config = DownloadConfig::new()
            .thread_count(16)
            .io_thread_count(4)
            .buffer_size(128 * 1024)
            .io_channel_capacity(256)
            .min_chunk_size(2 * 1024 * 1024);

        assert_eq!(config.thread_count, 16);
        assert_eq!(config.io_thread_count, 4);
        assert_eq!(config.buffer_size, 128 * 1024);
        assert_eq!(config.io_channel_capacity, 256);
        assert_eq!(config.min_chunk_size, 2 * 1024 * 1024);
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
