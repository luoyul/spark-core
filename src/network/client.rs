//! HTTP客户端
//!
//! 提供支持分块下载的HTTP客户端，包含重试机制和代理支持

use std::env;
use std::time::Duration;

use bytes::Bytes;
use futures::{Stream, StreamExt};
use reqwest::{
    header::{HeaderValue, CONTENT_LENGTH, CONTENT_RANGE, RANGE},
    Client, Response, Proxy,
};
use tokio::time::sleep;
use tracing::{debug, info, warn};

use crate::types::range::{HttpRange, RangeError};

/// 最大重试次数
const MAX_RETRIES: u32 = 3;
/// 基础重试延迟（毫秒）
const BASE_RETRY_DELAY_MS: u64 = 1000;
/// 连接超时（秒）
const CONNECT_TIMEOUT_SECS: u64 = 30;
/// 请求超时（秒）
const REQUEST_TIMEOUT_SECS: u64 = 60;
/// 连接池空闲超时（秒）
const POOL_IDLE_TIMEOUT_SECS: u64 = 90;

/// HTTP客户端错误
#[derive(Debug, thiserror::Error)]
pub enum HttpClientError {
    #[error("Network error: {0}")]
    Network(#[from] reqwest::Error),
    #[error("Range error: {0}")]
    Range(#[from] RangeError),
    #[error("Server error: status={status}, message={message}")]
    Server { status: u16, message: String },
    #[error("Max retries exceeded: {0}")]
    MaxRetriesExceeded(String),
    #[error("Invalid response: {0}")]
    InvalidResponse(String),
    #[error("Invalid proxy URL: {0}")]
    InvalidProxy(String),
}

/// 代理配置
#[derive(Debug, Clone)]
pub struct ProxyConfig {
    /// HTTP 代理地址 (e.g. "http://127.0.0.1:8080")
    pub http_proxy: Option<String>,
    /// HTTPS 代理地址
    pub https_proxy: Option<String>,
    /// 不走代理的主机列表（逗号分隔）
    pub no_proxy: Option<String>,
}

impl ProxyConfig {
    /// 无代理
    pub fn none() -> Self {
        Self { http_proxy: None, https_proxy: None, no_proxy: None }
    }

    /// 从环境变量自动检测
    /// 支持 HTTP_PROXY、HTTPS_PROXY、NO_PROXY（大小写不敏感）
    pub fn from_env() -> Self {
        Self {
            http_proxy: env::var("HTTP_PROXY")
                .or_else(|_| env::var("http_proxy"))
                .ok(),
            https_proxy: env::var("HTTPS_PROXY")
                .or_else(|_| env::var("https_proxy"))
                .ok(),
            no_proxy: env::var("NO_PROXY")
                .or_else(|_| env::var("no_proxy"))
                .ok(),
        }
    }

    /// 是否配置了代理
    pub fn is_enabled(&self) -> bool {
        self.http_proxy.is_some() || self.https_proxy.is_some()
    }
}

/// HTTP客户端
///
/// 包装reqwest::Client，提供分块下载和重试机制
#[derive(Debug)]
pub struct HttpClient {
    client: Client,
    max_retries: u32,
    base_delay: Duration,
}

impl Clone for HttpClient {
    fn clone(&self) -> Self {
        Self {
            client: self.client.clone(),
            max_retries: self.max_retries,
            base_delay: self.base_delay,
        }
    }
}

impl HttpClient {
    /// 创建新的HTTP客户端（自动从环境变量检测代理）
    pub fn new() -> Result<Self, HttpClientError> {
        Self::with_proxy(ProxyConfig::from_env())
    }

    /// 创建带代理配置的HTTP客户端
    pub fn with_proxy(proxy_config: ProxyConfig) -> Result<Self, HttpClientError> {
        let mut builder = Client::builder()
            .pool_max_idle_per_host(10)
            .pool_idle_timeout(Duration::from_secs(POOL_IDLE_TIMEOUT_SECS))
            .connect_timeout(Duration::from_secs(CONNECT_TIMEOUT_SECS))
            .timeout(Duration::from_secs(REQUEST_TIMEOUT_SECS))
            .gzip(true)
            .brotli(true)
            .redirect(reqwest::redirect::Policy::limited(5));

        // 配置 HTTP 代理
        if let Some(ref url) = proxy_config.http_proxy {
            let proxy = Proxy::http(url)
                .map_err(|e| HttpClientError::InvalidProxy(format!("HTTP 代理无效: {}", e)))?;
            builder = builder.proxy(proxy);
            info!("HTTP 代理: {}", url);
        }

        // 配置 HTTPS 代理
        if let Some(ref url) = proxy_config.https_proxy {
            let proxy = Proxy::https(url)
                .map_err(|e| HttpClientError::InvalidProxy(format!("HTTPS 代理无效: {}", e)))?;
            builder = builder.proxy(proxy);
            info!("HTTPS 代理: {}", url);
        }

        // NO_PROXY 由 reqwest 内部处理（基于系统环境变量），此处记录
        if let Some(ref no_proxy) = proxy_config.no_proxy {
            debug!("NO_PROXY: {}", no_proxy);
        }

        let client = builder.build()?;

        Ok(Self {
            client,
            max_retries: MAX_RETRIES,
            base_delay: Duration::from_millis(BASE_RETRY_DELAY_MS),
        })
    }

    /// 创建自定义配置的HTTP客户端（自动检测代理）
    pub fn with_config(
        max_retries: u32,
        connect_timeout_secs: u64,
        request_timeout_secs: u64,
    ) -> Result<Self, HttpClientError> {
        Self::with_config_and_proxy(max_retries, connect_timeout_secs, request_timeout_secs, ProxyConfig::from_env())
    }

    /// 创建自定义配置+代理的HTTP客户端
    pub fn with_config_and_proxy(
        max_retries: u32,
        connect_timeout_secs: u64,
        request_timeout_secs: u64,
        proxy_config: ProxyConfig,
    ) -> Result<Self, HttpClientError> {
        let mut builder = Client::builder()
            .pool_max_idle_per_host(10)
            .pool_idle_timeout(Duration::from_secs(POOL_IDLE_TIMEOUT_SECS))
            .connect_timeout(Duration::from_secs(connect_timeout_secs))
            .timeout(Duration::from_secs(request_timeout_secs))
            .gzip(true)
            .brotli(true)
            .redirect(reqwest::redirect::Policy::limited(5));

        if let Some(ref url) = proxy_config.http_proxy {
            let proxy = Proxy::http(url)
                .map_err(|e| HttpClientError::InvalidProxy(format!("HTTP 代理无效: {}", e)))?;
            builder = builder.proxy(proxy);
        }
        if let Some(ref url) = proxy_config.https_proxy {
            let proxy = Proxy::https(url)
                .map_err(|e| HttpClientError::InvalidProxy(format!("HTTPS 代理无效: {}", e)))?;
            builder = builder.proxy(proxy);
        }

        let client = builder.build()?;

        Ok(Self {
            client,
            max_retries,
            base_delay: Duration::from_millis(BASE_RETRY_DELAY_MS),
        })
    }

    /// 获取文件总大小
    ///
    /// 通过发送HEAD请求获取Content-Length
    ///
    /// # Arguments
    /// * `url` - 文件URL
    ///
    /// # Returns
    /// 成功返回文件总大小（字节），失败返回错误
    pub async fn get_file_size(&self, url: &str) -> Result<u64, HttpClientError> {
        let response = self
            .execute_with_retry(|| async {
                self.client
                    .head(url)
                    .send()
                    .await
                    .map_err(HttpClientError::Network)
            })
            .await?;

        // 检查状态码
        let status = response.status();
        if !status.is_success() {
            return Err(HttpClientError::Server {
                status: status.as_u16(),
                message: format!("HEAD request failed: {}", status),
            });
        }

        // 获取Content-Length
        let content_length = response.headers().get(CONTENT_LENGTH).ok_or_else(|| {
            HttpClientError::InvalidResponse("Missing Content-Length header".to_string())
        })?;

        let size: u64 = content_length
            .to_str()
            .map_err(|_| {
                HttpClientError::InvalidResponse("Invalid Content-Length header".to_string())
            })?
            .parse()
            .map_err(|_| {
                HttpClientError::InvalidResponse("Invalid Content-Length value".to_string())
            })?;

        info!("File size for {}: {} bytes", url, size);
        Ok(size)
    }

    /// 获取指定范围的数据流
    ///
    /// 发送带有Range头的GET请求，返回字节流
    ///
    /// # Arguments
    /// * `url` - 文件URL
    /// * `range` - 字节范围
    ///
    /// # Returns
    /// 成功返回字节流，失败返回错误
    pub async fn fetch_range(
        &self,
        url: &str,
        range: HttpRange,
    ) -> Result<impl Stream<Item = Result<Bytes, HttpClientError>>, HttpClientError> {
        let range_header = range.to_http_header();
        debug!("Fetching range: {} from {}", range_header, url);

        let response = self
            .execute_with_retry(|| async {
                self.client
                    .get(url)
                    .header(
                        RANGE,
                        HeaderValue::from_str(&range_header)
                            .map_err(|e| HttpClientError::InvalidResponse(e.to_string()))?,
                    )
                    .send()
                    .await
                    .map_err(HttpClientError::Network)
            })
            .await?;

        // 检查状态码 (206 Partial Content 或 200 OK)
        let status = response.status();
        if status == reqwest::StatusCode::PARTIAL_CONTENT {
            debug!("Received 206 Partial Content for range {}", range_header);
        } else if status.is_success() {
            warn!("Server returned 200 OK instead of 206 Partial Content");
        } else {
            return Err(HttpClientError::Server {
                status: status.as_u16(),
                message: format!("Range request failed: {}", status),
            });
        }

        // 验证Content-Range响应头（如果存在）
        if let Some(content_range) = response.headers().get(CONTENT_RANGE) {
            let content_range_str = content_range.to_str().map_err(|_| {
                HttpClientError::InvalidResponse("Invalid Content-Range header".to_string())
            })?;
            debug!("Content-Range: {}", content_range_str);
        }

        // 转换为字节流
        let stream = response
            .bytes_stream()
            .map(|result| result.map_err(HttpClientError::Network));

        Ok(stream)
    }

    /// 获取原始reqwest客户端
    pub fn inner(&self) -> &Client {
        &self.client
    }

    /// 获取指定范围的响应（简化接口）
    ///
    /// # Arguments
    /// * `url` - 文件URL
    /// * `range` - 字节范围
    ///
    /// # Returns
    /// 成功返回Response，失败返回错误
    pub async fn get_with_range(
        &self,
        url: &str,
        range: HttpRange,
    ) -> Result<Response, HttpClientError> {
        let range_header = range.to_http_header();
        debug!("Fetching range: {} from {}", range_header, url);

        let response = self
            .execute_with_retry(|| async {
                self.client
                    .get(url)
                    .header(
                        RANGE,
                        HeaderValue::from_str(&range_header)
                            .map_err(|e| HttpClientError::InvalidResponse(e.to_string()))?,
                    )
                    .send()
                    .await
                    .map_err(HttpClientError::Network)
            })
            .await?;

        // 检查状态码 (206 Partial Content 或 200 OK)
        let status = response.status();
        if status == reqwest::StatusCode::PARTIAL_CONTENT {
            debug!("Received 206 Partial Content for range {}", range_header);
        } else if status.is_success() {
            warn!("Server returned 200 OK instead of 206 Partial Content");
        } else {
            return Err(HttpClientError::Server {
                status: status.as_u16(),
                message: format!("Range request failed: {}", status),
            });
        }

        Ok(response)
    }

    /// 获取最大重试次数
    pub fn max_retries(&self) -> u32 {
        self.max_retries
    }

    /// 带重试机制执行异步操作
    ///
    /// 使用指数退避策略进行重试
    ///
    /// # Arguments
    /// * `operation` - 要执行的操作
    ///
    /// # Returns
    /// 操作结果
    async fn execute_with_retry<F, Fut, T>(&self, operation: F) -> Result<T, HttpClientError>
    where
        F: Fn() -> Fut,
        Fut: std::future::Future<Output = Result<T, HttpClientError>>,
    {
        let mut last_error = None;

        for attempt in 0..=self.max_retries {
            match operation().await {
                Ok(result) => return Ok(result),
                Err(e) => {
                    last_error = Some(e);

                    if attempt < self.max_retries {
                        let delay = self.base_delay * 2_u32.pow(attempt);
                        warn!(
                            "Request failed (attempt {}/{}), retrying after {:?}...",
                            attempt + 1,
                            self.max_retries + 1,
                            delay
                        );
                        sleep(delay).await;
                    }
                }
            }
        }

        Err(HttpClientError::MaxRetriesExceeded(
            last_error.map(|e| e.to_string()).unwrap_or_default(),
        ))
    }

    /// 发送简单GET请求
    ///
    /// # Arguments
    /// * `url` - 请求URL
    ///
    /// # Returns
    /// 成功返回响应，失败返回错误
    pub async fn get(&self, url: &str) -> Result<Response, HttpClientError> {
        self.execute_with_retry(|| async {
            self.client
                .get(url)
                .send()
                .await
                .map_err(HttpClientError::Network)
        })
        .await
    }

    /// 检查服务器是否支持Range请求
    ///
    /// # Arguments
    /// * `url` - 文件URL
    ///
    /// # Returns
    /// 如果支持Range请求返回true
    pub async fn supports_range(&self, url: &str) -> Result<bool, HttpClientError> {
        let response = self
            .client
            .head(url)
            .send()
            .await
            .map_err(HttpClientError::Network)?;

        // 检查Accept-Ranges头
        let supports = response
            .headers()
            .get("accept-ranges")
            .map(|v| v == "bytes")
            .unwrap_or(false);

        info!("Server supports range requests: {}", supports);
        Ok(supports)
    }
}

impl Default for HttpClient {
    fn default() -> Self {
        Self::new().expect("Failed to create default HttpClient")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_http_client_new() {
        let client = HttpClient::new();
        assert!(client.is_ok());
    }

    #[test]
    fn test_http_client_config() {
        let client = HttpClient::with_config(5, 10, 30);
        assert!(client.is_ok());
        let client = client.unwrap();
        assert_eq!(client.max_retries(), 5);
    }

    // 注意：以下测试需要网络连接
    // #[tokio::test]
    // async fn test_get_file_size() {
    //     let client = HttpClient::new().unwrap();
    //     let size = client.get_file_size("https://example.com/file.txt").await;
    //     assert!(size.is_ok());
    // }
}
