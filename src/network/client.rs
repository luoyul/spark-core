//! HTTP客户端
//!
//! 提供支持分块下载的HTTP客户端，包含重试机制

use std::time::Duration;

use bytes::Bytes;
use futures::{Stream, StreamExt};
use reqwest::{
    header::{HeaderValue, CONTENT_LENGTH, CONTENT_RANGE, RANGE},
    Client, Response,
};
use tokio::time::sleep;
use tracing::{debug, info, warn};

use super::range::{Range, RangeError};

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
    /// 创建新的HTTP客户端
    ///
    /// 配置连接池、超时和重试参数
    pub fn new() -> Result<Self, HttpClientError> {
        let client = Client::builder()
            // 连接池配置
            .pool_max_idle_per_host(10)
            .pool_idle_timeout(Duration::from_secs(POOL_IDLE_TIMEOUT_SECS))
            // 超时配置
            .connect_timeout(Duration::from_secs(CONNECT_TIMEOUT_SECS))
            .timeout(Duration::from_secs(REQUEST_TIMEOUT_SECS))
            // 自动处理gzip/deflate压缩
            .gzip(true)
            .brotli(true)
            // 禁用重定向自动跟随（手动处理）
            .redirect(reqwest::redirect::Policy::limited(5))
            .build()?;

        Ok(Self {
            client,
            max_retries: MAX_RETRIES,
            base_delay: Duration::from_millis(BASE_RETRY_DELAY_MS),
        })
    }

    /// 创建自定义配置的HTTP客户端
    ///
    /// # Arguments
    /// * `max_retries` - 最大重试次数
    /// * `connect_timeout_secs` - 连接超时（秒）
    /// * `request_timeout_secs` - 请求超时（秒）
    pub fn with_config(
        max_retries: u32,
        connect_timeout_secs: u64,
        request_timeout_secs: u64,
    ) -> Result<Self, HttpClientError> {
        let client = Client::builder()
            .pool_max_idle_per_host(10)
            .pool_idle_timeout(Duration::from_secs(POOL_IDLE_TIMEOUT_SECS))
            .connect_timeout(Duration::from_secs(connect_timeout_secs))
            .timeout(Duration::from_secs(request_timeout_secs))
            .gzip(true)
            .brotli(true)
            .redirect(reqwest::redirect::Policy::limited(5))
            .build()?;

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
        range: Range,
    ) -> Result<impl Stream<Item = Result<Bytes, HttpClientError>>, HttpClientError> {
        let range_header = range.to_header_string();
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
        range: super::range::Range,
    ) -> Result<Response, HttpClientError> {
        let range_header = range.to_header_string();
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
