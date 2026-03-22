//! 配置管理
//!
//! 应用程序配置解析和管理

use anyhow::Result;

/// 应用配置
#[derive(Debug, Clone)]
pub struct Config {
    /// 并发连接数
    pub concurrency: usize,
    /// 每个分块大小
    pub chunk_size: u64,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            concurrency: 4,
            chunk_size: 1024 * 1024, // 1MB
        }
    }
}

impl Config {
    /// 从环境变量加载配置
    pub fn from_env() -> Result<Self> {
        Ok(Self::default())
    }
}
