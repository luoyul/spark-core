//! 配置管理
//!
//! 从环境变量加载配置

use anyhow::Result;

use crate::config::download::DownloadConfig;

/// 从环境变量加载下载配置
pub fn from_env() -> Result<DownloadConfig> {
    Ok(DownloadConfig::default())
}
