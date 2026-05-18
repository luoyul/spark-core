//! 断点续传元数据
//!
//! 持久化下载进度，支持中断后从断点继续。
//! 使用简单的 JSON 格式（手动序列化，无需 serde 依赖）。

use std::fs;
use std::path::{Path, PathBuf};

use anyhow::{Context, Result};
use tracing::{debug, info};

use crate::types::range::HttpRange;

/// 断点续传元数据
///
/// 记录已完成的分片区间，下载中断后可从已完成区间排除后继续。
#[derive(Debug, Clone)]
pub struct ResumeMetadata {
    /// 下载 URL
    pub url: String,
    /// 文件总大小
    pub file_size: u64,
    /// 已完成的分片区间列表
    pub completed_ranges: Vec<HttpRange>,
}

impl ResumeMetadata {
    /// 创建空的元数据
    pub fn new(url: &str, file_size: u64) -> Self {
        Self {
            url: url.to_string(),
            file_size,
            completed_ranges: Vec::new(),
        }
    }

    /// 标记分片完成
    pub fn mark_completed(&mut self, range: HttpRange) {
        self.completed_ranges.push(range);
    }

    /// 获取已完成的总字节数
    pub fn completed_bytes(&self) -> u64 {
        self.completed_ranges.iter().map(|r| r.size()).sum()
    }

    /// 进度百分比
    pub fn progress_percent(&self) -> f64 {
        if self.file_size == 0 {
            return 0.0;
        }
        (self.completed_bytes() as f64 / self.file_size as f64) * 100.0
    }

    /// 计算尚未完成的分片（从给定的全部区间中减去已完成区间）
    pub fn pending_ranges(&self, all_chunks: &[HttpRange]) -> Vec<HttpRange> {
        all_chunks
            .iter()
            .filter(|chunk| {
                !self
                    .completed_ranges
                    .iter()
                    .any(|completed| completed.overlaps(chunk))
            })
            .copied()
            .collect()
    }

    /// 获取元数据文件路径（下载文件 + ".spark" 后缀）
    pub fn meta_path(output_path: &Path) -> PathBuf {
        let mut meta_name = output_path
            .file_name()
            .unwrap_or_default()
            .to_os_string();
        meta_name.push(".spark");
        output_path.with_file_name(meta_name)
    }

    /// 保存到文件（简单 JSON 格式）
    pub fn save(&self, output_path: &Path) -> Result<()> {
        let meta_path = Self::meta_path(output_path);

        // 简洁的 CSV 格式：URL\nFileSize\nstart,end\nstart,end...
        let content = format!(
            "{}\n{}\n{}",
            self.url,
            self.file_size,
            self.completed_ranges
                .iter()
                .map(|r| format!("{},{}", r.start, r.end))
                .collect::<Vec<_>>()
                .join("\n")
        );

        fs::write(&meta_path, &content)
            .with_context(|| format!("写入续传文件失败: {:?}", meta_path))?;

        debug!("续传元数据已保存: {:?}", meta_path);
        Ok(())
    }

    /// 从文件加载元数据
    pub fn load(output_path: &Path) -> Result<Option<Self>> {
        let meta_path = Self::meta_path(output_path);
        if !meta_path.exists() {
            return Ok(None);
        }

        let content = fs::read_to_string(&meta_path)
            .with_context(|| format!("读取续传文件失败: {:?}", meta_path))?;

        let lines: Vec<&str> = content.lines().collect();
        if lines.len() < 2 {
            return Ok(None);
        }

        let url = lines[0].to_string();
        let file_size: u64 = lines[1].parse().context("解析文件大小失败")?;

        let mut completed_ranges = Vec::new();
        for line in &lines[2..] {
            if line.is_empty() {
                continue;
            }
            let parts: Vec<&str> = line.split(',').collect();
            if parts.len() == 2 {
                let start: u64 = parts[0].parse().unwrap_or(0);
                let end: u64 = parts[1].parse().unwrap_or(0);
                if start < end {
                    completed_ranges.push(HttpRange::new_unchecked(start, end));
                }
            }
        }

        completed_ranges.sort_by_key(|r| r.start);

        info!(
            "加载续传元数据: {} 个已完成分片, {:.1}%",
            completed_ranges.len(),
            (completed_ranges.iter().map(|r| r.size()).sum::<u64>() as f64
                / file_size as f64)
                * 100.0
        );

        Ok(Some(Self {
            url,
            file_size,
            completed_ranges,
        }))
    }

    /// 删除续传元数据文件
    pub fn remove(output_path: &Path) -> Result<()> {
        let meta_path = Self::meta_path(output_path);
        if meta_path.exists() {
            fs::remove_file(&meta_path)
                .with_context(|| format!("删除续传文件失败: {:?}", meta_path))?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[test]
    fn test_resume_metadata_new() {
        let meta = ResumeMetadata::new("http://example.com/file.zip", 1024 * 1024);
        assert_eq!(meta.url, "http://example.com/file.zip");
        assert_eq!(meta.file_size, 1024 * 1024);
        assert!(meta.completed_ranges.is_empty());
    }

    #[test]
    fn test_resume_metadata_mark_completed() {
        let mut meta = ResumeMetadata::new("http://example.com/file.zip", 1024 * 1024);
        meta.mark_completed(HttpRange::new_unchecked(0, 512 * 1024));
        meta.mark_completed(HttpRange::new_unchecked(512 * 1024, 1024 * 1024));
        assert_eq!(meta.completed_bytes(), 1024 * 1024);
        assert!((meta.progress_percent() - 100.0).abs() < 0.01);
    }

    #[test]
    fn test_pending_ranges() {
        let mut meta = ResumeMetadata::new("http://example.com/file.zip", 1024 * 1024);
        meta.mark_completed(HttpRange::new_unchecked(0, 256 * 1024));

        let all_chunks = vec![
            HttpRange::new_unchecked(0, 256 * 1024),
            HttpRange::new_unchecked(256 * 1024, 512 * 1024),
            HttpRange::new_unchecked(512 * 1024, 768 * 1024),
            HttpRange::new_unchecked(768 * 1024, 1024 * 1024),
        ];

        let pending = meta.pending_ranges(&all_chunks);
        assert_eq!(pending.len(), 3); // 第一个已完成，剩余 3 个
    }

    #[test]
    fn test_save_and_load() {
        let dir = tempdir().unwrap();
        let output = dir.path().join("test_download.bin");

        let mut meta = ResumeMetadata::new("http://example.com/file.zip", 1000);
        meta.mark_completed(HttpRange::new_unchecked(0, 500));
        meta.save(&output).unwrap();

        let loaded = ResumeMetadata::load(&output).unwrap().unwrap();
        assert_eq!(loaded.url, "http://example.com/file.zip");
        assert_eq!(loaded.file_size, 1000);
        assert_eq!(loaded.completed_ranges.len(), 1);
        assert_eq!(loaded.completed_ranges[0].start, 0);
        assert_eq!(loaded.completed_ranges[0].end, 500);

        // 清理
        ResumeMetadata::remove(&output).unwrap();
        let meta_path = ResumeMetadata::meta_path(&output);
        assert!(!meta_path.exists());
    }
}
